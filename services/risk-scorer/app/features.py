"""
Streaming features: what the scorer knows about a merchant at the moment a
payment arrives.

Real fraud systems compute these from the event stream itself rather than
querying a database per payment — that is what keeps the hot path fast. The
per-merchant history lives in a STATE BACKEND:

  * MemoryState — a dict of deques inside the process. Fast, but a restart
    forgets everything: the first payments after a redeploy are scored as if
    every merchant were brand new. Used in tests and when REDIS_URL is unset.
  * RedisState  — one sorted set per merchant (score = timestamp), so history
    survives restarts and is shared by every scorer replica. The Redis that
    Transakt already runs, on its own database number.
"""

import math
from collections import defaultdict, deque
from statistics import median

# The order the model was trained on. Changing it silently breaks every
# prediction, which is why it lives in one place and is saved with the model.
FEATURE_ORDER = [
    "count_2m",          # this merchant's payments in the last 2 minutes, incl. this one
    "count_10m",         # ... in the last 10 minutes
    "history_count",     # payments seen from this merchant BEFORE this one
    "amount_ratio",      # this amount / the merchant's own median (NaN if < 2 prior)
    "log_amount",        # log10 of the amount in paise
    "merchant_age_min",  # minutes since the merchant signed up (NaN if unknown)
    "failed_ratio_10m",  # share of the last 10 minutes' payments the bank declined
]

KEEP_SECONDS = 24 * 3600
MAX_PER_MERCHANT = 2000


# ---------------------------------------------------------------- state backends
# Both answer two questions: "what happened in the last 24 h before ts?" and
# "how many payments have we ever seen?", then record the new payment.
# history() returns events as (ts_seconds, amount_paise, status).


class MemoryState:
    def __init__(self):
        self.events = defaultdict(lambda: deque(maxlen=MAX_PER_MERCHANT))
        self.total = defaultdict(int)

    async def history(self, merchant_id, ts, exclude_payment_id=None):
        q = self.events[merchant_id]
        while q and ts - q[0][1] > KEEP_SECONDS:
            q.popleft()
        return ([e[1:] for e in q if e[1] <= ts and e[0] != exclude_payment_id],
                self.total[merchant_id] - any(e[0] == exclude_payment_id for e in q))

    async def record(self, merchant_id, payment_id, ts, amount, status):
        if any(e[0] == payment_id for e in self.events[merchant_id]):
            return                                      # replay: already recorded
        self.events[merchant_id].append((payment_id, ts, amount, status))
        self.total[merchant_id] += 1


class RedisState:
    """
    Key layout (all under one prefix, on its own Redis database):
      sentinel:events:<merchant>  sorted set, score = ts, member = "<payment>|<amount>|<status>"
      sentinel:count:<merchant>   all-time payment count

    The payment id inside the member makes a replayed event the same member,
    so ZADD on a redelivery doesn't double-count it (the INCR is guarded below).
    """

    def __init__(self, redis, prefix="sentinel"):
        self.r = redis
        self.prefix = prefix

    def _k(self, kind, merchant_id):
        return f"{self.prefix}:{kind}:{merchant_id}"

    async def history(self, merchant_id, ts, exclude_payment_id=None):
        ev = self._k("events", merchant_id)
        async with self.r.pipeline(transaction=True) as p:
            p.zremrangebyscore(ev, "-inf", ts - KEEP_SECONDS)
            p.zrangebyscore(ev, ts - KEEP_SECONDS, ts, withscores=True)
            p.get(self._k("count", merchant_id))
            _, rows, total = await p.execute()
        events, replayed = [], 0
        for member, score in rows:
            member = member.decode() if isinstance(member, bytes) else member
            pid, amount, status = member.split("|", 2)
            if pid == exclude_payment_id:
                # A replay after a crash between record() and the score being
                # saved. Don't compare the payment with itself.
                replayed = 1
                continue
            events.append((float(score), int(amount), status))
        return events, int(total or 0) - replayed

    async def record(self, merchant_id, payment_id, ts, amount, status):
        ev, cnt = self._k("events", merchant_id), self._k("count", merchant_id)
        added = await self.r.zadd(ev, {f"{payment_id}|{amount}|{status}": ts})
        async with self.r.pipeline(transaction=True) as p:
            if added:                                   # a replay adds nothing and counts nothing
                p.incr(cnt)
            p.zremrangebyrank(ev, 0, -MAX_PER_MERCHANT - 1)
            p.expire(ev, KEEP_SECONDS + 3600)
            p.expire(cnt, 30 * 24 * 3600)
            await p.execute()


def state_from_env(url):
    if not url:
        return MemoryState()
    import redis.asyncio as aioredis
    return RedisState(aioredis.from_url(url))


# ---------------------------------------------------------------- features


class FeatureBuilder:
    def __init__(self, state=None):
        self.state = state or MemoryState()

    async def compute(self, merchant_id, ts, amount_paise, status,
                      merchant_created_ts=None, payment_id=None):
        """
        Features for ONE payment, then remember it.

        Order matters: compare the payment against history BEFORE adding it, or
        a Rs 50,000 spike would raise its own baseline and partly hide itself.
        """
        payment_id = payment_id or f"anon-{ts}-{amount_paise}"
        prior, total_seen = await self.state.history(merchant_id, ts, payment_id)
        prior_amounts = [a for _, a, _ in prior]

        recent_2m = [e for e in prior if ts - e[0] <= 120]
        recent_10m = [e for e in prior if ts - e[0] <= 600]
        failed_10m = sum(1 for e in recent_10m if e[2] == "FAILED") + (status == "FAILED")

        feats = {
            "count_2m": len(recent_2m) + 1,
            "count_10m": len(recent_10m) + 1,
            "history_count": total_seen,
            "amount_ratio": (amount_paise / median(prior_amounts)) if len(prior_amounts) >= 2 else math.nan,
            "log_amount": math.log10(max(amount_paise, 1)),
            "merchant_age_min": ((ts - merchant_created_ts) / 60) if merchant_created_ts else math.nan,
            "failed_ratio_10m": failed_10m / (len(recent_10m) + 1),
        }

        await self.state.record(merchant_id, payment_id, ts, amount_paise, status)
        return feats


def vector(feats):
    return [feats[name] for name in FEATURE_ORDER]


def reason_codes(feats, amount_paise):
    """
    Human-readable reasons, from plain thresholds rather than the model.

    The model decides WHETHER a payment is risky; these say WHICH pattern it
    resembles, so the investigator knows which specialists to send. Keeping them
    separate means a reason is always explainable in one sentence.
    """
    reasons = []
    if feats["count_2m"] >= 8:
        reasons.append("VELOCITY")
    ratio = feats["amount_ratio"]
    if not math.isnan(ratio) and ratio >= 10:
        reasons.append("AMOUNT_SPIKE")
    if feats["history_count"] < 15 and amount_paise >= 1_000_000:   # >= Rs 10,000
        reasons.append("NEW_MERCHANT_HIGH_VALUE")
    return reasons or ["MODEL_ONLY"]
