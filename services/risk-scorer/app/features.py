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
# Both expose ONE operation, observe(): "give me this merchant's last 24 h and
# its all-time count as they were BEFORE this payment, and record this payment"
# — read and write together, so no other scorer can slip in between them.
# Events come back as (ts_seconds, amount_paise, status).


class MemoryState:
    def __init__(self):
        self.events = defaultdict(lambda: deque(maxlen=MAX_PER_MERCHANT))
        self.total = defaultdict(int)

    async def observe(self, merchant_id, ts, payment_id, amount, status):
        q = self.events[merchant_id]
        while q and ts - q[0][1] > KEEP_SECONDS:
            q.popleft()
        replay = any(e[0] == payment_id for e in q)
        prior = [e[1:] for e in q if e[1] <= ts and e[0] != payment_id]
        total_before = self.total[merchant_id] - replay
        if not replay:
            q.append((payment_id, ts, amount, status))
            self.total[merchant_id] += 1
        return prior, total_before


# Runs inside Redis as one step: nothing else touches these keys until it
# returns. That is what makes two scorer replicas safe on the same merchant.
#   KEYS: events zset, count
#   ARGV: ts, keep_seconds, payment_id, member, max_events, events_ttl, count_ttl
OBSERVE_LUA = """
local ts = tonumber(ARGV[1])
local floor = ts - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. floor)
local rows = redis.call('ZRANGEBYSCORE', KEYS[1], floor, ts, 'WITHSCORES')
local total = tonumber(redis.call('GET', KEYS[2]) or '0')
local prefix = ARGV[3] .. '|'
local prior, replay = {}, 0
for i = 1, #rows, 2 do
  if string.sub(rows[i], 1, #prefix) == prefix then
    replay = 1                       -- a Kafka redelivery: never compare a payment with itself
  else
    prior[#prior + 1] = rows[i]
    prior[#prior + 1] = rows[i + 1]
  end
end
if redis.call('ZADD', KEYS[1], ts, ARGV[4]) == 1 then
  redis.call('INCR', KEYS[2])        -- only a genuinely new payment is counted
end
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -tonumber(ARGV[5]) - 1)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[6]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[7]))
return {total - replay, prior}
"""


class RedisState:
    """
    Key layout (on its own Redis database):
      sentinel:events:<merchant>  sorted set, score = ts, member = "<payment>|<amount>|<status>"
      sentinel:count:<merchant>   all-time payment count
    """

    def __init__(self, redis, prefix="sentinel"):
        self.r = redis
        self.prefix = prefix
        self.script = redis.register_script(OBSERVE_LUA)

    async def observe(self, merchant_id, ts, payment_id, amount, status):
        keys = [f"{self.prefix}:events:{merchant_id}", f"{self.prefix}:count:{merchant_id}"]
        total, rows = await self.script(keys=keys, args=[
            ts, KEEP_SECONDS, payment_id, f"{payment_id}|{amount}|{status}",
            MAX_PER_MERCHANT, KEEP_SECONDS + 3600, 30 * 24 * 3600])
        prior = []
        for i in range(0, len(rows), 2):
            member = rows[i].decode() if isinstance(rows[i], bytes) else rows[i]
            _, amt, st = member.split("|", 2)
            prior.append((float(rows[i + 1]), int(amt), st))
        return prior, int(total)


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

        observe() returns history as it was BEFORE this payment (and records the
        payment in the same step), so a Rs 50,000 spike is compared with the old
        baseline rather than one it has already raised.
        """
        payment_id = payment_id or f"anon-{ts}-{amount_paise}"
        prior, total_seen = await self.state.observe(merchant_id, ts, payment_id, amount_paise, status)
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
