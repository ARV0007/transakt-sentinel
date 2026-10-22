"""
Streaming features: what the scorer knows about a merchant at the moment a
payment arrives.

Real fraud systems compute these from the event stream itself rather than
querying a database per payment — that is what keeps the hot path fast. Here
the state is a per-merchant list of recent payments held in memory.

KNOWN LIMITATION: the state is lost on restart (history rebuilds from the
Kafka topic only if the consumer group is reset). The production answer is a
shared store such as Redis with TTLs, the same tool Transakt already uses.
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


class MerchantState:
    def __init__(self):
        self.events = deque(maxlen=MAX_PER_MERCHANT)   # (ts_seconds, amount_paise, status)
        self.total_seen = 0

    def prune(self, now):
        while self.events and now - self.events[0][0] > KEEP_SECONDS:
            self.events.popleft()


class FeatureBuilder:
    def __init__(self):
        self.merchants = defaultdict(MerchantState)

    def compute(self, merchant_id, ts, amount_paise, status, merchant_created_ts=None):
        """
        Features for ONE payment, then remember it.

        Order matters: compare the payment against history BEFORE adding it, or
        a Rs 50,000 spike would raise its own baseline and partly hide itself.
        """
        st = self.merchants[merchant_id]
        st.prune(ts)
        prior = [e for e in st.events if e[0] <= ts]
        prior_amounts = [a for _, a, _ in prior]

        recent_2m = [e for e in prior if ts - e[0] <= 120]
        recent_10m = [e for e in prior if ts - e[0] <= 600]
        failed_10m = sum(1 for e in recent_10m if e[2] == "FAILED") + (status == "FAILED")

        feats = {
            "count_2m": len(recent_2m) + 1,
            "count_10m": len(recent_10m) + 1,
            "history_count": st.total_seen,
            "amount_ratio": (amount_paise / median(prior_amounts)) if len(prior_amounts) >= 2 else math.nan,
            "log_amount": math.log10(max(amount_paise, 1)),
            "merchant_age_min": ((ts - merchant_created_ts) / 60) if merchant_created_ts else math.nan,
            "failed_ratio_10m": failed_10m / (len(recent_10m) + 1),
        }

        st.events.append((ts, amount_paise, status))
        st.total_seen += 1
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
