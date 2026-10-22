"""
The scoring step itself, kept free of Kafka so it can be tested with plain
dicts: event in -> features -> score -> (maybe) a case to open.
"""

import logging
import time

from . import metrics
from .features import FeatureBuilder, MemoryState, reason_codes
from .transakt_client import parse_ts

log = logging.getLogger("risk-scorer")


class MerchantDirectory:
    """Caches each merchant's signup time. Account age never changes, so one
    lookup per merchant is enough, and a failed lookup just means "unknown"."""

    def __init__(self, client):
        self.client = client
        self.created = {}

    async def created_ts(self, merchant_id):
        if merchant_id in self.created:
            return self.created[merchant_id]
        try:
            m = await self.client.get_merchant(merchant_id) if self.client else None
            ts = parse_ts(m["createdAt"]).timestamp() if m and m.get("createdAt") else None
        except Exception as e:
            log.warning("merchant lookup failed for %s: %s", merchant_id, e)
            return None          # don't cache a failure; try again next time
        self.created[merchant_id] = ts
        return ts


class Scorer:
    def __init__(self, model, store, directory, state=None):
        self.model = model
        self.store = store
        self.directory = directory
        self.features = FeatureBuilder(state or MemoryState())

    async def handle(self, event, event_ts):
        """
        Returns the case message to publish, or None.

        Kafka delivers at least once, so the same settled event can arrive twice.
        The store's primary key on payment_id makes a second delivery a no-op —
        the same reason Transakt's outbox gives every event a stable id.
        """
        payment_id = event["paymentId"]
        if self.store.seen(payment_id):
            metrics.REPLAYS.inc()
            return None

        merchant_id = event["merchantId"]
        amount = int(event["amountPaise"])
        created = await self.directory.created_ts(merchant_id)
        # If Redis is down this raises, the Kafka offset is NOT committed, and the
        # consumer loop retries: scoring pauses and events wait in Kafka rather
        # than being scored with an empty history (which would wave fraud through).
        feats = await self.features.compute(merchant_id, event_ts, amount, event.get("status"),
                                            created, payment_id)

        started = time.perf_counter()
        score = self.model.score(feats)
        latency_ms = (time.perf_counter() - started) * 1000

        flagged = score >= self.model.threshold
        reasons = reason_codes(feats, amount) if flagged else []
        metrics.MODEL_SECONDS.observe(latency_ms / 1000)
        metrics.EVENT_LAG.observe(max(0.0, time.time() - event_ts))
        metrics.SCORED.labels(flagged=str(flagged).lower()).inc()
        for r in reasons:
            metrics.REASONS.labels(reason=r).inc()
        self.store.save(payment_id, merchant_id, amount, event.get("status"),
                        score, flagged, reasons, feats, event_ts)

        if not flagged:
            return None
        log.info("FLAG payment=%s merchant=%s score=%.3f reasons=%s (%.2f ms)",
                 payment_id, merchant_id, score, reasons, latency_ms)
        return {
            "payment_id": payment_id,
            "merchant_id": merchant_id,
            "amount_paise": amount,
            "currency": event.get("currency", "INR"),
            "status": event.get("status"),
            "score": round(score, 4),
            "reasons": reasons,
            "features": {k: (None if v != v else v) for k, v in feats.items()},  # NaN -> null
            "model_version": self.model.version,
            "flagged_at": time.time(),
        }
