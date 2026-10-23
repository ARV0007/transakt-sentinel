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
        """One payment. Returns the case message to publish, or None."""
        cases = await self.handle_many([(event, event_ts)])
        return cases[0] if cases else None

    async def handle_many(self, items):
        """
        A batch of (event, kafka_timestamp) pairs, in arrival order. Returns the
        case messages to publish (flagged payments only).

        Why batches: the model call has a fixed cost of ~0.7 ms whether it scores
        one payment or a hundred, so scoring a Kafka poll's worth at once is ~65x
        cheaper per payment. Features are still computed one payment at a time,
        in order, because each payment's history includes the ones before it.

        Kafka delivers at least once, so the same settled event can arrive twice —
        in different polls, or even twice in one. The store's primary key and the
        in-batch set make every repeat a no-op, the same reason Transakt's outbox
        gives every event a stable id.
        """
        pending, in_batch, resent = [], set(), []
        for event, event_ts in items:
            try:
                payment_id, merchant_id = event["paymentId"], event["merchantId"]
                amount = int(event["amountPaise"])
            except (KeyError, TypeError, ValueError) as e:
                log.error("skipping malformed event %r: %s", event, e)
                continue
            if payment_id in in_batch:
                metrics.REPLAYS.inc()
                continue
            in_batch.add(payment_id)
            stored = self.store.get(payment_id)
            if stored:
                # Already scored: a redelivery. If it was flagged, send its case AGAIN.
                # The score is saved before the case is sent, so a crash in between
                # would otherwise lose the case for good. The investigator ignores a
                # payment it already has, so a repeat costs nothing.
                metrics.REPLAYS.inc()
                if stored["flagged"]:
                    resent.append(self._case(payment_id, stored["merchant_id"], stored["amount_paise"],
                                             event.get("currency", "INR"), stored["status"], stored["score"],
                                             stored["reasons"], stored["features"], stored["scored_at"]))
                continue
            created = await self.directory.created_ts(merchant_id)
            pending.append((event, event_ts, amount, created))
        if not pending:
            return resent
        # One Redis round trip for the whole batch (see RedisState.observe_many). If
        # Redis is down this raises, the Kafka offset is NOT committed, and the consumer
        # loop retries: scoring pauses and events wait in Kafka rather than being scored
        # with an empty history (which would wave fraud through).
        all_feats = await self.features.compute_many([
            (e["merchantId"], ts, amt, e.get("status"), created, e["paymentId"])
            for e, ts, amt, created in pending])
        pending = [(e, ts, amt, f) for (e, ts, amt, _), f in zip(pending, all_feats)]

        started = time.perf_counter()
        scores = self.model.score_many([f for *_, f in pending])
        per_payment_s = (time.perf_counter() - started) / len(pending)

        rows, cases, now = [], [], time.time()
        for (event, event_ts, amount, feats), score in zip(pending, scores):
            payment_id, merchant_id = event["paymentId"], event["merchantId"]
            flagged = score >= self.model.threshold
            reasons = reason_codes(feats, amount) if flagged else []
            metrics.MODEL_SECONDS.observe(per_payment_s)
            metrics.EVENT_LAG.observe(max(0.0, now - event_ts))
            metrics.SCORED.labels(flagged=str(flagged).lower()).inc()
            for r in reasons:
                metrics.REASONS.labels(reason=r).inc()
            rows.append((payment_id, merchant_id, amount, event.get("status"),
                         score, flagged, reasons, feats, event_ts))
            if flagged:
                log.info("FLAG payment=%s merchant=%s score=%.3f reasons=%s",
                         payment_id, merchant_id, score, reasons)
                cases.append(self._case(payment_id, merchant_id, amount, event.get("currency", "INR"),
                                        event.get("status"), score, reasons, feats, now))
        self.store.save_many(rows)          # one transaction for the whole batch
        return resent + cases

    def _case(self, payment_id, merchant_id, amount, currency, status, score, reasons, feats, flagged_at):
        return {
            "payment_id": payment_id,
            "merchant_id": merchant_id,
            "amount_paise": amount,
            "currency": currency,
            "status": status,
            "score": round(score, 4),
            "reasons": reasons,
            "features": {k: (None if v != v else v) for k, v in (feats or {}).items()},  # NaN -> null
            "model_version": self.model.version,
            "flagged_at": flagged_at,
        }
