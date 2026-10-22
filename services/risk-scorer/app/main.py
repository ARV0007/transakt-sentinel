"""
risk-scorer: listens to Transakt's `payment.settled` topic, scores every
payment, and publishes the risky ones to `fraud.cases`.

Why Kafka on BOTH sides:
  * In: Transakt already publishes payment.settled through its outbox, so the
    scorer needed zero changes to Transakt — it is just one more consumer group.
  * Out: publishing cases to a topic (instead of calling the investigator over
    HTTP) means an investigator that is down, slow, or waiting on an LLM can
    never block scoring. Cases queue durably and are picked up when it is back.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query

from .model import RiskModel, load_bundle
from .scoring import MerchantDirectory, Scorer
from .store import ScoreStore
from .transakt_client import TransaktClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("risk-scorer")

KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
IN_TOPIC = os.environ.get("IN_TOPIC", "payment.settled")
OUT_TOPIC = os.environ.get("OUT_TOPIC", "fraud.cases")


async def consume_forever(scorer, stop):
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    delay = 2
    while not stop.is_set():
        consumer = AIOKafkaConsumer(
            IN_TOPIC, bootstrap_servers=KAFKA, group_id="risk-scorer",
            auto_offset_reset="earliest",     # a scorer started late still sees everything
            enable_auto_commit=False)         # commit only after a payment is handled
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all")
        try:
            await consumer.start()
            await producer.start()
            log.info("consuming %s -> producing %s on %s", IN_TOPIC, OUT_TOPIC, KAFKA)
            delay = 2
            async for msg in consumer:
                try:
                    event = json.loads(msg.value)
                    case = await scorer.handle(event, msg.timestamp / 1000)
                    if case:
                        # Keyed by merchant: every case for one merchant lands on the
                        # same partition, so the investigator sees them in order.
                        await producer.send_and_wait(OUT_TOPIC, json.dumps(case).encode(),
                                                     key=case["merchant_id"].encode())
                except (ValueError, KeyError) as e:
                    log.error("skipping malformed event at offset %s: %s", msg.offset, e)
                await consumer.commit()
                if stop.is_set():
                    break
        except Exception as e:
            log.warning("Kafka unavailable (%s); retrying in %ss", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
        finally:
            for c in (consumer, producer):
                try:
                    await c.stop()
                except Exception:
                    pass


def build_scorer():
    store = ScoreStore(os.environ.get("SCORES_DB", "scores.db"))
    model = RiskModel(load_bundle())
    client = TransaktClient(os.environ.get("TRANSAKT_URL", "http://localhost:8080"),
                            os.environ.get("ADMIN_EMAIL", "sentinel-admin@transakt.local"),
                            os.environ.get("ADMIN_PASSWORD", "change-me-sentinel"))
    return Scorer(model, store, MerchantDirectory(client))


@asynccontextmanager
async def lifespan(app):
    app.state.scorer = build_scorer()
    stop = asyncio.Event()
    task = asyncio.create_task(consume_forever(app.state.scorer, stop)) if KAFKA else None
    if not KAFKA:
        log.warning("KAFKA_BOOTSTRAP_SERVERS not set; API only, no consumer")
    yield
    stop.set()
    if task:
        task.cancel()


app = FastAPI(title="risk-scorer", lifespan=lifespan)


@app.get("/health")
def health():
    m = app.state.scorer.model
    return {"status": "UP", "service": "risk-scorer", "model_version": m.version,
            "model_source": m.source, "threshold": m.threshold}


@app.get("/stats")
def stats():
    return app.state.scorer.store.stats()


@app.get("/scores")
def scores(flagged: bool | None = None, limit: int = Query(200, ge=1, le=5000)):
    return app.state.scorer.store.list(flagged, limit)
