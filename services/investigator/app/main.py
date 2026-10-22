"""
investigator: consumes `fraud.cases`, investigates with a LangGraph agent,
and waits for an analyst's decision.

API (port 8003)
  GET  /                      analyst review page
  GET  /health
  GET  /cases?status=...      list cases
  GET  /cases/{id}            one case with findings and recommendation
  POST /cases                 open a case by hand (testing; normally Kafka)
  POST /cases/{id}/decision   {"decision": "CONFIRM_FRAUD"|"DISMISS", "note": "...", "analyst": "..."}
  POST /cases/{id}/retry      re-run a case that ended in ERROR
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from pydantic import BaseModel, Field

from . import metrics
from .a2a_agent import mount_a2a
from .graph import DECISIONS, build_graph
from .llm import build_llm
from .service import CaseError, CaseService
from .store import CaseStore
from .toolbox import MCPToolbox

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("investigator")

KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
CASES_TOPIC = os.environ.get("CASES_TOPIC", "fraud.cases")
STATIC = Path(__file__).parent / "static"


async def consume_cases(service, stop):
    from aiokafka import AIOKafkaConsumer

    delay = 2
    while not stop.is_set():
        consumer = AIOKafkaConsumer(CASES_TOPIC, bootstrap_servers=KAFKA, group_id="investigator",
                                    auto_offset_reset="earliest", enable_auto_commit=False)
        try:
            await consumer.start()
            log.info("consuming %s on %s", CASES_TOPIC, KAFKA)
            delay = 2
            async for msg in consumer:
                try:
                    case_id, is_new = service.open(json.loads(msg.value))
                    log.info("%s %s", "opened" if is_new else "attached to", case_id)
                except (ValueError, KeyError) as e:
                    log.error("skipping malformed case message: %s", e)
                # Committed once the case row exists. If the process dies before the
                # graph finishes, startup recovery resumes it from its checkpoint.
                await consumer.commit()
        except Exception as e:
            log.warning("Kafka unavailable (%s); retrying in %ss", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass


@asynccontextmanager
async def lifespan(app):
    DATA_DIR = Path(os.environ.get("DATA_DIR", "."))
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(DATA_DIR / "checkpoints.db")
    checkpointer = AsyncSqliteSaver(conn)
    await checkpointer.setup()
    tools = app.state.tools_override or MCPToolbox(os.environ.get("MCP_URL", "http://localhost:8001/mcp"),
                                                   os.environ.get("MCP_TOKEN"))
    app.state.tools = tools          # also used by the A2A agent to look payments up
    graph = build_graph(tools, app.state.llm_override or build_llm(), checkpointer)
    store = CaseStore(str(DATA_DIR / "cases.db"))
    app.state.service = CaseService(store, graph, int(os.environ.get("MAX_CONCURRENT_CASES", "4")))
    resumed = await app.state.service.recover()
    if resumed:
        log.info("resumed %d interrupted case(s)", resumed)
    stop = asyncio.Event()
    task = asyncio.create_task(consume_cases(app.state.service, stop)) if KAFKA else None
    yield
    stop.set()
    if task:
        task.cancel()
    await conn.close()


app = FastAPI(title="investigator", lifespan=lifespan)
app.state.tools_override = None     # tests inject fakes here
app.state.llm_override = None
mount_a2a(app)                      # /.well-known/agent-card.json and POST /a2a


class CaseIn(BaseModel):
    payment_id: str
    merchant_id: str
    amount_paise: int = Field(gt=0)
    currency: str = "INR"
    status: str | None = "CAPTURED"
    score: float = 0.0
    reasons: list[str] = []
    features: dict = {}


class DecisionIn(BaseModel):
    decision: str
    note: str = ""
    analyst: str = "analyst"


def _svc():
    return app.state.service


@app.get("/")
def ui():
    return FileResponse(STATIC / "index.html")


@app.get("/health")
def health():
    return {"status": "UP", "service": "investigator", "cases": _svc().store.stats(),
            "llm": "configured" if (os.environ.get("LLM_API_KEY") or os.environ.get("HF_TOKEN")) else "rules-only"}


@app.get("/metrics")
def prometheus_metrics():
    # Case counts by status are read from the table at scrape time, so the
    # gauge is always the truth rather than a running tally that can drift.
    counts = _svc().store.stats()
    for status in ("INVESTIGATING", "AWAITING_REVIEW", "DECIDING", "CLOSED", "ERROR"):
        metrics.CASES_BY_STATUS.labels(status=status).set(counts.get(status, 0))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/feedback")
def feedback(limit: int = 5000):
    """
    Analyst decisions as training labels, for `./sentinel.sh retrain`.

    One row per decided case: the features the scorer saw on the payment that
    opened it, and the human's verdict. CONFIRM_FRAUD -> 1, DISMISS -> 0.
    """
    rows = []
    for c in _svc().store.list("CLOSED", min(limit, 20000)):
        d, t = c.get("decision") or {}, c.get("trigger") or {}
        if d.get("decision") not in DECISIONS or not t.get("features"):
            continue
        rows.append({"case_id": c["id"], "payment_id": t["payment_id"], "features": t["features"],
                     "label": 1 if d["decision"] == "CONFIRM_FRAUD" else 0,
                     "recommended": (c.get("recommendation") or {}).get("action"),
                     "model_version": t.get("model_version"), "decided_at": c["updated_at"]})
    return rows


@app.get("/cases")
def list_cases(status: str | None = None, limit: int = 200):
    return _svc().store.list(status, min(limit, 1000))


@app.get("/cases/{case_id}")
def get_case(case_id: str):
    case = _svc().store.get(case_id)
    if not case:
        raise HTTPException(404, "case not found")
    return case


@app.post("/cases", status_code=201)
async def open_case(body: CaseIn):
    # async on purpose: opening a case starts the investigation with
    # asyncio.create_task, which needs the event loop. A plain `def` endpoint
    # runs in a worker thread that has none.
    case_id, is_new = _svc().open(body.model_dump())
    return {"case_id": case_id, "new": is_new}


@app.post("/cases/{case_id}/decision")
async def decide(case_id: str, body: DecisionIn):
    if body.decision.upper() not in DECISIONS:
        raise HTTPException(422, f"decision must be one of {DECISIONS}")
    try:
        status = await _svc().decide(case_id, body.decision.upper(), body.note, body.analyst)
    except CaseError as e:
        raise HTTPException(e.status, str(e))
    return {"case_id": case_id, "status": status}


@app.post("/cases/{case_id}/retry", status_code=202)
async def retry(case_id: str):
    try:
        await _svc().retry(case_id)
    except CaseError as e:
        raise HTTPException(e.status, str(e))
    return {"case_id": case_id, "status": "INVESTIGATING"}
