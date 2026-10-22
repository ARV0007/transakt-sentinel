"""
Sentinel as an A2A agent: other AI agents can ask it to investigate a payment.

MCP and A2A are two different doors:
  * MCP is how OUR agent reaches TOOLS (transakt-mcp -> Transakt's data).
  * A2A is how OTHER AGENTS reach OUR AGENT — a chargeback bot, a merchant
    onboarding agent, another company's risk system.

What a caller gets:
  GET  /.well-known/agent-card.json   who this agent is, its one skill, how to authenticate
  POST /a2a   JSON-RPC "SendMessage"  {"payment_id": "..."} (as a data part, or any text containing it)

The agent runs the same LangGraph investigation an analyst would see, and
returns the recommendation as an artifact. It never decides: the case still
lands on the review page for a human. Asking twice for one merchant's payments
returns the same open case, exactly like a second Kafka flag would.
"""

import asyncio
import hmac
import os
import re

from a2a.helpers import get_data_parts, get_message_text, new_data_part, new_task_from_user_message, new_text_message
from a2a.server.agent_execution import AgentExecutor
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import add_a2a_routes_to_fastapi, create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (AgentCapabilities, AgentCard, AgentInterface, AgentSkill, HTTPAuthSecurityScheme,
                       SecurityRequirement, SecurityScheme, StringList)
from a2a.utils.errors import UnsupportedOperationError
from starlette.responses import JSONResponse

from .toolbox import ToolError

RPC_PATH = "/a2a"
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
DONE = ("AWAITING_REVIEW", "CLOSED", "ERROR")


def agent_card(public_url):
    return AgentCard(
        name="Transakt Sentinel",
        description="Investigates a payment on the Transakt gateway for merchant fraud and returns a "
                    "recommendation with evidence. Recommends only; a human analyst decides.",
        version="1.2.0",
        supported_interfaces=[AgentInterface(url=public_url, protocol_binding="JSONRPC", protocol_version="1.0")],
        capabilities=AgentCapabilities(streaming=False, push_notifications=False),
        default_input_modes=["application/json", "text/plain"],
        default_output_modes=["application/json"],
        security_schemes={"bearer": SecurityScheme(
            http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer"))},
        security_requirements=[SecurityRequirement(schemes={"bearer": StringList()})],
        skills=[AgentSkill(
            id="investigate_payment",
            name="Investigate a payment",
            description="Checks payment velocity, amount against the merchant's own history and account "
                        "age, then recommends HOLD_PAYOUT, ALLOW or NEEDS_MANUAL_REVIEW with the evidence.",
            tags=["fraud", "payments", "risk"],
            examples=['{"payment_id": "3f2c9a1e-…"}', "Investigate payment 3f2c9a1e-…"],
            input_modes=["application/json", "text/plain"],
            output_modes=["application/json"],
        )],
    )


def payment_id_from(message):
    for d in get_data_parts(message.parts):
        if isinstance(d, dict) and d.get("payment_id"):
            return str(d["payment_id"])
    m = UUID.search(get_message_text(message) or "")
    return m.group(0) if m else None


class InvestigatePayment(AgentExecutor):
    def __init__(self, state, timeout_s=None):
        self.state = state            # the FastAPI app.state, read lazily: the service exists after startup
        self.timeout_s = timeout_s or float(os.environ.get("A2A_TIMEOUT_S", "90"))

    async def execute(self, context, event_queue):
        task = context.current_task or new_task_from_user_message(context.message)
        if not context.current_task:
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)

        pid = payment_id_from(context.message)
        if not pid:
            await updater.reject(new_text_message("Send a payment_id (a data part, or text containing the id).",
                                                  context_id=task.context_id, task_id=task.id))
            return
        await updater.start_work()
        service, tools = self.state.service, self.state.tools
        try:
            p = await tools.call("get_payment", payment_id=pid)
        except ToolError as e:
            await updater.failed(new_text_message(f"Could not load payment {pid}: {e}",
                                                  context_id=task.context_id, task_id=task.id))
            return

        case_id, _ = service.open({
            "payment_id": p["id"], "merchant_id": p["merchantId"], "amount_paise": int(p["amountPaise"]),
            "currency": p.get("currency", "INR"), "status": p.get("status"), "score": 0.0,
            # Not flagged by our scorer, so no pattern to route on: every specialist goes.
            "reasons": ["EXTERNAL_REQUEST"], "features": {},
        })
        case = await self._wait(service, case_id)
        rec = case.get("recommendation") or {}
        await updater.add_artifact([new_data_part({
            "case_id": case_id,
            "case_status": case["status"],
            "recommendation": rec.get("action"),
            "confidence": rec.get("confidence"),
            "rationale": rec.get("rationale"),
            "evidence": {k: v.get("summary") or v.get("error") for k, v in (case.get("findings") or {}).items()},
            "decided_by_human": (case.get("decision") or {}).get("decision"),
            "review_url": f"{os.environ.get('PUBLIC_URL', 'http://localhost:8003')}/#{case_id}",
            "note": "A recommendation, not a decision. A human analyst makes the final call.",
        })], name="investigation")
        await updater.complete()

    async def _wait(self, service, case_id):
        deadline = asyncio.get_running_loop().time() + self.timeout_s
        while True:
            case = service.store.get(case_id)
            if case["status"] in DONE or asyncio.get_running_loop().time() > deadline:
                return case
            await asyncio.sleep(0.2)

    async def cancel(self, context, event_queue):
        raise UnsupportedOperationError()


class A2AAuth:
    """
    Bearer token on the RPC endpoint only; the agent card stays public, because
    discovery is the point of it. Starting an investigation can cost an LLM call,
    so anonymous callers don't get to.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].rstrip("/") == RPC_PATH:
            token = os.environ.get("A2A_TOKEN")
            if not token:
                await JSONResponse({"error": "A2A is disabled: set A2A_TOKEN"}, 503)(scope, receive, send)
                return
            got = dict(scope["headers"]).get(b"authorization", b"")
            if not hmac.compare_digest(got, f"Bearer {token}".encode()):
                await JSONResponse({"error": "missing or invalid bearer token"}, 401,
                                   headers={"WWW-Authenticate": "Bearer"})(scope, receive, send)
                return
        await self.app(scope, receive, send)


def mount_a2a(app):
    public = os.environ.get("PUBLIC_URL", "http://localhost:8003")
    card = agent_card(public + RPC_PATH)
    handler = DefaultRequestHandler(InvestigatePayment(app.state), InMemoryTaskStore(), card)
    add_a2a_routes_to_fastapi(app, agent_card_routes=create_agent_card_routes(card),
                              jsonrpc_routes=create_jsonrpc_routes(handler, rpc_url=RPC_PATH))
    app.add_middleware(A2AAuth)
