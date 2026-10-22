"""
transakt-mcp: Transakt's data, published as MCP tools.

MCP (Model Context Protocol) is a standard menu. This server lists what can be
asked of Transakt — each tool has a name, a plain-English description and typed
inputs — and any MCP client can read the menu and order from it. The LangGraph
investigator is one such client; Claude Desktop or the MCP Inspector could be
another, with zero extra code.

Every tool is READ-ONLY on purpose. The fraud system recommends; a human
decides. Nothing reachable from here can move money or change a merchant.

The descriptions below are not comments for humans only. An LLM chooses tools
by reading them, so they say what the tool is FOR, not just what it returns.
"""

import os
from datetime import datetime, timedelta, timezone

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from .analytics import activity_summary, amount_profile
from .transakt_client import TransaktClient

client = TransaktClient(
    base_url=os.environ.get("TRANSAKT_URL", "http://localhost:8080"),
    email=os.environ.get("ADMIN_EMAIL", "sentinel-admin@transakt.local"),
    password=os.environ.get("ADMIN_PASSWORD", "change-me-sentinel"),
)

# stateless_http + json_response: each tool call is one plain HTTP request and
# one JSON answer. No sessions to keep alive between calls, which is also the
# direction the MCP spec itself moved in 2026.
mcp = FastMCP(
    "transakt",
    instructions="Read-only access to the Transakt payment gateway for fraud investigation. "
                 "Amounts are integer paise (Rs 1 = 100 paise). Timestamps are UTC.",
    host=os.environ.get("MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("MCP_PORT", "8001")),
    stateless_http=True,
    json_response=True,
)


def _since(minutes):
    return datetime.now(timezone.utc) - timedelta(minutes=minutes)


@mcp.tool()
async def get_payment(payment_id: str) -> dict:
    """Fetch one payment by id: merchant, amount in paise, currency, status
    (PENDING, CAPTURED or FAILED) and creation time. Use it to see the facts of
    the payment under investigation."""
    p = await client.get_payment(payment_id)
    return p or {"error": f"payment {payment_id} not found"}


@mcp.tool()
async def get_payment_ledger(payment_id: str) -> dict:
    """The double-entry ledger rows for one payment. A captured payment has a
    balancing CREDIT and DEBIT; a failed one has none because no money moved."""
    rows = await client.get_ledger(payment_id)
    return {"payment_id": payment_id, "entries": rows or []}


@mcp.tool()
async def get_merchant(merchant_id: str) -> dict:
    """A merchant's profile: name, business name and when the account was
    created. Account age matters: brand-new merchants moving large amounts are
    a classic fraud pattern."""
    m = await client.get_merchant(merchant_id)
    if not m:
        return {"error": f"merchant {merchant_id} not found"}
    created = m.get("createdAt")
    age = None
    if created:
        age = round((datetime.now(timezone.utc)
                     - datetime.fromisoformat(created.replace("Z", "+00:00"))).total_seconds() / 60, 1)
    return {
        "id": m.get("id"),
        "name": m.get("name"),
        "business_name": m.get("businessName"),
        "created_at": created,
        "account_age_minutes": age,
    }


@mcp.tool()
async def list_merchant_payments(merchant_id: str, window_minutes: int = 60, limit: int = 50) -> dict:
    """A merchant's payments in the last `window_minutes`, newest first, at most
    `limit` of them. Use it to eyeball the raw sequence of payments."""
    window_minutes = max(1, min(window_minutes, 24 * 60))
    rows = await client.payments_since(_since(window_minutes), merchant_id)
    return {"merchant_id": merchant_id, "window_minutes": window_minutes,
            "total": len(rows), "payments": rows[:max(1, min(limit, 200))]}


@mcp.tool()
async def merchant_activity(merchant_id: str, window_minutes: int = 30) -> dict:
    """How busy a merchant has been: payment count, failures, average per
    minute, and the most payments inside ANY 2-minute stretch. A high 2-minute
    maximum against a low average is the signature of a velocity burst."""
    window_minutes = max(1, min(window_minutes, 24 * 60))
    rows = await client.payments_since(_since(window_minutes), merchant_id)
    return {"merchant_id": merchant_id, **activity_summary(rows, window_minutes)}


@mcp.tool()
async def merchant_amount_profile(merchant_id: str, window_minutes: int = 1440,
                                  exclude_payment_id: str | None = None) -> dict:
    """A merchant's normal ticket size over the window: median, 95th percentile
    and maximum, in paise. Pass the suspicious payment as `exclude_payment_id`
    so it does not distort the baseline it is being compared against."""
    window_minutes = max(1, min(window_minutes, 24 * 60))
    rows = await client.payments_since(_since(window_minutes), merchant_id)
    return {"merchant_id": merchant_id, "window_minutes": window_minutes,
            **amount_profile(rows, exclude_payment_id)}


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "UP", "service": "transakt-mcp"})


def main():
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
