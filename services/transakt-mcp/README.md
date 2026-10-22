# transakt-mcp

MCP server exposing **read-only** Transakt data as tools for the fraud agent.

| Tool | What it is for |
|---|---|
| `get_payment` | Facts of one payment |
| `get_payment_ledger` | Its double-entry ledger rows |
| `get_merchant` | Merchant profile + account age |
| `list_merchant_payments` | Raw recent payments for one merchant |
| `merchant_activity` | Counts, failures, max payments in any 2-minute stretch |
| `merchant_amount_profile` | Median / p95 / max ticket size |

Endpoint: `http://localhost:8001/mcp` (streamable HTTP, stateless). Health: `/health`.

Logs into Transakt as an ADMIN (`ADMIN_EMAIL` / `ADMIN_PASSWORD`), because only an
admin may read across merchants.

Try it with the MCP Inspector: `npx @modelcontextprotocol/inspector`, transport
"Streamable HTTP", URL `http://localhost:8001/mcp`.

Tests: `pip install -r requirements-dev.txt && pytest`
