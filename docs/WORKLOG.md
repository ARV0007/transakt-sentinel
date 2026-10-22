# Worklog

## Day 1 — 22 Oct 2026 · Sentinel v1, end to end

**Built**
- New repo `transakt-sentinel`, three Python services in one repo with one compose overlay.
- `transakt-mcp`: six read-only MCP tools over Transakt's admin REST API (FastMCP, streamable HTTP, stateless).
- `risk-scorer`: Kafka consumer on Transakt's existing `payment.settled` topic, streaming per-merchant
  features, gradient-boosted model trained at image build (optionally loaded from the Hugging Face Hub),
  flagged payments published to `fraud.cases`.
- `investigator`: LangGraph supervisor → three parallel specialists via MCP → LLM synthesis with rules
  fallback and grounding check → `interrupt()` for the analyst → finalize. SQLite checkpoints; FastAPI API;
  analyst review page.
- Simulator that plants labelled fraud (velocity burst, amount spike, new-merchant blitz) into local traffic;
  eval script grading payment- and incident-level results; `sentinel.sh` to run it all.
- 36 unit/API tests (9 MCP, 9 scorer, 18 investigator), a full-pipeline e2e test against a fake Transakt, GitHub Actions CI.

**Why**
- Zero changes to Transakt: an ADMIN can already read every merchant's payments, and the outbox already
  publishes every settled payment. Sentinel is one more consumer group.
- Services split by speed (scorer: every payment, microseconds; investigator: few cases, seconds) and by
  failure behaviour (an LLM outage must never stop scoring).

**Concepts**
MCP servers/clients and tool descriptions · LangGraph state, reducers, `Send`, `interrupt`, `Command(resume)`,
checkpointers · consumer groups, at-least-once delivery + idempotent consumers · keyed partitions ·
gradient boosting with missing values · grounding checks · fail-open vs fail-to-human.

**Interview line**
"I built an agentic fraud-investigation layer on top of my own payment gateway without changing it: a
microsecond ML scorer consumes the gateway's outbox events from Kafka, and only flagged payments reach a
LangGraph agent that gathers evidence through MCP tools, has an LLM write a grounded recommendation, and
pauses for a human — durably, so a paused case survives a restart."

**Mistakes & fixes**
1. *MCP SDK 2.x* had just shipped: it renamed `FastMCP` and crashed `langchain-mcp-adapters` on import.
   Pinned `mcp<2` after proving a 1.30 server and the 0.3.2 adapter talk end to end.
2. *Synthetic data taught the model to ignore the blitz pattern.* The normal-row generator produced
   "minutes-old account, Rs 30,000 tickets" as legitimate half the time; the model caught 5 of 12 blitz
   payments. Made a young account's history and ticket size consistent with its age.
3. *Simulator traffic was too busy to have a "burst".* At one sale per 25 s a normal shop already did ~5
   payments per 2 minutes. Slowed background traffic to ~1/min and added shops.
4. *`POST /cases` as a sync endpoint* would run in a worker thread with no event loop, so starting the
   investigation task would fail. Made it `async def`.
5. *Case dedup via `LIKE` over JSON text* was fragile; replaced with a `case_payments` table keyed by payment id.
6. *`./sentinel.sh reset` would also delete Transakt's Docker database volume* (shared compose project).
   It now asks for confirmation.

**Earlier the same day:** the first `mkdir`/`git init` ran in a new Terminal tab sitting in `~`, so they
landed in the home folder — the wrong-directory trap again. Rule: run `pwd` before anything project-relative.
