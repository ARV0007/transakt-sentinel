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

7. *The live demo log looked frozen after bootstrap.* Python block-buffers `print` when stdout is a
   pipe, so the simulator's progress only appeared at exit. `sentinel.sh` now runs Python with `-u`.

**Verified on real infrastructure (23 Oct, 02:34):** `./sentinel.sh demo` on the Mac — all seven
containers built and healthy, admin bootstrap OK, 199 payments, 0 failed. 6/6 incidents became cases,
all `HOLD_PAYOUT`; 0 cases on normal merchants; precision 0.947, recall 0.818; send→score p50 2.7 s.

**Earlier the same day:** the first `mkdir`/`git init` ran in a new Terminal tab sitting in `~`, so they
landed in the home folder — the wrong-directory trap again. Rule: run `pwd` before anything project-relative.

---

## Day 2 — 23 Oct 2026 · v1.1: restart-safe scoring, no pickles

**Built**
- Async state backends for the scorer's features: `MemoryState` (tests, no Redis) and `RedisState`
  (one sorted set + one counter per merchant, 24 h window, capped at 2,000 events). Compose points the
  scorer at `redis://redis:6379/2`; `/health` reports which backend is live.
- Model saved/loaded with **skops** behind a one-entry type allow-list; Hub push uploads `model.skops`.
  A tampered Hub model raises instead of silently falling back to the local file.
- Eval report now says who wrote the recommendations and, with an LLM on, how often it disagreed with
  the rules and how often it failed back to them.
- 10 new tests (46 total): backends give identical features, history survives a restart, a replayed
  payment isn't compared with itself or double-counted, the window expires, skops round-trips exactly,
  an unknown type and a raw pickle are both refused. E2E still 6/6 incidents, precision 1.0.

**Why**
The v1 limitations list. A redeploy wiping merchant history is a real hole (a burst timed with a deploy
looks like a new shop), and loading pickles from a public Hub is a real supply-chain risk.

**Concepts**
Redis sorted sets (`ZADD`/`ZRANGEBYSCORE`/`ZREMRANGEBYSCORE`), idempotent writes via natural keys,
pause-on-dependency-failure vs fail-open, pickle deserialisation risk, allow-listed deserialisation.

**Interview line**
"I moved the scorer's per-merchant state into Redis so a redeploy can't give fraudsters a clean slate, made
Kafka replays idempotent by putting the payment id in the set member, and replaced pickle with skops plus a
type allow-list so a tampered model on the Hub is rejected rather than executed."

**Mistakes & fixes**
1. *A crash between "remember this payment" and "save its score"* would make the Kafka redelivery compare the
   payment with itself (and count it twice). Caught while writing the Redis backend, before any test existed for it;
   the history read now skips the payment being scored. `test_a_replayed_payment_is_not_compared_with_itself`.
2. *The memory backend's pruning read the wrong tuple field* after the event tuple gained a payment id
   (`q[0][0]` is now the id, not the timestamp). Caught by the existing suite.
