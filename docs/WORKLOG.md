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

---

## Day 2 (cont.) — 23 Oct 2026 · v1.2: hardening, observability, feedback, A2A

**Built**
- **MCP auth.** transakt-mcp rejects any request without `Authorization: Bearer <MCP_TOKEN>` (constant-time
  compare), refuses to start without a token; `/health` and `/metrics` stay open. The investigator sends it;
  `sentinel.sh` generates `MCP_TOKEN` and `A2A_TOKEN` into `.env` without printing them.
- **Atomic Redis state.** Read-and-record is one Lua script. Real-Redis test: two replicas, 200 concurrent
  payments — every payment sees a distinct prior count and none are lost. CI now runs a Redis service.
- **Monitoring.** Prometheus metrics on all three services (counters, gauges, histograms); Prometheus +
  Grafana in compose with a provisioned 13-panel dashboard (`:3000`). Config and all 17 PromQL queries pass
  `promtool`; a real Prometheus scraped an e2e run and the investigator and MCP panels showed live data.
- **Feedback loop.** `GET /feedback` exports decided cases as labels; `train.py --feedback` weights them ×25;
  `./sentinel.sh retrain` rebuilds the scorer on them (Redis history is kept across the rebuild). Test: 12
  dismissals of a "flash sale" burst stop it being flagged, and a new-merchant blitz is still caught.
- **A2A.** The investigator publishes an agent card and accepts JSON-RPC `SendMessage` (A2A v1.0, official
  `a2a-sdk`) with a payment id; it runs the same graph and returns the recommendation as a task artifact.
  Bearer-protected; the card is public; the case still waits for a human.
- Tests: 60 unit/API (+2 real-Redis), and the e2e run now also exercises A2A, metrics and the feedback loop.

**Why**
The v1.1 limitations list plus what a reviewer would poke at first: an unauthenticated admin proxy, a
documented race, no way to see the system running, no way for humans to correct the model.

**Concepts**
Bearer tokens and constant-time comparison · Redis Lua scripting and atomicity · Prometheus pull model,
counter/gauge/histogram, `rate()` and `histogram_quantile()` · provisioning Grafana as code · human-in-the-loop
learning and sample weights · A2A agent cards, tasks and their lifecycle.

**Interview line**
"I treated the MCP server as what it is — an admin proxy — and put auth in front of it; proved a Redis race with
a test where a naive read-then-write kept 2 of 200 updates and the Lua version kept all 200; made the whole
pipeline observable in Grafana; closed the loop so analysts' decisions retrain the model; and exposed the agent
over A2A so other agents can request investigations while a human still makes the call."

**Mistakes & fixes**
1. *Test-only:* the MCP SDK lets a session manager start once per process; the second test that started a
   server failed. An autouse fixture gives each test a fresh one.
2. *macOS portability:* `grep -q '^MCP_TOKEN=.\+'` is a GNU-ism; BSD grep on the Mac reads `\+` differently.
   Switched to `grep -E`.
3. `pkill -f prometheus` in a one-liner matched (and killed) the shell running it, because the pattern was on
   its own command line. Use `pgrep` on a pattern that can't match the caller.

---

## Day 3 — 23 Oct 2026 · Published, and the first CI run failed

Pushed to **github.com/ARV0007/transakt-sentinel**. CI ran on the first push: transakt-mcp passed; investigator
(pytest exit 2) and risk-scorer (exit 1) failed.

**Cause:** both were "works on my machine". The development environment had packages installed by hand that
the requirements files never listed — `a2a-sdk` for the investigator (so `import` failed before any test ran:
exit code 2 is pytest's "couldn't collect"), and the Lua support for `fakeredis` that the scorer's Redis tests
need (tests ran and failed: exit code 1).

**Fix:** added `a2a-sdk>=1.2,<2` and `fakeredis[lua]`; then reproduced CI exactly — a fresh Python 3.12 venv
per service with *only* that service's requirements — before pushing again: 12 / 24 / 26 passing and the e2e
run green. Also bumped `actions/checkout` v4→v5 and `actions/setup-python` v5→v6 (Node 20 deprecation).

**Lesson:** CI's first job is to be the clean machine you don't have. Test in a fresh venv built from the
requirements file before trusting "all green locally". **Interview line:** "My first CI run caught two
undeclared dependencies that my own environment had been hiding."
