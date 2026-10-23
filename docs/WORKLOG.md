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

---

## Day 3 (cont.) — CI green; the real LLM client tested; screenshots

**CI:** second push (`18bdedb`) green on every job — three unit suites (with a real Redis service) and the
3½-minute end-to-end run.

**LLM over real HTTP.** Every earlier LLM test used `FakeLLM`, which bypasses the actual client, so the request
we send to Hugging Face had never been checked. `tests/test_llm_http.py` now runs the real `ChatOpenAI` against a
local OpenAI-compatible server: request shape (bearer key, model, temperature 0, system + user messages with the
facts as JSON), a reply wrapped in a code fence with chatter around it, an invented number caught by the grounding
check, and 401 / 429 / 503 all falling back to rules. A real model run still needs an API key.

**Mistakes & fixes**
1. *Mocking the wrong library.* `respx` intercepted nothing: the OpenAI SDK v3 moved from `httpx` to its fork
   `httpx2`. A real localhost server doesn't care which HTTP library the client uses.
2. *"Event loop is closed"* on the second test: langchain-openai caches its HTTP client, and pytest-asyncio gave
   each test a new loop. The service runs on one loop; the test file now shares one too (`loop_scope="module"`).

**Polish caught by looking at the screenshots:** the rules' rationale read "Fraud signals from velocity_analyst" —
an internal node name in text written for a person. Now "from the velocity analyst".

**Showcase:** screenshots of the review page (light, dark, a decided case, phone) taken with Playwright from a live
e2e run, in `docs/images/`; a timed 2-minute recording script in `docs/DEMO.md`; `run_e2e.py --keep-running` for a
Docker-free demo.

---

## Day 3 (cont.) — the v1.2 Docker run failed on file permissions

`./sentinel.sh demo` on the Mac: everything built, then transakt-mcp and risk-scorer exited at once with
`PermissionError: [Errno 13] Permission denied: '/app/app/server.py'`.

**Cause.** The source files on the Mac were owner-read-only (mode 600). `COPY` keeps the build machine's
permissions and makes root the owner, and the services then run as the unprivileged `sentinel` user — who
could not read its own code. Tests, the e2e run and CI never build the images, so none of them could see it.

**Fix.** Create the user *before* copying and `COPY --chown=sentinel:sentinel`, so the service owns its code
whatever mode the files arrive with. Reproduced with Docker before fixing: the old Dockerfile shape gives
"Permission denied" on a mode-600 file, the `--chown` shape reads it. New CI job builds all three images from a
deliberately `chmod -R go-rwx` checkout and imports each service's code as the user it runs as.

**Same trap, next in line:** Prometheus and Grafana bind-mount their config from the repo and run as their own
users (`nobody`, 472), so owner-only config files would have crashed them too. `sentinel.sh` now makes
`monitoring/` world-readable before starting; it holds no secrets (those live only in `.env`).

**Also:** `sentinel.sh` now prints the logs of any container that exits during `up` — this run's failure only
said `exited (1)` until that existed.

**Lesson.** "Tested" means tested in the shape it ships in. The unit, e2e and CI runs all used plain Python; the
first time the Dockerfiles met unusual file permissions was on the user's laptop.

**Verified on real infrastructure (23 Oct, 17:38) — v1.2:** after the `--chown` fix, `./sentinel.sh demo` on the
Mac brought every container up healthy, bootstrap OK, 199 payments, 0 failed. 6/6 incidents became `HOLD_PAYOUT`
cases, 0 cases on normal merchants, precision 0.947 / recall 0.807, send→score p50 2.6 s. Every case carried
specialist evidence, so the investigator → transakt-mcp bearer-token path worked, and the scorer ran on its
Redis-backed history throughout.

---

## Day 4 — 23 Oct 2026 (evening) · v1.3: batched scoring, no lost cases, gated retraining

**Built**
- **Batched + pipelined scoring.** The consumer polls up to 200 messages (`getmany`), orders them by timestamp,
  and `Scorer.handle_many` computes features in order, scores the batch in one model call, writes it in one SQLite
  transaction, and does all its Redis work in one pipelined round trip. Benchmark (`scripts/bench_scorer.py`,
  20,000 payments, one core of a 2-core VM): **memory 465 → 15,453/s; Redis 355 → 3,505/s.** Batched results are
  identical to one-at-a-time — tested on fakeredis and real Redis.
- **No lost cases.** A redelivered *flagged* payment re-sends its case from the stored score; malformed events in a
  batch are skipped individually.
- **Retraining gate.** Newest 25% of analyst decisions held out; a candidate ships only if it agrees with them at
  least as well as the current model and keeps synthetic recall within 2 points. `/health` and
  `./sentinel.sh retrain` report the verdict. Test: twelve mistaken dismissals of real fraud → rejected, and the
  shipped model still catches it.
- **Threshold sweep** in the eval report (0.2…0.9: flagged, precision, recall, incidents, honest merchants with a case).

**Findings**
1. *The first benchmark was wrong:* 70% of payments flagged, because it sent each merchant six payments a minute —
   which *is* a velocity burst. Realistic traffic (~1/min/merchant) gives 0.2%. Benchmarks need realistic inputs or
   they measure the wrong code path.
2. *The model call dominates:* 0.69 ms for 1 row, 1.06 ms for 100 — a fixed cost. That one measurement is the whole
   argument for batching.
3. *A real at-least-once bug, present since v1.0:* score saved before the case is sent, and a redelivery was
   skipped as "already scored" — so a crash in between lost the case. Found while rewriting the consumer loop.
4. *The threshold barely matters on this run:* precision/recall are flat from 0.2 to 0.9 because scores cluster at
   0 and 1. That is synthetic-data overconfidence, now visible in the report; real data would spread the scores.

**Interview line**
"I profiled the scoring path, found the model call had a fixed ~0.7 ms cost, and batched Kafka polls into one
model call and one pipelined Redis round trip — 10× throughput with byte-identical results. Along the way I found
and fixed an at-least-once bug that could lose a case, and put a gate in front of retraining so analysts' mistakes
can't teach the model to miss fraud."

**Consumer loops under test (Day 4, late).** Neither Kafka loop had ever run in a test — only on the Mac. Both now
run against in-memory fakes of aiokafka's classes (no broker reachable from the build environment: Docker Hub,
Apache and Maven Central are all blocked there):
- *scorer:* batched polls, time order restored across partitions (features identical to true-order scoring),
  every commit straight after a successful flush, malformed messages skipped, and a broker failure mid-flush
  losing no case.
- *investigator:* each commit after its case row exists; poison messages skipped *and* committed.

**The fake has to be honest or the test proves nothing.** The first fake producer logged a message as delivered
when `send()` was called. A planted mutation (removing the re-send-on-redelivery fix) then *passed* the crash test.
Real producers buffer until `flush()`; once the fake did the same, the mutation failed the test, as it should.
Every new test here was checked the same way — plant the bug, watch it fail, restore.
