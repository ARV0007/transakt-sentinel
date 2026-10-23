# Notes — concepts and the why

## The restaurant, extended

Transakt is the restaurant (front door = Tomcat, waiter = controller, chef = service,
stock-keeper = repository, fridge = Postgres). Sentinel is an **outside security firm**
with three departments:

| Sentinel piece | In the restaurant |
|---|---|
| `payment.settled` topic | The receipt printer — every sale prints a slip the firm can read |
| risk-scorer | The metal detector at the door: fast, cheap, checks everyone |
| investigator / supervisor | Head of security, deciding which guards to send |
| specialists | Guards who each watch one thing |
| transakt-mcp | The records clerk with a standard request form |
| LLM synthesis | The detective who writes up the case |
| human_review | The manager who signs off — nothing happens without them |

If the security firm has a bad day, the restaurant keeps serving.

## What an agent is

A chatbot answers once. An agent is an LLM (or code) **in a loop with tools**: decide what
to look up → look it up → read the result → decide again → stop. The LLM never runs a tool;
it writes a request, and *our* code runs it. That is why "the agent recommends, a human
decides" is enforceable rather than a promise.

In Sentinel the loop is mostly deterministic code, and the LLM is used for the one step that
genuinely needs judgement: weighing the evidence and explaining it. That is a design choice,
not a shortcut — it's cheaper, faster and easier to test.

## LangGraph in four words: nodes, edges, state, checkpoints

- **State** is the case file (`CaseState`). Every node reads it and returns *updates* to it.
- **Nodes** are steps. **Edges** are arrows; conditional edges choose the next step.
- **`Send`** fans out to several nodes in parallel. Each specialist writes its own key of
  `findings`; the `merge` reducer combines them so parallel writes don't overwrite each other.
- **Checkpointer** saves the state after every step under a `thread_id` (we use the case id).
- **`interrupt()`** saves and stops. `Command(resume=...)` continues from exactly that point —
  even in a different process after a restart. Tested in `test_a_paused_case_survives_a_restart`.

## MCP

A standard way for an AI app to discover and call tools. The **server** publishes a menu
(name, description, typed inputs); any **client** can read it. We use streamable HTTP in
stateless mode: one tool call = one POST = one JSON answer.

Tool descriptions are written for the model, not for humans: they say what the tool is *for*
("a high 2-minute maximum against a low average is the signature of a velocity burst").

## Why Kafka on both sides of the scorer

- **In:** Transakt's transactional outbox already publishes `payment.settled`. Sentinel is just
  another consumer group — every group gets every event — so Transakt didn't change at all.
- **Out:** the scorer publishes cases to `fraud.cases` instead of calling the investigator.
  An investigator waiting 20 s on an LLM can't slow scoring, and if it's down, cases queue.
- **At-least-once:** both consumers commit offsets only after handling a message, so a crash
  re-delivers. Both are idempotent on `payment_id` (a primary key), so re-delivery is harmless —
  the same reason Transakt's outbox gives every event a stable `eventId`.
- **Keys:** cases are keyed by merchant so one merchant's cases stay in order on one partition.

## Failure policies are a choice per component

| Component | When its dependency fails | Why |
|---|---|---|
| Transakt rate limiter (Redis down) | fails **open**: allow the request | Protecting availability; a limiter must not cause an outage |
| Scorer (merchant lookup fails) | feature = unknown (NaN), keep scoring | The model handles missing values |
| Investigator (MCP down) | finding = "could not check" | One broken tool shouldn't sink the case |
| Investigator (no evidence at all) | `NEEDS_MANUAL_REVIEW`, **never** `ALLOW` | Unknown is not safe; a human decides |
| LLM (down / bad JSON) | rules write the recommendation | A flaky model never blocks a case |

## Classic ML vs LLM

The risk model is a `HistGradientBoostingClassifier` on seven tabular features. It handles
NaN natively, trains in seconds, and predicts in microseconds. The LLM runs only on flagged
payments — in the simulator, that's roughly one LLM call per *incident*, not per payment.

**Grounding check:** every number in the LLM's rationale must appear in the facts it was
given (in paise, rupees, or as a percentage). Invented numbers are shown to the analyst as a
warning. Cheap, and it catches the most dangerous kind of hallucination in a fraud tool.

## Synthetic data, honestly

The training rows are generated from the same patterns the simulator plants. The first
version of the generator produced "minutes-old merchant moving Rs 30,000" as *normal* half the
time, so the model shrugged at exactly the blitz pattern (only 5 of 12 caught; the first
payment scored 0.06). Fixing the generator to keep a young account's history and ticket size
consistent with its age fixed it — see `test_new_merchant_blitz_is_caught`. Lesson: **with synthetic data, the generator *is* your assumptions** — test the model
against scenario-shaped traffic, not only against a held-out split of its own data.

## Why the scorer's memory moved to Redis (v1.1)

The scorer judges a payment against the merchant's recent history. In v1 that history lived in
a Python dict, so every restart wiped it: the first payments after a redeploy were compared with
nothing, and a fraudster who timed a burst with a deploy would look like a quiet new shop.

Now each merchant has a Redis **sorted set**: the score is the timestamp, the member is
`payment|amount|status`. "Everything in the last 24 h" is one `ZRANGEBYSCORE`, old entries are
trimmed with `ZREMRANGEBYSCORE`, and an all-time counter sits beside it.

Two subtleties worth being able to explain:
- **Replays.** If the scorer crashes after writing to Redis but before saving the score, Kafka
  redelivers the payment. Putting the payment id inside the member makes the second `ZADD` a
  no-op, the counter is only incremented when `ZADD` actually added something, and the history
  read skips the payment being scored — so it is never compared with itself.
- **Failure policy.** If Redis is down, `compute` raises, the Kafka offset isn't committed, and
  the consumer retries. Scoring *pauses*; it does not continue with an empty history. Payments are
  unaffected either way — they were settled before the scorer ever saw them.

## Why not pickle (v1.1)

`pickle.load` can run arbitrary code: a pickle is a small program that rebuilds an object. Any
model file you download is therefore only as safe as everyone who can push to that repository.
skops stores plain data and, on load, lists every type in the file; we refuse anything not on a
one-line allow-list (`TreePredictor`). A test dumps a file containing an unknown class and checks
it is rejected.

## Securing the MCP server (v1.2)

transakt-mcp logs into Transakt as an ADMIN, so it is an admin proxy: whatever can reach port
8001 can read every merchant's payments. It now refuses any request without
`Authorization: Bearer <MCP_TOKEN>` (except `/health` and `/metrics`, which carry no data). The
comparison is `hmac.compare_digest` — constant time, same idea as Transakt's `MessageDigest.isEqual`.
And it refuses to *start* without a token, so a misconfigured deploy fails loudly instead of running open.

The MCP spec's answer for outside clients is OAuth 2.1 with the server as a resource server. A shared
secret is the right size for two services we run ourselves; OAuth is the upgrade path.

## Atomic read-and-record with Lua (v1.2)

"Read the history, then record the payment" as two Redis calls has a gap. Two scorer replicas
working on the same merchant can both read before either writes — both see "5 payments so far",
both write "6". A test makes this concrete: 200 concurrent payments through a naive read-then-write
ended with a count of **2**. Through the Lua script, all 200 are kept and every payment sees a
different "count before me".

Redis runs a Lua script as one step: nothing else touches those keys until it returns. The script
trims the window, reads the history, skips the payment itself on a replay, adds it, increments the
counter only if the add was new, caps and sets expiries — then returns the history *as it was before*.

## Metrics: counters, gauges, histograms

- **Counter** — only goes up (payments scored, cases opened). Prometheus's `rate()` turns it into "per second".
- **Gauge** — goes up and down (cases by status). Ours is set from the database at scrape time, so it
  can't drift from the truth.
- **Histogram** — counts observations into buckets (scoring lag, investigation time).
  `histogram_quantile(0.95, ...)` estimates the p95 from those buckets.

Prometheus *pulls*: every 5 s it GETs `/metrics` from each service. Grafana only reads Prometheus.
Neither can break the pipeline — if they're down, nothing else notices.

## The feedback loop

The synthetic training data encodes our guesses about fraud. Analyst decisions are the only data
about how wrong those guesses are — especially false alarms, which synthetic data never predicts.
`GET /feedback` exports each decided case as (features at the time, human verdict), and retraining
weights each row 25× because there are so few of them.

Risk worth naming: the model learns analysts' mistakes too. A real system would hold out recent
decisions and only ship a retrained model that does at least as well on them.

## A2A vs MCP

| | MCP | A2A |
|---|---|---|
| Who's calling whom | an agent → a **tool** | an agent → another **agent** |
| In Sentinel | investigator → transakt-mcp | other agents → investigator |
| Discovery | `tools/list` | `/.well-known/agent-card.json` |
| Unit of work | a tool call, answered at once | a **task** with a lifecycle (submitted → working → completed/rejected/failed) |

Sentinel's A2A skill runs the same graph an analyst sees and returns the recommendation as a task
artifact. It is a recommendation, not a decision: the case still waits on the review page.

## Batching: paying a fixed cost once (v1.3)

Scoring one payment with scikit-learn costs ~0.7 ms. Scoring 100 costs ~1 ms. Almost all of the cost is the call
itself (input checks, setting up the tree walk), not the rows. So the consumer now asks Kafka for up to 200
messages at once (`getmany`), computes features for each **in order**, scores them in **one** model call and
writes them in **one** SQLite transaction. Throughput went from ~465 to ~15,000 payments/s in memory.

With Redis the remaining cost was one network round trip per payment. **Pipelining** sends all of a batch's Lua
scripts in one go. The subtle part is why that's still correct: Redis executes a pipeline's commands in the order
it received them, so payment #7 of a merchant still sees payments #1–6 — even when they're in the same batch.
A test computes a whole batch through one pipeline and checks every feature against one-at-a-time.

## At-least-once, end to end (v1.3)

The scorer saves a score, then sends the case. If it crashes in between, Kafka redelivers the payment — and v1.2
said "already scored, skip it", so the case was gone. Now a redelivered *flagged* payment re-sends its case from
the stored score. That can produce duplicates, which is fine: the investigator's `case_payments` table has the
payment id as its primary key, so a repeat attaches to nothing. **At-least-once delivery + idempotent receivers =
effectively exactly-once results.** Same idea as Transakt's outbox and its stable event ids.

## Gating a retrained model (v1.3)

Learning from analysts is only safe if a few wrong decisions can't undo it. Before shipping, the newest 25% of
decisions are held back; a candidate trained on the rest must (1) agree with those held-out decisions at least as
often as the current model, and (2) keep synthetic-fraud recall within 2 points. Pass → retrain on everything and
ship. Fail → keep the old model and say why (`/health` → `retraining_gate`). Holding out the *newest* rather than
a random sample matters: the question is whether the model matches what analysts are deciding now.

## Choosing a threshold

0.5 is a default, not a decision. The eval report re-flags the same run at 0.2…0.9 — possible because every
score is stored — and shows the trade: lower thresholds catch incidents sooner but open cases on honest merchants
(analyst time); higher ones miss fraud (money). Pick where those two costs balance for your business.
