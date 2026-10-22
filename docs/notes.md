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
