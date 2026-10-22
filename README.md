# Transakt Sentinel

**An agentic fraud-investigation system for the [Transakt](https://github.com/ARV0007/transakt) payment gateway.**
Every settled payment is scored by a fast ML model; risky ones are investigated by a
LangGraph agent that gathers evidence through **MCP** tools and writes a recommendation
with an LLM — then **stops and waits for a human analyst**. Transakt itself is not modified.

`Python 3.12` · `LangGraph` · `LangChain` · `MCP` · `FastAPI` · `Kafka` · `scikit-learn` · `Hugging Face` · `LangSmith` · `Docker`

```
Transakt ──payment.settled──► risk-scorer ──fraud.cases──► investigator ──► analyst
 (Java,      (Kafka, outbox)   (ML, <1 ms,     (Kafka)      (LangGraph +      review page
 unchanged)                     no LLM)                      MCP + LLM,        :8003
                                                             human-in-loop)
                                      transakt-mcp ◄── MCP tools ──┘
```

## What makes it interesting

- **MCP as the integration layer.** The agent reaches Transakt only through six read-only
  MCP tools — the same server works with the MCP Inspector or Claude Desktop, no extra code.
- **LLM only where reasoning is needed.** A gradient-boosted model scores every payment in
  microseconds; the LLM sees only flagged cases (one per incident, not one per payment).
- **Human-in-the-loop that survives restarts.** LangGraph checkpoints every step; a case paused
  for review resumes after the container restarts (there's a test for exactly that).
- **Deliberate failure policies.** Broken tool → "could not check". LLM down → rules.
  No evidence at all → `NEEDS_MANUAL_REVIEW`, never `ALLOW`.
- **Grounding check** on every LLM rationale: numbers not present in the evidence are flagged.
- **Measured, not claimed.** A simulator plants labelled fraud; `eval/evaluate.py` reports
  precision/recall, incident detection, false alarms and how much traffic reached the LLM.
- **Event-driven microservices** split by speed, scaling and failure behaviour, at-least-once
  Kafka consumers made idempotent by primary keys.

## Run it locally

You need Docker Desktop, Python 3.12 and Transakt cloned next to this repo
(`~/Documents/Coding/transakt` and `~/Documents/Coding/transakt-sentinel`).

```bash
./sentinel.sh up          # Transakt + Postgres + Redis + Kafka + the three Sentinel services
./sentinel.sh bootstrap   # creates the admin account Sentinel reads with (local DB only)
./sentinel.sh simulate    # ~6 minutes of traffic with planted fraud
./sentinel.sh eval        # grades the run -> eval/results/latest.md
```

Then open **http://localhost:8003** to review cases. `./sentinel.sh demo` runs all four.

**Turn the LLM on (optional):** put a Hugging Face token in `.env` as `LLM_API_KEY`, then
`./sentinel.sh up` again. Without one, deterministic rules write the recommendations and
everything else works the same. **Tracing:** set `LANGSMITH_TRACING=true` and `LANGSMITH_API_KEY`.

## Tests

```bash
cd services/<service> && pip install -r requirements-dev.txt && pytest
python e2e/run_e2e.py      # whole pipeline against a fake Transakt (no Java/Kafka needed)
```

CI runs all unit suites plus the end-to-end run on every push.

## Repository

| Path | What |
|---|---|
| `services/transakt-mcp` | MCP server: Transakt data as read-only tools |
| `services/risk-scorer` | Kafka consumer, streaming features, ML model, HF Hub loading |
| `services/investigator` | LangGraph agent, FastAPI, review page, checkpoints |
| `simulator/` | Plants labelled fraud into local Transakt traffic |
| `eval/` | Grades a run against the simulator's labels |
| `e2e/` | Fake Transakt + full-pipeline test used by CI |
| `docs/` | `architecture.md`, `notes.md`, `WORKLOG.md`, `UNDERSTANDING.md` |

## Honest limitations

Merchant fraud only (Transakt records no card/device data); scorer state is in-memory;
training data is synthetic; decisions are recorded but not yet enforced in Transakt.
Details in [docs/architecture.md](docs/architecture.md#known-limitations-v1).
