# Demo — a 2-minute walkthrough to record

What to show, in order, and roughly what to say. Record the screen with
**Cmd + Shift + 5 → Record Selected Portion** (built into macOS). Keep it under
two minutes; one take with small stumbles beats five polished ones.

## Before you press record (~10 minutes earlier)

1. Open Docker Desktop and wait until it says *Engine running*.
2. Double-click `~/Documents/Coding/sentinel-demo.command`. It builds everything,
   plants ~6 minutes of fraud traffic and grades it. Leave it running.
3. Open three browser tabs:
   - `http://localhost:8003` — the review page
   - `http://localhost:3000` — Grafana (opens on the Sentinel dashboard)
   - `https://github.com/ARV0007/transakt-sentinel` — the repo, with the green CI badge
4. Have Terminal ready with the A2A command from the README (the `TOKEN=…` line already run).

Start recording when the simulator is about halfway (Grafana's lines will be moving).

## The script

**0:00 — The problem (GitHub tab, 15 s)**
> "Transakt is a payment gateway I built in Spring Boot. Sentinel is an AI fraud
> investigator that sits beside it without changing a line of it."

Point at the README diagram: payments → Kafka → ML scorer → LangGraph agent → human.

**0:15 — Live traffic (Grafana, 25 s)**
> "This is live. A simulator is sending payments with fraud planted in. Every
> payment is scored by a small ML model in about a millisecond — no LLM on this path.
> Only the risky few percent go to the agent."

Point at *Payments scored / min*, *Flag rate*, *Model predict time*.

**0:40 — An investigation (review page, 35 s)**
Click a case marked **HOLD PAYOUT** with reason VELOCITY.
> "The agent's supervisor sent specialists to gather evidence through MCP tools —
> payment velocity, amount against the merchant's own history, account age.
> It recommends holding the payout, with the evidence. But it doesn't decide."

Type a note, click **Confirm fraud**.
> "A human does. And that decision becomes a training label: `./sentinel.sh retrain`
> teaches the model from analysts."

**1:15 — Agent to agent (Terminal, 20 s)**
Run the A2A `curl`.
> "Other AI agents can ask Sentinel too, over the A2A protocol. They get a
> recommendation back; the case still lands on this page for a person."

**1:35 — Proof (Terminal output of the demo, 20 s)**
Scroll to the evaluation table.
> "Every run is graded against the planted fraud: six of six incidents caught,
> zero cases opened on honest merchants. And it's tested on every push."

Flick to the green CI badge. Stop.

## Screenshots for the README

Already in `docs/images/` from an automated run. Replace them with ones from your
own Docker run if you like — especially a Grafana screenshot
(`docs/images/grafana.png`), which only exists once Docker is running.
