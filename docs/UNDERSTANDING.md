# Understanding Sentinel — from scratch, in build order

Each chapter explains one piece in plain English, then asks questions an
interviewer might ask. Answer them out loud before reading the hints.

---

## Chapter 1 — Why a fraud system, and why beside Transakt

Transakt takes payments. It has no idea whether a merchant is a real shop or a
fraudster pushing stolen money through a fresh account. Sentinel is an outside
security firm: it watches every sale, digs into the suspicious ones, and asks a
human before anything happens.

It lives in its own repository and Transakt is **not modified**. Everything it
needs already existed: an admin can read every merchant's payments, and every
settled payment is already published to Kafka by the outbox.

**Counter-questions**
1. Why build this as separate services instead of more classes inside Transakt?
   *(Hint: what happens to payments if the fraud code is slow or crashes?)*
2. What does "Transakt is not modified" buy you, concretely?
3. v1 catches *merchant* fraud but not *card* fraud. What data is missing for card fraud?

---

## Chapter 2 — The simulator and the answer key

Real fraud is rare and nobody labels it for you. So the simulator plays a few
minutes of normal traffic and **plants** fraud on purpose — a velocity burst, an
amount spike, a brand-new merchant moving big money — and writes down which
payments were planted. That file, `labels.csv`, is the answer key.

Normal shops sell about once a minute. Not faster: at one sale every 25 seconds a
normal shop already does ~5 payments in two minutes, and a "burst" would stop
meaning anything.

**Counter-questions**
1. Why can't we just run the simulator against the live Render deployment?
2. Transakt sets `createdAt` itself. Why does that make an "odd hours" pattern hard to plant?
3. Why is a fixed random seed useful here?

---

## Chapter 3 — MCP: the records clerk

`transakt-mcp` publishes six read-only tools (`get_payment`, `merchant_activity`, …).
Any MCP client can list them and call them — our agent, the MCP Inspector, Claude
Desktop — with no custom code per client.

Behind each tool is a normal REST call to Transakt using an **admin JWT**, because
only an admin may read across merchants. The client logs in lazily, refreshes the
token before it expires, and re-logs in once on a 403 (Transakt answers an expired
JWT with an empty 403, not a 401).

**Counter-questions**
1. The LLM "calls" `merchant_activity`. Who actually executes it?
2. Why are all six tools read-only?
3. Transakt's admin list has no merchant filter. What does that cost, and what's the fix?

---

## Chapter 4 — The risk scorer: the metal detector

Every settled payment is scored by a gradient-boosted tree on seven numbers:
how many payments in 2 and 10 minutes, how much history the merchant has, this
amount compared with the merchant's own median, the amount itself, the account's
age, and the recent failure rate.

Order matters: the payment is compared with history **before** it is added, or a
Rs 50,000 spike would raise its own baseline and partly hide itself.

Risky payments (score ≥ 0.5) go to the `fraud.cases` topic with **reason codes**
(`VELOCITY`, `AMOUNT_SPIKE`, `NEW_MERCHANT_HIGH_VALUE`) from simple thresholds, so
a human can always say in one sentence why something was flagged.

**Counter-questions**
1. Why not send every payment to an LLM? Give the cost *and* the latency argument.
2. Why does the scorer publish to Kafka instead of calling the investigator's API?
3. Kafka delivers at least once. What stops a re-delivered event being scored twice?
4. The training data is synthetic. Why does that make the offline metrics optimistic?

---

## Chapter 5 — LangGraph: the case file passed desk to desk

The investigator is a graph. The **state** is the case file. The **supervisor**
reads the reason codes and sends specialists **in parallel** (`Send`). Each
specialist gathers evidence through MCP and writes into its own slot of
`findings`. **Synthesize** turns the findings into a recommendation. Then
**human_review** calls `interrupt()`: the graph saves a checkpoint and stops.

The case id is the graph's `thread_id`, so the analyst's decision —
`Command(resume=...)` — continues that exact case, even after a restart.

**Counter-questions**
1. Two specialists finish at the same moment and both write `findings`. Why doesn't one overwrite the other?
2. Why is the supervisor plain code and not an LLM?
3. Walk through what happens if the container restarts while a case is waiting for review.
4. Two analysts click "decide" at the same instant. What happens, and why?

---

## Chapter 6 — The LLM step and its safety nets

The LLM gets the facts as JSON and must reply with
`{"action", "confidence", "rationale"}`. Three nets:

1. No API key → rules write the recommendation.
2. Malformed reply or provider down → rules, and the case says so.
3. **Grounding check** → every number in the rationale must appear in the facts.

If no specialist could gather any evidence, the answer is `NEEDS_MANUAL_REVIEW`,
never `ALLOW`.

**Counter-questions**
1. Transakt's rate limiter fails *open*. The investigator fails to a *human*. Why are both right?
2. What kind of hallucination does the grounding check catch, and what kind does it miss?
3. Why keep `rules_would_say` next to the LLM's answer?

---

## Chapter 7 — Measuring it

`eval/evaluate.py` grades the system against `labels.csv` at two levels:
- **payment level** — precision and recall of the scorer's flags;
- **incident level** — did each fraud incident end up as a case, how fast, and was HOLD recommended?

Also: cases on normal merchants (false alarms), and how much traffic reached the LLM.

**Counter-questions**
1. A velocity burst's first few payments look normal. Why does payment-level recall
   understate how well the system works, and which metric tells the truer story?
2. What number would you put on your resume from this, and how would you defend it?

---

## Chapter 8 — Making the scorer restart-safe, and the model file safe (v1.1)

**Memory.** The scorer used to remember each merchant's recent payments in a Python dict. A
restart forgot all of it. Now that history lives in Redis — the same Redis Transakt already
runs, on database 2 — as one sorted set per merchant, ordered by time. Restart the scorer and
it picks up exactly where it was.

**The file.** The trained model used to be saved as a pickle. Loading a pickle runs code, so a
tampered model downloaded from Hugging Face could take over the scorer. It's now saved with
skops, which refuses to build any type that isn't on a short allow-list.

**Counter-questions**
1. Why does the payment id go *inside* the Redis set member instead of just the amount and status?
2. Redis goes down mid-traffic. What does the scorer do, and why is that better than carrying on?
3. Transakt's Redis rate limiter fails *open* when Redis is down, but the scorer *pauses*. Same
   Redis, opposite policies — defend both.
4. `payment.settled` is keyed by payment id, not merchant. Why does that matter once you run two
   scorer replicas?
5. Someone says "just load the model with joblib, it's from our own Hub repo". What's your answer?

---

## Chapter 9 — Production hardening (v1.2)

Five changes, each closing a gap a reviewer would find:

- **The MCP server needs a password now.** It holds admin access to Transakt, so leaving it open meant
  anyone on the network could read every merchant's payments.
- **Redis updates happen in one step** (a Lua script), so two scorers can't trip over each other.
- **Every service reports numbers** that Prometheus collects and Grafana draws.
- **Analysts teach the model.** Their confirm/dismiss decisions become training labels.
- **Other agents can ask Sentinel for help** over A2A — the agent-to-agent protocol.

**Counter-questions**
1. Why does transakt-mcp refuse to *start* without a token, instead of starting and warning?
2. Two scorers, one merchant, a naive read-then-write. Walk through how 200 payments become a count of 2.
3. Why is `/metrics` left open on the MCP server when everything else needs the token?
4. An analyst dismisses a real fraud case by mistake, twice. What happens on the next retrain, and what
   safeguard would you add?
5. MCP and A2A both look like "JSON over HTTP". What's the actual difference, and where does each appear here?
6. The A2A caller gets back `HOLD_PAYOUT`. Has anything been held? Who decides?
