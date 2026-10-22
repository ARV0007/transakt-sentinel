"""
The investigation, as a LangGraph.

    START -> supervisor -> (fan out, in parallel) specialists -> synthesize
          -> human_review (PAUSES here) -> finalize -> END

* State is the case file every node reads and writes.
* The supervisor reads the scorer's reason codes and decides which specialists
  to send. Routing is a cheap decision, so it is plain code, not an LLM call.
* Specialists run in parallel (LangGraph's Send API) and each one gathers
  evidence through MCP tools. Each writes into its own key of `findings`, and
  the merge reducer combines them, so parallel writes never overwrite each other.
* human_review calls interrupt(): the graph saves a checkpoint and stops. The
  case can wait an hour, the container can restart, and the analyst's decision
  resumes it exactly where it paused.
"""

from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send, interrupt

from . import metrics
from .llm import synthesize
from .toolbox import ToolError

SPECIALISTS = ("velocity_analyst", "amount_analyst", "merchant_profile_analyst")
ROUTES = {
    "VELOCITY": ["velocity_analyst"],
    "AMOUNT_SPIKE": ["amount_analyst"],
    "NEW_MERCHANT_HIGH_VALUE": ["merchant_profile_analyst", "velocity_analyst"],
    "MODEL_ONLY": list(SPECIALISTS),
}
DECISIONS = ("CONFIRM_FRAUD", "DISMISS")


def merge(a, b):
    return {**(a or {}), **(b or {})}


class CaseState(TypedDict, total=False):
    case_id: str
    merchant_id: str
    payment: dict            # id, amount_paise, currency, status
    score: float
    reasons: list
    features: dict
    plan: list
    findings: Annotated[dict, merge]
    recommendation: dict
    decision: dict


def rupees(paise):
    return None if paise is None else round(paise / 100, 2)


# ---------------------------------------------------------------- specialists
# Each returns {"signal": bool, "summary": str, "facts": {...}} — or
# {"error": "..."} when it could not gather evidence. An error is recorded,
# not raised: one broken tool must not sink the whole investigation.


async def velocity_analyst(state, tools):
    m = state["merchant_id"]
    recent = await tools.call("merchant_activity", merchant_id=m, window_minutes=30)
    day = await tools.call("merchant_activity", merchant_id=m, window_minutes=1440)
    peak = recent["max_in_any_2_minutes"]
    signal = peak >= 8
    return {
        "signal": signal,
        "summary": (f"{peak} payments inside a 2-minute stretch; {recent['payments']} in the last 30 min, "
                    f"{day['payments']} in 24 h") + (" — a burst" if signal else " — normal pace"),
        "facts": {"max_in_any_2_minutes": peak, "payments_30m": recent["payments"],
                  "payments_24h": day["payments"], "failed_30m": recent["failed"],
                  "avg_per_minute_24h": day["avg_per_minute"]},
    }


async def amount_analyst(state, tools):
    m, p = state["merchant_id"], state["payment"]
    prof = await tools.call("merchant_amount_profile", merchant_id=m, window_minutes=1440,
                            exclude_payment_id=p["id"])
    med = prof.get("median_paise")
    if not med:
        big = p["amount_paise"] >= 1_000_000
        return {"signal": big,
                "summary": f"no earlier payments to compare with; this one is Rs {rupees(p['amount_paise'])}",
                "facts": {"amount_rupees": rupees(p["amount_paise"]), "baseline_payments": 0}}
    ratio = round(p["amount_paise"] / med, 1)
    signal = ratio >= 10
    return {
        "signal": signal,
        "summary": (f"Rs {rupees(p['amount_paise'])} is {ratio}x this merchant's median of Rs {rupees(med)} "
                    f"over {prof['payments']} payments"),
        "facts": {"amount_rupees": rupees(p["amount_paise"]), "median_rupees": rupees(med),
                  "p95_rupees": rupees(prof.get("p95_paise")), "ratio_to_median": ratio,
                  "baseline_payments": prof["payments"]},
    }


async def merchant_profile_analyst(state, tools):
    m, p = state["merchant_id"], state["payment"]
    prof = await tools.call("get_merchant", merchant_id=m)
    day = await tools.call("merchant_activity", merchant_id=m, window_minutes=1440)
    age = prof.get("account_age_minutes")
    signal = age is not None and age < 60 and p["amount_paise"] >= 1_000_000
    age_txt = "unknown age" if age is None else (f"{age} minutes old" if age < 120 else f"{round(age / 60, 1)} hours old")
    return {
        "signal": signal,
        "summary": (f"account is {age_txt} with {day['payments']} payments in 24 h; "
                    f"this payment is Rs {rupees(p['amount_paise'])}"),
        "facts": {"merchant_name": prof.get("name"), "account_age_minutes": age,
                  "payments_24h": day["payments"], "amount_rupees": rupees(p["amount_paise"])},
    }


ANALYSTS = {"velocity_analyst": velocity_analyst, "amount_analyst": amount_analyst,
            "merchant_profile_analyst": merchant_profile_analyst}


# ---------------------------------------------------------------- graph

def build_graph(tools, llm, checkpointer=None):

    def supervisor(state):
        plan = []
        for reason in state.get("reasons") or ["MODEL_ONLY"]:
            for name in ROUTES.get(reason, SPECIALISTS):
                if name not in plan:
                    plan.append(name)
        if "merchant_profile_analyst" not in plan:     # always worth knowing who this is
            plan.append("merchant_profile_analyst")
        return {"plan": plan}

    def fan_out(state):
        return [Send(name, state) for name in state["plan"]]

    def specialist(name):
        async def run(state):
            try:
                result = await ANALYSTS[name](state, tools)
            except (ToolError, KeyError, TypeError) as e:
                result = {"error": str(e)[:300]}
                metrics.TOOL_ERRORS.labels(specialist=name).inc()
            return {"findings": {name: result}}
        run.__name__ = name
        return run

    async def synthesize_node(state):
        p = state["payment"]
        facts = {
            "payment": {**p, "amount_rupees": rupees(p["amount_paise"])},
            "risk_score": state.get("score"),
            "reason_codes": state.get("reasons"),
            "findings": state.get("findings", {}),
        }
        rec = await synthesize(llm, facts, state.get("findings", {}))
        metrics.RECOMMENDATIONS.labels(action=rec["action"], source=rec["source"]).inc()
        if not rec.get("grounding", {}).get("ok", True):
            metrics.GROUNDING_WARNINGS.inc()
        return {"recommendation": rec}

    def human_review(state):
        answer = interrupt({
            "case_id": state["case_id"],
            "recommendation": state["recommendation"],
            "ask": f"Decide: one of {DECISIONS}",
        })
        decision = str(answer.get("decision", "")).upper()
        if decision not in DECISIONS:
            raise ValueError(f"decision must be one of {DECISIONS}")
        return {"decision": {"decision": decision, "note": answer.get("note", ""),
                             "analyst": answer.get("analyst", "analyst"),
                             "agreed_with_recommendation":
                                 (decision == "CONFIRM_FRAUD") == (state["recommendation"]["action"] == "HOLD_PAYOUT")}}

    def finalize(state):
        # v1 records the decision only. Transakt has no "hold payout" endpoint
        # yet; when it does, this is the one node that would call it — after a
        # human said yes, never before.
        return {}

    g = StateGraph(CaseState)
    g.add_node("supervisor", supervisor)
    for name in SPECIALISTS:
        g.add_node(name, specialist(name))
        g.add_edge(name, "synthesize")
    g.add_node("synthesize", synthesize_node)
    g.add_node("human_review", human_review)
    g.add_node("finalize", finalize)
    g.add_edge(START, "supervisor")
    g.add_conditional_edges("supervisor", fan_out, list(SPECIALISTS))
    g.add_edge("synthesize", "human_review")
    g.add_edge("human_review", "finalize")
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer)
