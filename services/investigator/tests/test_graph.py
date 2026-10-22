from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from app.graph import build_graph
from app.llm import _parse, grounding_check, rule_recommendation, synthesize

from .fakes import FakeLLM, FakeTools


def state(merchant, reasons, amount=80_000):
    return {"case_id": f"c-{merchant}", "merchant_id": merchant, "score": 0.97, "reasons": reasons,
            "payment": {"id": "p1", "amount_paise": amount, "currency": "INR", "status": "CAPTURED"}}


def cfg(name):
    return {"configurable": {"thread_id": name}}


# ---------------------------------------------------------------- graph

async def test_velocity_case_pauses_for_a_human_with_a_hold_recommendation():
    tools = FakeTools()
    g = build_graph(tools, None, InMemorySaver())
    out = await g.ainvoke(state("burst", ["VELOCITY"]), cfg("a"))
    assert "__interrupt__" in out                                  # paused, waiting for a person
    snap = await g.aget_state(cfg("a"))
    assert snap.next == ("human_review",)
    rec = snap.values["recommendation"]
    assert rec["action"] == "HOLD_PAYOUT" and rec["source"] == "rules"
    assert snap.values["findings"]["velocity_analyst"]["signal"] is True
    # supervisor sent only the specialists the reason called for (+ the profile)
    assert set(snap.values["findings"]) == {"velocity_analyst", "merchant_profile_analyst"}


async def test_resume_records_the_analysts_decision():
    g = build_graph(FakeTools(), None, InMemorySaver())
    await g.ainvoke(state("burst", ["VELOCITY"]), cfg("b"))
    out = await g.ainvoke(Command(resume={"decision": "DISMISS", "note": "promo day", "analyst": "aman"}), cfg("b"))
    assert out["decision"]["decision"] == "DISMISS"
    assert out["decision"]["agreed_with_recommendation"] is False   # overrides are worth tracking
    assert (await g.aget_state(cfg("b"))).next == ()


async def test_amount_spike_routes_to_the_amount_analyst():
    g = build_graph(FakeTools(), None, InMemorySaver())
    await g.ainvoke(state("spike", ["AMOUNT_SPIKE"], amount=60_000 * 45), cfg("c"))
    v = (await g.aget_state(cfg("c"))).values
    f = v["findings"]["amount_analyst"]
    assert f["signal"] is True and f["facts"]["ratio_to_median"] == 45.0
    assert "velocity_analyst" not in v["findings"]


async def test_model_only_flag_on_a_calm_merchant_is_recommended_allow():
    g = build_graph(FakeTools(), None, InMemorySaver())
    await g.ainvoke(state("calm", ["MODEL_ONLY"], amount=150_000), cfg("d"))
    v = (await g.aget_state(cfg("d"))).values
    assert set(v["findings"]) == {"velocity_analyst", "amount_analyst", "merchant_profile_analyst"}
    assert v["recommendation"]["action"] == "ALLOW"


async def test_one_broken_tool_degrades_instead_of_failing():
    g = build_graph(FakeTools(broken={"merchant_amount_profile"}), None, InMemorySaver())
    await g.ainvoke(state("spike", ["AMOUNT_SPIKE"]), cfg("e"))
    v = (await g.aget_state(cfg("e"))).values
    assert "error" in v["findings"]["amount_analyst"]
    assert "error" not in v["findings"]["merchant_profile_analyst"]


async def test_no_evidence_at_all_goes_to_a_human_not_to_allow():
    g = build_graph(FakeTools(broken={"*"}), None, InMemorySaver())
    await g.ainvoke(state("burst", ["VELOCITY"]), cfg("f"))
    rec = (await g.aget_state(cfg("f"))).values["recommendation"]
    assert rec["action"] == "NEEDS_MANUAL_REVIEW" and rec["confidence"] == 0.0


async def test_invalid_decision_is_rejected():
    g = build_graph(FakeTools(), None, InMemorySaver())
    await g.ainvoke(state("burst", ["VELOCITY"]), cfg("g"))
    try:
        await g.ainvoke(Command(resume={"decision": "MAYBE"}), cfg("g"))
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    assert (await g.aget_state(cfg("g"))).next == ("human_review",)   # still waiting


# ---------------------------------------------------------------- the LLM step

FACTS = {"payment": {"amount_paise": 80_000, "amount_rupees": 800.0},
         "findings": {"velocity_analyst": {"facts": {"max_in_any_2_minutes": 30, "payments_30m": 31}}}}


def test_grounding_accepts_numbers_from_the_facts_and_catches_invented_ones():
    assert grounding_check("30 payments in 2 minutes, Rs 800 each, 31 in total.", FACTS)["ok"]
    bad = grounding_check("47 payments in 2 minutes totalling Rs 9,999.", FACTS)
    assert not bad["ok"] and set(bad["ungrounded_numbers"]) == {"47", "9,999"}


def test_parse_validates_the_action():
    assert _parse('Sure! {"action":"hold_payout","confidence":1.4,"rationale":"x"}')["confidence"] == 1.0
    for bad in ("no json here", '{"action":"BLOCK_CARD","confidence":0.5}'):
        try:
            _parse(bad)
            raise AssertionError("should have failed")
        except ValueError:
            pass


async def test_llm_answer_is_used_and_checked():
    llm = FakeLLM('{"action":"HOLD_PAYOUT","confidence":0.9,"rationale":"30 payments in 2 minutes; 99 declined."}')
    rec = await synthesize(llm, FACTS, {"velocity_analyst": {"signal": True, "summary": "burst"}})
    assert rec["source"] == "llm" and rec["action"] == "HOLD_PAYOUT"
    assert rec["grounding"] == {"ok": False, "ungrounded_numbers": ["99"]}
    assert rec["rules_would_say"] == "HOLD_PAYOUT"


async def test_broken_llm_falls_back_to_rules():
    for llm in (FakeLLM(error=TimeoutError("provider down")), FakeLLM("I think it is fine")):
        rec = await synthesize(llm, FACTS, {"velocity_analyst": {"signal": True, "summary": "burst"}})
        assert rec["source"] == "rules (llm failed)" and rec["action"] == "HOLD_PAYOUT"


def test_rule_recommendation_confidence_grows_with_signals():
    one = rule_recommendation({"a": {"signal": True, "summary": "x"}, "b": {"signal": False, "summary": "y"}})
    two = rule_recommendation({"a": {"signal": True, "summary": "x"}, "b": {"signal": True, "summary": "y"}})
    assert one["confidence"] < two["confidence"] <= 0.95
