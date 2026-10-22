"""
The one place the system pays for an LLM: turning the specialists' findings
into a recommendation a human can read in ten seconds.

Provider: any OpenAI-compatible chat endpoint. The default is Hugging Face's
Inference Providers router, so an HF token is all you need; Groq, OpenRouter,
OpenAI or a local Ollama work by changing LLM_BASE_URL and LLM_MODEL.

Three safety nets, because an LLM is the least reliable part of the system:
  1. No API key -> the deterministic rules write the recommendation instead.
  2. The LLM's answer is parsed and validated; anything malformed falls back
     to the rules too. A broken model never blocks a case.
  3. A grounding check: every number in the rationale must appear in the facts
     the LLM was given. Invented numbers are flagged to the analyst, not hidden.
"""

import json
import logging
import os
import re

log = logging.getLogger("investigator.llm")

ACTIONS = ("HOLD_PAYOUT", "ALLOW", "NEEDS_MANUAL_REVIEW")

SYSTEM = """You are a payments fraud analyst at an Indian payment gateway.
You receive one flagged payment, the risk model's score, and findings from
specialist checks. Recommend exactly one action:
- HOLD_PAYOUT: evidence of merchant fraud; hold the merchant's settlement.
- ALLOW: the flag looks like a false positive.
- NEEDS_MANUAL_REVIEW: the evidence is missing or contradictory.

Rules:
- Use ONLY numbers that appear in the facts. Do not estimate or invent figures.
- Amounts in the facts are given in paise and in rupees; quote rupees.
- Keep the rationale under 90 words and cite the specific facts you relied on.

Reply with ONLY a JSON object:
{"action": "...", "confidence": 0.0-1.0, "rationale": "..."}"""


def build_llm():
    key = os.environ.get("LLM_API_KEY") or os.environ.get("HF_TOKEN")
    if not key:
        log.warning("No LLM_API_KEY/HF_TOKEN set; recommendations will come from rules only")
        return None
    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model=os.environ.get("LLM_MODEL", "meta-llama/Llama-3.1-8B-Instruct"),
        base_url=os.environ.get("LLM_BASE_URL", "https://router.huggingface.co/v1"),
        api_key=key,
        temperature=0,
        timeout=60,
        max_retries=1,
    )


# ---------------------------------------------------------------- rules

def rule_recommendation(findings):
    """The fallback, and also the baseline the LLM is compared against."""
    ok = {k: v for k, v in findings.items() if "error" not in v}
    if not ok:
        # Unknown is not safe. The rate limiter fails OPEN to protect
        # availability; an investigation with no evidence fails to a HUMAN,
        # because quietly allowing it would be the costly mistake.
        return {"action": "NEEDS_MANUAL_REVIEW", "confidence": 0.0,
                "rationale": "No specialist could gather evidence (data source unavailable)."}
    signals = [k for k, v in ok.items() if v.get("signal")]
    if signals:
        parts = "; ".join(ok[k]["summary"] for k in signals)
        return {"action": "HOLD_PAYOUT", "confidence": round(min(0.95, 0.6 + 0.15 * len(signals)), 2),
                "rationale": f"Fraud signals from {', '.join(signals)}: {parts}"}
    return {"action": "ALLOW", "confidence": 0.6,
            "rationale": "Model flagged the payment but no specialist found a supporting pattern."}


# ---------------------------------------------------------------- grounding

_NUM = re.compile(r"(?<![\w.])\d[\d,]*(?:\.\d+)?")


def _numbers_in(obj, out):
    if isinstance(obj, bool) or obj is None:
        return out
    if isinstance(obj, (int, float)):
        for v in (obj, obj / 100):                # paise and rupees
            for nd in (0, 1, 2):
                out.add(round(float(v), nd))
        if isinstance(obj, float) and obj <= 1:   # 0.983 may be quoted as 98.3%
            for nd in (0, 1):
                out.add(round(obj * 100, nd))
    elif isinstance(obj, dict):
        for v in obj.values():
            _numbers_in(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _numbers_in(v, out)
    elif isinstance(obj, str):
        for m in _NUM.findall(obj):
            _numbers_in(float(m.replace(",", "")), out)
    return out


def grounding_check(rationale, facts):
    known = _numbers_in(facts, set())
    ungrounded = []
    for m in _NUM.findall(rationale or ""):
        n = float(m.replace(",", ""))
        if n <= 2:                                 # "1 payment", "2 minutes"
            continue
        if not any(round(n, nd) in known for nd in (0, 1, 2)):
            ungrounded.append(m)
    return {"ok": not ungrounded, "ungrounded_numbers": ungrounded}


# ---------------------------------------------------------------- the call

def _parse(text):
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("no JSON object in reply")
    data = json.loads(text[start:end + 1])
    action = str(data.get("action", "")).upper()
    if action not in ACTIONS:
        raise ValueError(f"unknown action {action!r}")
    conf = float(data.get("confidence", 0.5))
    return {"action": action, "confidence": round(min(max(conf, 0.0), 1.0), 2),
            "rationale": str(data.get("rationale", "")).strip()[:1200]}


async def synthesize(llm, facts, findings):
    rules = rule_recommendation(findings)
    if llm is None:
        return {**rules, "source": "rules", "grounding": {"ok": True, "ungrounded_numbers": []}}
    try:
        reply = await llm.ainvoke(
            [("system", SYSTEM), ("human", json.dumps(facts, indent=1, default=str))],
            config={"run_name": "synthesize_recommendation"})
        rec = _parse(reply.content if hasattr(reply, "content") else str(reply))
    except Exception as e:
        log.warning("LLM synthesis failed (%s); using rules", e)
        return {**rules, "source": "rules (llm failed)", "llm_error": str(e)[:300],
                "grounding": {"ok": True, "ungrounded_numbers": []}}
    rec["source"] = "llm"
    rec["grounding"] = grounding_check(rec["rationale"], facts)
    rec["rules_would_say"] = rules["action"]      # disagreement is worth seeing
    return rec
