"""
The real LangChain client (ChatOpenAI) talking real HTTP to an OpenAI-compatible
server running on localhost. FakeLLM in the other tests skips the client
entirely; these check the request we actually send to Hugging Face's router and
that real-world replies (code fences, chatter, errors) are handled.

A real server rather than a mocking library on purpose: the OpenAI SDK has
switched HTTP libraries before (v3 uses httpx2), and a socket doesn't care.
"""

import json
import socket
import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.llm import build_llm, synthesize

# One event loop for the whole file, like the real service. langchain-openai caches
# its HTTP client, and a cached client can't outlive the loop it was created on.
pytestmark = pytest.mark.asyncio(loop_scope="module")

FINDINGS = {"velocity_analyst": {"signal": True, "summary": "30 payments in 2 minutes"}}
FACTS = {"payment": {"amount_paise": 80_000, "amount_rupees": 800.0}, "risk_score": 0.98,
         "findings": {"velocity_analyst": {"facts": {"max_in_any_2_minutes": 30, "payments_30m": 31}}}}


class Provider:
    """What the fake provider will answer next, and every request it received."""
    def __init__(self):
        self.reply, self.status, self.requests = "{}", 200, []


def completion(content):
    return {"id": "chatcmpl-1", "object": "chat.completion", "created": 1761200000,
            "model": "meta-llama/Llama-3.1-8B-Instruct",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": content}}],
            "usage": {"prompt_tokens": 320, "completion_tokens": 60, "total_tokens": 380}}


@pytest.fixture(scope="module")
def provider():
    state, api = Provider(), FastAPI()

    @api.post("/v1/chat/completions")
    async def chat(req: Request):
        state.requests.append({"headers": dict(req.headers), "body": await req.json()})
        if state.status != 200:
            return JSONResponse({"error": {"message": "provider error"}}, status_code=state.status)
        return completion(state.reply)

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(api, host="127.0.0.1", port=port, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)
    state.url = f"http://127.0.0.1:{port}/v1"
    yield state
    server.should_exit = True


@pytest.fixture
def llm(provider, monkeypatch):
    provider.status, provider.requests = 200, []
    monkeypatch.setenv("LLM_API_KEY", "hf_test_key")
    monkeypatch.setenv("LLM_BASE_URL", provider.url)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")      # never route a local call via a proxy
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    return build_llm()


async def test_request_shape_sent_to_the_provider(llm, provider):
    provider.reply = '{"action":"HOLD_PAYOUT","confidence":0.9,"rationale":"30 payments inside 2 minutes, 31 in 30 minutes."}'
    rec = await synthesize(llm, FACTS, FINDINGS)
    req = provider.requests[-1]
    body = req["body"]
    assert req["headers"]["authorization"] == "Bearer hf_test_key"
    assert body["model"] == "meta-llama/Llama-3.1-8B-Instruct" and body["temperature"] == 0
    assert [m["role"] for m in body["messages"]] == ["system", "user"]
    assert "Use ONLY numbers that appear in the facts" in body["messages"][0]["content"]
    assert json.loads(body["messages"][1]["content"])["risk_score"] == 0.98      # the facts, as JSON
    assert rec["source"] == "llm" and rec["action"] == "HOLD_PAYOUT" and rec["grounding"]["ok"]


async def test_reply_wrapped_in_a_code_fence_and_chatter_still_parses(llm, provider):
    provider.reply = ('Here is my assessment:\n```json\n{"action": "HOLD_PAYOUT", "confidence": 0.85, '
                      '"rationale": "Burst of 30 payments in 2 minutes."}\n```\nLet me know if you need more.')
    rec = await synthesize(llm, FACTS, FINDINGS)
    assert rec["source"] == "llm" and rec["confidence"] == 0.85


async def test_invented_numbers_in_a_real_reply_are_flagged(llm, provider):
    provider.reply = '{"action":"HOLD_PAYOUT","confidence":0.95,"rationale":"30 payments in 2 minutes and 14 chargebacks last week."}'
    rec = await synthesize(llm, FACTS, FINDINGS)
    assert rec["grounding"] == {"ok": False, "ungrounded_numbers": ["14"]}


@pytest.mark.parametrize("status", [401, 429, 503])     # bad key, rate limited, model still loading
async def test_provider_errors_fall_back_to_rules(llm, provider, status):
    provider.status = status
    rec = await synthesize(llm, FACTS, FINDINGS)
    assert rec["source"] == "rules (llm failed)" and rec["action"] == "HOLD_PAYOUT"


async def test_model_is_configurable(llm, provider, monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "llama-3.1-8b-instant")       # e.g. Groq, by changing two env vars
    provider.reply = '{"action":"ALLOW","confidence":0.6,"rationale":"No pattern."}'
    rec = await synthesize(build_llm(), FACTS, {"velocity_analyst": {"signal": False, "summary": "calm"}})
    assert provider.requests[-1]["body"]["model"] == "llama-3.1-8b-instant"
    assert rec["action"] == "ALLOW" and rec["rules_would_say"] == "ALLOW"
