"""Another agent talking to Sentinel over A2A (JSON-RPC, protocol v1.0)."""

import pytest
from fastapi.testclient import TestClient

from app.main import app

from .fakes import FakeTools

PID = "11111111-2222-3333-4444-555555555555"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("A2A_TOKEN", "agent-secret")
    for k in ("KAFKA_BOOTSTRAP_SERVERS", "LLM_API_KEY", "HF_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    app.state.tools_override = FakeTools()
    with TestClient(app) as c:
        yield c
    app.state.tools_override = None


def send(client, parts, token="agent-secret", msg_id="m1"):
    return client.post("/a2a", headers={"Authorization": f"Bearer {token}", "A2A-Version": "1.0"}, json={
        "jsonrpc": "2.0", "id": 1, "method": "SendMessage",
        "params": {"message": {"messageId": msg_id, "role": "ROLE_USER", "parts": parts}}})


def test_agent_card_is_public_and_describes_the_skill(client):
    card = client.get("/.well-known/agent-card.json").json()
    assert card["name"] == "Transakt Sentinel"
    assert card["skills"][0]["id"] == "investigate_payment"
    assert card["supportedInterfaces"][0]["url"].endswith("/a2a")
    assert "bearer" in card["securitySchemes"]


def test_rpc_needs_the_token(client):
    assert send(client, [{"text": f"investigate {PID}"}], token="wrong").status_code == 401


def test_another_agent_gets_a_recommendation_and_a_human_still_decides(client):
    r = send(client, [{"data": {"payment_id": PID}}])
    assert r.status_code == 200, r.text
    task = r.json()["result"]["task"]
    assert task["status"]["state"] == "TASK_STATE_COMPLETED"
    result = task["artifacts"][0]["parts"][0]["data"]
    assert result["recommendation"] == "HOLD_PAYOUT"                  # the "burst" merchant
    assert result["case_status"] == "AWAITING_REVIEW"                  # waiting for a person
    assert result["decided_by_human"] is None
    # the same case shows up for the analyst
    assert client.get(f"/cases/{result['case_id']}").json()["trigger"]["reasons"] == ["EXTERNAL_REQUEST"]


def test_text_with_a_payment_id_works_too_and_reuses_the_open_case(client):
    first = send(client, [{"data": {"payment_id": PID}}], msg_id="a").json()["result"]["task"]
    again = send(client, [{"text": f"Please look at payment {PID}"}], msg_id="b").json()["result"]["task"]
    case = lambda t: t["artifacts"][0]["parts"][0]["data"]["case_id"]
    assert case(first) == case(again)


def test_no_payment_id_is_rejected_politely(client):
    task = send(client, [{"text": "is this merchant ok?"}]).json()["result"]["task"]
    assert task["status"]["state"] == "TASK_STATE_REJECTED"


def test_unknown_payment_fails_cleanly(client):
    task = send(client, [{"data": {"payment_id": "00000000-0000-0000-0000-000000000000"}}]).json()["result"]["task"]
    assert task["status"]["state"] == "TASK_STATE_FAILED"
