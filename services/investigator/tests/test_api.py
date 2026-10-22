import time

import pytest
from fastapi.testclient import TestClient

from app.main import app

from .fakes import FakeTools, case_msg


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("HF_TOKEN", raising=False)
    app.state.tools_override = FakeTools()
    yield tmp_path
    app.state.tools_override = None


def wait_for(client, case_id, status, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        c = client.get(f"/cases/{case_id}").json()
        if c["status"] == status:
            return c
        time.sleep(0.05)
    raise AssertionError(f"{case_id} never reached {status}; last: {c['status']} {c.get('error')}")


def test_full_case_lifecycle(data_dir):
    with TestClient(app) as client:
        r = client.post("/cases", json=case_msg("burst"))
        assert r.status_code == 201 and r.json()["new"] is True
        cid = r.json()["case_id"]

        c = wait_for(client, cid, "AWAITING_REVIEW")
        assert c["recommendation"]["action"] == "HOLD_PAYOUT"
        assert c["findings"]["velocity_analyst"]["signal"] is True

        r = client.post(f"/cases/{cid}/decision", json={"decision": "CONFIRM_FRAUD", "note": "classic burst"})
        assert r.status_code == 200 and r.json()["status"] == "CLOSED"
        c = client.get(f"/cases/{cid}").json()
        assert c["decision"]["decision"] == "CONFIRM_FRAUD" and c["decision"]["agreed_with_recommendation"]

        # deciding twice is a conflict, not a second decision
        assert client.post(f"/cases/{cid}/decision", json={"decision": "DISMISS"}).status_code == 409


def test_flags_from_one_merchant_share_one_case(data_dir):
    with TestClient(app) as client:
        first = client.post("/cases", json=case_msg("burst", "p1")).json()
        again = client.post("/cases", json=case_msg("burst", "p2")).json()
        dup = client.post("/cases", json=case_msg("burst", "p2")).json()     # Kafka redelivery
        other = client.post("/cases", json=case_msg("spike", "p9", reasons=["AMOUNT_SPIKE"])).json()
        assert again == {"case_id": first["case_id"], "new": False}
        assert dup == {"case_id": first["case_id"], "new": False}
        assert other["new"] and other["case_id"] != first["case_id"]
        c = wait_for(client, first["case_id"], "AWAITING_REVIEW")
        assert c["payment_ids"] == ["p1", "p2"]


def test_a_paused_case_survives_a_restart(data_dir):
    with TestClient(app) as client:
        cid = client.post("/cases", json=case_msg("burst")).json()["case_id"]
        wait_for(client, cid, "AWAITING_REVIEW")
    # The process is gone. A brand-new app instance, same data directory:
    with TestClient(app) as client:
        assert client.get(f"/cases/{cid}").json()["status"] == "AWAITING_REVIEW"
        r = client.post(f"/cases/{cid}/decision", json={"decision": "DISMISS"})
        assert r.status_code == 200 and r.json()["status"] == "CLOSED"


def test_bad_decision_value_is_422_and_case_stays_open(data_dir):
    with TestClient(app) as client:
        cid = client.post("/cases", json=case_msg("burst")).json()["case_id"]
        wait_for(client, cid, "AWAITING_REVIEW")
        assert client.post(f"/cases/{cid}/decision", json={"decision": "BLOCK"}).status_code == 422
        assert client.get(f"/cases/{cid}").json()["status"] == "AWAITING_REVIEW"


def test_unknown_case_is_404(data_dir):
    with TestClient(app) as client:
        assert client.get("/cases/nope").status_code == 404
        assert client.post("/cases/nope/decision", json={"decision": "DISMISS"}).status_code == 404


def test_review_page_and_health(data_dir):
    with TestClient(app) as client:
        assert "Sentinel" in client.get("/").text
        h = client.get("/health").json()
        assert h["status"] == "UP" and h["llm"] == "rules-only"
