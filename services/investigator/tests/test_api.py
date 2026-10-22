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


def test_metrics_reflect_the_case_lifecycle(data_dir):
    def value(text, name):
        for line in text.splitlines():
            if line.startswith(name + " ") or line.startswith(name + "{"):
                return float(line.rsplit(" ", 1)[1])
        return 0.0

    with TestClient(app) as client:
        before = client.get("/metrics").text
        cid = client.post("/cases", json=case_msg("burst", "m1")).json()["case_id"]
        client.post("/cases", json=case_msg("burst", "m2"))
        wait_for(client, cid, "AWAITING_REVIEW")
        client.post(f"/cases/{cid}/decision", json={"decision": "CONFIRM_FRAUD"})
        after = client.get("/metrics").text
        assert value(after, "sentinel_cases_opened_total") - value(before, "sentinel_cases_opened_total") == 1
        assert value(after, "sentinel_case_flags_attached_total") - value(before, "sentinel_case_flags_attached_total") == 1
        assert 'sentinel_decisions_total{agreed="true",decision="CONFIRM_FRAUD"}' in after
        assert 'sentinel_cases{status="CLOSED"} 1.0' in after


def test_feedback_exports_decisions_as_labels(data_dir):
    feats = {"count_2m": 12, "count_10m": 14, "history_count": 30, "amount_ratio": 1.1,
             "log_amount": 4.9, "merchant_age_min": None, "failed_ratio_10m": 0.0}
    with TestClient(app) as client:
        a = client.post("/cases", json={**case_msg("burst", "f1"), "features": feats}).json()["case_id"]
        b = client.post("/cases", json={**case_msg("calm", "f2", reasons=["MODEL_ONLY"]), "features": feats}).json()["case_id"]
        client.post("/cases", json={**case_msg("spike", "f3", reasons=["AMOUNT_SPIKE"]), "features": feats})  # left undecided
        for cid in (a, b):
            wait_for(client, cid, "AWAITING_REVIEW")
        client.post(f"/cases/{a}/decision", json={"decision": "CONFIRM_FRAUD"})
        client.post(f"/cases/{b}/decision", json={"decision": "DISMISS"})
        rows = {r["payment_id"]: r for r in client.get("/feedback").json()}
        assert set(rows) == {"f1", "f2"}                     # undecided cases are not labels
        assert rows["f1"]["label"] == 1 and rows["f2"]["label"] == 0
        assert rows["f1"]["features"]["count_2m"] == 12
