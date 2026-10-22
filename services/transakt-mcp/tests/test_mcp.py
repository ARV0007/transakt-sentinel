from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from app.analytics import activity_summary, amount_profile, max_in_sliding_window
from app.transakt_client import TransaktClient

BASE = "http://transakt.test"
NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def pay(i, minutes_ago, merchant="m1", amount=50_000, status="CAPTURED"):
    ts = (NOW - timedelta(minutes=minutes_ago)).isoformat().replace("+00:00", "Z")
    return {"id": f"p{i}", "merchantId": merchant, "amountPaise": amount,
            "currency": "INR", "status": status, "createdAt": ts}


# ---------------------------------------------------------------- analytics

def test_sliding_window_finds_the_burst_not_the_average():
    spread = [i * 300 for i in range(10)]          # one every 5 minutes
    burst = [3000 + i * 3 for i in range(20)]      # 20 inside a minute
    assert max_in_sliding_window(spread, 120) == 1
    assert max_in_sliding_window(spread + burst, 120) == 20


def test_activity_summary_counts_failures():
    rows = [pay(1, 1), pay(2, 2, status="FAILED"), pay(3, 3)]
    s = activity_summary(rows, 30)
    assert s["payments"] == 3 and s["failed"] == 1 and s["failed_ratio"] == 0.333
    assert s["max_in_any_2_minutes"] == 3


def test_amount_profile_excludes_the_suspect_payment():
    rows = [pay(i, i, amount=50_000) for i in range(9)] + [pay(99, 1, amount=5_000_000)]
    with_suspect = amount_profile(rows)
    without = amount_profile(rows, exclude_payment_id="p99")
    assert without["max_paise"] == 50_000 and without["median_paise"] == 50_000
    assert with_suspect["max_paise"] == 5_000_000


def test_amount_profile_empty():
    assert amount_profile([])["median_paise"] is None


# ---------------------------------------------------------------- client

def page(rows, n=0):
    return {"content": rows, "page": {"size": 100, "number": n,
                                      "totalElements": len(rows), "totalPages": 1}}


@respx.mock
async def test_login_then_reuse_token():
    login = respx.post(f"{BASE}/api/v1/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "t1", "merchantId": "a", "email": "a"}))
    respx.get(f"{BASE}/api/v1/payments/p1").mock(return_value=httpx.Response(200, json=pay(1, 0)))
    c = TransaktClient(BASE, "a@x", "pw")
    await c.get_payment("p1")
    await c.get_payment("p1")
    assert login.call_count == 1


@respx.mock
async def test_expired_token_empty_403_triggers_one_relogin():
    # Transakt answers an expired JWT with an empty 403, not a 401.
    respx.post(f"{BASE}/api/v1/auth/login").mock(
        side_effect=[httpx.Response(200, json={"token": "old"}),
                     httpx.Response(200, json={"token": "new"})])
    route = respx.get(f"{BASE}/api/v1/merchants/m1")
    route.side_effect = lambda req: (
        httpx.Response(200, json={"id": "m1"}) if req.headers["Authorization"] == "Bearer new"
        else httpx.Response(403))
    c = TransaktClient(BASE, "a@x", "pw")
    assert (await c.get_merchant("m1"))["id"] == "m1"


@respx.mock
async def test_404_is_none_not_an_exception():
    respx.post(f"{BASE}/api/v1/auth/login").mock(return_value=httpx.Response(200, json={"token": "t"}))
    respx.get(f"{BASE}/api/v1/payments/nope").mock(return_value=httpx.Response(404))
    assert await TransaktClient(BASE, "a", "b").get_payment("nope") is None


@respx.mock
async def test_payments_since_stops_at_window_edge_and_filters_merchant():
    respx.post(f"{BASE}/api/v1/auth/login").mock(return_value=httpx.Response(200, json={"token": "t"}))
    first = [pay(i, i, merchant="m1" if i % 2 else "m2") for i in range(100)]   # 0..99 min ago
    second = [pay(100 + i, 100 + i) for i in range(100)]                        # older
    pages = respx.get(f"{BASE}/api/v1/payments").mock(
        side_effect=[httpx.Response(200, json=page(first, 0)),
                     httpx.Response(200, json=page(second, 1))])
    c = TransaktClient(BASE, "a", "b")
    rows = await c.payments_since(NOW - timedelta(minutes=30, seconds=30), "m1")
    assert {r["merchantId"] for r in rows} == {"m1"}
    assert len(rows) == 15                       # odd minutes 1..29
    assert pages.call_count == 1                 # never fetched the older page


@respx.mock
async def test_failed_login_is_a_clear_error():
    respx.post(f"{BASE}/api/v1/auth/login").mock(return_value=httpx.Response(401))
    with pytest.raises(Exception, match="bootstrap"):
        await TransaktClient(BASE, "a", "b").get_payment("p1")


# ---------------------------------------------------------------- auth

from starlette.testclient import TestClient

from app.server import build_app


def test_requests_without_the_token_are_refused():
    with TestClient(build_app("s3cret")) as c:
        assert c.get("/health").status_code == 200                          # health stays open
        assert c.post("/mcp", json={}).status_code == 401
        assert c.post("/mcp", json={}, headers={"Authorization": "Bearer wrong"}).status_code == 401
        r = c.post("/mcp", headers={"Authorization": "Bearer s3cret", "Accept": "application/json, text/event-stream",
                                    "Content-Type": "application/json"},
                   json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
        assert r.status_code == 200
        names = {t["name"] for t in r.json()["result"]["tools"]}
        assert {"get_payment", "merchant_activity"} <= names


def test_refuses_to_start_without_a_token():
    with pytest.raises(RuntimeError, match="MCP_TOKEN"):
        build_app("")


@respx.mock
def test_tool_calls_are_counted_and_metrics_need_no_token():
    respx.post("http://localhost:8080/api/v1/auth/login").mock(return_value=httpx.Response(200, json={"token": "t"}))
    respx.get("http://localhost:8080/api/v1/payments/p1").mock(return_value=httpx.Response(200, json=pay(1, 0)))
    respx.route(host="testserver").pass_through()
    hdrs = {"Authorization": "Bearer s3cret", "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json"}
    with TestClient(build_app("s3cret")) as c:
        r = c.post("/mcp", headers=hdrs, json={"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                               "params": {"name": "get_payment", "arguments": {"payment_id": "p1"}}})
        assert r.status_code == 200 and '"id": "p1"' in r.json()["result"]["content"][0]["text"]
        m = c.get("/metrics")
        assert m.status_code == 200
        assert 'sentinel_mcp_tool_calls_total{outcome="ok",tool="get_payment"}' in m.text
