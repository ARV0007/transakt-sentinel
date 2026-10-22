import math
import random

import pytest

from app.features import FeatureBuilder, reason_codes
from app.model import RiskModel
from app.scoring import Scorer
from app.store import ScoreStore
from app.train import train


@pytest.fixture(scope="session")
def bundle():
    b = train()
    b["source"] = "test"
    return b


class FixedDirectory:
    """Every merchant signed up at a known time; no network."""
    def __init__(self, created):
        self.created = created

    async def created_ts(self, merchant_id):
        return self.created.get(merchant_id)


def make_scorer(bundle, created=None, tmp_path=None):
    store = ScoreStore(str(tmp_path / "s.db") if tmp_path else ":memory:")
    return Scorer(RiskModel(bundle), store, FixedDirectory(created or {}))


def evt(i, merchant, amount, status="CAPTURED"):
    return {"paymentId": f"{merchant}-{i}", "merchantId": merchant,
            "amountPaise": amount, "currency": "INR", "status": status}


# ---------------------------------------------------------------- features

async def test_features_compare_against_history_before_adding_the_payment():
    fb = FeatureBuilder()
    for t in range(5):
        await fb.compute("m", 1000 + t * 60, 50_000, "CAPTURED")
    f = await fb.compute("m", 1400, 2_500_000, "CAPTURED")
    assert f["amount_ratio"] == 50          # vs the old median, not one it raised itself
    assert f["history_count"] == 5


async def test_ratio_is_unknown_with_too_little_history():
    f = await FeatureBuilder().compute("m", 0, 50_000, "CAPTURED")
    assert math.isnan(f["amount_ratio"]) and f["count_2m"] == 1


def test_reason_codes():
    base = {"count_2m": 1, "amount_ratio": 1.0, "history_count": 50}
    assert reason_codes({**base, "count_2m": 12}, 50_000) == ["VELOCITY"]
    assert reason_codes({**base, "amount_ratio": 40}, 2_000_000) == ["AMOUNT_SPIKE"]
    assert reason_codes({**base, "history_count": 3}, 2_000_000) == ["NEW_MERCHANT_HIGH_VALUE"]
    assert reason_codes(base, 50_000) == ["MODEL_ONLY"]


# ------------------------------------------- the model against simulator-shaped traffic

async def test_normal_shops_are_mostly_left_alone(bundle):
    rng = random.Random(1)
    s = make_scorer(bundle, {f"m{k}": 0 for k in range(20)})
    flagged = total = 0
    for k in range(20):
        median = rng.randint(200, 3000) * 100
        t = 600.0
        for i in range(12):                                 # ~one a minute, like the simulator
            t += rng.expovariate(1 / 60)
            case = await s.handle(evt(i, f"m{k}", int(median * rng.lognormvariate(0, 0.35))), t)
            flagged += case is not None
            total += 1
    assert flagged / total < 0.05, f"{flagged}/{total} normal payments flagged"


async def test_velocity_burst_is_caught_once_it_ramps(bundle):
    s = make_scorer(bundle, {"v": 0})
    for i in range(5):                                       # quiet history
        await s.handle(evt(i, "v", 80_000), 600 + i * 60)
    cases = [await s.handle(evt(100 + i, "v", 80_000), 1000 + i * 2) for i in range(30)]
    hit = [c for c in cases if c]
    assert len(hit) >= 18
    assert all("VELOCITY" in c["reasons"] for c in hit)


async def test_amount_spike_is_caught(bundle):
    s = make_scorer(bundle, {"a": 0})
    for i in range(4):
        await s.handle(evt(i, "a", 60_000), 600 + i * 60)
    case = await s.handle(evt(9, "a", 60_000 * 45), 900)
    assert case and "AMOUNT_SPIKE" in case["reasons"]


async def test_new_merchant_blitz_is_caught(bundle):
    s = make_scorer(bundle, {"b": 1000})
    cases = [await s.handle(evt(i, "b", random.Random(i).randint(15_000, 45_000) * 100), 1010 + i * 10)
             for i in range(12)]
    assert sum(c is not None for c in cases) >= 9
    assert any("NEW_MERCHANT_HIGH_VALUE" in c["reasons"] for c in cases if c)


async def test_redelivered_event_is_a_no_op(bundle, tmp_path):
    s = make_scorer(bundle, {"b": 1000}, tmp_path)
    e = evt(1, "b", 3_000_000)
    first = await s.handle(e, 1010)
    second = await s.handle(e, 1010)
    assert first is not None and second is None
    assert s.store.stats()["scored"] == 1


async def test_case_message_has_no_nan(bundle):
    s = make_scorer(bundle, {})                               # unknown merchant age
    case = await s.handle(evt(1, "x", 4_000_000), 50)
    assert case is None or all(v is None or v == v for v in case["features"].values())
