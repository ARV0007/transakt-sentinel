"""The Redis feature state: same features as memory, survives restarts, ignores replays."""

import fakeredis
import pytest

from app.features import FeatureBuilder, MemoryState, RedisState


def redis_state(server):
    return RedisState(fakeredis.FakeAsyncRedis(server=server))


TRAFFIC = [("m1", 1000 + i * 50, 40_000 + i * 1000, "FAILED" if i == 3 else "CAPTURED") for i in range(8)] + \
          [("m1", 1500 + i * 3, 41_000, "CAPTURED") for i in range(10)] + \
          [("m2", 1100, 900_000, "CAPTURED"), ("m2", 1150, 950_000, "CAPTURED")]


@pytest.mark.parametrize("make", ["memory", "redis"])
async def test_backends_produce_identical_features(make):
    mem = FeatureBuilder(MemoryState())
    other = FeatureBuilder(MemoryState() if make == "memory" else redis_state(fakeredis.FakeServer()))
    for i, (m, ts, amt, st) in enumerate(TRAFFIC):
        a = await mem.compute(m, ts, amt, st, payment_id=f"p{i}")
        b = await other.compute(m, ts, amt, st, payment_id=f"p{i}")
        assert {k: round(v, 6) for k, v in a.items() if v == v} == {k: round(v, 6) for k, v in b.items() if v == v}


async def test_history_survives_a_restart():
    server = fakeredis.FakeServer()                    # Redis outlives the scorer process
    before = FeatureBuilder(redis_state(server))
    for i in range(6):
        await before.compute("m", 1000 + i * 60, 50_000, "CAPTURED", payment_id=f"p{i}")
    after = FeatureBuilder(redis_state(server))        # a brand-new process
    f = await after.compute("m", 1400, 2_500_000, "CAPTURED", payment_id="spike")
    assert f["history_count"] == 6 and f["amount_ratio"] == 50

    # The memory backend is exactly the problem this fixes:
    forgot = await FeatureBuilder(MemoryState()).compute("m", 1400, 2_500_000, "CAPTURED", payment_id="spike")
    assert forgot["history_count"] == 0 and forgot["amount_ratio"] != forgot["amount_ratio"]   # NaN


@pytest.mark.parametrize("make", ["memory", "redis"])
async def test_a_replayed_payment_is_not_compared_with_itself(make):
    fb = FeatureBuilder(MemoryState() if make == "memory" else redis_state(fakeredis.FakeServer()))
    for i in range(4):
        await fb.compute("m", 1000 + i * 60, 50_000, "CAPTURED", payment_id=f"p{i}")
    first = await fb.compute("m", 1300, 2_000_000, "CAPTURED", payment_id="x")
    # crash before the score was saved -> Kafka redelivers the same payment
    again = await fb.compute("m", 1300, 2_000_000, "CAPTURED", payment_id="x")
    assert first == again
    nxt = await fb.compute("m", 1360, 50_000, "CAPTURED", payment_id="y")
    assert nxt["history_count"] == 5                    # the replay counted once, not twice


async def test_old_events_expire_from_the_window():
    fb = FeatureBuilder(redis_state(fakeredis.FakeServer()))
    await fb.compute("m", 0, 50_000, "CAPTURED", payment_id="old")
    f = await fb.compute("m", 25 * 3600, 50_000, "CAPTURED", payment_id="new")
    assert f["count_10m"] == 1 and f["history_count"] == 1   # all-time count remembers; the window doesn't
