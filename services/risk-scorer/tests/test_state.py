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


# ---------------------------------------------------------------- real Redis
# Set REDIS_TEST_URL (CI runs a Redis service container) to run these against a
# real server; fakeredis can't show what concurrency does to a real one.

import asyncio
import os

real = pytest.mark.skipif(not os.environ.get("REDIS_TEST_URL"), reason="REDIS_TEST_URL not set")


@real
async def test_two_replicas_hammering_one_merchant_lose_nothing():
    import redis.asyncio as aioredis
    url = os.environ["REDIS_TEST_URL"]
    a, b = aioredis.from_url(url), aioredis.from_url(url)
    await a.flushdb()
    replica_1, replica_2 = RedisState(a, prefix="t"), RedisState(b, prefix="t")
    n = 200
    results = await asyncio.gather(*[
        (replica_1 if i % 2 else replica_2).observe("m", 1000 + i * 0.01, f"p{i}", 50_000, "CAPTURED")
        for i in range(n)])
    counts_before = sorted(total for _, total in results)
    # Atomic read+write: every payment saw a different "count before me", 0..n-1.
    # With a separate read and write, two payments could both see the same count.
    assert counts_before == list(range(n))
    assert int(await a.get("t:count:m")) == n
    await a.aclose()
    await b.aclose()


@real
async def test_real_redis_matches_memory():
    import redis.asyncio as aioredis
    r = aioredis.from_url(os.environ["REDIS_TEST_URL"])
    await r.flushdb()
    mem, red = FeatureBuilder(MemoryState()), FeatureBuilder(RedisState(r, prefix="t2"))
    for i, (m, ts, amt, st) in enumerate(TRAFFIC):
        x = await mem.compute(m, ts, amt, st, payment_id=f"p{i}")
        y = await red.compute(m, ts, amt, st, payment_id=f"p{i}")
        assert {k: round(v, 6) for k, v in x.items() if v == v} == {k: round(v, 6) for k, v in y.items() if v == v}
    await r.aclose()
