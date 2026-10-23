"""
How fast is the scoring hot path?

    python scripts/bench_scorer.py                       # in-memory history
    REDIS_URL=redis://localhost:6379/14 python scripts/bench_scorer.py   # + real Redis

Pushes N synthetic settled-payment events through the real Scorer.handle() —
features, model, SQLite write, reason codes, everything except Kafka itself —
and reports throughput and per-payment latency (in a batch, a payment's
latency is the batch time divided by its size). One process, one core.

Run it with the risk-scorer's requirements installed.
"""

import argparse
import asyncio
import os
import random
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "services", "risk-scorer"))

from app.features import MemoryState, RedisState  # noqa: E402
from app.model import RiskModel  # noqa: E402
from app.scoring import Scorer  # noqa: E402
from app.store import ScoreStore  # noqa: E402
from app.train import train  # noqa: E402


class Directory:
    def __init__(self, created):
        self.created = created

    async def created_ts(self, merchant_id):
        return self.created.get(merchant_id)


def events(n, merchants, rng):
    medians = {m: rng.randint(200, 3000) * 100 for m in merchants}
    t = 1_761_200_000.0
    for i in range(n):
        m = rng.choice(merchants)
        # ~1 payment per merchant per minute, like the simulator's normal shops. Faster
        # than that and every merchant looks like a velocity burst.
        t += rng.expovariate(len(merchants) / 60)
        yield {"paymentId": f"bench-{i}", "merchantId": m, "status": "CAPTURED", "currency": "INR",
               "amountPaise": int(medians[m] * rng.lognormvariate(0, 0.35))}, t


async def run(label, state, model, n, merchants, batch):
    tmp = tempfile.mkdtemp()
    scorer = Scorer(model, ScoreStore(os.path.join(tmp, "scores.db")), Directory({m: 0 for m in merchants}), state)
    rng = random.Random(1)
    lat = []
    started = time.perf_counter()
    flagged = 0
    all_events = list(events(n, merchants, rng))
    for k in range(0, n, batch):
        chunk = all_events[k:k + batch]
        t0 = time.perf_counter()
        flagged += len(await scorer.handle_many(chunk))
        per = (time.perf_counter() - t0) * 1000 / len(chunk)
        lat.extend([per] * len(chunk))
    total = time.perf_counter() - started
    lat.sort()
    q = lambda p: lat[min(len(lat) - 1, int(p * len(lat)))]
    return {"backend": label, "batch": batch, "payments": n, "per_second": round(n / total),
            "p50_ms": round(q(0.50), 3), "p99_ms": round(q(0.99), 3), "max_ms": round(lat[-1], 2),
            "flag_rate": f"{100 * flagged / n:.1f}%"}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=20_000)
    ap.add_argument("--merchants", type=int, default=500)
    ap.add_argument("--batch", type=int, nargs="+", default=[1, 200],
                    help="batch sizes to compare (the consumer polls up to MAX_BATCH=200)")
    args = ap.parse_args()
    print("training the model…", flush=True)
    bundle = train()
    bundle["source"] = "bench"
    model = RiskModel(bundle)

    # The model alone, so the rest of the per-payment cost can be read off by subtraction.
    row = {"count_2m": 2, "count_10m": 4, "history_count": 40, "amount_ratio": 1.1,
           "log_amount": 5.0, "merchant_age_min": 9000.0, "failed_ratio_10m": 0.0}
    t0 = time.perf_counter()
    for _ in range(2000):
        model.score(row)
    model_ms = (time.perf_counter() - t0) / 2000 * 1000

    merchants = [f"m{i:04d}" for i in range(args.merchants)]
    results = []
    url = os.environ.get("REDIS_URL")
    for batch in args.batch:
        results.append(await run("memory", MemoryState(), model, args.n, merchants, batch))
        if url:
            import redis.asyncio as aioredis
            r = aioredis.from_url(url)
            await r.flushdb()
            results.append(await run("redis", RedisState(r, prefix=f"bench{batch}"), model, args.n, merchants, batch))
            await r.flushdb()
            await r.aclose()

    print(f"\nmodel predict alone: {model_ms:.3f} ms per payment\n")
    cols = ["backend", "batch", "payments", "per_second", "p50_ms", "p99_ms", "max_ms", "flag_rate"]
    print("| " + " | ".join(cols) + " |\n|" + "---|" * len(cols))
    for res in results:
        print("| " + " | ".join(str(res[c]) for c in cols) + " |")


if __name__ == "__main__":
    asyncio.run(main())
