"""
Traffic simulator for Transakt Sentinel.

Plays a few minutes of realistic payment traffic against a LOCAL Transakt, with
fraud planted on purpose, and writes the true answer for every payment to
simulator/output/labels.csv. That file is the answer key the eval script grades
the fraud system against.

Standard library only, so it runs with a bare `python3.12 simulator/simulate.py`.

Why planted fraud instead of real data: real fraud is rare and unlabelled, and
this Transakt has no history. Planting it means we KNOW which payments are bad,
which is the only way to measure precision and recall honestly.

Never point this at the live Render deployment. Its rate limiter allows 20
requests per minute per merchant, so the velocity burst would hit 429s halfway
through and the "burst" Transakt records would not be the burst we planted.
"""

import argparse
import csv
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

# ---------------------------------------------------------------- HTTP helpers


def http_json(method, url, body=None, headers=None, timeout=15):
    """One JSON request. Returns (status_code, parsed_body_or_None)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else None
        except ValueError:
            return e.code, None


# ---------------------------------------------------------------- data model


@dataclass
class Merchant:
    id: str
    name: str
    api_key: str
    median_paise: int          # this shop's "normal" ticket size
    kind: str                  # "normal" or "new_merchant_blitz"


@dataclass
class Planned:
    at: float                  # seconds after the run starts
    merchant: Merchant
    amount_paise: int
    scenario: str              # "normal" or the fraud pattern's name
    incident: str = ""         # groups the payments of one fraud incident


@dataclass
class Run:
    base_url: str
    rows: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    failures: int = 0


# ---------------------------------------------------------------- merchants


def create_merchant(run, label, kind, rng, median_paise):
    """Signs a merchant up. The API key is returned exactly once, so we keep it."""
    tag = uuid.uuid4().hex[:8]
    status, body = http_json("POST", f"{run.base_url}/api/v1/merchants", {
        "name": f"{label} {tag}",
        "email": f"sim-{tag}@shop.test",
        "password": "sim-password",
        "businessName": f"{label} Pvt Ltd",
    })
    if status != 200 or not body or not body.get("apiKey"):
        raise SystemExit(f"Could not create merchant ({status}): {body}")
    return Merchant(body["id"], body["name"], body["apiKey"], median_paise, kind)


def normal_amount(rng, median_paise):
    """Ticket sizes cluster around a shop's median with a realistic right skew."""
    return max(100, int(median_paise * rng.lognormvariate(0, 0.35)))


# ---------------------------------------------------------------- the schedule


def build_schedule(normal, rng, total_s):
    """
    Everything that will happen, decided up front. A fixed schedule plus a seed
    means two runs with the same seed plant the same fraud at the same moments.
    """
    plan = []

    # 1. Background noise: every normal shop sells something about once a
    #    minute. Not faster: at one every 25 s a normal shop already does ~5
    #    payments in 2 minutes, which is the velocity signal, and "burst" would
    #    stop meaning anything.
    for m in normal:
        t = rng.uniform(0, 30)
        while t < total_s:
            plan.append(Planned(t, m, normal_amount(rng, m.median_paise), "normal"))
            t += rng.expovariate(1 / 60)

    # The first 40% of the run is clean on purpose, so every shop has a history
    # to compare against before anything strange happens.
    shops = rng.sample(normal, 4)

    # 2. Velocity burst: 30 payments in a minute from an established shop.
    for i, m in enumerate(shops[:2]):
        start = total_s * (0.55 + 0.08 * i)
        incident = f"velocity_burst:{m.id}"
        for k in range(30):
            plan.append(Planned(start + k * 2, m, normal_amount(rng, m.median_paise),
                                "velocity_burst", incident))

    # 3. Amount spike: a shop that sells Rs 500 suddenly charges Rs 25,000.
    for i, m in enumerate(shops[2:]):
        start = total_s * (0.70 + 0.06 * i)
        incident = f"amount_spike:{m.id}"
        for k in range(2):
            plan.append(Planned(start + k * 20, m, m.median_paise * rng.randint(30, 60),
                                "amount_spike", incident))

    # Velocity bursts sit on top of the background traffic, so drop the shop's
    # normal payments that overlap the burst window — otherwise a "normal" label
    # would sit inside a fraud incident and muddy the answer key.
    burst_windows = {}
    for p in plan:
        if p.scenario == "velocity_burst":
            lo, hi = burst_windows.get(p.merchant.id, (p.at, p.at))
            burst_windows[p.merchant.id] = (min(lo, p.at), max(hi, p.at))
    plan = [p for p in plan if not (
        p.scenario == "normal" and p.merchant.id in burst_windows
        and burst_windows[p.merchant.id][0] - 5 <= p.at <= burst_windows[p.merchant.id][1] + 5)]

    plan.sort(key=lambda p: p.at)
    return plan


# ---------------------------------------------------------------- sending


def send_payment(run, p):
    status, body = http_json(
        "POST", f"{run.base_url}/api/v1/payments",
        {"amountPaise": p.amount_paise, "currency": "INR"},
        {"X-API-Key": p.merchant.api_key, "Idempotency-Key": str(uuid.uuid4())},
    )
    sent_at = time.time()
    if status != 200 or not body:
        with run.lock:
            run.failures += 1
        print(f"  ! payment failed ({status}) for {p.merchant.name}: {body}")
        return
    row = {
        "payment_id": body["id"],
        "merchant_id": p.merchant.id,
        "amount_paise": p.amount_paise,
        "status": body.get("status"),
        "scenario": p.scenario,
        "incident": p.incident,
        "label": 0 if p.scenario == "normal" else 1,
        "sent_at": round(sent_at, 3),
    }
    with run.lock:
        run.rows.append(row)


def blitz(run, rng, at_s, start, pool, idx):
    """
    A brand-new merchant that starts moving big money immediately. Created
    mid-run on purpose: its whole point is that it has no history.
    """
    time.sleep(max(0, start + at_s - time.time()))
    m = create_merchant(run, f"Blitz Traders {idx}", "new_merchant_blitz", rng, 0)
    incident = f"new_merchant_blitz:{m.id}"
    print(f"  * planted new-merchant blitz: {m.name}")
    for k in range(12):
        p = Planned(0, m, rng.randint(15_000, 45_000) * 100, "new_merchant_blitz", incident)
        pool.submit(send_payment, run, p)
        time.sleep(10)


def main():
    ap = argparse.ArgumentParser(description="Plant labelled fraud into local Transakt traffic.")
    ap.add_argument("--base-url", default=os.environ.get("TRANSAKT_URL", "http://localhost:8080"))
    ap.add_argument("--minutes", type=float, default=6.0)
    ap.add_argument("--merchants", type=int, default=20)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="simulator/output/labels.csv")
    args = ap.parse_args()

    if "onrender.com" in args.base_url:
        raise SystemExit("Refusing to run against the live deployment. Use local Transakt.")

    rng = random.Random(args.seed)
    run = Run(args.base_url.rstrip("/"))
    total_s = args.minutes * 60

    status, _ = http_json("GET", f"{run.base_url}/api/v1/health")
    if status != 200:
        raise SystemExit(f"Transakt is not healthy at {run.base_url} (got {status}). Is it running?")

    print(f"Creating {args.merchants} normal merchants...")
    normal = [create_merchant(run, f"Shop {i:02d}", "normal", rng,
                              rng.randint(200, 3000) * 100)  # Rs 200 to Rs 3,000
              for i in range(args.merchants)]

    plan = build_schedule(normal, rng, total_s)
    fraud = sum(1 for p in plan if p.scenario != "normal")
    print(f"Scheduled {len(plan)} payments ({fraud} planted fraud) + 2 new-merchant blitzes "
          f"over {args.minutes:g} min. Seed {args.seed}.")

    start = time.time()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for i, at_frac in enumerate((0.42, 0.50)):
            # Each blitz thread gets its own seeded generator. Sharing `rng` across
            # threads would make the draw order depend on thread timing, and the
            # same seed would stop producing the same run.
            own_rng = random.Random(args.seed * 1000 + i)
            threading.Thread(target=blitz, args=(run, own_rng, total_s * at_frac, start, pool, i + 1),
                             daemon=True).start()
        last_report = 0
        for p in plan:
            time.sleep(max(0, start + p.at - time.time()))
            pool.submit(send_payment, run, p)
            elapsed = time.time() - start
            if elapsed - last_report >= 30:
                last_report = elapsed
                print(f"  {elapsed / 60:4.1f} min - {len(run.rows)} payments recorded")
        # let the last blitz finish before the pool shuts down
        time.sleep(max(0, start + total_s * 0.5 + 125 - time.time()))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(run.rows[0].keys()))
        w.writeheader()
        w.writerows(sorted(run.rows, key=lambda r: r["sent_at"]))

    n_fraud = sum(r["label"] for r in run.rows)
    incidents = len({r["incident"] for r in run.rows if r["incident"]})
    print(f"\nDone. {len(run.rows)} payments ({n_fraud} fraud across {incidents} incidents), "
          f"{run.failures} failed. Labels written to {args.out}")


if __name__ == "__main__":
    main()
