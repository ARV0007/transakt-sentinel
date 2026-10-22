"""
Grades the fraud system against the simulator's answer key.

    python3.12 eval/evaluate.py

Reads simulator/output/labels.csv (the truth), asks the risk-scorer what it
flagged and the investigator what cases it opened and recommended, and writes
eval/results/latest.md and latest.json.

Measured at two levels, because they answer different questions:
  * payment level  — of every planted fraudulent payment, how many were flagged?
  * incident level — of every fraud INCIDENT (one merchant, one pattern), did an
    analyst end up with a case, and how fast? This is what a fraud team feels.

Standard library only.
"""

import argparse
import csv
import json
import os
import time
import urllib.request
from collections import defaultdict
from statistics import median


def get(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def pct(n, d):
    return round(100 * n / d, 1) if d else None


def p95(xs):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(0.95 * (len(xs) - 1))))] if xs else None


def wait_for_pipeline(labels, scorer, investigator, timeout):
    """The outbox publishes every 5 s and the LLM can take seconds per case, so
    give the pipeline time to drain before grading it."""
    want = {r["payment_id"] for r in labels}
    deadline = time.time() + timeout
    while True:
        scored = {s["payment_id"]: s for s in get(f"{scorer}/scores?limit=5000")}
        missing = want - scored.keys()
        busy = get(f"{investigator}/cases?status=INVESTIGATING")
        if (not missing and not busy) or time.time() > deadline:
            if missing:
                print(f"  ! {len(missing)} payments were never scored (Kafka or scorer down?)")
            return scored
        print(f"  waiting: {len(missing)} unscored payments, {len(busy)} cases investigating...")
        time.sleep(5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", default="simulator/output/labels.csv")
    ap.add_argument("--scorer", default="http://localhost:8002")
    ap.add_argument("--investigator", default="http://localhost:8003")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--out", default="eval/results")
    args = ap.parse_args()

    with open(args.labels) as f:
        labels = list(csv.DictReader(f))
    for r in labels:
        r["label"] = int(r["label"])
        r["sent_at"] = float(r["sent_at"])
    run_start = min(r["sent_at"] for r in labels) - 60

    print(f"Grading {len(labels)} payments...")
    scored = wait_for_pipeline(labels, args.scorer, args.investigator, args.timeout)
    cases = [c for c in get(f"{args.investigator}/cases?limit=1000") if c["opened_at"] >= run_start]
    case_by_merchant = defaultdict(list)
    for c in cases:
        case_by_merchant[c["merchant_id"]].append(c)

    # ------------------------------------------------ payment level
    tp = fp = fn = tn = 0
    by_scenario = defaultdict(lambda: [0, 0])          # scenario -> [flagged, total]
    lags = []
    for r in labels:
        s = scored.get(r["payment_id"])
        flagged = bool(s and s["flagged"])
        if s:
            lags.append(s["scored_at"] - r["sent_at"])
        if r["label"]:
            tp += flagged
            fn += not flagged
            by_scenario[r["scenario"]][0] += flagged
            by_scenario[r["scenario"]][1] += 1
        else:
            fp += flagged
            tn += not flagged
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    # ------------------------------------------------ incident level
    incidents = defaultdict(list)
    for r in labels:
        if r["incident"]:
            incidents[r["incident"]].append(r)
    incident_rows = []
    for inc, rows in sorted(incidents.items()):
        merchant = rows[0]["merchant_id"]
        first_sent = min(r["sent_at"] for r in rows)
        flagged_times = [scored[r["payment_id"]]["scored_at"] for r in rows
                         if r["payment_id"] in scored and scored[r["payment_id"]]["flagged"]]
        mc = case_by_merchant.get(merchant, [])
        rec = mc[0]["recommendation"]["action"] if mc and mc[0].get("recommendation") else None
        incident_rows.append({
            "incident": inc.split(":")[0], "merchant": merchant[:8],
            "payments": len(rows), "flagged": len(flagged_times),
            "case": bool(mc), "recommendation": rec,
            "seconds_to_first_flag": round(min(flagged_times) - first_sent, 1) if flagged_times else None,
        })
    detected = sum(1 for i in incident_rows if i["case"])
    held = sum(1 for i in incident_rows if i["recommendation"] == "HOLD_PAYOUT")

    fraud_merchants = {rows[0]["merchant_id"] for rows in incidents.values()}
    normal_merchants = {r["merchant_id"] for r in labels} - fraud_merchants
    false_cases = [c for c in cases if c["merchant_id"] in normal_merchants]
    false_cases_allowed = sum(1 for c in false_cases
                              if (c.get("recommendation") or {}).get("action") in ("ALLOW", "NEEDS_MANUAL_REVIEW"))
    llm_cases = sum(1 for c in cases if (c.get("recommendation") or {}).get("source") == "llm")
    grounding_flags = sum(1 for c in cases if not ((c.get("recommendation") or {}).get("grounding") or {}).get("ok", True))

    results = {
        "payments": len(labels), "fraud_payments": tp + fn,
        "payment_level": {"precision": round(precision, 3), "recall": round(recall, 3), "f1": round(f1, 3),
                          "tp": tp, "fp": fp, "fn": fn, "tn": tn,
                          "recall_by_scenario": {k: f"{v[0]}/{v[1]}" for k, v in by_scenario.items()}},
        "incident_level": {"incidents": len(incident_rows), "detected": detected,
                           "recommended_hold": held, "rows": incident_rows},
        "false_alarms": {"cases_on_normal_merchants": len(false_cases),
                         "of_which_recommended_allow_or_review": false_cases_allowed},
        "cost": {"cases": len(cases), "llm_recommendations": llm_cases,
                 "llm_share_of_payments_pct": pct(len(cases), len(labels)),
                 "grounding_warnings": grounding_flags},
        "latency_seconds": {"send_to_score_p50": round(median(lags), 2) if lags else None,
                            "send_to_score_p95": round(p95(lags), 2) if lags else None},
        "graded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "latest.json"), "w") as f:
        json.dump(results, f, indent=2)

    pl, il = results["payment_level"], results["incident_level"]
    md = [
        "# Sentinel evaluation", "", f"Graded {results['graded_at']} against `{args.labels}`.", "",
        "| Metric | Value |", "|---|---|",
        f"| Payments | {results['payments']} ({results['fraud_payments']} planted fraud) |",
        f"| Payment-level precision / recall / F1 | {pl['precision']} / {pl['recall']} / {pl['f1']} |",
        f"| Recall by pattern | {', '.join(f'{k} {v}' for k, v in pl['recall_by_scenario'].items())} |",
        f"| Fraud incidents with a case | {il['detected']} / {il['incidents']} |",
        f"| Incidents recommended HOLD_PAYOUT | {il['recommended_hold']} / {il['incidents']} |",
        f"| Cases opened on normal merchants | {results['false_alarms']['cases_on_normal_merchants']} "
        f"({results['false_alarms']['of_which_recommended_allow_or_review']} recommended ALLOW/REVIEW) |",
        f"| LLM work | {results['cost']['cases']} cases for {results['payments']} payments "
        f"({results['cost']['llm_share_of_payments_pct']}% of traffic reached the LLM step) |",
        f"| Grounding warnings | {results['cost']['grounding_warnings']} |",
        f"| Send → score latency p50 / p95 | {results['latency_seconds']['send_to_score_p50']} s / "
        f"{results['latency_seconds']['send_to_score_p95']} s (includes the outbox's 5 s sweep) |",
        "", "## Incidents", "", "| Pattern | Merchant | Flagged | Case | Recommendation | First flag after |",
        "|---|---|---|---|---|---|",
        *[f"| {i['incident']} | `{i['merchant']}` | {i['flagged']}/{i['payments']} | {'yes' if i['case'] else 'no'} "
          f"| {i['recommendation'] or '—'} | {i['seconds_to_first_flag']} s |" for i in il["rows"]],
    ]
    with open(os.path.join(args.out, "latest.md"), "w") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))
    print(f"\nSaved {args.out}/latest.md and latest.json")


if __name__ == "__main__":
    main()
