"""
End-to-end run of the whole pipeline without Java, Postgres or Kafka:

  fake Transakt  <-  simulator (real)                 [planted, labelled fraud]
        |  settled events, replayed in order (stands in for Kafka)
        v
  risk-scorer (real model, real features)  ->  investigator /cases (real HTTP)
                                                  |  real MCP over HTTP
                                                  v
                                           transakt-mcp (real)  ->  fake Transakt
  then eval/evaluate.py grades it all against the simulator's labels.

    python e2e/run_e2e.py --minutes 1.5

Needs each service's requirements installed in one environment. CI runs it.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FAKE, MCP, SCORER, INV = 18080, 18001, 18002, 18003
ADMIN = ("sentinel-admin@transakt.local", "change-me-sentinel")
TOKEN = "e2e-" + os.urandom(8).hex()
A2A_TOKEN = "e2e-a2a-" + os.urandom(8).hex()


def http(method, url, body=None, headers=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def wait(url, name, limit=60):
    for _ in range(limit * 2):
        try:
            urllib.request.urlopen(url, timeout=2)
            print(f"  ✓ {name}")
            return
        except Exception:
            time.sleep(0.5)
    raise SystemExit(f"{name} did not start")


def start(args, cwd, env, log):
    return subprocess.Popen(args, cwd=cwd, env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT)


async def replay(events, scores_db, model_path):
    """Kafka's job, done by hand: hand each settled event to the real scorer, in order."""
    sys.path.insert(0, os.path.join(ROOT, "services", "risk-scorer"))
    os.environ.update({"MODEL_PATH": model_path, "TRANSAKT_URL": f"http://127.0.0.1:{FAKE}",
                       "ADMIN_EMAIL": ADMIN[0], "ADMIN_PASSWORD": ADMIN[1], "SCORES_DB": scores_db})
    from app.main import build_scorer
    from app.transakt_client import parse_ts
    scorer = build_scorer()
    opened = 0
    items = [(e, parse_ts(e["_ts"]).timestamp()) for e in events]
    for k in range(0, len(items), 50):                 # in batches, like the Kafka consumer
        for case in await scorer.handle_many(items[k:k + 50]):
            http("POST", f"http://127.0.0.1:{INV}/cases", case)
            opened += 1
    return opened


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=1.5)
    ap.add_argument("--keep-running", action="store_true",
                    help="after the checks, leave everything up (review page on :18003) until Ctrl-C — "
                         "a Docker-free demo")
    args = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="sentinel-e2e-")
    log = open(os.path.join(tmp, "services.log"), "w")
    procs = []
    print(f"Working dir {tmp}")
    try:
        procs.append(start([PY, "-m", "uvicorn", "fake_transakt:app", "--port", str(FAKE)],
                           os.path.join(ROOT, "e2e"), {}, log))
        wait(f"http://127.0.0.1:{FAKE}/api/v1/health", "fake Transakt")

        http("POST", f"http://127.0.0.1:{FAKE}/api/v1/merchants",
             {"name": "Sentinel Admin", "email": ADMIN[0], "password": ADMIN[1]})
        http("POST", f"http://127.0.0.1:{FAKE}/_promote/{ADMIN[0]}")

        common = {"TRANSAKT_URL": f"http://127.0.0.1:{FAKE}", "ADMIN_EMAIL": ADMIN[0], "ADMIN_PASSWORD": ADMIN[1]}
        procs.append(start([PY, "-m", "app.server"], os.path.join(ROOT, "services", "transakt-mcp"),
                           {**common, "MCP_HOST": "127.0.0.1", "MCP_PORT": str(MCP), "MCP_TOKEN": TOKEN}, log))
        wait(f"http://127.0.0.1:{MCP}/health", "transakt-mcp")

        model = os.path.join(tmp, "model.skops")
        subprocess.run([PY, "-m", "app.train", "--out", model], cwd=os.path.join(ROOT, "services", "risk-scorer"),
                       check=True, stdout=log, stderr=subprocess.STDOUT)
        scores_db = os.path.join(tmp, "scores.db")
        procs.append(start([PY, "-m", "uvicorn", "app.main:app", "--port", str(SCORER)],
                           os.path.join(ROOT, "services", "risk-scorer"),
                           {**common, "MODEL_PATH": model, "SCORES_DB": scores_db, "KAFKA_BOOTSTRAP_SERVERS": ""}, log))
        wait(f"http://127.0.0.1:{SCORER}/health", "risk-scorer")

        procs.append(start([PY, "-m", "uvicorn", "app.main:app", "--port", str(INV)],
                           os.path.join(ROOT, "services", "investigator"),
                           {"DATA_DIR": os.path.join(tmp, "inv"), "MCP_URL": f"http://127.0.0.1:{MCP}/mcp", "MCP_TOKEN": TOKEN,
                            "A2A_TOKEN": A2A_TOKEN, "KAFKA_BOOTSTRAP_SERVERS": "", "LLM_API_KEY": "", "HF_TOKEN": ""}, log))
        wait(f"http://127.0.0.1:{INV}/health", "investigator")

        labels = os.path.join(tmp, "labels.csv")
        subprocess.run([PY, os.path.join(ROOT, "simulator", "simulate.py"), "--base-url", f"http://127.0.0.1:{FAKE}",
                        "--minutes", str(args.minutes), "--out", labels], check=True)

        events = http("GET", f"http://127.0.0.1:{FAKE}/_events")
        opened = asyncio.run(replay(events, scores_db, model))
        print(f"Replayed {len(events)} settled events; {opened} flags sent to the investigator")

        subprocess.run([PY, os.path.join(ROOT, "eval", "evaluate.py"), "--labels", labels,
                        "--scorer", f"http://127.0.0.1:{SCORER}", "--investigator", f"http://127.0.0.1:{INV}",
                        "--out", os.path.join(tmp, "results"), "--timeout", "90"], check=True)
        with open(os.path.join(tmp, "results", "latest.json")) as f:
            r = json.load(f)
        il = r["incident_level"]
        assert il["detected"] == il["incidents"], f"missed incidents: {il}"
        assert r["payment_level"]["precision"] >= 0.8, r["payment_level"]
        assert r["false_alarms"]["cases_on_normal_merchants"] <= 2, r["false_alarms"]

        # ---- v1.2 pieces, on the same running system ----
        inv = f"http://127.0.0.1:{INV}"
        # 1. Another agent asks over A2A about a planted velocity-burst payment.
        import csv
        with open(labels) as f:
            burst = next(row for row in csv.DictReader(f) if row["scenario"] == "velocity_burst")
        card = http("GET", f"{inv}/.well-known/agent-card.json")
        reply = http("POST", f"{inv}/a2a", {
            "jsonrpc": "2.0", "id": 1, "method": "SendMessage",
            "params": {"message": {"messageId": "e2e", "role": "ROLE_USER",
                                   "parts": [{"data": {"payment_id": burst["payment_id"]}}]}}},
            headers={"Authorization": f"Bearer {A2A_TOKEN}", "A2A-Version": "1.0"})
        a2a = reply["result"]["task"]["artifacts"][0]["parts"][0]["data"]
        print(f"A2A: {card['name']} -> {a2a['recommendation']} for {burst['payment_id'][:8]} (case {a2a['case_id']})")
        assert a2a["recommendation"] == "HOLD_PAYOUT", a2a

        # 2. Every service exposes Prometheus metrics with real numbers in them.
        for name, url, series in (("scorer", f"http://127.0.0.1:{SCORER}/metrics/", "sentinel_model_info"),
                                  ("investigator", f"{inv}/metrics", "sentinel_cases_opened_total"),
                                  ("mcp", f"http://127.0.0.1:{MCP}/metrics", "sentinel_mcp_tool_calls_total")):
            with urllib.request.urlopen(url, timeout=10) as r:
                assert series in r.read().decode(), f"{name} metrics missing {series}"
        print("metrics: scorer, investigator and MCP all exporting")

        # 3. An analyst confirms one case and dismisses another; the decisions come back as labels
        #    and retraining folds them in.
        waiting = http("GET", f"{inv}/cases?status=AWAITING_REVIEW")
        for c, d in zip(waiting[:2], ("CONFIRM_FRAUD", "DISMISS")):
            http("POST", f"{inv}/cases/{c['id']}/decision", {"decision": d, "analyst": "e2e"})
        fb = http("GET", f"{inv}/feedback")
        fb_path = os.path.join(tmp, "feedback.json")
        with open(fb_path, "w") as f:
            json.dump(fb, f)
        out = subprocess.run([PY, "-m", "app.train", "--out", os.path.join(tmp, "retrained.skops"),
                              "--feedback", fb_path], cwd=os.path.join(ROOT, "services", "risk-scorer"),
                             check=True, capture_output=True, text=True).stdout
        assert f'"feedback_rows": {len(fb)}' in out, out
        print(f"feedback loop: {len(fb)} analyst decisions -> retrained model")

        print("\n✓ end-to-end checks passed")
        print("  (Ignore the latency row here: this harness replays every event AFTER the simulation "
              "ends. With real Kafka, send-to-score is roughly the outbox's 5 s sweep.)")
        if args.keep_running:
            print(f"\nStill running. Review page: http://127.0.0.1:{INV}   Agent card: "
                  f"http://127.0.0.1:{INV}/.well-known/agent-card.json   Ctrl-C to stop.", flush=True)
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                pass
    finally:
        for p in procs:
            p.terminate()
        log.close()


if __name__ == "__main__":
    main()
