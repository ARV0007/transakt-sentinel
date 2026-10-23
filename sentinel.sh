#!/usr/bin/env bash
# One entry point for running Sentinel locally on top of Transakt.
#
#   ./sentinel.sh up          build and start Transakt + Kafka + the three Sentinel services
#   ./sentinel.sh bootstrap   create the admin account Sentinel reads Transakt with
#   ./sentinel.sh simulate    plant labelled fraud into live traffic (~6 minutes)
#   ./sentinel.sh eval        grade the system against the simulator's answer key
#   ./sentinel.sh demo        all four, in order
#   ./sentinel.sh retrain     retrain the scorer on analysts' decisions from the review page
#   ./sentinel.sh status | logs [service] | down | reset
#
# Expects Transakt checked out next to this repo (../transakt). Override with
# TRANSAKT_DIR=/path/to/transakt ./sentinel.sh up
set -euo pipefail

cd "$(dirname "$0")"
export SENTINEL_DIR="$(pwd)"
TRANSAKT_DIR="${TRANSAKT_DIR:-$SENTINEL_DIR/../transakt}"
PY="${PYTHON:-python3.12}"

compose() {
  docker compose -p transakt -f "$TRANSAKT_DIR/docker-compose.yml" -f "$SENTINEL_DIR/docker-compose.yml" "$@"
}

need() { command -v "$1" >/dev/null 2>&1 || { echo "✗ '$1' not found. $2"; exit 1; }; }

preflight() {
  [ -f "$TRANSAKT_DIR/docker-compose.yml" ] || { echo "✗ Transakt not found at $TRANSAKT_DIR (set TRANSAKT_DIR)"; exit 1; }
  need docker "Install Docker Desktop."
  # `docker --version` passes even when Docker Desktop is closed; `docker info` talks to the daemon.
  docker info >/dev/null 2>&1 || { echo "✗ Docker Desktop isn't running. Open it and wait for the whale to settle."; exit 1; }
  if [ ! -f .env ]; then
    cp .env.example .env
    echo "• Created .env from .env.example (add an HF token there to turn the LLM on)."
  fi
  # Prometheus and Grafana read these straight from this folder and run as their
  # own unprivileged users, so the files must be world-readable. Nothing secret
  # lives in monitoring/ (secrets are only in .env, which stays private).
  chmod -R a+rX monitoring
  # Random shared secrets, generated straight into .env and never printed:
  #   MCP_TOKEN  investigator -> transakt-mcp
  #   A2A_TOKEN  other agents -> the investigator's A2A endpoint
  for name in MCP_TOKEN A2A_TOKEN; do
    if ! grep -Eq "^$name=.+" .env; then        # -E: macOS grep has no \+ in basic regex
      token=$(openssl rand -hex 32)
      if grep -q "^$name=" .env; then
        sed -i.bak "s/^$name=.*/$name=$token/" .env && rm -f .env.bak
      else
        echo "$name=$token" >> .env
      fi
      echo "• Generated $name in .env."
    fi
  done
}

load_env() { set -a; # shellcheck disable=SC1091
  source .env; set +a; }

wait_http() {   # url, name, seconds
  local url="$1" name="$2" limit="${3:-180}" waited=0
  printf "• Waiting for %s" "$name"
  until curl -fsS "$url" >/dev/null 2>&1; do
    sleep 3; waited=$((waited + 3)); printf "."
    if [ "$waited" -ge "$limit" ]; then echo " ✗ not up after ${limit}s"; return 1; fi
  done
  echo " ✓"
}

cmd_up() {
  preflight
  if ! compose up -d --build; then
    # Compose only says "exited (1)"; the reason is in the container's own log.
    echo "✗ Start-up failed. Last log lines of every container that stopped:"
    for svc in $(compose ps -a --status exited --format '{{.Service}}'); do
      echo "----- $svc"
      compose logs --no-color --tail=40 "$svc"
    done
    exit 1
  fi
  wait_http http://localhost:8080/api/v1/health "Transakt" 240
  wait_http http://localhost:8002/health "risk-scorer"
  wait_http http://localhost:8003/health "investigator"
  echo "✓ Up."
  echo "  Review page : http://localhost:8003"
  echo "  Dashboard   : http://localhost:3000   (Grafana, opens on the Sentinel dashboard)"
  echo "  Prometheus  : http://localhost:9090"
  echo "  MCP server  : http://localhost:8001/mcp (needs MCP_TOKEN from .env)"
}

cmd_bootstrap() {
  preflight; load_env
  wait_http http://localhost:8080/api/v1/health "Transakt" 240
  # 1. Sign up through the public API like any merchant (no-op if it exists).
  curl -sS -o /dev/null -X POST http://localhost:8080/api/v1/merchants \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"Sentinel Admin\",\"email\":\"$ADMIN_EMAIL\",\"password\":\"$ADMIN_PASSWORD\"}" || true
  # 2. Promote it. Transakt deliberately has NO API that grants ADMIN (signup
  #    can't set a role), so promotion happens in the database, by someone with
  #    database access — which is the point. Local Docker database only.
  #    psql variables (:'email') quote the value; they only work on stdin, not -c.
  echo "UPDATE merchants SET role = 'ADMIN' WHERE email = :'email';" \
    | compose exec -T postgres psql -q -U transakt -d transakt -v email="$ADMIN_EMAIL" >/dev/null
  # 3. Prove it: an admin login must succeed.
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/auth/login \
    -H "Content-Type: application/json" -d "{\"email\":\"$ADMIN_EMAIL\",\"password\":\"$ADMIN_PASSWORD\"}")
  [ "$code" = "200" ] || { echo "✗ admin login returned $code"; exit 1; }
  echo "✓ Admin $ADMIN_EMAIL ready. Sentinel services log in with it automatically."
}

# -u: unbuffered output, so progress shows up live even when piped into a log.
cmd_retrain() {
  preflight
  local out="services/risk-scorer/feedback/feedback.json"
  curl -fsS http://localhost:8003/feedback -o "$out" || { echo "✗ investigator isn't reachable on :8003"; exit 1; }
  local n; n=$("$PY" -c "import json,sys; print(len(json.load(open(sys.argv[1]))))" "$out")
  if [ "$n" = "0" ]; then
    echo "• No decided cases yet. Confirm or dismiss some at http://localhost:8003 first."; exit 0
  fi
  echo "• $n analyst decisions -> retraining the scorer (merchant history in Redis is kept)."
  compose up -d --build risk-scorer
  wait_http http://localhost:8002/health "risk-scorer"
  # Training happens inside the image build, behind a gate that holds back the
  # newest decisions (see app/train.py). The version says whether it shipped.
  local health version gate
  health=$(curl -fsS http://localhost:8002/health)
  version=$(echo "$health" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['model_version'])")
  gate=$(echo "$health" | "$PY" -c "import json,sys; print(json.load(sys.stdin).get('retraining_gate',''))")
  case "$version" in
    *+fb*) echo "✓ Retrained model is live: $version (gate: $gate)" ;;
    *)     echo "✗ The retrained model was not shipped; the synthetic-only model is live ($version)."
           echo "  Gate: $gate" ;;
  esac
}

cmd_simulate() { need "$PY" "Install it with: brew install python@3.12"; "$PY" -u simulator/simulate.py "$@"; }
cmd_eval()     { need "$PY" "Install it with: brew install python@3.12"; "$PY" -u eval/evaluate.py "$@"; }

case "${1:-help}" in
  up)        cmd_up ;;
  bootstrap) cmd_bootstrap ;;
  simulate)  shift; cmd_simulate "$@" ;;
  eval)      shift; cmd_eval "$@" ;;
  retrain)   cmd_retrain ;;
  demo)      cmd_up; cmd_bootstrap; cmd_simulate; cmd_eval ;;
  status)    compose ps ;;
  logs)      shift; compose logs -f --tail=100 "$@" ;;
  down)      compose down ;;
  reset)
    # down -v removes EVERY volume in the project — including Transakt's own
    # Docker Postgres data, because both share project "transakt". Ask first.
    read -r -p "This deletes Sentinel's data AND Transakt's Docker database volume. Type 'yes': " ok
    [ "$ok" = "yes" ] || { echo "Cancelled."; exit 0; }
    compose down -v; rm -rf simulator/output eval/results; echo "✓ Volumes and outputs wiped." ;;
  *)         sed -n '2,12p' "$0" ;;
esac
