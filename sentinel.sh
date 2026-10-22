#!/usr/bin/env bash
# One entry point for running Sentinel locally on top of Transakt.
#
#   ./sentinel.sh up          build and start Transakt + Kafka + the three Sentinel services
#   ./sentinel.sh bootstrap   create the admin account Sentinel reads Transakt with
#   ./sentinel.sh simulate    plant labelled fraud into live traffic (~6 minutes)
#   ./sentinel.sh eval        grade the system against the simulator's answer key
#   ./sentinel.sh demo        all four, in order
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
  compose up -d --build
  wait_http http://localhost:8080/api/v1/health "Transakt" 240
  wait_http http://localhost:8002/health "risk-scorer"
  wait_http http://localhost:8003/health "investigator"
  echo "✓ Up. Review page: http://localhost:8003   MCP: http://localhost:8001/mcp"
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

cmd_simulate() { need "$PY" "Install it with: brew install python@3.12"; "$PY" simulator/simulate.py "$@"; }
cmd_eval()     { need "$PY" "Install it with: brew install python@3.12"; "$PY" eval/evaluate.py "$@"; }

case "${1:-help}" in
  up)        cmd_up ;;
  bootstrap) cmd_bootstrap ;;
  simulate)  shift; cmd_simulate "$@" ;;
  eval)      shift; cmd_eval "$@" ;;
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
  *)         sed -n '2,11p' "$0" ;;
esac
