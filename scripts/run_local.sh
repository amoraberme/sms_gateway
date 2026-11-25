#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/.pids"
LOG_DIR="$ROOT_DIR/.pids/logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

VENV_PY="$ROOT_DIR/.venv/bin/python"
VENV_UVICORN="$ROOT_DIR/.venv/bin/uvicorn"

start_services() {
  echo "Ensuring PostgreSQL (or PGlite fallback) & Redis are running..."
  if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
    export DATABASE_URL="${DATABASE_URL:-postgresql://sms_gateway:sms_gateway@127.0.0.1:5432/sms_gateway}"
    export DB_POOL_MIN_SIZE="${DB_POOL_MIN_SIZE:-2}"
    export DB_POOL_MAX_SIZE="${DB_POOL_MAX_SIZE:-25}"
    export RUN_EMBEDDED_WORKERS="${RUN_EMBEDDED_WORKERS:-false}"
  else
    echo "Native PostgreSQL unavailable; starting PGliteSocketServer on 127.0.0.1:5432..."
    if [ ! -d "$ROOT_DIR/scripts/pglite_ctl/node_modules" ]; then
      (cd "$ROOT_DIR/scripts/pglite_ctl" && npm install --no-audit --no-fund)
    fi
    node "$ROOT_DIR/scripts/pglite_ctl/server.mjs" --port 5432 --data-dir "$ROOT_DIR/.pids/pglite_main" >"$LOG_DIR/pglite.log" 2>&1 &
    echo $! > "$PID_DIR/pglite.pid"
    sleep 2
    export DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/postgres}"
    export DB_POOL_MIN_SIZE=1
    export DB_POOL_MAX_SIZE=1
    export RUN_EMBEDDED_WORKERS=true
  fi

  if ! redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
    redis-server --daemonize yes --bind 127.0.0.1 --port 6379 --save "" --appendonly no
  fi

  echo "Running migrations..."
  (cd "$ROOT_DIR/backend" && "$VENV_PY" -m app.db.migrate)

  export DLR_SECRET="${DLR_SECRET:-dcc-shared-dlr-hmac-secret-2026}"
  export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
  export CARRIER_PRIMARY_URL="${CARRIER_PRIMARY_URL:-http://127.0.0.1:8001}"
  export CARRIER_SECONDARY_URL="${CARRIER_SECONDARY_URL:-http://127.0.0.1:8002}"

  echo "Starting Mock Carrier Primary (:8001)..."
  (cd "$ROOT_DIR/mock_carrier" && \
    CARRIER_NAME=carrier-primary GATEWAY_DLR_URL="http://127.0.0.1:8000/carrier/dlr" \
    "$VENV_UVICORN" app:app --host 127.0.0.1 --port 8001 >"$LOG_DIR/carrier1.log" 2>&1 &
    echo $! > "$PID_DIR/carrier1.pid")

  echo "Starting Mock Carrier Secondary (:8002)..."
  (cd "$ROOT_DIR/mock_carrier" && \
    CARRIER_NAME=carrier-secondary GATEWAY_DLR_URL="http://127.0.0.1:8000/carrier/dlr" \
    "$VENV_UVICORN" app:app --host 127.0.0.1 --port 8002 >"$LOG_DIR/carrier2.log" 2>&1 &
    echo $! > "$PID_DIR/carrier2.pid")

  echo "Starting Gateway API (:8000)..."
  (cd "$ROOT_DIR/backend" && \
    "$VENV_UVICORN" app.main:app --host 127.0.0.1 --port 8000 >"$LOG_DIR/gateway_api.log" 2>&1 &
    echo $! > "$PID_DIR/gateway_api.pid")

  if [ "$RUN_EMBEDDED_WORKERS" != "true" ]; then
    echo "Starting Gateway Worker..."
    (cd "$ROOT_DIR/backend" && \
      "$VENV_PY" -m app.worker_main >"$LOG_DIR/gateway_worker.log" 2>&1 &
      echo $! > "$PID_DIR/gateway_worker.pid")
  fi

  if [ -d "$ROOT_DIR/frontend/.next" ]; then
    echo "Starting Next.js Frontend (:3000)..."
    (cd "$ROOT_DIR/frontend" && \
      BACKEND_INTERNAL_URL="http://127.0.0.1:8000" \
      nohup npm run start -- -H 127.0.0.1 -p 3000 >"$LOG_DIR/frontend.log" 2>&1 &
      echo $! > "$PID_DIR/frontend.pid")
  fi

  sleep 2
  echo "Stack started. API=http://127.0.0.1:8000 Frontend=http://127.0.0.1:3000"
}

stop_services() {
  for f in "$PID_DIR"/*.pid; do
    [ -e "$f" ] || continue
    pid=$(cat "$f" || true)
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$f"
  done
  echo "Stopped local gateway processes."
}

case "${1:-start}" in
  start) start_services ;;
  stop) stop_services ;;
  restart) stop_services; sleep 1; start_services ;;
  *) echo "Usage: $0 {start|stop|restart}"; exit 1 ;;
esac
