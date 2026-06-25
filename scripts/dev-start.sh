#!/usr/bin/env bash
# Start Control API + dashboard for local dev (honeypot or minimal Flink must already be up).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

"$ROOT/scripts/dev-stop.sh"

if [ -f "$ROOT/.venv/bin/apemosyne" ]; then
  APEMOSYNE="$ROOT/.venv/bin/apemosyne"
else
  APEMOSYNE=apemosyne
fi

echo "Starting Control API on :8090 (loads .env automatically)..."
"$APEMOSYNE" api start &
API_PID=$!
sleep 2

if ! curl -sf --max-time 5 http://127.0.0.1:8090/v1/health >/dev/null; then
  echo "API not healthy yet — check: curl http://127.0.0.1:8090/v1/health" >&2
fi

echo ""
echo "Starting dashboard on :3000..."
echo "  Dashboard: http://localhost:3000"
echo "  API docs:  http://127.0.0.1:8090/docs"
echo "  Stop all:  ./scripts/dev-stop.sh"
echo ""

cd "$ROOT/dashboard"
trap "$ROOT/scripts/dev-stop.sh" EXIT INT TERM
npm run dev

wait $API_PID
