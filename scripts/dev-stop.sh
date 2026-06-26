#!/usr/bin/env bash
# Stop local Ratatoskr dev processes (API + dashboard).
set -euo pipefail

for port in 8090 8091 3000 5173; do
  pids=$(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  if [ -n "$pids" ]; then
    echo "Stopping port $port (PID $pids)"
    kill $pids 2>/dev/null || kill -9 $pids 2>/dev/null || true
  fi
done

echo "Dev ports cleared."
