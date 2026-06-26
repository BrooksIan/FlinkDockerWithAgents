#!/usr/bin/env bash
# Restart the Studio minimal Flink cluster + Kafka after code/image updates.
#
# Typical use after pulling changes or editing apemosyne pipeline/runtime code:
#   ./scripts/restart-studio-cluster.sh
#   ./scripts/restart-studio-cluster.sh --build --smoke
#   ./scripts/restart-studio-cluster.sh --sync-only   # hot-sync code, no container restart
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BUILD=false
SMOKE=false
RESTART_API=false
SKIP_KAFKA=false
SYNC_ONLY=false

usage() {
  cat <<'EOF'
Usage: ./scripts/restart-studio-cluster.sh [options]

Restart the Studio stack used for pipeline cluster runs:
  - minimal Flink (JobManager + TaskManager on FLINK_REST_PORT, default 8082)
  - Studio Kafka (localhost:9094)
  - sync apemosyne runtime + pipeline code into containers
  - bootstrap Flink Agents thin JARs (Pemja classloader fix)

Options:
  --build       Rebuild agent_flink_image before restarting Flink
  --smoke       Run cluster launch smoke job after sync
  --api         Restart Control API on :8090 (stops dev ports first)
  --no-kafka    Skip Studio Kafka restart
  --sync-only   Do not restart Docker services; copy code + bootstrap only
  -h, --help    Show this help

Environment (from .env):
  APEMOSYNE_PROFILE=minimal
  FLINK_REST_PORT=8082
  KAFKA_BOOTSTRAP_SERVERS=localhost:9094
  APEMOSYNE_API_PORT=8090

After restart:
  Flink UI:    http://localhost:8082
  Control API: http://127.0.0.1:8090/docs
  Dashboard:   cd dashboard && npm run dev  → http://localhost:5173
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --build) BUILD=true ;;
    --smoke) SMOKE=true ;;
    --api) RESTART_API=true ;;
    --no-kafka) SKIP_KAFKA=true ;;
    --sync-only) SYNC_ONLY=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
  shift
done

if [ -f "$ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

export APEMOSYNE_PROFILE="${APEMOSYNE_PROFILE:-minimal}"
export FLINK_REST_PORT="${FLINK_REST_PORT:-8082}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}"
export APEMOSYNE_API_PORT="${APEMOSYNE_API_PORT:-8090}"

if [ -x "$ROOT/.venv/bin/apemosyne" ]; then
  run_apemosyne() { "$ROOT/.venv/bin/apemosyne" "$@"; }
elif [ -x "$ROOT/.venv/bin/python" ]; then
  run_apemosyne() { "$ROOT/.venv/bin/python" -m apemosyne.main "$@"; }
else
  run_apemosyne() { apemosyne "$@"; }
fi

if [ -x "$ROOT/.venv/bin/python" ]; then
  PYTHON="$ROOT/.venv/bin/python"
else
  PYTHON=python3
fi

echo "=== Studio cluster restart ==="
echo "  profile:  $APEMOSYNE_PROFILE"
echo "  Flink UI: http://localhost:${FLINK_REST_PORT}"
echo "  Kafka:    ${KAFKA_BOOTSTRAP_SERVERS}"
echo ""

if ! $SYNC_ONLY; then
  if $BUILD; then
    echo "[1/5] Building Flink Agents image..."
    run_apemosyne build
  else
    echo "[1/5] Skipping image build (pass --build to rebuild)"
  fi

  echo "[2/5] Restarting minimal Flink stack (force recreate)..."
  run_apemosyne down --profile minimal 2>/dev/null || true
  docker compose -f "$ROOT/docker-compose.yml" up -d --force-recreate --remove-orphans
  echo "  waiting 15s for JobManager health..."
  sleep 15

  if ! $SKIP_KAFKA; then
    echo "[3/5] Restarting Studio Kafka..."
    run_apemosyne kafka down 2>/dev/null || true
    run_apemosyne kafka up --wait 12
  else
    echo "[3/5] Skipping Kafka (--no-kafka)"
  fi
else
  echo "[1-3/5] Skipping Docker restarts (--sync-only)"
fi

echo "[4/5] Syncing code + bootstrapping cluster runtime..."
if $SMOKE; then SMOKE_FLAG=True; else SMOKE_FLAG=False; fi
"$PYTHON" - <<PY
from apemosyne.env import load_workspace_env
load_workspace_env()
from apemosyne.runtime.studio_cluster_sync import restart_studio_cluster
restart_studio_cluster(smoke=${SMOKE_FLAG})
PY

if $RESTART_API; then
  echo "[5/5] Restarting Control API..."
  "$ROOT/scripts/dev-stop.sh"
  run_apemosyne api start &
  sleep 2
  if curl -sf --max-time 5 "http://127.0.0.1:${APEMOSYNE_API_PORT}/v1/health" >/dev/null; then
    echo "  API healthy on :${APEMOSYNE_API_PORT}"
  else
    echo "  API not healthy yet — check: curl http://127.0.0.1:${APEMOSYNE_API_PORT}/v1/health" >&2
  fi
else
  echo "[5/5] API not restarted (pass --api to restart Control API)"
fi

echo ""
echo "=== Ready ==="
echo "  Flink UI:    http://localhost:${FLINK_REST_PORT}"
echo "  Kafka:       ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  Control API: http://127.0.0.1:${APEMOSYNE_API_PORT}/docs"
echo ""
echo "Start dashboard: cd dashboard && npm run dev"
echo "Submit pipeline: Studio → Run on Flink cluster"
