#!/usr/bin/env bash
# Load / repair the Ratatoskr Data Plane NiFi flow (schema / route / replay).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
exec python3 scripts/nifi_load_dataplane_flow.py --wait "$@"
