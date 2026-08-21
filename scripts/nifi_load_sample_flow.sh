#!/usr/bin/env bash
# Load the Ratatoskr sample NiFi flow via REST.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec python3 scripts/nifi_load_sample_flow.py --wait "$@"
