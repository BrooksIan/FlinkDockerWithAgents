#!/usr/bin/env bash
# Export OpenAPI from Apemosyne and optionally generate TypeScript types.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT}/dashboard/src/api"
OPENAPI="${OUT_DIR}/openapi.json"

cd "${ROOT}"

mkdir -p "${OUT_DIR}"

if command -v apemosyne >/dev/null 2>&1; then
  apemosyne api openapi -o "${OPENAPI}"
elif [[ -x "${ROOT}/.venv/bin/apemosyne" ]]; then
  "${ROOT}/.venv/bin/apemosyne" api openapi -o "${OPENAPI}"
else
  python3 -c "
from apemosyne.api.app import create_app
import json
from pathlib import Path
p = Path('${OPENAPI}')
p.write_text(json.dumps(create_app().openapi(), indent=2), encoding='utf-8')
print('Wrote', p)
"
fi

echo "OpenAPI written to ${OPENAPI}"

if command -v npx >/dev/null 2>&1; then
  if npx --yes openapi-typescript "${OPENAPI}" -o "${OUT_DIR}/schema.d.ts" 2>/dev/null; then
    echo "Generated ${OUT_DIR}/schema.d.ts via openapi-typescript"
  else
    echo "openapi-typescript skipped (install Node deps in dashboard/ for codegen)"
  fi
else
  echo "npx not found — types remain in src/api/types.ts (hand-maintained)"
fi
