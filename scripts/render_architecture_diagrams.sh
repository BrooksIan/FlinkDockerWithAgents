#!/usr/bin/env bash
# Render honeypot/docs/images/*.mmd → PNG via @mermaid-js/mermaid-cli (npx).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMG_DIR="$ROOT/honeypot/docs/images"
cd "$IMG_DIR"

for mmd in *.mmd; do
  [[ -f "$mmd" ]] || continue
  out="${mmd%.mmd}.png"
  echo "Rendering $mmd → $out"
  npx --yes @mermaid-js/mermaid-cli@11.4.0 \
    -i "$mmd" \
    -o "$out" \
    -b white \
    -w 1400 \
    -H 900 \
    --scale 2
done

echo "Done. PNG files in $IMG_DIR"
