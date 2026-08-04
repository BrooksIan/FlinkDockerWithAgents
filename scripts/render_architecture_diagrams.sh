#!/usr/bin/env bash
# Render assets/images/*.mmd and honeypot/docs/images/*.mmd → PNG via @mermaid-js/mermaid-cli (npx).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

render_dir() {
  local IMG_DIR="$1"
  [[ -d "$IMG_DIR" ]] || return 0
  cd "$IMG_DIR"
  for mmd in *.mmd; do
    [[ -f "$mmd" ]] || continue
    out="${mmd%.mmd}.png"
    echo "Rendering $IMG_DIR/$mmd → $out"
    npx --yes @mermaid-js/mermaid-cli@11.4.0 \
      -i "$mmd" \
      -o "$out" \
      -b white \
      -w 1400 \
      -H 900 \
      --scale 2
  done
}

render_dir "$ROOT/assets/images"
render_dir "$ROOT/honeypot/docs/images"

echo "Done."
