#!/bin/bash
# Lab entrypoint: relax HTTPS SNI host checks so Flink can reach NiFi via
# compose DNS (https://nifi:8443) while the default cert remains localhost.
set -euo pipefail

props="${NIFI_HOME:-/opt/nifi/nifi-current}/conf/nifi.properties"

patch_sni() {
  if [[ ! -f "$props" ]]; then
    return 0
  fi
  if ! grep -q '^nifi.web.https.sni.required=' "$props" 2>/dev/null; then
    echo 'nifi.web.https.sni.required=false' >>"$props"
  else
    sed -i 's/^nifi.web.https.sni.required=.*/nifi.web.https.sni.required=false/' "$props"
  fi
  if ! grep -q '^nifi.web.https.sni.host.check=' "$props" 2>/dev/null; then
    echo 'nifi.web.https.sni.host.check=false' >>"$props"
  else
    sed -i 's/^nifi.web.https.sni.host.check=.*/nifi.web.https.sni.host.check=false/' "$props"
  fi
}

patch_sni
# start.sh may regenerate/replace properties on first boot — patch again after a short wait in background
(
  for _ in $(seq 1 30); do
    sleep 2
    patch_sni || true
  done
) &

exec /opt/nifi/scripts/start.sh "$@"
