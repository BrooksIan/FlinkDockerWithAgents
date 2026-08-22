#!/usr/bin/env python3
"""Demo: publish valid/invalid events → schema gate → monitor cycle."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _publish(topic: str, payloads: list[dict]) -> None:
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for p in payloads:
            producer.send(topic, p)
        producer.flush()
    finally:
        producer.close()


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", default="monitor", help="monitor|safe|lab")
    parser.add_argument("--wait", type=float, default=8.0, help="Seconds for NiFi to process")
    parser.add_argument("--skip-load", action="store_true", help="Do not ensure dataplane flow")
    args = parser.parse_args()

    from ratatoskr.dataplane.flow import ensure_dataplane_flow
    from ratatoskr.dataplane.topics import TOPIC_RAW, TOPIC_VALID, TOPIC_VIOLATIONS
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.schema import run_schema_gate_cycle
    from ratatoskr.schema.policy import topic_approx_count

    if not args.skip_load:
        print("Ensuring dataplane flow + topics...")
        print(ensure_dataplane_flow(NiFiClient()))

    valid = {"id": "evt-1", "type": "order", "payload": {"sku": "A1", "qty": 1}}
    invalid = {"id": 99, "payload": "nope"}  # fails id:string + missing type + payload:object
    print(f"Publishing to {TOPIC_RAW}: 1 valid, 1 invalid")
    _publish(TOPIC_RAW, [valid, invalid])
    time.sleep(args.wait)

    valid_c = topic_approx_count(TOPIC_VALID)
    viol_c = topic_approx_count(TOPIC_VIOLATIONS)
    print("Topic counts:", json.dumps({"valid": valid_c, "violations": viol_c}, indent=2))

    result = run_schema_gate_cycle(phase=args.phase)
    print("Schema gate cycle:")
    print(json.dumps(result, indent=2, default=str))

    ok = int(valid_c.get("count") or 0) >= 1 and int(viol_c.get("count") or 0) >= 1
    if not ok:
        print(
            "WARN: expected both valid and violations topics to grow "
            "(NiFi ValidateRecord may still be catching up, or flow not running).",
            file=sys.stderr,
        )
        return 1
    print("OK: schema gate saw valid + violation traffic")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
