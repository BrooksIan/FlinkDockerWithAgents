#!/usr/bin/env python3
"""Demo: publish typed events → route/enrich → apply rule drift."""

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
    parser.add_argument("--phase", default="safe", help="monitor|safe|lab")
    parser.add_argument("--wait", type=float, default=8.0)
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args()

    from ratatoskr.dataplane.flow import ensure_dataplane_flow
    from ratatoskr.dataplane.topics import TOPIC_ENRICHED, TOPIC_RAW, TOPIC_VALID
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.routing import run_route_enrich_cycle
    from ratatoskr.schema.policy import topic_approx_count

    if not args.skip_load:
        print("Ensuring dataplane flow + topics...")
        print(ensure_dataplane_flow(NiFiClient()))

    # Seed via raw so schema gate also exercises; order type should enrich.
    events = [
        {"id": "o-1", "type": "order", "payload": {"sku": "X"}},
        {"id": "m-1", "type": "metric", "payload": {"v": 1}},
    ]
    print(f"Publishing {len(events)} events to {TOPIC_RAW}")
    _publish(TOPIC_RAW, events)
    time.sleep(args.wait)

    rule = {
        "match": {"type": "order"},
        "set": {"env": "demo", "pipeline": "dataplane"},
        "route": "enriched",
    }
    result = run_route_enrich_cycle(phase=args.phase, rule=rule)
    print("Route/enrich cycle:")
    print(json.dumps(result, indent=2, default=str))

    enriched = topic_approx_count(TOPIC_ENRICHED)
    valid = topic_approx_count(TOPIC_VALID)
    print("Counts:", json.dumps({"valid": valid, "enriched": enriched}, indent=2))
    print("OK: route/enrich cycle complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
