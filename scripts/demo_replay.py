#!/usr/bin/env python3
"""Demo: seed events.valid then run a lab replay into events.replay.out."""

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
    parser.add_argument("--phase", default="lab", help="monitor|lab")
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--seed", type=int, default=3, help="Messages to seed on source")
    args = parser.parse_args()

    from ratatoskr.dataplane.flow import ensure_dataplane_flow
    from ratatoskr.dataplane.topics import TOPIC_REPLAY_OUT, TOPIC_VALID
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.replay import run_replay_cycle
    from ratatoskr.schema.policy import topic_approx_count

    if not args.skip_load:
        print("Ensuring dataplane flow + topics...")
        print(ensure_dataplane_flow(NiFiClient()))

    before = topic_approx_count(TOPIC_REPLAY_OUT)
    seeds = [
        {"id": f"replay-{i}", "type": "order", "payload": {"n": i}}
        for i in range(args.seed)
    ]
    print(f"Seeding {len(seeds)} messages onto {TOPIC_VALID}")
    _publish(TOPIC_VALID, seeds)
    time.sleep(1.0)

    result = run_replay_cycle(
        phase=args.phase,
        dry_run=True if args.dry_run else None,
        source=TOPIC_VALID,
        dest=TOPIC_REPLAY_OUT,
        hours=args.hours,
    )
    print("Replay cycle:")
    print(json.dumps(result, indent=2, default=str))

    if args.dry_run or args.phase == "monitor":
        print("OK: plan/dry-run complete (no dest assert)")
        return 0

    time.sleep(2.0)
    after = topic_approx_count(TOPIC_REPLAY_OUT)
    grew = int(after.get("count") or 0) - int(before.get("count") or 0)
    print("Dest growth:", grew, json.dumps({"before": before, "after": after}, indent=2))
    if grew < 1:
        print("WARN: events.replay.out did not grow; check Replay* processors.", file=sys.stderr)
        return 1
    print("OK: replay published into events.replay.out")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
