#!/usr/bin/env python3
"""Publish monitor poll ticks to Studio Kafka (drives continuous cluster jobs).

Requires: ``ratatoskr kafka up``

Examples:
  # Forever ticks for both NiFi + Kafka monitor topics (default interval 10s)
  python scripts/publish_monitor_poll_ticks.py --continuous

  # Finite burst
  python scripts/publish_monitor_poll_ticks.py --count 5 --interval 2 --target nifi

  # Pair with:
  ratatoskr agent run workflow_nifi_monitor --cluster --continuous
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _ensure_topic(topic: str) -> None:
    import subprocess

    from ratatoskr.constants import KAFKA_PROFILE
    from ratatoskr.docker_utils import container_id, project_root

    cid = container_id("kafka", profile=KAFKA_PROFILE)
    if not cid:
        return
    subprocess.run(
        [
            "docker",
            "exec",
            cid,
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        cwd=project_root(),
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> int:
    _bootstrap()
    from ratatoskr.monitor_mode import (
        DEFAULT_MONITOR_INTERVAL_SEC,
        kafka_poll_topic,
        monitor_interval_sec,
        nifi_poll_topic,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=("nifi", "kafka", "both"),
        default="both",
        help="Which poll topic(s) to publish",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Publish forever (same as --count 0)",
    )
    parser.add_argument("--count", type=int, default=5, help="0 = forever")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help=f"Seconds between ticks (default {DEFAULT_MONITOR_INTERVAL_SEC})",
    )
    parser.add_argument(
        "--phase",
        default=os.environ.get("NIFI_HEAL_PHASE")
        or os.environ.get("KAFKA_HEAL_PHASE")
        or "monitor",
    )
    parser.add_argument(
        "--process-group-id",
        default=os.environ.get("NIFI_PROCESS_GROUP_ID", "root"),
    )
    args = parser.parse_args()

    interval = (
        float(args.interval)
        if args.interval is not None
        else monitor_interval_sec(DEFAULT_MONITOR_INTERVAL_SEC)
    )
    count = 0 if args.continuous else args.count

    topics: list[str] = []
    if args.target in ("nifi", "both"):
        topics.append(nifi_poll_topic())
    if args.target in ("kafka", "both"):
        topics.append(kafka_poll_topic())

    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    for topic in topics:
        _ensure_topic(topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(
        f"Publishing to {topics} @ {bootstrap} "
        f"interval={interval}s count={'forever' if count == 0 else count} "
        f"phase={args.phase}"
    )
    n = 0
    try:
        while True:
            n += 1
            payload = {
                "tick": n,
                "phase": args.phase,
                "process_group_id": args.process_group_id,
                "ts": time.time(),
            }
            for topic in topics:
                producer.send(topic, payload)
            producer.flush()
            print(f"tick {n} → {topics}", flush=True)
            if count > 0 and n >= count:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped publishing.", flush=True)
    finally:
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
