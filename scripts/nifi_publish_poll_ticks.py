#!/usr/bin/env python3
"""Publish NiFi monitor poll ticks to Studio Kafka (continuous trigger stream).

Requires: ratatoskr kafka up

  python scripts/nifi_publish_poll_ticks.py --count 5 --interval 2
  python examples/agents/run_workflow_nifi_monitor_local.py --kafka-topic nifi.monitor.poll
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
    from ratatoskr.docker_utils import container_id, project_root
    from ratatoskr.constants import KAFKA_PROFILE
    import subprocess

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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="nifi.monitor.poll")
    parser.add_argument("--count", type=int, default=5, help="0 = forever")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--phase", default=os.environ.get("NIFI_HEAL_PHASE", "monitor"))
    parser.add_argument(
        "--process-group-id",
        default=os.environ.get("NIFI_PROCESS_GROUP_ID", "root"),
    )
    args = parser.parse_args()

    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    _ensure_topic(args.topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    n = 0
    print(f"Publishing to {args.topic} @ {bootstrap}")
    try:
        while True:
            n += 1
            payload = {
                "tick": n,
                "phase": args.phase,
                "process_group_id": args.process_group_id,
                "ts": time.time(),
            }
            producer.send(args.topic, payload)
            producer.flush()
            print({"published": payload})
            if args.count > 0 and n >= args.count:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
