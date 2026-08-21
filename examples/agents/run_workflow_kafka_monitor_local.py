#!/usr/bin/env python3
"""Local / continuous runner for ``workflow_kafka_monitor``.

Modes:
  - one-shot (default)
  - ``--continuous`` / ``--interval SEC`` host polling (``--count 0`` = forever)
  - ``--kafka-topic TOPIC`` consume poll triggers from Studio Kafka
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _one_cycle(*, previous_health: dict | None = None) -> dict:
    from ratatoskr.kafka import KafkaClient, heal_phase, run_monitor_cycle

    phase = os.environ.get("KAFKA_HEAL_PHASE") or heal_phase()
    client = KafkaClient()
    try:
        return run_monitor_cycle(
            client, phase=phase, previous_health=previous_health
        )
    finally:
        client.close()


def _print_result(result: dict, *, label: str) -> None:
    print(label)
    print(json.dumps(result, indent=2, default=str))
    print("---", flush=True)


def _health_snapshot(result: dict) -> dict:
    h = result.get("health") or {}
    return {
        "severities": h.get("severities"),
        "missing_topics": h.get("missing_topics"),
        "lag_warn_groups": h.get("lag_warn_groups"),
        "lag_crit_groups": h.get("lag_crit_groups"),
        "stalled_groups": h.get("stalled_groups"),
        "under_replicated_topics": h.get("under_replicated_topics"),
    }


def _run_direct_loop(*, interval: float, count: int) -> int:
    n = 0
    previous = None
    while True:
        n += 1
        result = _one_cycle(previous_health=previous)
        previous = _health_snapshot(result)
        _print_result(
            result,
            label=f"Kafka monitor poll #{n} (interval={interval}s)",
        )
        if count > 0 and n >= count:
            return 0
        time.sleep(interval)


def _run_kafka_consumer(*, topic: str, group: str) -> int:
    from kafka import KafkaConsumer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    print(f"Consuming poll triggers from {topic} @ {bootstrap} (group={group})")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: b.decode("utf-8", errors="replace"),
    )
    n = 0
    previous = None
    for msg in consumer:
        n += 1
        phase = os.environ.get("KAFKA_HEAL_PHASE", "monitor")
        try:
            payload = json.loads(msg.value) if msg.value else {}
            if isinstance(payload, dict) and payload.get("phase"):
                phase = str(payload["phase"])
        except json.JSONDecodeError:
            pass
        os.environ["KAFKA_HEAL_PHASE"] = phase
        result = _one_cycle(previous_health=previous)
        previous = _health_snapshot(result)
        _print_result(
            result, label=f"Kafka monitor Kafka poll #{n} (offset={msg.offset})"
        )
    return 0


def main() -> int:
    _bootstrap()
    os.environ.setdefault("KAFKA_HEAL_PHASE", "monitor")

    from ratatoskr.monitor_mode import (
        DEFAULT_MONITOR_INTERVAL_SEC,
        is_continuous,
        monitor_interval_sec,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Forever host polls (sets MONITOR_MODE=continuous; uses --interval).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help=f"Seconds between polls (default {DEFAULT_MONITOR_INTERVAL_SEC} when continuous).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of polls when interval > 0 (0 = forever until Ctrl-C).",
    )
    parser.add_argument(
        "--kafka-topic",
        default="",
        help="Consume poll triggers from this Kafka topic.",
    )
    parser.add_argument(
        "--kafka-group",
        default="ratatoskr-kafka-monitor",
        help="Kafka consumer group id",
    )
    args = parser.parse_args()

    if args.continuous or is_continuous():
        os.environ["MONITOR_MODE"] = "continuous"
        interval = (
            float(args.interval)
            if args.interval is not None and args.interval > 0
            else monitor_interval_sec(DEFAULT_MONITOR_INTERVAL_SEC)
        )
        return _run_direct_loop(interval=interval, count=args.count)

    if args.kafka_topic:
        return _run_kafka_consumer(topic=args.kafka_topic, group=args.kafka_group)

    if args.interval is not None and args.interval > 0:
        return _run_direct_loop(interval=args.interval, count=args.count)

    result = _one_cycle()
    _print_result(result, label="Kafka monitor results:")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
