#!/usr/bin/env python3
"""Local / continuous runner for ``workflow_nifi_monitor``.

Modes:
  - one-shot (default)
  - ``--interval SEC`` continuous host polling (``--count 0`` = forever)
  - ``--kafka-topic TOPIC`` consume poll triggers from Studio Kafka

Prefers Flink Agents when installed; otherwise direct ``ratatoskr.nifi`` polls.
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


def _one_cycle() -> dict:
    from ratatoskr.nifi.client import NiFiClient, heal_phase
    from ratatoskr.nifi.policy import run_monitor_cycle

    phase = os.environ.get("NIFI_HEAL_PHASE") or heal_phase()
    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    return run_monitor_cycle(NiFiClient(), pg, phase=phase)


def _print_result(result: dict, *, label: str) -> None:
    print(label)
    print(json.dumps(result, indent=2, default=str))
    print("---", flush=True)


def _run_direct_loop(*, interval: float, count: int) -> int:
    n = 0
    while True:
        n += 1
        result = _one_cycle()
        _print_result(
            result,
            label=f"NiFi monitor poll #{n} (direct host — interval={interval}s)",
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
    for msg in consumer:
        n += 1
        # Optional JSON payload may override phase / pg
        phase = os.environ.get("NIFI_HEAL_PHASE", "monitor")
        pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
        try:
            payload = json.loads(msg.value) if msg.value else {}
            if isinstance(payload, dict):
                phase = payload.get("phase") or phase
                pg = payload.get("process_group_id") or pg
        except json.JSONDecodeError:
            pass
        os.environ["NIFI_HEAL_PHASE"] = str(phase)
        os.environ["NIFI_PROCESS_GROUP_ID"] = str(pg)
        result = _one_cycle()
        _print_result(result, label=f"NiFi monitor Kafka poll #{n} (offset={msg.offset})")
    return 0


def _run_flink_agents_oneshot() -> int:
    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.workflow_nifi_monitor import NiFiMonitorAgent

    env = AgentsExecutionEnvironment.get_execution_environment()
    input_data = [
        {
            "key": "poll-1",
            "value": {
                "process_group_id": os.environ.get("NIFI_PROCESS_GROUP_ID", "root"),
                "phase": os.environ["NIFI_HEAL_PHASE"],
            },
        },
    ]
    agent = NiFiMonitorAgent()
    output_data = env.from_list(input_data).apply(agent).to_list()
    env.execute()
    print("NiFi monitor results (Flink Agents local runner):")
    for record in output_data:
        print(record)
    return 0


def main() -> int:
    _bootstrap()
    os.environ.setdefault("NIFI_HEAL_PHASE", "monitor")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Seconds between polls (host continuous mode). 0 = one-shot.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of polls when --interval > 0 (0 = forever until Ctrl-C).",
    )
    parser.add_argument(
        "--kafka-topic",
        default="",
        help="Consume poll triggers from this Kafka topic (requires ratatoskr kafka up).",
    )
    parser.add_argument(
        "--kafka-group",
        default="ratatoskr-nifi-monitor",
        help="Kafka consumer group id",
    )
    args = parser.parse_args()

    if args.kafka_topic:
        return _run_kafka_consumer(topic=args.kafka_topic, group=args.kafka_group)

    if args.interval > 0:
        return _run_direct_loop(interval=args.interval, count=args.count)

    try:
        import flink_agents  # noqa: F401
    except ImportError:
        result = _one_cycle()
        _print_result(result, label="NiFi monitor results (direct host runner — flink_agents not on PATH):")
        return 0
    return _run_flink_agents_oneshot()


if __name__ == "__main__":
    raise SystemExit(main())
