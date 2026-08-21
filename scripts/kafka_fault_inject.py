#!/usr/bin/env python3
"""Inject faults into Studio Kafka for kafka monitor heal demos.

Examples:
  python scripts/kafka_fault_inject.py --delete-topic          # 1B safe heal
  python scripts/kafka_fault_inject.py --lag-group             # LAG / stalled group
  python scripts/kafka_fault_inject.py --empty-lag-group       # members=0 + lag
  python scripts/kafka_fault_inject.py --lab-demo              # lag group for 1C
  python scripts/kafka_fault_inject.py --restore
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

DEFAULT_DEMO_TOPIC = "kafka.monitor.poll"
DEFAULT_LAB_GROUP = "ratatoskr-kafka-fault-lab"


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def inject_delete_topic(client, topic: str) -> dict:
    live = client.list_topics()
    if topic not in live:
        return {"deleted": False, "topic": topic, "note": "already absent"}
    client.delete_topic(topic)
    # Broker may take a moment to drop metadata
    for _ in range(20):
        if topic not in client.list_topics():
            break
        time.sleep(0.25)
    return {"deleted": True, "topic": topic}


def inject_lag_group(
    client,
    *,
    topic: str,
    group_id: str,
    messages: int = 50,
) -> dict:
    """Produce messages and commit the group to earliest → lag with optional members=0."""
    from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition

    if topic not in client.list_topics():
        client.create_topic(topic)

    producer = KafkaProducer(
        bootstrap_servers=client.bootstrap,
        client_id="ratatoskr-kafka-fault-producer",
    )
    try:
        for i in range(max(1, messages)):
            producer.send(topic, json.dumps({"fault": True, "i": i}).encode("utf-8"))
        producer.flush()
    finally:
        producer.close()

    consumer = KafkaConsumer(
        bootstrap_servers=client.bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
        client_id="ratatoskr-kafka-fault-consumer",
    )
    try:
        parts = consumer.partitions_for_topic(topic) or set()
        if not parts:
            raise RuntimeError(f"topic {topic!r} has no partitions")
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        consumer.assign(tps)
        begins = consumer.beginning_offsets(tps)
        consumer.commit({tp: OffsetAndMetadata(begins[tp], "", -1) for tp in tps})
    finally:
        consumer.close()

    lag = client.consumer_group_lag(group_id)
    return {
        "lag_group": True,
        "topic": topic,
        "group_id": group_id,
        "messages": messages,
        "lag": lag.get("lag"),
        "note": "consumer closed → members=0 while lag remains",
    }


def restore_catalog(client) -> dict:
    from ratatoskr.kafka.client import canonical_topic_catalog

    catalog = canonical_topic_catalog()
    live = client.list_topics()
    created = []
    for name, meta in catalog.items():
        if name in live:
            continue
        try:
            client.create_topic(
                name,
                partitions=meta.get("partitions"),
                replication_factor=meta.get("replication_factor"),
            )
            created.append(name)
        except Exception as exc:  # noqa: BLE001
            created.append(f"{name}:ERROR:{exc}")
    # Wait briefly for creates
    if created:
        time.sleep(1.0)
    return {
        "restored": True,
        "created": created,
        "live_topics": len(client.list_topics()),
        "catalog_topics": len(catalog),
    }


def inject_undersize_topic(client, topic: str, *, partitions: int = 1) -> dict:
    """Recreate topic with fewer partitions than catalog (lab increase_partitions)."""
    live = client.list_topics()
    if topic in live:
        client.delete_topic(topic)
    # Broker may keep the name "marked for deletion" briefly — wait it out.
    for _ in range(60):
        if topic not in client.list_topics():
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"topic {topic!r} still present after delete")
    last_err = None
    for _ in range(10):
        try:
            client.create_topic(topic, partitions=max(1, partitions))
            last_err = None
            break
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(0.75)
    if last_err is not None:
        raise RuntimeError(f"create after undersize delete failed: {last_err}") from last_err
    time.sleep(0.5)
    details = client.describe_topics([topic])
    have = int((details[0].get("partition_count") if details else 0) or 0)
    return {
        "undersize": True,
        "topic": topic,
        "partition_count": have,
        "note": "raise KAFKA_TOPIC_PARTITIONS above this for TOPIC_PARTITIONS_LOW",
    }


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--delete-topic",
        nargs="?",
        const=DEFAULT_DEMO_TOPIC,
        metavar="TOPIC",
        help=f"Delete a catalog topic (default {DEFAULT_DEMO_TOPIC}) for safe create_topic",
    )
    parser.add_argument(
        "--undersize-topic",
        nargs="?",
        const=DEFAULT_DEMO_TOPIC,
        metavar="TOPIC",
        help="Recreate topic with 1 partition (pair with KAFKA_TOPIC_PARTITIONS>1)",
    )
    parser.add_argument(
        "--lag-group",
        action="store_true",
        help=f"Build lag on group {DEFAULT_LAB_GROUP} (produce + commit earliest)",
    )
    parser.add_argument(
        "--empty-lag-group",
        action="store_true",
        help="Alias for --lag-group (closed consumer ⇒ empty + stalled)",
    )
    parser.add_argument(
        "--lab-demo",
        action="store_true",
        help="Lag/empty group fault for Phase lab (use with KAFKA_HEAL_ALLOW_GROUPS)",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_DEMO_TOPIC,
        help=f"Topic for lag inject (default {DEFAULT_DEMO_TOPIC})",
    )
    parser.add_argument(
        "--group",
        default=DEFAULT_LAB_GROUP,
        help=f"Consumer group id (default {DEFAULT_LAB_GROUP})",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=50,
        help="Messages to produce for lag inject",
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Recreate any missing studio/full catalog topics",
    )
    args = parser.parse_args()

    from ratatoskr.kafka.client import KafkaClient

    client = KafkaClient()
    try:
        if args.restore:
            print(restore_catalog(client))
            return 0

        if args.delete_topic is not None:
            print(inject_delete_topic(client, args.delete_topic))
            return 0

        if args.undersize_topic is not None:
            print(inject_undersize_topic(client, args.undersize_topic, partitions=1))
            return 0

        if args.lab_demo or args.lag_group or args.empty_lag_group:
            print(
                inject_lag_group(
                    client,
                    topic=args.topic,
                    group_id=args.group,
                    messages=args.messages,
                )
            )
            return 0

        parser.print_help()
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
