"""Shared helpers for continuous monitor cluster jobs."""

from __future__ import annotations

import json
import os
import time
from typing import Any


def _parse_tick(raw: str, *, defaults: dict[str, Any]) -> dict[str, Any]:
    value = dict(defaults)
    try:
        payload = json.loads(raw) if raw else {}
        if isinstance(payload, dict):
            value.update({k: v for k, v in payload.items() if v is not None})
    except json.JSONDecodeError:
        pass
    tick = value.get("tick", 0)
    return {"key": f"poll-{tick}", "value": value}


def interval_tick_stream(
    env,
    *,
    interval_sec: float,
    defaults: dict[str, Any],
):
    """
    Unbounded in-job tick source (no Kafka).

    Uses ``NumberSequenceSource`` over a huge range plus a rate-limit sleep.
    Preferred for ``--cluster --continuous`` because Studio Kafka's EXTERNAL
    listener advertises ``localhost:9094``, which TaskManagers cannot use.
    """
    from pyflink.common import WatermarkStrategy
    from pyflink.datastream.connectors.number_seq import NumberSequenceSource

    interval = max(1.0, float(interval_sec))
    base = dict(defaults)
    # Practical infinity for continuous monitor polls.
    source = NumberSequenceSource(1, 10**12)
    nums = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "monitor-interval-ticks",
    )

    def _to_record(n: int) -> dict:
        value = dict(base)
        value["tick"] = int(n)
        value["ts"] = time.time()
        # Sleep after building the record so the first poll is prompt.
        time.sleep(interval)
        return {"key": f"poll-{int(n)}", "value": value}

    return nums.map(_to_record)


def kafka_tick_stream(
    env,
    *,
    topic: str,
    bootstrap: str,
    group_id: str,
    defaults: dict[str, Any],
):
    """Unbounded FlinkKafkaConsumer → {key, value} (requires TM→broker reachability)."""
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

    from ratatoskr.runtime.kafka_jars import attach_kafka_jars

    attach_kafka_jars(env)
    props = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": os.environ.get("MONITOR_KAFKA_OFFSET", "latest"),
    }
    consumer = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=props,
    )

    def _map(raw: str) -> dict:
        return _parse_tick(raw, defaults=defaults)

    return env.add_source(consumer).map(_map)


def use_kafka_ticks() -> bool:
    """Opt-in Kafka tick source: MONITOR_CONTINUOUS_SOURCE=kafka."""
    raw = (os.environ.get("MONITOR_CONTINUOUS_SOURCE") or "interval").strip().lower()
    return raw in ("kafka", "ticks", "topic")


def resolve_cluster_bootstrap() -> str:
    from pathlib import Path

    from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers

    bootstrap = (os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or "").strip()
    if bootstrap:
        return bootstrap
    if Path("/opt/flink").is_dir():
        try:
            import socket

            socket.getaddrinfo("kafka", 9092)
            return "kafka:9092"
        except OSError:
            return cluster_kafka_bootstrap_servers()
    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    return kafka_bootstrap_servers()
