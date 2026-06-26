#!/usr/bin/env python3
"""Cluster runner — Kafka (or demo collection) → dynamic session window → session detect."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _bootstrap() -> None:
    root = Path("/opt/flink")
    if root.is_dir():
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        return
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _kafka_bootstrap() -> str:
    from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers

    return (
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("SESSION_WINDOW_KAFKA_BOOTSTRAP")
        or cluster_kafka_bootstrap_servers()
    )


def _kafka_source(env, topic: str, bootstrap: str):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

    from examples.agents.session_window_fixtures import parse_kafka_line

    props = {
        "bootstrap.servers": bootstrap,
        "group.id": os.environ.get("SESSION_WINDOW_KAFKA_GROUP", "ratatoskr-session-window"),
        "auto.offset.reset": os.environ.get("SESSION_WINDOW_KAFKA_OFFSET", "earliest"),
    }
    consumer = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=props,
    )

    def _parse(raw: str) -> dict:
        event = parse_kafka_line(raw)
        if event is None:
            return {"src_ip": "unknown", "eventid": "unknown", "timestamp": 0}
        return event

    return env.add_source(consumer).map(_parse).filter(lambda e: e.get("src_ip") != "unknown")


def main() -> None:
    _bootstrap()
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.window import DynamicProcessingTimeSessionWindows

    from ratatoskr.runtime.kafka_jars import attach_kafka_jars
    from examples.agents.session_detect_logic import process_session_summary
    from examples.agents.session_window_fixtures import DEMO_TOPIC, demo_session_events
    from examples.agents.session_window_ops import CowrieActivityGapExtractor, SessionSummaryFunction

    use_kafka = _truthy("SESSION_WINDOW_KAFKA")
    topic = os.environ.get("SESSION_WINDOW_KAFKA_TOPIC", DEMO_TOPIC).strip() or DEMO_TOPIC

    env = StreamExecutionEnvironment.get_execution_environment()
    if use_kafka:
        attach_kafka_jars(env)
    env.set_parallelism(1)

    if use_kafka:
        stream = _kafka_source(env, topic, _kafka_bootstrap())
    else:
        stream = env.from_collection(demo_session_events())

    out = (
        stream.key_by(lambda e: str(e.get("src_ip") or "unknown"))
        .window(DynamicProcessingTimeSessionWindows.with_dynamic_gap(CowrieActivityGapExtractor()))
        .process(SessionSummaryFunction())
        .map(process_session_summary)
    )
    out.print()

    job_name = "Ratatoskr Session Window Detect"
    if use_kafka:
        job_name += f" (Kafka:{topic})"
    env.execute(job_name)


if __name__ == "__main__":
    main()
