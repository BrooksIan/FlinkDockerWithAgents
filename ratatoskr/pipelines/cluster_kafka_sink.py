"""Flink Agents sink helpers for publishing pipeline output to Kafka on cluster."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flink_agents.api.events.event import Event
    from flink_agents.api.runner_context import RunnerContext
    from ratatoskr.pipelines.models import Pipeline


def _record_for_kafka(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        if "output" in payload or "key" in payload or "value" in payload:
            return dict(payload)
        return {"key": "1", "output": payload}
    return {"key": "1", "output": payload}


def deliver_pipeline_kafka_sink(
    pipeline: "Pipeline",
    *,
    root: Path | None = None,
    profile: str | None = None,
) -> int | None:
    """Publish pipeline output to Kafka after cluster submit (agents run in JobManager)."""
    from ratatoskr.constants import DEFAULT_PROFILE
    from ratatoskr.pipelines.docker_runner import run_pipeline_in_container
    from ratatoskr.pipelines.executor import _deliver_sink_output

    sink = next((n for n in pipeline.nodes if n.kind == "sink"), None)
    if sink is None:
        return None
    sink_type = str(sink.config.get("sink_type") or "capture").strip().lower()
    if sink_type != "kafka":
        return None

    # Window/streaming runners embed FlinkKafkaProducer; batch agents jobs use post-submit delivery.
    from ratatoskr.pipelines.window_config import pipeline_window_node

    if pipeline_window_node(pipeline) is not None:
        return None

    output, _steps = run_pipeline_in_container(
        pipeline,
        profile=profile or DEFAULT_PROFILE,
    )
    _deliver_sink_output(sink.config, output)
    return len(output)


def publish_record_to_kafka(
    topic: str,
    event: "Event",
    ctx: "RunnerContext",
    *,
    bootstrap: str | None = None,
) -> None:
    """Publish one agent input event to Kafka and forward it downstream."""
    from flink_agents.api.events.event import InputEvent, OutputEvent
    from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers, publish_topic_records

    topic = topic.strip()
    if not topic:
        raise ValueError("Kafka sink missing topic")

    payload = InputEvent.from_event(event).input
    row = _record_for_kafka(payload)
    servers = bootstrap
    if not servers:
        servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or cluster_kafka_bootstrap_servers()
    publish_topic_records(topic, [row], bootstrap=servers)
    output = row.get("output", row.get("value", row))
    ctx.send_event(OutputEvent(output=output))


def publish_output_to_kafka(
    topic: str,
    event: "Event",
    ctx: "RunnerContext",
    *,
    bootstrap: str | None = None,
) -> None:
    """Publish an OutputEvent to Kafka and forward it downstream."""
    from flink_agents.api.events.event import OutputEvent
    from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers, publish_topic_records

    topic = topic.strip()
    if not topic:
        raise ValueError("Kafka sink missing topic")

    output = OutputEvent.from_event(event).output
    row = _record_for_kafka(output)
    servers = bootstrap
    if not servers:
        servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or cluster_kafka_bootstrap_servers()
    publish_topic_records(topic, [row], bootstrap=servers)
    ctx.send_event(OutputEvent(output=output))
