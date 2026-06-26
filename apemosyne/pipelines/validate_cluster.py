"""Cluster submit validation for Studio pipelines."""

from __future__ import annotations

from typing import Any

from apemosyne.agents.published_copy import is_published_agent_spec
from apemosyne.agents.registry import AgentRegistryError, get_agent_spec
from apemosyne.pipelines.models import Pipeline
from apemosyne.pipelines.validate import validate_pipeline

PUBLISHED_REACT_CLUSTER_WARNING = (
    "Published ReAct agents are not reliable on Flink cluster yet (known Pemja classloader issue). "
    "Use Run locally for designer ReAct pipelines, or use a built-in workflow agent for cluster jobs."
)


def pipeline_published_react_agents(pipeline: Pipeline) -> list[str]:
    """Return agent slugs that are published designer ReAct definitions."""
    names: list[str] = []
    for node in pipeline.nodes:
        if node.kind != "agent" or not node.agent:
            continue
        try:
            spec = get_agent_spec(node.agent)
        except AgentRegistryError:
            continue
        if spec.type == "react" and is_published_agent_spec(spec):
            names.append(node.agent)
    return names


def published_react_cluster_warnings(pipeline: Pipeline) -> list[str]:
    agents = pipeline_published_react_agents(pipeline)
    if not agents:
        return []
    label = ", ".join(agents)
    return [
        f"Pipeline uses published ReAct agent(s) ({label}). {PUBLISHED_REACT_CLUSTER_WARNING}"
    ]


def validate_pipeline_cluster(pipeline: Pipeline) -> dict[str, Any]:
    """Return {valid, errors, warnings, mode} for cluster submission."""
    base = validate_pipeline(pipeline)
    errors = list(base["errors"])
    warnings = list(base["warnings"])

    source = next((n for n in pipeline.nodes if n.kind == "source"), None)
    sink = next((n for n in pipeline.nodes if n.kind == "sink"), None)

    mode = "batch"

    if source is None:
        errors.append("Pipeline must have a source node")
    else:
        source_type = str(source.config.get("source_type") or "records").strip().lower()
        if source_type == "kafka":
            errors.append(
                "Cluster streaming from Kafka is not supported yet — use static records for batch cluster jobs"
            )
        elif not source.config.get("records"):
            errors.append("Cluster submit requires source records")

    if sink is None:
        errors.append("Pipeline must have a sink node")
    else:
        sink_type = str(sink.config.get("sink_type") or "capture").strip().lower()
        if sink_type == "kafka":
            topic = str(sink.config.get("topic") or "").strip()
            if not topic:
                from apemosyne.kafka_sources import DEFAULT_KAFKA_OUTPUT_TOPIC

                topic = DEFAULT_KAFKA_OUTPUT_TOPIC
            mode = "batch_kafka_sink"
            try:
                from apemosyne.kafka_sources import kafka_reachable

                if not kafka_reachable():
                    warnings.append(
                        "Kafka broker unreachable — start Studio Kafka: "
                        "apemosyne kafka up"
                    )
            except Exception:
                warnings.append("Could not verify Kafka broker reachability")

    warnings.extend(published_react_cluster_warnings(pipeline))

    return {
        "valid": base["valid"] and not errors,
        "errors": errors,
        "warnings": warnings,
        "mode": mode,
    }
