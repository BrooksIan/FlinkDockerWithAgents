"""Pipeline graph validation."""

from __future__ import annotations

from typing import Any

from apemosyne.agents.registry import list_agent_names
from apemosyne.pipelines.models import Pipeline

MAX_NODES = 5

# Known schema hints for cross-agent edge warnings.
_AGENT_INPUT_FIELDS: dict[str, set[str]] = {
    "workflow_counter": {"value"},
    "react_echo": {"message"},
}

_AGENT_OUTPUT_FIELDS: dict[str, set[str]] = {
    "workflow_counter": {"input", "doubled", "agent"},
    "react_echo": {"message", "severity", "summary", "agent", "pattern"},
}


def validate_pipeline(pipeline: Pipeline) -> dict[str, Any]:
    """Return {valid, errors, warnings}."""
    errors: list[str] = []
    warnings: list[str] = []

    if len(pipeline.nodes) > MAX_NODES:
        errors.append(f"Pipeline has {len(pipeline.nodes)} nodes; maximum is {MAX_NODES}")

    sources = [n for n in pipeline.nodes if n.kind == "source"]
    sinks = [n for n in pipeline.nodes if n.kind == "sink"]
    agents = [n for n in pipeline.nodes if n.kind == "agent"]

    if len(sources) != 1:
        errors.append("Pipeline must have exactly one source node")
    if len(sinks) != 1:
        errors.append("Pipeline must have exactly one sink node")
    if not agents:
        errors.append("Pipeline must include at least one agent node")

    known_agents = set(list_agent_names())
    node_ids = {n.id for n in pipeline.nodes}
    for node in pipeline.nodes:
        if node.kind == "source":
            source_type = str(node.config.get("source_type") or "records").strip().lower()
            if source_type == "kafka":
                topic = str(node.config.get("topic") or "").strip()
                if not topic:
                    errors.append("Kafka source node missing topic")
            elif not node.config.get("records"):
                errors.append("Source node has no input records")
        if node.kind == "sink":
            sink_type = str(node.config.get("sink_type") or "capture").strip().lower()
            if sink_type == "kafka":
                topic = str(node.config.get("topic") or "").strip()
                if not topic:
                    errors.append("Kafka sink node missing topic")
        if node.kind == "agent":
            if not node.agent:
                errors.append(f"Agent node {node.id!r} missing agent name")
            elif node.agent not in known_agents:
                errors.append(f"Unknown agent {node.agent!r} on node {node.id!r}")

    for edge in pipeline.edges:
        if edge.source not in node_ids:
            errors.append(f"Edge {edge.id!r} references unknown source {edge.source!r}")
        if edge.target not in node_ids:
            errors.append(f"Edge {edge.id!r} references unknown target {edge.target!r}")

    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}

    adjacency: dict[str, list[str]] = {n.id: [] for n in pipeline.nodes}
    in_degree: dict[str, int] = {n.id: 0 for n in pipeline.nodes}
    for edge in pipeline.edges:
        adjacency.setdefault(edge.source, []).append(edge.target)
        in_degree[edge.target] = in_degree.get(edge.target, 0) + 1

    if _has_cycle(adjacency, list(node_ids)):
        errors.append("Pipeline graph contains a cycle")

    ordered = _topological_sort(adjacency, in_degree)
    if ordered and not _is_linear_chain(ordered, pipeline):
        errors.append("MVP supports linear pipelines only (Source → agents → Sink, no forks)")

    if len(pipeline.edges) != len(pipeline.nodes) - 1:
        errors.append(
            f"Expected {len(pipeline.nodes) - 1} edges for a linear chain, got {len(pipeline.edges)}"
        )

    _check_edge_mappings(pipeline, warnings)
    _check_kafka_nodes(pipeline, warnings)

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def _check_kafka_node(
    node_kind: str,
    config: dict[str, Any],
    *,
    warnings: list[str],
    known: set[str],
) -> None:
    type_key = "source_type" if node_kind == "source" else "sink_type"
    kafka_type = "kafka"
    node_type = str(config.get(type_key) or ("records" if node_kind == "source" else "capture")).strip().lower()
    if node_type != kafka_type:
        return
    topic = str(config.get("topic") or "").strip()
    if known and topic and topic not in known:
        warnings.append(
            f"Kafka topic {topic!r} is not a standard pipeline topic; ensure it exists on the broker"
        )


def _check_kafka_nodes(pipeline: Pipeline, warnings: list[str]) -> None:
    from apemosyne.kafka_sources import kafka_reachable, known_pipeline_topics

    known = set(known_pipeline_topics())
    kafka_configured = False
    for node in pipeline.nodes:
        if node.kind == "source":
            _check_kafka_node("source", node.config, warnings=warnings, known=known)
            if str(node.config.get("source_type") or "").strip().lower() == "kafka":
                kafka_configured = True
        if node.kind == "sink":
            _check_kafka_node("sink", node.config, warnings=warnings, known=known)
            if str(node.config.get("sink_type") or "").strip().lower() == "kafka":
                kafka_configured = True

    if kafka_configured and not kafka_reachable():
        warnings.append(
            "Kafka broker unreachable from the host — pipeline runs will use the Docker "
            "Kafka container when available, or start the full stack: apemosyne up --profile full"
        )


def _has_cycle(adjacency: dict[str, list[str]], nodes: list[str]) -> bool:
    visited: set[str] = set()
    stack: set[str] = set()

    def visit(node: str) -> bool:
        if node in stack:
            return True
        if node in visited:
            return False
        visited.add(node)
        stack.add(node)
        for nxt in adjacency.get(node, []):
            if visit(nxt):
                return True
        stack.remove(node)
        return False

    return any(visit(n) for n in nodes)


def _topological_sort(
    adjacency: dict[str, list[str]], in_degree: dict[str, int]
) -> list[str]:
    queue = [n for n, d in in_degree.items() if d == 0]
    order: list[str] = []
    degree = dict(in_degree)
    while queue:
        node = queue.pop(0)
        order.append(node)
        for nxt in adjacency.get(node, []):
            degree[nxt] -= 1
            if degree[nxt] == 0:
                queue.append(nxt)
    return order if len(order) == len(in_degree) else []


def _is_linear_chain(order: list[str], pipeline: Pipeline) -> bool:
    if len(order) != len(pipeline.nodes):
        return False
    by_id = {n.id: n for n in pipeline.nodes}
    kinds = [by_id[nid].kind for nid in order]
    if kinds[0] != "source" or kinds[-1] != "sink":
        return False
    for kind in kinds[1:-1]:
        if kind != "agent":
            return False
    return True


def _check_edge_mappings(pipeline: Pipeline, warnings: list[str]) -> None:
    by_id = {n.id: n for n in pipeline.nodes}
    for edge in pipeline.edges:
        src = by_id.get(edge.source)
        tgt = by_id.get(edge.target)
        if not src or not tgt or src.kind != "agent" or tgt.kind != "agent":
            if src and src.kind == "agent" and tgt and tgt.kind == "agent":
                pass
            continue
        if src.kind == "agent" and tgt.kind == "agent" and not edge.mapping:
            out_fields = _AGENT_OUTPUT_FIELDS.get(src.agent or "", set())
            in_fields = _AGENT_INPUT_FIELDS.get(tgt.agent or "", set())
            if out_fields and in_fields and not (out_fields & in_fields):
                warnings.append(
                    f"Edge {edge.source} → {edge.target} may need field mapping "
                    f"(output {sorted(out_fields)} vs input {sorted(in_fields)})"
                )
