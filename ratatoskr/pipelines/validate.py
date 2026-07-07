"""Pipeline graph validation."""

from __future__ import annotations

from typing import Any

from ratatoskr.agents.registry import list_agent_names
from ratatoskr.pipelines.models import Pipeline

MAX_NODES = 6

# Known schema hints for cross-agent edge warnings.
_AGENT_INPUT_FIELDS: dict[str, set[str]] = {
    "workflow_counter": {"value"},
    "react_echo": {"message"},
    "session_detect": {"events", "event_count", "src_ip"},
}

_AGENT_OUTPUT_FIELDS: dict[str, set[str]] = {
    "workflow_counter": {"input", "doubled", "agent"},
    "react_echo": {"message", "severity", "summary", "agent", "pattern"},
    "session_detect": {
        "src_ip",
        "severity",
        "event_count",
        "first_ts",
        "last_ts",
        "response_actions",
        "agent",
    },
}


def validate_pipeline(
    pipeline: Pipeline,
    *,
    extra_known_agents: set[str] | None = None,
) -> dict[str, Any]:
    """Return {valid, errors, warnings}."""
    errors: list[str] = []
    warnings: list[str] = []

    if len(pipeline.nodes) > MAX_NODES:
        errors.append(f"Pipeline has {len(pipeline.nodes)} nodes; maximum is {MAX_NODES}")

    sources = [n for n in pipeline.nodes if n.kind == "source"]
    sinks = [n for n in pipeline.nodes if n.kind == "sink"]
    agents = [n for n in pipeline.nodes if n.kind == "agent"]
    windows = [n for n in pipeline.nodes if n.kind == "window"]

    from ratatoskr.pipelines.agent_settings import is_self_sourcing

    has_self_sourcing_agent = any(is_self_sourcing(n.agent) for n in agents)
    if len(sources) > 1:
        errors.append("Pipeline must have at most one source node")
    elif len(sources) == 0 and not has_self_sourcing_agent:
        errors.append("Pipeline must have exactly one source node")
    if len(sinks) != 1:
        errors.append("Pipeline must have exactly one sink node")
    if not agents:
        errors.append("Pipeline must include at least one agent node")
    if len(windows) > 1:
        errors.append("Pipeline may include at most one window node")

    known_agents = set(list_agent_names())
    if extra_known_agents:
        known_agents |= extra_known_agents
    node_ids = {n.id for n in pipeline.nodes}
    if len(node_ids) != len(pipeline.nodes):
        errors.append("Duplicate node ids in graph")

    pipeline.edges = [
        edge
        for edge in pipeline.edges
        if edge.source in node_ids and edge.target in node_ids
    ]

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
                    from ratatoskr.kafka_sources import DEFAULT_KAFKA_OUTPUT_TOPIC

                    warnings.append(
                        f"Kafka sink has no topic; using default {DEFAULT_KAFKA_OUTPUT_TOPIC!r}"
                    )
        if node.kind == "agent":
            if not node.agent:
                errors.append(f"Agent node {node.id!r} missing agent name")
            elif node.agent not in known_agents:
                errors.append(f"Unknown agent {node.agent!r} on node {node.id!r}")
            else:
                from ratatoskr.pipelines.agent_settings import missing_required_settings

                missing = missing_required_settings(node.agent, node.config)
                if missing:
                    labels = ", ".join(missing)
                    errors.append(
                        f"Agent {node.agent!r} on node {node.id!r} missing required settings: {labels}"
                    )
        if node.kind == "window":
            _validate_window_node(node, errors, warnings)

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

    ordered = _validate_linear_topology(pipeline, adjacency, in_degree, errors)
    if ordered:
        _check_window_topology(pipeline, ordered, errors, warnings)

    _check_edge_mappings(pipeline, warnings)
    _check_kafka_nodes(pipeline, warnings, errors)

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


def _check_kafka_nodes(pipeline: Pipeline, warnings: list[str], errors: list[str]) -> None:
    from ratatoskr.kafka_sources import kafka_reachable, known_pipeline_topics

    known = set(known_pipeline_topics())
    kafka_configured = False
    for node in pipeline.nodes:
        if node.kind == "source":
            source_type = str(node.config.get("source_type") or "records").strip().lower()
            if source_type == "kafka":
                kafka_configured = True
                if not any(n.kind == "window" for n in pipeline.nodes):
                    errors.append(
                        "Kafka source requires a dynamic session window node directly after the source"
                    )
            _check_kafka_node("source", node.config, warnings=warnings, known=known)
        if node.kind == "sink":
            _check_kafka_node("sink", node.config, warnings=warnings, known=known)
            if str(node.config.get("sink_type") or "").strip().lower() == "kafka":
                kafka_configured = True

    if kafka_configured and not kafka_reachable():
        warnings.append(
            "Kafka broker unreachable from the host — start Studio Kafka: "
            "ratatoskr kafka up  (or honeypot: ratatoskr up --profile full)"
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


def _validate_linear_topology(
    pipeline: Pipeline,
    adjacency: dict[str, list[str]],
    in_degree: dict[str, int],
    errors: list[str],
) -> list[str]:
    """Validate a single-path pipeline and return execution order when valid."""
    by_id = {n.id: n for n in pipeline.nodes}
    node_ids = set(by_id)
    expected_edges = len(pipeline.nodes) - 1
    actual_edges = sum(len(targets) for targets in adjacency.values())

    if len(node_ids) > 1:
        for nid in node_ids:
            if in_degree.get(nid, 0) == 0 and not adjacency.get(nid):
                kind = by_id[nid].kind
                label = kind if kind != "agent" else f"agent {by_id[nid].agent}"
                errors.append(
                    f"Node {label!r} is not connected — click Connect chain in Studio"
                )

    for nid, targets in adjacency.items():
        if len(targets) > 1:
            errors.append(
                f"Node {nid!r} splits into multiple branches; use a linear chain only"
            )

    for nid, degree in in_degree.items():
        if degree > 1:
            errors.append(
                f"Node {nid!r} merges multiple inputs; use a linear chain only"
            )

    if actual_edges != expected_edges:
        errors.append(
            f"Expected {expected_edges} edges for source → window? → agents → sink, "
            f"got {actual_edges}. Click Connect chain to rewire."
        )

    ordered = _topological_sort(adjacency, in_degree)
    if not ordered:
        if not any("edge" in err.lower() or "connected" in err.lower() for err in errors):
            errors.append("Pipeline graph is not fully connected")
        return []

    if _is_linear_chain(ordered, pipeline):
        return ordered

    from ratatoskr.pipelines.agent_settings import is_self_sourcing

    kinds = [by_id[nid].kind for nid in ordered]
    head = by_id[ordered[0]]
    head_ok = kinds[0] == "source" or (kinds[0] == "agent" and is_self_sourcing(head.agent))
    has_window = any(n.kind == "window" for n in pipeline.nodes)
    if has_window and "window" not in kinds[1:-1]:
        errors.append(
            "Window node is not in the execution path — click Connect chain to wire "
            "source → window → agent → sink"
        )
    elif not head_ok:
        errors.append("Pipeline must start with a source node")
    elif kinds[-1] != "sink":
        errors.append("Pipeline must end with a sink node")
    else:
        errors.append(
            "Invalid node order — allowed layout is source → window (optional) → "
            "agent(s) → sink"
        )
    return ordered


def _validate_window_node(node, errors: list[str], warnings: list[str]) -> None:
    from ratatoskr.pipelines.window_config import (
        EXECUTION_AGENT_BRIDGE,
        WINDOW_TYPE_DYNAMIC_SESSION,
        parse_window_config,
    )

    parsed = parse_window_config(node.config)
    if parsed.window_type != WINDOW_TYPE_DYNAMIC_SESSION:
        errors.append(f"Unsupported window_type {parsed.window_type!r} on node {node.id!r}")
    if not parsed.key_field:
        errors.append(f"Window node {node.id!r} missing key_field")
    if parsed.execution_mode == EXECUTION_AGENT_BRIDGE and not parsed.bridge_topic:
        warnings.append(
            "Agent bridge mode will use an auto-generated Kafka topic unless bridge_topic is set"
        )


def _check_window_topology(
    pipeline: Pipeline,
    order: list[str],
    errors: list[str],
    warnings: list[str],
) -> None:
    from ratatoskr.pipelines.window_config import agent_suggested_for_window, parse_window_config

    by_id = {n.id: n for n in pipeline.nodes}
    windows = [n for n in pipeline.nodes if n.kind == "window"]
    if not windows:
        return

    window = windows[0]
    if order and order[1] != window.id:
        errors.append("Window node must immediately follow the source node")

    parsed = parse_window_config(window.config)
    for node_id in order:
        node = by_id[node_id]
        if node.kind != "agent" or not node.agent:
            continue
        hint = agent_suggested_for_window(node.agent, parsed)
        if hint:
            warnings.append(hint)

    downstream_agents = [
        by_id[nid].agent
        for nid in order
        if by_id[nid].kind == "agent" and by_id[nid].agent
    ]
    if downstream_agents:
        warnings.append(
            f"Window closes sessions on key {parsed.key_field!r} "
            f"({parsed.gap_policy} policy) → {len(downstream_agents)} agent step(s)"
        )


def _is_linear_chain(order: list[str], pipeline: Pipeline) -> bool:
    if len(order) != len(pipeline.nodes):
        return False
    from ratatoskr.pipelines.agent_settings import is_self_sourcing

    by_id = {n.id: n for n in pipeline.nodes}
    kinds = [by_id[nid].kind for nid in order]
    head = by_id[order[0]]
    head_ok = kinds[0] == "source" or (
        kinds[0] == "agent" and is_self_sourcing(head.agent)
    )
    if not head_ok or kinds[-1] != "sink":
        return False
    middle = kinds[1:-1]
    window_count = sum(1 for kind in middle if kind == "window")
    if window_count > 1:
        return False
    if window_count == 1 and middle[0] != "window":
        return False
    for kind in middle:
        if kind not in ("window", "agent"):
            return False
    return True


def _check_edge_mappings(pipeline: Pipeline, warnings: list[str]) -> None:
    by_id = {n.id: n for n in pipeline.nodes}
    for edge in pipeline.edges:
        src = by_id.get(edge.source)
        tgt = by_id.get(edge.target)
        if not src or not tgt:
            continue
        if src.kind == "window" and tgt.kind == "agent" and not edge.mapping:
            warnings.append(
                f"Edge {edge.source} → {edge.target}: window emits session summaries; "
                "add field mapping if the agent expects a different shape"
            )
        if src.kind != "agent" or tgt.kind != "agent":
            continue
        if not edge.mapping:
            out_fields = _AGENT_OUTPUT_FIELDS.get(src.agent or "", set())
            in_fields = _AGENT_INPUT_FIELDS.get(tgt.agent or "", set())
            if out_fields and in_fields and not (out_fields & in_fields):
                warnings.append(
                    f"Edge {edge.source} → {edge.target} may need field mapping "
                    f"(output {sorted(out_fields)} vs input {sorted(in_fields)})"
                )
