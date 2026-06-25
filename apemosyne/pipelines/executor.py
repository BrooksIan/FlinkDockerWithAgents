"""Local linear pipeline executor."""

from __future__ import annotations

import re
import time
from dataclasses import asdict
from typing import Any

from apemosyne.agents.registry import get_agent_spec
from apemosyne.agents.submit import _import_agent_class
from apemosyne.pipelines.models import Pipeline
from apemosyne.pipelines.validate import validate_pipeline
from apemosyne.runs.service import RunService

_JSONPATH = re.compile(r"^\$\.([a-zA-Z_][a-zA-Z0-9_]*)$")


def apply_edge_mapping(
    records: list[dict[str, Any]],
    mapping: dict[str, str],
) -> list[dict[str, Any]]:
    """Transform output records into input records for the next agent."""
    if not mapping:
        return [dict(r) for r in records]

    result: list[dict[str, Any]] = []
    for record in records:
        payload = _extract_payload(record)
        mapped: dict[str, Any] = {"key": record.get("key", "1")}
        for target_field, source_expr in mapping.items():
            mapped[target_field] = _resolve_mapping_value(source_expr, payload, record)
        result.append(mapped)
    return result


def _extract_payload(record: dict[str, Any]) -> dict[str, Any]:
    if "output" in record and isinstance(record["output"], dict):
        return record["output"]
    return record


def _resolve_mapping_value(
    expr: str,
    payload: dict[str, Any],
    record: dict[str, Any],
) -> Any:
    match = _JSONPATH.match(expr.strip())
    if match:
        key = match.group(1)
        if key in payload:
            return payload[key]
        return record.get(key)
    return expr


def linear_execution_order(pipeline: Pipeline) -> list[str]:
    """Return node ids in execution order (source first)."""
    adjacency: dict[str, list[str]] = {n.id: [] for n in pipeline.nodes}
    in_degree: dict[str, int] = {n.id: 0 for n in pipeline.nodes}
    for edge in pipeline.edges:
        adjacency[edge.source].append(edge.target)
        in_degree[edge.target] += 1

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
    return order


def run_pipeline_local(
    pipeline: Pipeline,
    *,
    run_service: RunService,
    input_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute a validated linear pipeline in-process."""
    validation = validate_pipeline(pipeline)
    if not validation["valid"]:
        raise ValueError("; ".join(validation["errors"]))

    by_id = {n.id: n for n in pipeline.nodes}
    edge_by_target = {e.target: e for e in pipeline.edges}
    order = linear_execution_order(pipeline)

    run_id = run_service.create_pipeline_run(
        f"pipeline:{pipeline.name}",
        kind="local",
        status="running",
    )

    records: list[dict[str, Any]] = []
    sink_output: list[dict[str, Any]] = []

    try:
        from flink_agents.api.execution_environment import AgentsExecutionEnvironment
    except ImportError as exc:
        run_service.finish_run(run_id, status="failed", error=f"flink_agents not available: {exc}")
        raise RuntimeError(f"flink_agents not available: {exc}") from exc

    try:
        for node_id in order:
            node = by_id[node_id]
            if node.kind == "source":
                records = input_override or list(node.config.get("records") or [])
                if not records:
                    raise ValueError("Source node has no input records")
                continue

            if node.kind == "sink":
                sink_output = list(records)
                continue

            if node.kind != "agent" or not node.agent:
                continue

            edge = edge_by_target.get(node_id)
            if edge and edge.source in by_id:
                src_node = by_id[edge.source]
                if src_node.kind == "agent" and edge.mapping:
                    records = apply_edge_mapping(records, edge.mapping)

            spec = get_agent_spec(node.agent)
            agent_cls = _import_agent_class(spec)
            started = time.perf_counter()

            env = AgentsExecutionEnvironment.get_execution_environment()
            output_data = env.from_list(records).apply(agent_cls()).to_list()
            env.execute()

            duration_ms = int((time.perf_counter() - started) * 1000)
            run_service.append_span(
                run_id,
                kind="agent",
                name=node.agent,
                duration_ms=duration_ms,
                input_data=records,
                output_data=output_data,
            )
            records = [_normalize_record(r) for r in output_data]

        run_service.finish_run(
            run_id,
            status="finished",
            record_count=len(sink_output) or len(records),
        )
    except Exception as exc:
        run_service.finish_run(run_id, status="failed", error=str(exc))
        raise

    return {
        "run_id": run_id,
        "status": "finished",
        "output": sink_output or records,
        "validation": validation,
    }


def _normalize_record(record: Any) -> dict[str, Any]:
    if isinstance(record, dict):
        return dict(record)
    return {"value": record}
