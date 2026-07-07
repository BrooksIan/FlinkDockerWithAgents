"""Local linear pipeline executor."""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ratatoskr.runs.service import RunService

from ratatoskr.agents.registry import get_agent_spec
from ratatoskr.agents.submit import _import_agent_class
from ratatoskr.pipelines.agent_settings import apply_agent_node_config
from ratatoskr.pipelines.models import AgentStepResult, Pipeline
from ratatoskr.pipelines.validate import validate_pipeline

_JSONPATH = re.compile(r"^\$\.([a-zA-Z_][a-zA-Z0-9_]*)$")


def flink_agents_available() -> bool:
    try:
        import flink_agents  # noqa: F401
    except ImportError:
        return False
    return True


def apply_edge_mapping(
    records: list[dict[str, Any]],
    mapping: dict[str, str],
) -> list[dict[str, Any]]:
    """Transform output records into Flink input records for the next agent."""
    if not mapping:
        return [_agent_output_to_input(r) for r in records]

    result: list[dict[str, Any]] = []
    for record in records:
        payload = _extract_payload(record)
        inner: dict[str, Any] = {}
        for target_field, source_expr in mapping.items():
            inner[target_field] = _resolve_mapping_value(source_expr, payload, record)
        result.append({"key": _record_key(record), "value": inner})
    return result


def _agent_output_to_input(record: dict[str, Any]) -> dict[str, Any]:
    return {"key": _record_key(record), "value": _extract_payload(record)}


def _record_key(record: dict[str, Any]) -> str:
    if "key" in record:
        return str(record["key"])
    if "k" in record:
        return str(record["k"])
    if len(record) == 1:
        only = next(iter(record))
        if only not in ("value", "v", "output"):
            return str(only)
    return "1"


def _extract_payload(record: dict[str, Any]) -> Any:
    if "output" in record:
        return record["output"]
    if "value" in record:
        return record["value"]
    if "v" in record:
        return record["v"]
    if len(record) == 1:
        key, payload = next(iter(record.items()))
        if key not in ("key", "k", "value", "v", "output"):
            return payload
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


def _resolve_source_records(
    node_config: dict[str, Any],
    *,
    input_override: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if input_override is not None:
        return input_override

    source_type = str(node_config.get("source_type") or "records").strip().lower()
    if source_type == "kafka":
        from ratatoskr.kafka_sources import sample_topic_records

        topic = str(node_config.get("topic") or "").strip()
        if not topic:
            raise ValueError("Kafka source missing topic")
        limit = int(node_config.get("max_records") or 10)
        bootstrap = node_config.get("bootstrap")
        return sample_topic_records(
            topic,
            limit=limit,
            bootstrap=str(bootstrap) if bootstrap else None,
        )

    records = list(node_config.get("records") or [])
    if not records:
        raise ValueError("Source node has no input records")
    return records


def _deliver_sink_output(config: dict[str, Any], records: list[dict[str, Any]]) -> None:
    sink_type = str(config.get("sink_type") or "capture").strip().lower()
    if sink_type != "kafka":
        return

    from ratatoskr.kafka_sources import publish_topic_records

    topic = str(config.get("topic") or "").strip()
    if not topic:
        from ratatoskr.kafka_sources import DEFAULT_KAFKA_OUTPUT_TOPIC

        topic = DEFAULT_KAFKA_OUTPUT_TOPIC
    bootstrap = config.get("bootstrap")
    publish_topic_records(
        topic,
        records,
        bootstrap=str(bootstrap) if bootstrap else None,
    )


def execute_pipeline_agents(
    pipeline: Pipeline,
    *,
    input_override: list[dict[str, Any]] | None = None,
    deliver_sinks: bool = True,
) -> tuple[list[dict[str, Any]], list[AgentStepResult]]:
    """Run agent steps in-process. Requires flink_agents on PYTHONPATH."""
    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    by_id = {n.id: n for n in pipeline.nodes}
    edge_by_target = {e.target: e for e in pipeline.edges}
    order = linear_execution_order(pipeline)

    records: list[dict[str, Any]] = []
    sink_output: list[dict[str, Any]] = []
    steps: list[AgentStepResult] = []

    if not any(n.kind == "source" for n in pipeline.nodes):
        records = input_override or [{"key": "1", "value": {}}]
        input_override = None

    for node_id in order:
        node = by_id[node_id]
        if node.kind == "source":
            records = _resolve_source_records(node.config, input_override=input_override)
            input_override = None
            continue

        if node.kind == "window":
            from ratatoskr.pipelines.window_local import apply_window_node, session_summary_to_agent_record

            started = time.perf_counter()
            summaries = apply_window_node(records, node.config)
            duration_ms = int((time.perf_counter() - started) * 1000)
            steps.append(
                AgentStepResult(
                    agent="window",
                    duration_ms=duration_ms,
                    input_data=records,
                    output_data=summaries,
                )
            )
            records = [session_summary_to_agent_record(summary) for summary in summaries]
            continue

        if node.kind == "sink":
            sink_output = list(records)
            if deliver_sinks:
                _deliver_sink_output(node.config, sink_output)
            continue

        if node.kind != "agent" or not node.agent:
            continue

        edge = edge_by_target.get(node_id)
        if edge and edge.source in by_id:
            src_node = by_id[edge.source]
            if src_node.kind == "agent":
                if edge.mapping:
                    records = apply_edge_mapping(records, edge.mapping)
                else:
                    records = [_agent_output_to_input(r) for r in records]
            elif src_node.kind == "window":
                records = [_agent_output_to_input(r) for r in records]

        records = apply_agent_node_config(records, node.config)

        spec = get_agent_spec(node.agent)
        agent_cls = _import_agent_class(spec)
        started = time.perf_counter()

        env = AgentsExecutionEnvironment.get_execution_environment()
        output_data = env.from_list(records).apply(agent_cls()).to_list()
        env.execute()

        duration_ms = int((time.perf_counter() - started) * 1000)
        steps.append(
            AgentStepResult(
                agent=node.agent,
                duration_ms=duration_ms,
                input_data=records,
                output_data=output_data,
            )
        )
        records = [_normalize_record(r) for r in output_data]

    return sink_output or records, steps


def run_pipeline_local(
    pipeline: Pipeline,
    *,
    run_service: "RunService",
    input_override: list[dict[str, Any]] | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """Execute a validated linear pipeline (host or JobManager container)."""
    validation = validate_pipeline(pipeline)
    if not validation["valid"]:
        raise ValueError("; ".join(validation["errors"]))

    run_id = run_service.create_pipeline_run(
        f"pipeline:{pipeline.name}",
        kind="local",
        status="running",
    )

    source_node = next((n for n in pipeline.nodes if n.kind == "source"), None)
    effective_input = input_override
    if effective_input is None and source_node is not None:
        effective_input = _resolve_source_records(source_node.config)

    try:
        if flink_agents_available():
            output, steps = execute_pipeline_agents(pipeline, input_override=effective_input)
        else:
            from ratatoskr.pipelines.docker_runner import run_pipeline_in_container

            output, steps = run_pipeline_in_container(
                pipeline,
                input_override=effective_input,
                profile=profile,
            )
            sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
            if sink_node is not None:
                _deliver_sink_output(sink_node.config, output)

        for step in steps:
            span_kind = "window" if step.agent == "window" else "agent"
            run_service.append_span(
                run_id,
                kind=span_kind,
                name=step.agent,
                duration_ms=step.duration_ms,
                input_data=step.input_data,
                output_data=step.output_data,
            )

        sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
        if sink_node is not None and output:
            sink_type = str(sink_node.config.get("sink_type") or "capture").strip().lower()
            sink_name = (
                str(sink_node.config.get("topic") or "").strip()
                if sink_type == "kafka"
                else "capture"
            )
            run_service.append_span(
                run_id,
                kind="sink",
                name=sink_name or "capture",
                output_data=output,
                input_data={"sink_type": sink_type, "topic": sink_node.config.get("topic")},
            )

        run_service.finish_run(
            run_id,
            status="finished",
            record_count=len(output),
        )
    except Exception as exc:
        run_service.finish_run(run_id, status="failed", error=str(exc))
        raise

    return {
        "run_id": run_id,
        "status": "finished",
        "output": output,
        "validation": validation,
    }


def _normalize_record(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return {"key": "1", "output": record}
    if "output" in record:
        return dict(record)
    if len(record) == 1:
        key, payload = next(iter(record.items()))
        if key not in ("key", "k", "value", "v", "output"):
            return {"key": str(key), "output": payload}
    if "key" in record or "value" in record or "v" in record:
        return dict(record)
    return {"key": _record_key(record), "output": record}
