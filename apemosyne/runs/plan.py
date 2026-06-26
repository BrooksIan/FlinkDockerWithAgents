"""Declarative execution plans for registered agents."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from apemosyne.pipelines.models import Pipeline

from apemosyne.agents.registry import AgentRegistryError, get_agent_spec
from apemosyne.flink_rest import studio_flink_rest_port

# Flink job names from cluster runner scripts (for job linkage after submit).
CLUSTER_JOB_NAMES: dict[str, str] = {
    "workflow_counter": "Apemosyne Workflow Counter",
    "react_echo": "Apemosyne React Echo",
    "react_double_value": "Apemosyne React Double Value",
}

_AGENT_PLANS: dict[str, list[dict[str, Any]]] = {
    "workflow_counter": [
        {"kind": "action", "name": "process", "description": "Handle InputEvent"},
        {"kind": "tool", "name": "double", "description": "Double integer value", "parent": "process"},
        {"kind": "output", "name": "OutputEvent", "description": "Emit doubled result", "parent": "process"},
    ],
    "react_echo": [
        {"kind": "action", "name": "process", "description": "Handle InputEvent"},
        {"kind": "tool", "name": "classify", "description": "Severity from text", "parent": "process"},
        {"kind": "tool", "name": "summarize", "description": "Format summary", "parent": "process"},
        {"kind": "output", "name": "OutputEvent", "description": "Emit classification result", "parent": "process"},
    ],
    "react_double_value": [
        {"kind": "action", "name": "process", "description": "LLM prompt doubles numeric input"},
        {"kind": "tool", "name": "double", "description": "Verify doubled integer", "parent": "process"},
        {"kind": "output", "name": "OutputEvent", "description": "Emit doubled result", "parent": "process"},
    ],
}


def cluster_job_name(agent: str) -> str | None:
    return CLUSTER_JOB_NAMES.get(agent)


def agent_execution_plan(agent: str) -> list[dict[str, Any]]:
    """Expected steps for an agent (static until runtime tracing lands)."""
    if agent.startswith("pipeline:"):
        return []
    if agent not in _AGENT_PLANS:
        try:
            get_agent_spec(agent)
        except AgentRegistryError:
            return []
        return []
    return [dict(step) for step in _AGENT_PLANS[agent]]


def find_flink_job_for_agent(agent: str) -> str | None:
    """Match a Flink job id by the cluster runner's execute() name."""
    from apemosyne.runtime import flink_cluster_submit

    expected = cluster_job_name(agent)
    if not expected:
        return None
    try:
        data = flink_cluster_submit.fetch_json("/jobs/overview")
    except Exception:
        return None
    for job in data.get("jobs") or []:
        if job.get("name") == expected:
            jid = job.get("jid")
            return str(jid) if jid else None
    return None


def cluster_job_name_for_agent(agent: str) -> str | None:
    return CLUSTER_JOB_NAMES.get(agent)


def cluster_job_name_for_pipeline(pipeline: "Pipeline") -> str:
    from apemosyne.pipelines.cluster_codegen import cluster_job_name

    return cluster_job_name(pipeline)


def find_flink_job_for_pipeline(pipeline: "Pipeline") -> str | None:
    """Match a Flink job id for a submitted Studio pipeline."""
    from apemosyne.runtime import flink_cluster_submit

    expected = cluster_job_name_for_pipeline(pipeline)
    port = studio_flink_rest_port()
    try:
        data = flink_cluster_submit.fetch_json("/jobs/overview", rest_port=port)
    except Exception:
        return None
    for job in data.get("jobs") or []:
        if job.get("name") == expected:
            jid = job.get("jid")
            return str(jid) if jid else None
    return None


def resolve_pipeline_for_run(agent: str) -> "Pipeline | None":
    """Load a pipeline model from a run's ``pipeline:{name}`` agent field."""
    if not agent.startswith("pipeline:"):
        return None
    from apemosyne.pipelines.models import pipeline_from_dict
    from apemosyne.pipelines.service import default_pipeline_service

    name = agent.removeprefix("pipeline:")
    for row in default_pipeline_service().list_pipelines(limit=500):
        if row.get("name") == name:
            return pipeline_from_dict(row)
    return None


def pipeline_execution_plan_for_run(agent: str) -> list[dict[str, Any]]:
    pipeline = resolve_pipeline_for_run(agent)
    if pipeline is None:
        return []
    from apemosyne.pipelines.cluster_codegen import pipeline_execution_plan

    return pipeline_execution_plan(pipeline)


def flink_job_state(job_id: str, *, rest_port: int | None = None) -> str | None:
    """Return Flink job state string (RUNNING, FINISHED, FAILED, ...) or None."""
    from apemosyne.runtime import flink_cluster_submit

    port = rest_port if rest_port is not None else studio_flink_rest_port()
    try:
        data = flink_cluster_submit.fetch_json(f"/jobs/{job_id}", rest_port=port)
    except Exception:
        return None
    state = data.get("state")
    return str(state) if state else None
