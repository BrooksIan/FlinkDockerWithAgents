"""Business logic shared by the API and CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from apemosyne.agents.registry import (
    AgentRegistryError,
    get_agent_spec,
    list_agent_names,
    load_agent_registry,
)
from apemosyne.agents.submit import describe_agent, submit_agent_cluster
from apemosyne.api import flink_client
from apemosyne.api.config import ApiSettings
from apemosyne.runs.plan import find_flink_job_for_agent
from apemosyne.runs.service import default_run_service
from apemosyne.pipelines.introspect import agent_graph
from apemosyne.pipelines.service import default_pipeline_service


def _find_flink_job_for_agent(agent: str) -> str | None:
    return find_flink_job_for_agent(agent)


def list_runs(*, agent: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    return default_run_service().list_runs(agent=agent, limit=limit)


def get_run(run_id: str) -> dict[str, Any]:
    return default_run_service().get_run(run_id)


def list_agent_runs(agent: str, *, limit: int = 50) -> list[dict[str, Any]]:
    return default_run_service().list_runs(agent=agent, limit=limit)


def append_run_span(run_id: str, body: dict[str, Any]) -> dict[str, str]:
    span_id = default_run_service().append_span(
        run_id,
        kind=body["kind"],
        name=body["name"],
        status=body.get("status", "ok"),
        parent_id=body.get("parent_id"),
        duration_ms=body.get("duration_ms"),
        input_data=body.get("input"),
        output_data=body.get("output"),
    )
    return {"id": span_id, "run_id": run_id}


def submit_agent(name: str, *, settings: ApiSettings) -> dict[str, Any]:
    job_hint = _find_flink_job_for_agent(name)
    result = submit_agent_cluster(
        name,
        profile=settings.default_profile,
        flink_job_id=job_hint,
    )
    if result.return_code != 0:
        raise RuntimeError(f"Agent submit failed with exit code {result.return_code}")
    job_id = result.flink_job_id or _find_flink_job_for_agent(name)
    if job_id and result.run_id:
        default_run_service().set_running(result.run_id, flink_job_id=job_id)
    jobs = flink_client.list_jobs()
    return {
        "agent": name,
        "status": "submitted",
        "run_id": result.run_id,
        "flink_job_id": job_id,
        "jobs": jobs,
    }


def list_agents() -> list[dict[str, Any]]:
    registry = load_agent_registry(validate=False)
    return [
        {
            "name": spec.name,
            "type": spec.type,
            "description": spec.description,
            "entry": spec.entry,
            "runner": spec.runner,
            "cluster_script": spec.cluster_script,
            "flink_yaml": spec.flink_yaml or None,
        }
        for spec in registry.agents.values()
    ]


def get_agent(name: str) -> dict[str, Any]:
    if name not in list_agent_names():
        raise AgentRegistryError(f"Unknown agent {name!r}")
    return describe_agent(name)


def get_agent_definition(name: str) -> dict[str, Any]:
    spec = get_agent_spec(name)
    detail = describe_agent(name)
    flink_yaml_text: str | None = None
    if spec.flink_yaml:
        path = Path(spec.flink_yaml)
        if not path.is_absolute():
            from apemosyne.paths import project_root

            path = project_root() / spec.flink_yaml
        if path.is_file():
            flink_yaml_text = path.read_text(encoding="utf-8")
    return {
        **detail,
        "flink_yaml_path": spec.flink_yaml or None,
        "flink_yaml": flink_yaml_text,
    }


def pipeline_health(settings: ApiSettings) -> dict[str, Any]:
    flink_block: dict[str, Any] = {
        "reachable": False,
        "url": settings.flink_rest_url,
    }
    try:
        overview = flink_client.cluster_overview()
        tm = flink_client.taskmanager_summary()
        flink_block.update(
            {
                "reachable": True,
                "flink_version": overview.get("flink-version"),
                "taskmanagers": tm["count"],
                "slots_total": tm["slots_total"],
                "slots_free": tm["slots_free"],
                "jobs_running": overview.get("jobs-running"),
                "jobs_finished": overview.get("jobs-finished"),
            }
        )
    except flink_client.FlinkUnavailableError as exc:
        flink_block["error"] = str(exc)

    agents_ok = True
    agent_count = 0
    try:
        agent_count = len(list_agent_names())
    except AgentRegistryError:
        agents_ok = False

    overall = "ok" if flink_block.get("reachable") and agents_ok else "degraded"
    if not flink_block.get("reachable"):
        overall = "unavailable"

    return {
        "status": overall,
        "flink": flink_block,
        "agents": {"ok": agents_ok, "registered": agent_count},
        "api_version": "v1",
    }


def list_pipelines(*, limit: int = 100) -> list[dict[str, Any]]:
    return default_pipeline_service().list_pipelines(limit=limit)


def get_pipeline(pipeline_id: str) -> dict[str, Any]:
    return default_pipeline_service().get(pipeline_id)


def create_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    return default_pipeline_service().create(
        body.get("name") or "Untitled pipeline",
        nodes=body.get("nodes"),
        edges=body.get("edges"),
        layout=body.get("layout"),
    )


def update_pipeline(pipeline_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return default_pipeline_service().update(pipeline_id, body)


def delete_pipeline(pipeline_id: str) -> None:
    default_pipeline_service().delete(pipeline_id)


def validate_pipeline_by_id(pipeline_id: str) -> dict[str, Any]:
    return default_pipeline_service().validate(pipeline_id)


def run_pipeline_local(pipeline_id: str, *, input_override: list[dict[str, Any]] | None = None, profile: str | None = None) -> dict[str, Any]:
    return default_pipeline_service().run_local(pipeline_id, input_override=input_override, profile=profile)


def get_agent_graph(name: str) -> dict[str, Any]:
    return agent_graph(name)


def list_kafka_topics(*, bootstrap: str | None = None) -> dict[str, Any]:
    from apemosyne.kafka_sources import list_kafka_sources

    return list_kafka_sources(bootstrap=bootstrap)
