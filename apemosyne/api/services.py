"""Business logic shared by the API and CLI."""

from __future__ import annotations

from typing import Any

from apemosyne.agents.registry import AgentRegistryError, list_agent_names, load_agent_registry
from apemosyne.agents.submit import describe_agent, submit_agent_cluster
from apemosyne.api import flink_client
from apemosyne.api.config import ApiSettings


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
        }
        for spec in registry.agents.values()
    ]


def get_agent(name: str) -> dict[str, Any]:
    if name not in list_agent_names():
        raise AgentRegistryError(f"Unknown agent {name!r}")
    return describe_agent(name)


def submit_agent(name: str, *, settings: ApiSettings) -> dict[str, Any]:
    rc = submit_agent_cluster(name, profile=settings.default_profile)
    if rc != 0:
        raise RuntimeError(f"Agent submit failed with exit code {rc}")
    jobs = flink_client.list_jobs()
    return {"agent": name, "status": "submitted", "jobs": jobs}


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
