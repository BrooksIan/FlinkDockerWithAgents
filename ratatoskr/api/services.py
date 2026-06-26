"""Business logic shared by the API and CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ratatoskr.agents.catalog import catalog_entry_for_manifest
from ratatoskr.agents.registry import (
    AgentRegistryError,
    get_agent_spec,
    list_agent_names,
    load_agent_registry,
)
from ratatoskr.agents.submit import describe_agent, submit_agent_cluster
from ratatoskr.api import flink_client
from ratatoskr.api.config import ApiSettings
from ratatoskr.runs.plan import find_flink_job_for_agent
from ratatoskr.runs.service import default_run_service
from ratatoskr.pipelines.introspect import agent_graph
from ratatoskr.pipelines.service import default_pipeline_service


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
    result: list[dict[str, Any]] = []
    for spec in registry.agents.values():
        entry = catalog_entry_for_manifest(spec.name)
        item: dict[str, Any] = {
            "name": spec.name,
            "type": spec.type,
            "description": spec.description,
            "entry": spec.entry,
            "runner": spec.runner,
            "cluster_script": spec.cluster_script,
            "flink_yaml": spec.flink_yaml or None,
        }
        if entry is not None:
            item["catalog_id"] = entry.id
            item["display_name"] = entry.display_name
            item["tags"] = list(entry.tags)
        result.append(item)
    return result


def agent_catalog() -> dict[str, Any]:
    from ratatoskr.agents.catalog import agent_catalog_response

    return agent_catalog_response()


def get_agent(name: str) -> dict[str, Any]:
    if name not in list_agent_names():
        raise AgentRegistryError(f"Unknown agent {name!r}")
    return describe_agent(name)


def get_agent_runtime_definition(name: str) -> dict[str, Any]:
    spec = get_agent_spec(name)
    detail = describe_agent(name)
    flink_yaml_text: str | None = None
    if spec.flink_yaml:
        path = Path(spec.flink_yaml)
        if not path.is_absolute():
            from ratatoskr.paths import project_root

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


def cluster_readiness(settings: ApiSettings) -> dict[str, Any]:
    from ratatoskr.api.cluster_readiness import cluster_readiness as evaluate

    return evaluate(settings)


def list_pipelines(*, limit: int = 100) -> list[dict[str, Any]]:
    return default_pipeline_service().list_pipelines(limit=limit)


def get_pipeline(pipeline_id: str) -> dict[str, Any]:
    return default_pipeline_service().get(pipeline_id)


def create_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    return default_pipeline_service().create(
        str(body.get("name") or ""),
        nodes=body.get("nodes"),
        edges=body.get("edges"),
        layout=body.get("layout"),
    )


def update_pipeline(pipeline_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return default_pipeline_service().update(pipeline_id, body)


def delete_pipeline(pipeline_id: str) -> None:
    default_pipeline_service().delete(pipeline_id)


def validate_pipeline_by_id(pipeline_id: str, *, include_cluster: bool = True) -> dict[str, Any]:
    return default_pipeline_service().validate(pipeline_id, include_cluster=include_cluster)


def run_pipeline_local(pipeline_id: str, *, input_override: list[dict[str, Any]] | None = None, profile: str | None = None) -> dict[str, Any]:
    return default_pipeline_service().run_local(pipeline_id, input_override=input_override, profile=profile)


def submit_pipeline_cluster_api(pipeline_id: str, *, profile: str | None = None) -> dict[str, Any]:
    return default_pipeline_service().submit_cluster(pipeline_id, profile=profile)


def get_agent_graph(name: str) -> dict[str, Any]:
    return agent_graph(name)


def list_kafka_topics(*, bootstrap: str | None = None) -> dict[str, Any]:
    from ratatoskr.kafka_sources import list_kafka_sources

    return list_kafka_sources(bootstrap=bootstrap)


def get_react_llm_settings_api() -> dict[str, Any]:
    from ratatoskr.designer.llm_settings import llm_settings_for_api

    return llm_settings_for_api()


def update_react_llm_settings_api(body: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.llm_settings import update_react_llm_settings

    endpoint_url = str(body.get("endpoint_url") or "").strip()
    model_id = str(body.get("model_id") or "").strip()
    if not endpoint_url:
        raise ValueError("endpoint_url is required")
    if not model_id:
        raise ValueError("model_id is required")
    api_key_raw = body.get("api_key")
    api_key = None if api_key_raw is None else str(api_key_raw)
    return update_react_llm_settings(
        endpoint_url=endpoint_url,
        model_id=model_id,
        api_key=api_key,
    )


def test_react_llm_settings_api(body: dict[str, Any] | None = None) -> dict[str, Any]:
    from ratatoskr.designer.llm_client import LlmNotConfiguredError
    from ratatoskr.designer.llm_settings import test_react_llm_settings

    try:
        return test_react_llm_settings(body=body)
    except LlmNotConfiguredError as exc:
        raise ValueError(str(exc)) from exc


def mcp_catalog_api() -> dict[str, Any]:
    from ratatoskr.mcp.catalog import McpCatalogError, mcp_catalog_response

    try:
        return mcp_catalog_response()
    except McpCatalogError as exc:
        raise ValueError(str(exc)) from exc


def add_mcp_catalog_server_api(body: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.mcp.custom_catalog import add_mcp_catalog_server

    return add_mcp_catalog_server(body)


def list_mcp_instances_api() -> dict[str, Any]:
    from ratatoskr.mcp.instances import list_mcp_instances_api as list_instances

    return list_instances()


def upsert_mcp_instance_api(catalog_id: str, body: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.mcp.instances import upsert_mcp_instance

    return upsert_mcp_instance(
        catalog_id,
        enabled=bool(body.get("enabled")),
        secrets=body.get("secrets"),
        config=body.get("config"),
    )


def test_mcp_instance_api(catalog_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    from ratatoskr.mcp.instances import test_mcp_instance

    return test_mcp_instance(catalog_id, secrets=(body or {}).get("secrets"))


def list_agent_definitions(*, limit: int = 100) -> list[dict[str, Any]]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().list_definitions(limit=limit)


def get_designer_definition(definition_id: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().get(definition_id)


def create_agent_definition(body: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().create(
        body.get("name") or "Untitled agent",
        agent_type=body.get("type") or "workflow",
        description=body.get("description") or "",
        nodes=body.get("nodes"),
        edges=body.get("edges"),
        layout=body.get("layout"),
        input_schema=body.get("input_schema"),
        output_schema=body.get("output_schema"),
        manifest_name=body.get("manifest_name"),
        catalog_category_id=body.get("catalog_category_id"),
        catalog_subcategory_id=body.get("catalog_subcategory_id"),
        catalog_tags=body.get("catalog_tags"),
        mcp_servers=body.get("mcp_servers"),
    )


def update_agent_definition(definition_id: str, body: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().update(definition_id, body)


def delete_agent_definition(definition_id: str) -> None:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    default_agent_definition_service().delete(definition_id)


def validate_agent_definition_by_id(definition_id: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().validate(definition_id)


def compile_agent_definition_by_id(definition_id: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().compile(definition_id)


def publish_agent_definition_by_id(definition_id: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().publish(definition_id)


def run_agent_definition_local(definition_id: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.service import default_agent_definition_service

    return default_agent_definition_service().run_local(definition_id)
