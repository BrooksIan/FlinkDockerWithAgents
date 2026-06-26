"""FastAPI route handlers."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from apemosyne.agents.registry import AgentRegistryError
from apemosyne.api import flink_client, services
from apemosyne.api.auth import require_api_key
from apemosyne.api.config import ApiSettings
from apemosyne.api.events import event_stream
from apemosyne.api.observability import refresh_flink_gauges
from apemosyne.constants import DEFAULT_PROFILE

router = APIRouter(prefix="/v1")


class SubmitResponse(BaseModel):
    agent: str
    status: str
    run_id: str | None = None
    flink_job_id: str | None = None
    jobs: list[dict[str, Any]] = Field(default_factory=list)


class SpanCreate(BaseModel):
    kind: str
    name: str
    status: str = "ok"
    parent_id: str | None = None
    duration_ms: int | None = None
    input: Any | None = None
    output: Any | None = None


class PipelineCreate(BaseModel):
    name: str = ""
    nodes: list[dict[str, Any]] = Field(default_factory=list)
    edges: list[dict[str, Any]] = Field(default_factory=list)
    layout: dict[str, dict[str, float]] = Field(default_factory=dict)


class PipelineUpdate(BaseModel):
    name: str | None = None
    nodes: list[dict[str, Any]] | None = None
    edges: list[dict[str, Any]] | None = None
    layout: dict[str, dict[str, float]] | None = None


class PipelineRunRequest(BaseModel):
    records: list[dict[str, Any]] | None = None


class ReactLlmSettingsUpdate(BaseModel):
    endpoint_url: str
    model_id: str
    api_key: str | None = None


class ReactLlmSettingsTest(BaseModel):
    endpoint_url: str | None = None
    model_id: str | None = None
    api_key: str | None = None


class AgentDefinitionCreate(BaseModel):
    name: str = "Untitled agent"
    type: str = "workflow"
    description: str = ""
    nodes: list[dict[str, Any]] = Field(default_factory=list)
    edges: list[dict[str, Any]] = Field(default_factory=list)
    layout: dict[str, dict[str, float]] = Field(default_factory=dict)
    input_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)
    manifest_name: str | None = None
    catalog_category_id: str | None = None
    catalog_subcategory_id: str | None = None
    catalog_tags: list[str] = Field(default_factory=list)


class AgentDefinitionUpdate(BaseModel):
    name: str | None = None
    type: str | None = None
    description: str | None = None
    version: int | None = None
    status: str | None = None
    nodes: list[dict[str, Any]] | None = None
    edges: list[dict[str, Any]] | None = None
    layout: dict[str, dict[str, float]] | None = None
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
    manifest_name: str | None = None
    catalog_category_id: str | None = None
    catalog_subcategory_id: str | None = None
    catalog_tags: list[str] | None = None


def _settings(request: Request) -> ApiSettings:
    return request.app.state.settings


@router.get("/health", tags=["health"])
async def health(settings: ApiSettings = Depends(_settings)) -> dict[str, Any]:
    import asyncio

    body = await asyncio.to_thread(services.pipeline_health, settings)
    flink = body.get("flink") or {}
    agents = body.get("agents") or {}
    refresh_flink_gauges(
        reachable=bool(flink.get("reachable")),
        jobs_running=int(flink.get("jobs_running") or 0),
        slots_free=int(flink.get("slots_free") or 0),
        agents_registered=int(agents.get("registered") or 0),
    )
    return body


@router.get("/cluster/status", tags=["cluster"])
def cluster_status(settings: ApiSettings = Depends(_settings)) -> dict[str, Any]:
    """Flink cluster info and readiness checks for Studio cluster submit."""
    return services.cluster_readiness(settings)


@router.post("/cluster/validate", tags=["cluster"])
def cluster_validate(settings: ApiSettings = Depends(_settings)) -> dict[str, Any]:
    """Re-run Flink cluster readiness validation (same payload as GET /cluster/status)."""
    return services.cluster_readiness(settings)


@router.get("/cluster/overview", tags=["cluster"], dependencies=[Depends(require_api_key)])
def cluster_overview() -> dict[str, Any]:
    try:
        return flink_client.cluster_overview()
    except flink_client.FlinkUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/jobs", tags=["jobs"], dependencies=[Depends(require_api_key)])
def jobs_list() -> list[dict[str, Any]]:
    try:
        return flink_client.list_jobs()
    except flink_client.FlinkUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/jobs/{job_id}", tags=["jobs"], dependencies=[Depends(require_api_key)])
def job_detail(job_id: str) -> dict[str, Any]:
    try:
        return flink_client.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}") from exc
    except flink_client.FlinkUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.delete("/jobs/{job_id}", tags=["jobs"], dependencies=[Depends(require_api_key)])
def job_cancel(job_id: str) -> dict[str, str]:
    try:
        flink_client.cancel_job(job_id)
    except flink_client.FlinkUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"id": job_id, "status": "canceled"}


@router.get("/agents", tags=["agents"], dependencies=[Depends(require_api_key)])
def agents_list() -> list[dict[str, Any]]:
    return services.list_agents()


@router.get("/agents/catalog", tags=["agents"], dependencies=[Depends(require_api_key)])
def agents_catalog() -> dict[str, Any]:
    try:
        return services.agent_catalog()
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/designer/llm-settings", tags=["designer"], dependencies=[Depends(require_api_key)])
def designer_llm_settings_get() -> dict[str, Any]:
    return services.get_react_llm_settings_api()


@router.put("/designer/llm-settings", tags=["designer"], dependencies=[Depends(require_api_key)])
def designer_llm_settings_put(body: ReactLlmSettingsUpdate) -> dict[str, Any]:
    try:
        return services.update_react_llm_settings_api(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/designer/llm-settings/test", tags=["designer"], dependencies=[Depends(require_api_key)])
def designer_llm_settings_test(body: ReactLlmSettingsTest | None = None) -> dict[str, Any]:
    try:
        payload = body.model_dump() if body else None
        return services.test_react_llm_settings_api(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get(
    "/agent-definitions",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_list(limit: int = 100) -> list[dict[str, Any]]:
    return services.list_agent_definitions(limit=limit)


@router.post(
    "/agent-definitions",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_create(payload: AgentDefinitionCreate) -> dict[str, Any]:
    return services.create_agent_definition(payload.model_dump())


@router.get(
    "/agent-definitions/{definition_id}",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_get(definition_id: str) -> dict[str, Any]:
    try:
        return services.get_designer_definition(definition_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f"Agent definition not found: {definition_id}"
        ) from exc


@router.put(
    "/agent-definitions/{definition_id}",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_update(
    definition_id: str, payload: AgentDefinitionUpdate
) -> dict[str, Any]:
    try:
        return services.update_agent_definition(
            definition_id, payload.model_dump(exclude_unset=True)
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f"Agent definition not found: {definition_id}"
        ) from exc


@router.delete(
    "/agent-definitions/{definition_id}",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_delete(definition_id: str) -> dict[str, str]:
    try:
        services.delete_agent_definition(definition_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f"Agent definition not found: {definition_id}"
        ) from exc
    return {"id": definition_id, "status": "deleted"}


@router.post(
    "/agent-definitions/{definition_id}/validate",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_validate(definition_id: str) -> dict[str, Any]:
    try:
        return services.validate_agent_definition_by_id(definition_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f"Agent definition not found: {definition_id}"
        ) from exc


@router.post(
    "/agent-definitions/{definition_id}/compile",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_compile(definition_id: str) -> dict[str, Any]:
    try:
        return services.compile_agent_definition_by_id(definition_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f"Agent definition not found: {definition_id}"
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post(
    "/agent-definitions/{definition_id}/publish",
    tags=["designer"],
    dependencies=[Depends(require_api_key)],
)
def agent_definitions_publish(definition_id: str) -> dict[str, Any]:
    try:
        return services.publish_agent_definition_by_id(definition_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f"Agent definition not found: {definition_id}"
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/kafka/topics", tags=["kafka"], dependencies=[Depends(require_api_key)])
def kafka_topics_list(bootstrap: str | None = None) -> dict[str, Any]:
    try:
        return services.list_kafka_topics(bootstrap=bootstrap)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/agents/{name}", tags=["agents"], dependencies=[Depends(require_api_key)])
def agent_detail(name: str) -> dict[str, Any]:
    try:
        return services.get_agent(name)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/agents/{name}/definition", tags=["agents"], dependencies=[Depends(require_api_key)])
def agent_definition(name: str) -> dict[str, Any]:
    try:
        return services.get_agent_runtime_definition(name)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/agents/{name}/graph", tags=["agents"], dependencies=[Depends(require_api_key)])
def agent_graph_route(name: str) -> dict[str, Any]:
    try:
        return services.get_agent_graph(name)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/agents/{name}/runs", tags=["runs"], dependencies=[Depends(require_api_key)])
def agent_runs_list(name: str, limit: int = 50) -> list[dict[str, Any]]:
    try:
        return services.list_agent_runs(name, limit=limit)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs", tags=["runs"], dependencies=[Depends(require_api_key)])
def runs_list(agent: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    try:
        return services.list_runs(agent=agent, limit=limit)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}", tags=["runs"], dependencies=[Depends(require_api_key)])
def run_detail(run_id: str) -> dict[str, Any]:
    try:
        return services.get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc


@router.post("/runs/{run_id}/spans", tags=["runs"], dependencies=[Depends(require_api_key)])
async def run_append_span(run_id: str, request: Request) -> dict[str, str]:
    try:
        payload = SpanCreate.model_validate(await request.json())
        return services.append_run_span(run_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/events", tags=["events"])
async def events_sse(settings: ApiSettings = Depends(_settings)) -> StreamingResponse:
    """SSE stream of health + job snapshots (for dashboard live view)."""
    return StreamingResponse(
        event_stream(settings),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post(
    "/agents/{name}/submit",
    tags=["agents"],
    response_model=SubmitResponse,
    dependencies=[Depends(require_api_key)],
)
def agent_submit(name: str, settings: ApiSettings = Depends(_settings)) -> SubmitResponse:
    try:
        result = services.submit_agent(name, settings=settings)
        return SubmitResponse(**result)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/pipeline/health", tags=["health"])
def pipeline_health(settings: ApiSettings = Depends(_settings)) -> dict[str, Any]:
    return health(settings)


@router.get("/pipelines", tags=["pipelines"], dependencies=[Depends(require_api_key)])
def pipelines_list(limit: int = 100) -> list[dict[str, Any]]:
    return services.list_pipelines(limit=limit)


@router.post("/pipelines", tags=["pipelines"], dependencies=[Depends(require_api_key)])
def pipelines_create(payload: PipelineCreate) -> dict[str, Any]:
    return services.create_pipeline(payload.model_dump())


@router.get("/pipelines/{pipeline_id}", tags=["pipelines"], dependencies=[Depends(require_api_key)])
def pipelines_get(pipeline_id: str) -> dict[str, Any]:
    try:
        return services.get_pipeline(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}") from exc


@router.put("/pipelines/{pipeline_id}", tags=["pipelines"], dependencies=[Depends(require_api_key)])
def pipelines_update(pipeline_id: str, payload: PipelineUpdate) -> dict[str, Any]:
    try:
        data = payload.model_dump(exclude_unset=True)
        return services.update_pipeline(pipeline_id, data)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}") from exc


@router.delete("/pipelines/{pipeline_id}", tags=["pipelines"], dependencies=[Depends(require_api_key)])
def pipelines_delete(pipeline_id: str) -> dict[str, str]:
    try:
        services.delete_pipeline(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}") from exc
    return {"id": pipeline_id, "status": "deleted"}


@router.post(
    "/pipelines/{pipeline_id}/validate",
    tags=["pipelines"],
    dependencies=[Depends(require_api_key)],
)
def pipelines_validate(pipeline_id: str) -> dict[str, Any]:
    try:
        return services.validate_pipeline_by_id(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}") from exc


@router.post(
    "/pipelines/{pipeline_id}/run",
    tags=["pipelines"],
    dependencies=[Depends(require_api_key)],
)
def pipelines_run(
    pipeline_id: str,
    run_request: PipelineRunRequest | None = None,
    settings: ApiSettings = Depends(_settings),
) -> dict[str, Any]:
    try:
        records = run_request.records if run_request else None
        return services.run_pipeline_local(
            pipeline_id,
            input_override=records,
            profile=DEFAULT_PROFILE,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post(
    "/pipelines/{pipeline_id}/submit",
    tags=["pipelines"],
    dependencies=[Depends(require_api_key)],
)
def pipelines_submit(
    pipeline_id: str,
    settings: ApiSettings = Depends(_settings),
) -> dict[str, Any]:
    try:
        return services.submit_pipeline_cluster_api(
            pipeline_id,
            profile=DEFAULT_PROFILE,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
