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

router = APIRouter(prefix="/v1")


class SubmitResponse(BaseModel):
    agent: str
    status: str
    jobs: list[dict[str, Any]] = Field(default_factory=list)


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


@router.get("/agents/{name}", tags=["agents"], dependencies=[Depends(require_api_key)])
def agent_detail(name: str) -> dict[str, Any]:
    try:
        return services.get_agent(name)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/agents/{name}/definition", tags=["agents"], dependencies=[Depends(require_api_key)])
def agent_definition(name: str) -> dict[str, Any]:
    try:
        return services.get_agent_definition(name)
    except AgentRegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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
