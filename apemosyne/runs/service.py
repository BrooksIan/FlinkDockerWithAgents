"""High-level run registry operations."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apemosyne.agents.registry import AgentRegistryError, get_agent_spec
from apemosyne.runs.models import Run, RunKind, RunStatus, SpanKind, SpanStatus
from apemosyne.runs.plan import agent_execution_plan, find_flink_job_for_agent, flink_job_state
from apemosyne.runs.store import RunStore, runs_db_path

_default_service: "RunService | None" = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_to_dict(run: Run, *, include_plan: bool = True) -> dict[str, Any]:
    data: dict[str, Any] = {
        "id": run.id,
        "agent": run.agent,
        "kind": run.kind,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "flink_job_id": run.flink_job_id,
        "error": run.error,
        "record_count": run.record_count,
        "spans": [asdict(span) for span in run.spans],
    }
    if include_plan:
        data["plan"] = run.plan or agent_execution_plan(run.agent)
    return data


class RunService:
    def __init__(self, store: RunStore) -> None:
        self._store = store

    def create_run(
        self,
        agent: str,
        *,
        kind: RunKind,
        status: RunStatus = "starting",
        flink_job_id: str | None = None,
    ) -> str:
        get_agent_spec(agent)
        return self._insert_run(agent, kind=kind, status=status, flink_job_id=flink_job_id)

    def create_pipeline_run(
        self,
        agent: str,
        *,
        kind: RunKind,
        status: RunStatus = "starting",
        flink_job_id: str | None = None,
    ) -> str:
        """Create a run for a composed pipeline (agent name need not be in registry)."""
        return self._insert_run(agent, kind=kind, status=status, flink_job_id=flink_job_id)

    def _insert_run(
        self,
        agent: str,
        *,
        kind: RunKind,
        status: RunStatus,
        flink_job_id: str | None = None,
    ) -> str:
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        self._store.insert_run(
            run_id=run_id,
            agent=agent,
            kind=kind,
            status=status,
            started_at=_utc_now(),
            flink_job_id=flink_job_id,
        )
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: RunStatus,
        error: str | None = None,
        flink_job_id: str | None = None,
        record_count: int | None = None,
    ) -> None:
        self._store.update_run(
            run_id,
            status=status,
            finished_at=_utc_now(),
            error=error,
            flink_job_id=flink_job_id,
            record_count=record_count,
        )

    def set_running(self, run_id: str, *, flink_job_id: str | None = None) -> None:
        self._store.update_run(run_id, status="running", flink_job_id=flink_job_id)

    def append_span(
        self,
        run_id: str,
        *,
        kind: SpanKind,
        name: str,
        status: SpanStatus = "ok",
        parent_id: str | None = None,
        duration_ms: int | None = None,
        input_data: Any | None = None,
        output_data: Any | None = None,
    ) -> str:
        if self._store.get_run(run_id) is None:
            raise KeyError(f"Run not found: {run_id}")
        span_id = f"span_{uuid.uuid4().hex[:12]}"
        now = _utc_now()
        self._store.insert_span(
            span_id=span_id,
            run_id=run_id,
            kind=kind,
            name=name,
            status=status,
            started_at=now,
            finished_at=now,
            parent_id=parent_id,
            duration_ms=duration_ms,
            input_data=input_data,
            output_data=output_data,
        )
        return span_id

    def _refresh_cluster_status(self, run_id: str, job_id: str | None) -> None:
        if not job_id:
            return
        state = flink_job_state(job_id)
        if state in ("FINISHED", "SUCCEEDED"):
            self.finish_run(run_id, status="finished", flink_job_id=job_id)
        elif state in ("FAILED", "CANCELED", "CANCELLED"):
            self.finish_run(run_id, status="failed", flink_job_id=job_id, error=f"Flink job {state}")

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self._store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        if run.kind == "cluster":
            if not run.flink_job_id:
                job_id = find_flink_job_for_agent(run.agent)
                if job_id:
                    self.set_running(run_id, flink_job_id=job_id)
            run = self._store.get_run(run_id)
            if run is None:
                raise KeyError(run_id)
            if run.flink_job_id:
                self._refresh_cluster_status(run_id, run.flink_job_id)
            run = self._store.get_run(run_id)
            if run is None:
                raise KeyError(run_id)
        if run.agent.startswith("pipeline:"):
            run.plan = []
        else:
            run.plan = agent_execution_plan(run.agent)
        return _run_to_dict(run)

    def list_runs(self, *, agent: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if agent:
            try:
                get_agent_spec(agent)
            except AgentRegistryError as exc:
                raise AgentRegistryError(str(exc)) from exc
        runs = self._store.list_runs(agent=agent, limit=limit)
        return [_run_to_dict(run, include_plan=False) for run in runs]


def reset_run_service_for_tests() -> None:
    global _default_service
    _default_service = None


def default_run_service(root: Path | None = None) -> RunService:
    global _default_service
    if _default_service is None or root is not None:
        path = runs_db_path(root)
        _default_service = RunService(RunStore(path))
    return _default_service
