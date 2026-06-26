"""High-level pipeline registry operations."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ratatoskr.constants import DEFAULT_PROFILE
from ratatoskr.pipelines.executor import run_pipeline_local
from ratatoskr.pipelines.cluster_submit import submit_pipeline_cluster, submit_result_to_dict
from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode
from ratatoskr.pipelines.store import PipelineStore, pipelines_db_path
from ratatoskr.pipelines.validate import validate_pipeline

_default_service: "PipelineService | None" = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pipeline_to_dict(pipeline: Pipeline) -> dict[str, Any]:
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "nodes": [asdict(n) for n in pipeline.nodes],
        "edges": [asdict(e) for e in pipeline.edges],
        "layout": pipeline.layout,
        "created_at": pipeline.created_at,
        "updated_at": pipeline.updated_at,
    }


def _parse_nodes(raw: list[dict[str, Any]]) -> list[PipelineNode]:
    return [
        PipelineNode(
            id=n["id"],
            kind=n["kind"],
            agent=n.get("agent"),
            config=n.get("config") or {},
        )
        for n in raw
    ]


def _parse_edges(raw: list[dict[str, Any]]) -> list[PipelineEdge]:
    return [
        PipelineEdge(
            id=e["id"],
            source=e["source"],
            target=e["target"],
            mapping=e.get("mapping") or {},
        )
        for e in raw
    ]


class PipelineService:
    def __init__(self, store: PipelineStore) -> None:
        self._store = store

    def create(
        self,
        name: str,
        *,
        nodes: list[dict[str, Any]] | None = None,
        edges: list[dict[str, Any]] | None = None,
        layout: dict[str, dict[str, float]] | None = None,
    ) -> dict[str, Any]:
        pipeline_id = f"pipe_{uuid.uuid4().hex[:12]}"
        now = _utc_now()
        node_list = nodes or []
        edge_list = edges or []
        layout_dict = layout or {}
        self._store.insert(
            pipeline_id=pipeline_id,
            name=name,
            nodes=node_list,
            edges=edge_list,
            layout=layout_dict,
            created_at=now,
            updated_at=now,
        )
        return self.get(pipeline_id)

    def get(self, pipeline_id: str) -> dict[str, Any]:
        pipeline = self._store.get(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        return _pipeline_to_dict(pipeline)

    def list_pipelines(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return [_pipeline_to_dict(p) for p in self._store.list_pipelines(limit=limit)]

    def update(self, pipeline_id: str, body: dict[str, Any]) -> dict[str, Any]:
        pipeline = self._store.get(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)

        nodes = body.get("nodes")
        edges = body.get("edges")
        if nodes is not None:
            pipeline.nodes = _parse_nodes(nodes)
        if edges is not None:
            pipeline.edges = _parse_edges(edges)
        if "layout" in body:
            pipeline.layout = body["layout"] or {}
        if "name" in body:
            pipeline.name = str(body["name"] or "")

        updated = self._store.update(
            pipeline_id,
            name=pipeline.name,
            nodes=[asdict(n) for n in pipeline.nodes] if nodes is not None else None,
            edges=[asdict(e) for e in pipeline.edges] if edges is not None else None,
            layout=pipeline.layout if "layout" in body else None,
            updated_at=_utc_now(),
        )
        if not updated:
            raise KeyError(pipeline_id)
        return self.get(pipeline_id)

    def delete(self, pipeline_id: str) -> None:
        if not self._store.delete(pipeline_id):
            raise KeyError(pipeline_id)

    def validate(self, pipeline_id: str, *, include_cluster: bool = True) -> dict[str, Any]:
        pipeline = self._store.get(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        result = dict(validate_pipeline(pipeline))
        if include_cluster:
            from ratatoskr.pipelines.validate_cluster import validate_pipeline_cluster

            cluster = validate_pipeline_cluster(pipeline)
            result["cluster"] = {
                "valid": cluster["valid"],
                "errors": cluster["errors"],
                "warnings": cluster["warnings"],
                "mode": cluster.get("mode"),
            }
        return result

    def run_local(
        self,
        pipeline_id: str,
        *,
        input_override: list[dict[str, Any]] | None = None,
        profile: str | None = None,
    ) -> dict[str, Any]:
        from ratatoskr.runs.service import default_run_service

        pipeline = self._store.get(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        return run_pipeline_local(
            pipeline,
            run_service=default_run_service(),
            input_override=input_override,
            profile=profile,
        )

    def submit_cluster(
        self,
        pipeline_id: str,
        *,
        profile: str | None = None,
    ) -> dict[str, Any]:
        from ratatoskr.runs.service import default_run_service

        pipeline = self._store.get(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        result = submit_pipeline_cluster(
            pipeline,
            runs=default_run_service(),
            profile=profile or DEFAULT_PROFILE,
        )
        if result.return_code != 0:
            detail = f"Pipeline cluster submit failed with exit code {result.return_code}"
            run = default_run_service().get_run(result.run_id)
            if run and run.get("error"):
                detail = f"{detail}: {run['error']}"
            raise RuntimeError(detail)
        return submit_result_to_dict(result, pipeline=pipeline)


def seed_counter_echo_pipeline(service: PipelineService) -> dict[str, Any]:
    """Create the reference Counter → Echo demo pipeline if none exist."""
    existing = service.list_pipelines(limit=1)
    if existing:
        return existing[0]
    return service.create(
        name="Counter then Echo",
        nodes=[
            {
                "id": "src1",
                "kind": "source",
                "config": {
                    "records": [
                        {"key": "1", "value": 3},
                        {"key": "2", "value": 10},
                    ]
                },
            },
            {"id": "agent_wc", "kind": "agent", "agent": "workflow_counter"},
            {"id": "agent_re", "kind": "agent", "agent": "react_echo"},
            {"id": "sink1", "kind": "sink"},
        ],
        edges=[
            {"id": "e1", "source": "src1", "target": "agent_wc"},
            {"id": "e2", "source": "agent_wc", "target": "agent_re", "mapping": {"message": "$.doubled"}},
            {"id": "e3", "source": "agent_re", "target": "sink1"},
        ],
        layout={
            "src1": {"x": 80, "y": 200},
            "agent_wc": {"x": 320, "y": 200},
            "agent_re": {"x": 560, "y": 200},
            "sink1": {"x": 800, "y": 200},
        },
    )


def reset_pipeline_service_for_tests() -> None:
    global _default_service
    _default_service = None


def default_pipeline_service(root: Path | None = None) -> PipelineService:
    global _default_service
    if _default_service is None or root is not None:
        path = pipelines_db_path(root)
        _default_service = PipelineService(PipelineStore(path))
    return _default_service
