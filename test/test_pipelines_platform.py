#!/usr/bin/env python3
"""Pipeline composition and Agentic Studio API tests — no Docker required."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_pipeline_store_crud() -> None:
    from apemosyne.pipelines.service import PipelineService, reset_pipeline_service_for_tests
    from apemosyne.pipelines.store import PipelineStore

    reset_pipeline_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        service = PipelineService(PipelineStore(Path(tmp) / "pipelines.db"))
        created = service.create(
            name="Test pipeline",
            nodes=[
                {"id": "src1", "kind": "source", "config": {"records": [{"key": "1", "value": 1}]}},
                {"id": "a1", "kind": "agent", "agent": "workflow_counter"},
                {"id": "sink1", "kind": "sink"},
            ],
            edges=[
                {"id": "e1", "source": "src1", "target": "a1"},
                {"id": "e2", "source": "a1", "target": "sink1"},
            ],
            layout={"src1": {"x": 0, "y": 0}, "a1": {"x": 100, "y": 0}, "sink1": {"x": 200, "y": 0}},
        )
        pid = created["id"]
        detail = service.get(pid)
        assert detail["name"] == "Test pipeline"
        assert len(detail["nodes"]) == 3

        validation = service.validate(pid)
        assert validation["valid"] is True

        service.update(pid, {"name": "Renamed"})
        assert service.get(pid)["name"] == "Renamed"

        listed = service.list_pipelines()
        assert any(p["id"] == pid for p in listed)

        service.delete(pid)
        try:
            service.get(pid)
            raise AssertionError("expected KeyError")
        except KeyError:
            pass


def test_pipeline_validation_errors() -> None:
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from apemosyne.pipelines.validate import validate_pipeline

    bad = Pipeline(
        id="pipe_bad",
        name="bad",
        nodes=[
            PipelineNode(id="s1", kind="source"),
            PipelineNode(id="s2", kind="source"),
        ],
        edges=[],
    )
    result = validate_pipeline(bad)
    assert result["valid"] is False
    assert any("source" in e for e in result["errors"])


def test_agent_graph_introspect() -> None:
    from apemosyne.pipelines.introspect import agent_graph

    graph = agent_graph("workflow_counter")
    assert graph["agent"] == "workflow_counter"
    assert any(n["name"] == "double" for n in graph["nodes"])
    assert graph["edges"]

    echo = agent_graph("react_echo")
    assert any(n["name"] == "classify" for n in echo["nodes"])


def test_apply_edge_mapping() -> None:
    from apemosyne.pipelines.executor import apply_edge_mapping

    out = apply_edge_mapping(
        [{"key": "1", "doubled": 6, "agent": "workflow_counter"}],
        {"message": "$.doubled"},
    )
    assert out[0]["message"] == 6


def test_pipelines_api_routes() -> None:
    os.environ.pop("APEMOSYNE_API_KEY", None)
    from fastapi.testclient import TestClient

    from apemosyne.api.app import create_app
    from apemosyne.api.config import ApiSettings
    from apemosyne.pipelines.service import PipelineService, reset_pipeline_service_for_tests
    from apemosyne.pipelines.store import PipelineStore

    reset_pipeline_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "pipelines.db"
        os.environ["APEMOSYNE_PIPELINES_DB"] = str(db)
        service = PipelineService(PipelineStore(db))
        created = service.create(name="API test", nodes=[], edges=[])

        client = TestClient(
            create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1))
        )

        spec = client.get("/openapi.json").json()
        assert "/v1/pipelines" in spec["paths"]
        assert "/v1/pipelines/{pipeline_id}/run" in spec["paths"]
        assert "/v1/agents/{name}/graph" in spec["paths"]

        listed = client.get("/v1/pipelines").json()
        assert any(p["id"] == created["id"] for p in listed)

        graph = client.get("/v1/agents/workflow_counter/graph").json()
        assert graph["agent"] == "workflow_counter"
        assert graph["nodes"]

        missing = client.get("/v1/pipelines/does-not-exist")
        assert missing.status_code == 404

        created_via_api = client.post(
            "/v1/pipelines",
            json={"name": "POST test", "nodes": [], "edges": []},
        )
        assert created_via_api.status_code == 200
        assert created_via_api.json()["name"] == "POST test"

        del os.environ["APEMOSYNE_PIPELINES_DB"]
        reset_pipeline_service_for_tests()


def test_pipeline_local_run_optional() -> None:
    """Run Counter→Echo when flink_agents is installed."""
    try:
        import flink_agents  # noqa: F401
    except ImportError:
        print("SKIP  pipeline local run (flink_agents not installed)")
        return

    import tempfile

    from apemosyne.pipelines.service import PipelineService, reset_pipeline_service_for_tests
    from apemosyne.pipelines.store import PipelineStore
    from apemosyne.runs.service import RunService, reset_run_service_for_tests
    from apemosyne.runs.store import RunStore

    reset_pipeline_service_for_tests()
    reset_run_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        os.environ["APEMOSYNE_PIPELINES_DB"] = str(root / "pipelines.db")
        os.environ["APEMOSYNE_RUNS_DB"] = str(root / "runs.db")

        pipe_svc = PipelineService(PipelineStore(root / "pipelines.db"))
        from apemosyne.pipelines.service import seed_counter_echo_pipeline

        pipeline = seed_counter_echo_pipeline(pipe_svc)
        result = pipe_svc.run_local(pipeline["id"])
        assert result["status"] == "finished"
        assert result["run_id"]

        run_svc = RunService(RunStore(root / "runs.db"))
        detail = run_svc.get_run(result["run_id"])
        assert detail["status"] == "finished"
        assert len(detail["spans"]) >= 2

        del os.environ["APEMOSYNE_PIPELINES_DB"]
        del os.environ["APEMOSYNE_RUNS_DB"]
        reset_pipeline_service_for_tests()
        reset_run_service_for_tests()


def main() -> int:
    print("=" * 60)
    print("Pipeline / Agentic Studio platform tests")
    print("=" * 60)
    test_pipeline_store_crud()
    print("OK  pipeline store CRUD")
    test_pipeline_validation_errors()
    print("OK  pipeline validation")
    test_agent_graph_introspect()
    print("OK  agent graph introspection")
    test_apply_edge_mapping()
    print("OK  edge mapping")
    test_pipelines_api_routes()
    print("OK  pipelines API routes")
    test_pipeline_local_run_optional()
    print("OK  pipeline local run (or skipped)")
    print("=" * 60)
    print("PASS")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
