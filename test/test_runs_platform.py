#!/usr/bin/env python3
"""Agent run registry tests — no Docker required."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_run_store_crud() -> None:
    from ratatoskr.runs.service import RunService, reset_run_service_for_tests
    from ratatoskr.runs.store import RunStore

    reset_run_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        service = RunService(RunStore(root / "runs.db"))

        run_id = service.create_run("workflow_counter", kind="local", status="running")
        service.append_span(
            run_id,
            kind="tool",
            name="double",
            input_data={"value": 3},
            output_data=6,
        )
        service.finish_run(run_id, status="finished", record_count=3)

        detail = service.get_run(run_id)
        assert detail["agent"] == "workflow_counter"
        assert detail["kind"] == "local"
        assert detail["status"] == "finished"
        assert detail["record_count"] == 3
        assert len(detail["spans"]) == 1
        assert detail["spans"][0]["name"] == "double"
        assert detail["plan"][0]["name"] == "process"

        listed = service.list_runs(agent="workflow_counter")
        assert len(listed) == 1
        assert listed[0]["id"] == run_id


def test_agent_execution_plans() -> None:
    from ratatoskr.runs.plan import agent_execution_plan, cluster_job_name

    wf = agent_execution_plan("workflow_counter")
    assert any(s["name"] == "double" for s in wf)
    re = agent_execution_plan("react_echo")
    assert any(s["name"] == "classify" for s in re)
    assert cluster_job_name("workflow_counter") == "Ratatoskr Workflow Counter"


def test_find_flink_job_prefers_newest_match() -> None:
    from ratatoskr.runs.plan import _newest_job_id

    jobs = [
        {"jid": "old_failed", "name": "Ratatoskr Workflow Counter", "start-time": 100},
        {"jid": "new_finished", "name": "Ratatoskr Workflow Counter", "start-time": 200},
        {"jid": "other", "name": "Other Job", "start-time": 300},
    ]
    assert _newest_job_id(jobs, "Ratatoskr Workflow Counter") == "new_finished"
    assert _newest_job_id(jobs, "Missing") is None


def test_runs_api_routes() -> None:
    os.environ.pop("RATATOSKR_API_KEY", None)
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.runs.service import RunService, reset_run_service_for_tests
    from ratatoskr.runs.store import RunStore

    reset_run_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "runs.db"
        os.environ["RATATOSKR_RUNS_DB"] = str(db)
        service = RunService(RunStore(db))
        run_id = service.create_run("react_echo", kind="local", status="running")
        service.finish_run(run_id, status="finished")

        client = TestClient(
            create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1))
        )

        spec = client.get("/openapi.json").json()
        assert "/v1/runs" in spec["paths"]
        assert "/v1/runs/{run_id}" in spec["paths"]

        runs = client.get("/v1/runs").json()
        assert any(r["id"] == run_id for r in runs)

        detail = client.get(f"/v1/runs/{run_id}").json()
        assert detail["id"] == run_id
        assert len(detail["plan"]) >= 3

        agent_runs = client.get("/v1/agents/react_echo/runs").json()
        assert agent_runs[0]["id"] == run_id

        span = client.post(
            f"/v1/runs/{run_id}/spans",
            json={"kind": "action", "name": "process", "input": {"message": "test"}},
        )
        assert span.status_code == 200
        assert span.json()["run_id"] == run_id

        missing = client.get("/v1/runs/does-not-exist")
        assert missing.status_code == 404

        del os.environ["RATATOSKR_RUNS_DB"]
        reset_run_service_for_tests()


def main() -> int:
    print("=" * 60)
    print("Agent runs platform tests")
    print("=" * 60)
    test_run_store_crud()
    print("OK  run store CRUD + plan")
    test_agent_execution_plans()
    print("OK  execution plans")
    test_runs_api_routes()
    print("OK  runs API routes")
    print("=" * 60)
    print("PASS")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
