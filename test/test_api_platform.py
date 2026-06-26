#!/usr/bin/env python3
"""Control API tests — no Docker or running Flink required."""

from __future__ import annotations

import os


def test_openapi_and_health() -> None:
    os.environ.pop("RATATOSKR_API_KEY", None)
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    settings = ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1)
    client = TestClient(create_app(settings))

    root = client.get("/")
    assert root.status_code == 200
    assert root.json()["service"] == "ratatoskr-api"

    spec = client.get("/openapi.json")
    assert spec.status_code == 200
    assert "/v1/agents" in spec.json()["paths"]
    assert "/v1/runs" in spec.json()["paths"]

    health = client.get("/v1/health")
    assert health.status_code == 200
    body = health.json()
    assert "flink" in body
    assert body["status"] in ("ok", "degraded", "unavailable")

    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert b"ratatoskr_api_requests_total" in metrics.content


def test_cluster_status_route() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    client = TestClient(create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1)))
    response = client.get("/v1/cluster/status")
    assert response.status_code == 200
    body = response.json()
    assert "ready" in body
    assert "checks" in body
    assert isinstance(body["checks"], list)
    assert body["flink_rest_url"].startswith("http://")

    validate = client.post("/v1/cluster/validate")
    assert validate.status_code == 200
    assert validate.json()["checks"]


def test_api_key_auth() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    settings = ApiSettings(api_key="test-secret", flink_rest_host="127.0.0.1", flink_rest_port=1)
    client = TestClient(create_app(settings))

    denied = client.get("/v1/agents")
    assert denied.status_code == 401

    allowed = client.get("/v1/agents", headers={"X-API-Key": "test-secret"})
    assert allowed.status_code == 200
    names = {item["name"] for item in allowed.json()}
    assert "workflow_counter" in names

    # Health stays open without a key.
    health = client.get("/v1/health")
    assert health.status_code == 200


def test_agent_describe_route() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    client = TestClient(create_app(ApiSettings(api_key=None)))
    detail = client.get("/v1/agents/workflow_counter")
    assert detail.status_code == 200
    assert detail.json()["name"] == "workflow_counter"


def test_agent_definition_route() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    client = TestClient(create_app(ApiSettings(api_key=None)))
    detail = client.get("/v1/agents/workflow_counter/definition")
    assert detail.status_code == 200
    body = detail.json()
    assert body["name"] == "workflow_counter"
    assert "flink_yaml" in body
    assert "workflow_counter_actions" in (body.get("flink_yaml") or "")


def test_events_sse() -> None:
    import asyncio

    from ratatoskr.api.config import ApiSettings
    from ratatoskr.api.events import event_stream

    async def first_event() -> str:
        settings = ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1)
        gen = event_stream(settings, interval_sec=0.01)
        return await gen.__anext__()

    chunk = asyncio.run(first_event())
    assert chunk.startswith("data: ")
    assert "snapshot" in chunk


def test_events_sse_route_registered() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    client = TestClient(create_app(ApiSettings(api_key=None)))
    spec = client.get("/openapi.json").json()
    assert "/v1/events" in spec["paths"]


def main() -> int:
    print("=" * 60)
    print("Control API platform tests")
    print("=" * 60)
    test_openapi_and_health()
    print("OK  openapi + health + metrics")
    test_api_key_auth()
    print("OK  api key auth")
    test_agent_describe_route()
    print("OK  agent routes")
    test_agent_definition_route()
    print("OK  agent definition + flink yaml")
    test_events_sse()
    print("OK  SSE event_stream generator")
    test_events_sse_route_registered()
    print("OK  SSE /v1/events route")
    print("=" * 60)
    print("PASS")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
