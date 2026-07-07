#!/usr/bin/env python3
"""Tests for API fetch platform settings and workflow_api_fetch agent."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def test_api_fetch_settings_update_and_api_view(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ratatoskr.designer.api_fetch_settings import (
        api_fetch_settings_for_api,
        reset_api_fetch_store_for_tests,
        update_api_fetch_settings,
    )

    monkeypatch.setenv("RATATOSKR_DESIGNER_DB", str(tmp_path / "designer.db"))
    reset_api_fetch_store_for_tests()

    update_api_fetch_settings(
        endpoint_url="https://api.example.com/v1/",
        http_method="GET",
        api_key="secret-key",
        root=tmp_path,
    )
    view = api_fetch_settings_for_api(root=tmp_path)
    assert view["configured"] is True
    assert view["endpoint_url"] == "https://api.example.com/v1/"
    assert view["api_key_set"] is True
    assert view["source"] == "designer"


def test_build_fetch_url_with_query() -> None:
    from ratatoskr.designer.api_fetch_settings import build_fetch_url
    from ratatoskr.designer.models import ApiFetchSettings

    settings = ApiFetchSettings(endpoint_url="https://api.example.com/data")
    url = build_fetch_url(settings, {"query": {"limit": 5, "tag": "demo"}})
    assert url.startswith("https://api.example.com/data?")
    assert "limit=5" in url
    assert "tag=demo" in url


def test_fetch_with_settings_uses_http_helper() -> None:
    from ratatoskr.designer.api_fetch_settings import fetch_with_settings
    from ratatoskr.designer.models import ApiFetchSettings

    settings = ApiFetchSettings(endpoint_url="https://api.example.com/items")
    with patch(
        "ratatoskr.designer.api_fetch_settings.http_fetch_json",
        return_value={"ok": True, "status_code": 200, "url": "https://api.example.com/items", "data": {"id": 1}},
    ) as mocked:
        result = fetch_with_settings(settings)
    assert result["ok"] is True
    mocked.assert_called_once()


def test_workflow_api_fetch_agent_emits_output() -> None:
    pytest.importorskip("flink_agents")
    from flink_agents.api.events.event import InputEvent, OutputEvent
    from flink_agents.api.runner_context import RunnerContext

    from examples.agents.workflow_api_fetch import ApiFetchAgent

    class _Ctx(RunnerContext):
        def __init__(self) -> None:
            self.events: list[OutputEvent] = []

        def send_event(self, event) -> None:  # type: ignore[no-untyped-def]
            self.events.append(event)

    ctx = _Ctx()
    event = InputEvent(input={"query": {"foo": "bar"}}).to_event()
    with patch(
        "ratatoskr.designer.api_fetch_settings.fetch_with_settings",
        return_value={
            "ok": True,
            "status_code": 200,
            "url": "https://api.example.com/?foo=bar",
            "data": {"hello": "world"},
            "error": None,
        },
    ):
        ApiFetchAgent.process(event, ctx)

    assert len(ctx.events) == 1
    output = OutputEvent.from_event(ctx.events[0]).output
    assert output["agent"] == "workflow_api_fetch"
    assert output["event_type"] == "api.fetch.result"
    assert output["ok"] is True
    assert output["record_count"] == 1
    assert output["records"][0]["payload"] == {"hello": "world"}


def test_normalize_api_response_unwraps_list_wrappers() -> None:
    pytest.importorskip("flink_agents")
    from examples.agents.workflow_api_fetch import ApiFetchAgent

    records = ApiFetchAgent.normalize_api_response(
        {"items": [{"id": 1}, {"id": 2}]},
        url="https://api.example.com/items",
        fetched_at="2026-01-01T00:00:00+00:00",
    )
    assert len(records) == 2
    assert records[0]["payload"] == {"id": 1}
    assert records[0]["wrapper"] == "items"
    assert records[1]["index"] == 1


def test_workflow_api_fetch_expand_records_emits_one_event_per_record() -> None:
    pytest.importorskip("flink_agents")
    from flink_agents.api.events.event import InputEvent, OutputEvent
    from flink_agents.api.runner_context import RunnerContext

    from examples.agents.workflow_api_fetch import ApiFetchAgent

    class _Ctx(RunnerContext):
        def __init__(self) -> None:
            self.events: list[OutputEvent] = []

        def send_event(self, event) -> None:  # type: ignore[no-untyped-def]
            self.events.append(event)

    ctx = _Ctx()
    event = InputEvent(input={"expand_records": True}).to_event()
    with patch(
        "ratatoskr.designer.api_fetch_settings.fetch_with_settings",
        return_value={
            "ok": True,
            "status_code": 200,
            "url": "https://api.example.com/items",
            "data": [{"id": 1}, {"id": 2}],
            "error": None,
        },
    ):
        ApiFetchAgent.process(event, ctx)

    assert len(ctx.events) == 2
    first = OutputEvent.from_event(ctx.events[0]).output
    second = OutputEvent.from_event(ctx.events[1]).output
    assert first["record"]["payload"] == {"id": 1}
    assert second["record"]["payload"] == {"id": 2}


def test_api_fetch_settings_api_route(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.api_fetch_settings import reset_api_fetch_store_for_tests

    os.environ.pop("RATATOSKR_API_KEY", None)
    monkeypatch.setenv("RATATOSKR_DESIGNER_DB", str(tmp_path / "designer.db"))
    reset_api_fetch_store_for_tests()

    client = TestClient(create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1)))
    spec = client.get("/openapi.json").json()
    assert "/v1/designer/api-fetch-settings" in spec["paths"]

    with patch(
        "ratatoskr.designer.api_fetch_settings.http_fetch_json",
        return_value={"ok": True, "status_code": 200, "url": "https://httpbin.org/get", "data": {"ok": True}},
    ):
        response = client.put(
            "/v1/designer/api-fetch-settings",
            json={"endpoint_url": "https://httpbin.org/get", "http_method": "GET"},
        )
        assert response.status_code == 200
        assert response.json()["configured"] is True

        test_response = client.post("/v1/designer/api-fetch-settings/test")
        assert test_response.status_code == 200
        assert test_response.json()["ok"] is True
