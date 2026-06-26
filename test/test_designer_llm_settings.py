#!/usr/bin/env python3
"""Agent designer LLM settings tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_react_llm_settings_store_and_api() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.llm_settings import get_react_llm_settings, reset_designer_store_for_tests

    reset_designer_store_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)

        client = TestClient(create_app(ApiSettings(api_key=None)))

        empty = client.get("/v1/designer/llm-settings").json()
        assert empty["scope"] == "react"
        assert empty["configured"] is False

        saved = client.put(
            "/v1/designer/llm-settings",
            json={
                "endpoint_url": "https://llm.example/v1",
                "model_id": "test-model",
                "api_key": "secret-key-1234",
            },
        )
        assert saved.status_code == 200
        body = saved.json()
        assert body["endpoint_url"] == "https://llm.example/v1"
        assert body["model_id"] == "test-model"
        assert body["configured"] is True
        assert body["api_key_set"] is True
        assert body["api_key_hint"].endswith("1234")
        assert "secret-key" not in body["api_key_hint"][:-4]

        resolved = get_react_llm_settings()
        assert resolved.endpoint_url == "https://llm.example/v1"
        assert resolved.model_id == "test-model"
        assert resolved.api_key == "secret-key-1234"

        kept = client.put(
            "/v1/designer/llm-settings",
            json={
                "endpoint_url": "https://llm.example/v1",
                "model_id": "test-model-v2",
            },
        ).json()
        assert kept["model_id"] == "test-model-v2"
        assert get_react_llm_settings().api_key == "secret-key-1234"

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_designer_store_for_tests()


def test_react_llm_settings_test_endpoint_incomplete() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.llm_settings import reset_designer_store_for_tests

    reset_designer_store_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        resp = client.post("/v1/designer/llm-settings/test", json={})
        assert resp.status_code == 400
        assert "not configured" in resp.json()["detail"].lower()

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_designer_store_for_tests()


def test_react_llm_settings_test_endpoint_success(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.llm_settings import reset_designer_store_for_tests

    def _fake_chat_completion_json(**kwargs):
        return {"input": 3, "doubled": 6, "reasoning": "test double"}

    monkeypatch.setattr(
        "ratatoskr.designer.llm_client.chat_completion_json",
        _fake_chat_completion_json,
    )

    reset_designer_store_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        client.put(
            "/v1/designer/llm-settings",
            json={
                "endpoint_url": "https://llm.example/v1",
                "model_id": "test-model",
                "api_key": "secret-key-1234",
            },
        )

        resp = client.post(
            "/v1/designer/llm-settings/test",
            json={"endpoint_url": "https://llm.example/v1", "model_id": "test-model"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["result"]["input"] == 3
        assert body["result"]["doubled"] == 6
        assert body["duration_ms"] >= 0

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_designer_store_for_tests()


def test_catalog_includes_llm_required() -> None:
    from ratatoskr.agents.catalog import agent_catalog_response

    catalog = agent_catalog_response()
    react = next(c for c in catalog["categories"] if c["id"] == "react")
    assert react["llm_required"] is True
    assert "react_llm_defaults" in catalog


if __name__ == "__main__":
    test_react_llm_settings_store_and_api()
    print("OK  LLM settings store + API")
    test_react_llm_settings_test_endpoint_incomplete()
    print("OK  LLM test endpoint incomplete")
    import pytest

    test_react_llm_settings_test_endpoint_success(pytest.MonkeyPatch())
    print("OK  LLM test endpoint success")
    test_catalog_includes_llm_required()
    print("OK  catalog llm_required")
    print("PASS")
