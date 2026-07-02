#!/usr/bin/env python3
"""LLM-assisted agent definition generation tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest


def _workflow_llm_payload() -> dict[str, Any]:
    return {
        "definition": {
            "name": "Scale numbers",
            "type": "workflow",
            "description": "Scales numeric input by two.",
            "nodes": [
                {"id": "in1", "kind": "input_event", "name": "InputEvent", "config": {"event_type": "_input_event"}},
                {"id": "act1", "kind": "action", "name": "process", "config": {"listens_to": ["_input_event"]}},
                {
                    "id": "tool1",
                    "kind": "tool",
                    "name": "scale",
                    "config": {"tool_ref": "scale", "expression": "value * 2"},
                },
                {"id": "out1", "kind": "output_event", "name": "OutputEvent", "config": {"event_type": "_output_event"}},
            ],
            "edges": [
                {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
                {"id": "e2", "source": "act1", "target": "tool1", "kind": "calls"},
                {"id": "e3", "source": "act1", "target": "out1", "kind": "emits"},
            ],
            "layout": {
                "in1": {"x": 80, "y": 200},
                "act1": {"x": 320, "y": 200},
                "tool1": {"x": 560, "y": 120},
                "out1": {"x": 560, "y": 280},
            },
            "input_schema": {
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "integer"}},
            },
            "output_schema": {"type": "object", "properties": {"value": {"type": "integer"}}},
            "catalog_category_id": "workflow",
            "catalog_subcategory_id": "transform",
            "catalog_tags": ["llm-assisted"],
            "mcp_servers": [],
        },
        "rationale": "Simple deterministic workflow using built-in scale tool.",
        "test_records": [{"key": "1", "value": 4}],
        "warnings": ["Assumes integer input"],
    }


def _react_llm_payload() -> dict[str, Any]:
    return {
        "definition": {
            "name": "Message helper",
            "type": "react",
            "description": "ReAct agent that answers with JSON.",
            "nodes": [
                {"id": "in1", "kind": "input_event", "name": "InputEvent", "config": {"event_type": "_input_event"}},
                {"id": "act1", "kind": "action", "name": "process", "config": {"listens_to": ["_input_event"]}},
                {
                    "id": "prompt1",
                    "kind": "prompt",
                    "name": "prompt",
                    "config": {
                        "template": "assist",
                        "system": "Respond with JSON only.",
                        "user": "{message}",
                    },
                },
                {"id": "llm1", "kind": "llm_call", "name": "llm", "config": {"use_platform_llm": True, "mode": "simple"}},
                {"id": "out1", "kind": "output_event", "name": "OutputEvent", "config": {"event_type": "_output_event"}},
            ],
            "edges": [
                {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
                {"id": "e2", "source": "act1", "target": "prompt1", "kind": "calls"},
                {"id": "e3", "source": "act1", "target": "llm1", "kind": "calls"},
                {"id": "e4", "source": "act1", "target": "out1", "kind": "emits"},
            ],
            "layout": {
                "in1": {"x": 80, "y": 200},
                "act1": {"x": 320, "y": 200},
                "prompt1": {"x": 560, "y": 120},
                "llm1": {"x": 560, "y": 200},
                "out1": {"x": 560, "y": 280},
            },
            "input_schema": {
                "type": "object",
                "required": ["message"],
                "properties": {"message": {"type": "string"}},
            },
            "output_schema": {"type": "object", "properties": {"message": {"type": "string"}}},
            "catalog_category_id": "react",
            "catalog_subcategory_id": "numeric",
            "catalog_tags": ["llm-assisted"],
            "mcp_servers": [],
        },
        "rationale": "ReAct graph with prompt and llm_call nodes.",
        "test_records": [{"key": "1", "message": "hello"}],
        "warnings": [],
    }


def test_normalize_workflow_proposal() -> None:
    from ratatoskr.designer.definitions.assist import normalize_proposal

    result = normalize_proposal(_workflow_llm_payload(), preference="workflow")
    assert result["definition"]["name"] == "Scale numbers"
    assert result["definition"]["type"] == "workflow"
    assert result["validation"]["valid"] is True
    assert result["test_records"] == [{"key": "1", "value": 4}]


def test_normalize_react_proposal() -> None:
    from ratatoskr.designer.definitions.assist import normalize_proposal

    result = normalize_proposal(_react_llm_payload(), preference="react")
    assert result["definition"]["type"] == "react"
    assert result["validation"]["valid"] is True
    assert any(node["kind"] == "prompt" for node in result["definition"]["nodes"])


def test_normalize_rejects_missing_definition() -> None:
    from ratatoskr.designer.definitions.assist import normalize_proposal

    with pytest.raises(ValueError, match="missing definition"):
        normalize_proposal({"rationale": "bad"})


def test_normalize_sanitizes_invalid_node_kinds() -> None:
    from ratatoskr.designer.definitions.assist import normalize_proposal

    payload = _workflow_llm_payload()
    payload["definition"]["nodes"].append(
        {"id": "bad1", "kind": "unknown_kind", "name": "Bad", "config": {}}
    )
    result = normalize_proposal(payload, preference="workflow")
    kinds = {node["kind"] for node in result["definition"]["nodes"]}
    assert "unknown_kind" not in kinds
    assert result["validation"]["valid"] is True


def test_normalize_sanitizes_unknown_tool_ref() -> None:
    from ratatoskr.designer.definitions.assist import normalize_proposal

    payload = _workflow_llm_payload()
    for node in payload["definition"]["nodes"]:
        if node["kind"] == "tool":
            node["config"]["tool_ref"] = "not_a_real_tool"
    result = normalize_proposal(payload, preference="workflow")
    tool = next(node for node in result["definition"]["nodes"] if node["kind"] == "tool")
    assert tool["config"]["tool_ref"] in {"double", "scale", "identity"}


def test_generate_agent_definition_calls_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    from ratatoskr.designer.definitions import assist as assist_module

    monkeypatch.setattr(
        assist_module,
        "chat_completion_json",
        lambda **kwargs: _workflow_llm_payload(),
    )
    result = assist_module.generate_agent_definition("Scale incoming numbers")
    assert result["definition"]["type"] == "workflow"
    assert result["validation"]["valid"] is True


def test_refine_agent_definition_calls_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    from ratatoskr.designer.definitions import assist as assist_module
    from ratatoskr.designer.definitions.seed import double_value_definition_payload

    monkeypatch.setattr(
        assist_module,
        "chat_completion_json",
        lambda **kwargs: _react_llm_payload(),
    )
    result = assist_module.refine_agent_definition(
        double_value_definition_payload(),
        "Convert this into a ReAct agent with a prompt",
    )
    assert result["definition"]["type"] == "react"
    assert result["validation"]["valid"] is True


def test_assist_api_generate_and_refine(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.definitions import assist as assist_module
    from ratatoskr.designer.definitions.seed import DOUBLE_VALUE_ID
    from ratatoskr.designer.definitions.service import reset_agent_definition_service_for_tests

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        from ratatoskr.designer.definitions.service import AgentDefinitionService
        from ratatoskr.designer.definitions.store import AgentDefinitionStore

        service = AgentDefinitionService(AgentDefinitionStore(db))
        service.seed_double_value()

        monkeypatch.setattr(
            assist_module,
            "chat_completion_json",
            lambda **kwargs: _workflow_llm_payload(),
        )

        generated = client.post(
            "/v1/agent-definitions/assist/generate",
            json={"goal": "Scale numbers", "agent_type_preference": "workflow"},
        )
        assert generated.status_code == 200
        body = generated.json()
        assert body["definition"]["name"] == "Scale numbers"
        assert body["validation"]["valid"] is True
        assert body["test_records"]

        monkeypatch.setattr(
            assist_module,
            "chat_completion_json",
            lambda **kwargs: _react_llm_payload(),
        )
        refined = client.post(
            f"/v1/agent-definitions/{DOUBLE_VALUE_ID}/assist/refine",
            json={"instruction": "Make this a ReAct agent"},
        )
        assert refined.status_code == 200
        assert refined.json()["definition"]["type"] == "react"

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_assist_api_llm_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.definitions import assist as assist_module
    from ratatoskr.designer.definitions.service import reset_agent_definition_service_for_tests
    from ratatoskr.designer.llm_client import LlmNotConfiguredError

    reset_agent_definition_service_for_tests()

    def _raise_not_configured(**kwargs: Any) -> dict[str, Any]:
        raise LlmNotConfiguredError("LLM not configured for tests")

    monkeypatch.setattr(assist_module, "chat_completion_json", _raise_not_configured)

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        response = client.post(
            "/v1/agent-definitions/assist/generate",
            json={"goal": "Build an agent"},
        )
        assert response.status_code == 400
        assert "LLM not configured" in response.json()["detail"]

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_assist_design_context_includes_tools_and_examples() -> None:
    from ratatoskr.designer.definitions.assist import assist_design_context

    context = assist_design_context()
    assert context["builtin_tools"]
    assert "workflow" in context["examples"]
    assert "react" in context["examples"]
    assert "input_event" in context["node_kinds"]
