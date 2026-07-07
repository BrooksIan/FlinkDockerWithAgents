#!/usr/bin/env python3
"""Pipeline assist generation tests."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import patch

import pytest


def _yggdrasil_llm_payload() -> dict[str, Any]:
    from ratatoskr.pipelines.service import yggdrasil_event_pipeline_template

    return {
        "pipeline": yggdrasil_event_pipeline_template(),
        "rationale": "Security pipeline with session window, detect, and ReAct enrichment.",
        "warnings": [],
    }


def test_normalize_intent_requires_goal() -> None:
    from ratatoskr.pipelines.assist import normalize_intent

    with pytest.raises(ValueError, match="Goal is required"):
        normalize_intent({})


def test_build_baseline_yggdrasil_intent() -> None:
    from ratatoskr.pipelines.assist import build_baseline_pipeline, normalize_intent

    intent = normalize_intent(
        {
            "goal": "Detect suspicious Cowrie sessions and enrich alerts",
            "domain": "cowrie_security",
            "use_windowing": True,
            "window_key_field": "src_ip",
            "window_gap_policy": "session_detect",
            "use_react_enrichment": True,
            "sink_type": "kafka",
            "sink_topic": "cowrie.react_alerts",
        }
    )
    pipeline = build_baseline_pipeline(intent)
    agents = [n["agent"] for n in pipeline["nodes"] if n.get("kind") == "agent"]
    assert agents == ["session_detect", "react_echo"]
    assert pipeline["edges"][2]["mapping"] == {"message": "$.severity"}


def test_build_baseline_counter_echo_intent() -> None:
    from ratatoskr.pipelines.assist import build_baseline_pipeline, normalize_intent

    intent = normalize_intent(
        {
            "goal": "Double values then classify the result",
            "domain": "numeric_transform",
            "use_react_enrichment": True,
        }
    )
    pipeline = build_baseline_pipeline(intent)
    agents = [n["agent"] for n in pipeline["nodes"] if n.get("kind") == "agent"]
    assert agents == ["workflow_counter", "react_echo"]


def test_normalize_pipeline_proposal_validates() -> None:
    from ratatoskr.pipelines.assist import build_baseline_pipeline, normalize_intent, normalize_pipeline_proposal

    intent = normalize_intent({"goal": "Counter then echo", "use_react_enrichment": True})
    baseline = build_baseline_pipeline(intent)
    result = normalize_pipeline_proposal(
        {"pipeline": baseline, "rationale": "Baseline draft", "warnings": []},
        intent=intent,
        baseline=baseline,
    )
    assert result["validation"]["valid"] is True
    assert result["pipeline"]["nodes"]


def test_generate_pipeline_assist_without_llm() -> None:
    from ratatoskr.designer.llm_client import LlmNotConfiguredError
    from ratatoskr.pipelines.assist import generate_pipeline_assist

    with patch(
        "ratatoskr.designer.llm_client.chat_completion_json",
        side_effect=LlmNotConfiguredError("LLM not configured"),
    ):
        result = generate_pipeline_assist(
            {
                "goal": "Detect suspicious Cowrie sessions and enrich alerts",
                "domain": "cowrie_security",
                "use_windowing": True,
                "use_react_enrichment": True,
            }
        )
    assert result["validation"]["valid"] is True
    assert any("LLM is not configured" in warning for warning in result["warnings"])


def test_generate_pipeline_assist_calls_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    from ratatoskr.pipelines.assist import generate_pipeline_assist

    monkeypatch.setattr(
        "ratatoskr.designer.llm_client.chat_completion_json",
        lambda **kwargs: _yggdrasil_llm_payload(),
    )
    result = generate_pipeline_assist(
        {
            "goal": "Build a Cowrie security enrichment pipeline",
            "domain": "cowrie_security",
            "use_windowing": True,
            "use_react_enrichment": True,
        }
    )
    assert result["validation"]["valid"] is True
    assert result["pipeline"]["name"] == "Yggdrasil Event Pipeline"


def test_pipeline_assist_api(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ.pop("RATATOSKR_API_KEY", None)
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    monkeypatch.setattr(
        "ratatoskr.designer.llm_client.chat_completion_json",
        lambda **kwargs: _yggdrasil_llm_payload(),
    )

    client = TestClient(create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1)))
    spec = client.get("/openapi.json").json()
    assert "/v1/pipelines/assist/generate" in spec["paths"]

    response = client.post(
        "/v1/pipelines/assist/generate",
        json={
            "goal": "Detect suspicious Cowrie sessions and enrich alerts",
            "domain": "cowrie_security",
            "use_windowing": True,
            "use_react_enrichment": True,
            "sink_type": "kafka",
            "sink_topic": "cowrie.react_alerts",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["validation"]["valid"] is True
    assert body["pipeline"]["nodes"]


def test_suggest_missing_agents_for_generic_goal() -> None:
    from ratatoskr.pipelines.agent_factory import suggest_missing_agents
    from ratatoskr.pipelines.assist import build_baseline_pipeline, normalize_intent, pipeline_assist_context

    intent = normalize_intent(
        {
            "goal": "Score customer support tickets by urgency and route escalations",
            "domain": "generic",
            "agent_creation_mode": "suggest",
        }
    )
    baseline = build_baseline_pipeline(intent)
    context = pipeline_assist_context()
    suggestions = suggest_missing_agents(intent, baseline, context)
    assert len(suggestions) >= 1
    assert suggestions[0]["role"] == "workflow"
    assert suggestions[0]["replaces_manifest"]
    assert suggestions[0]["definition"]


def test_generate_includes_suggested_agents_for_generic() -> None:
    from ratatoskr.designer.llm_client import LlmNotConfiguredError
    from ratatoskr.pipelines.assist import generate_pipeline_assist

    with patch(
        "ratatoskr.designer.llm_client.chat_completion_json",
        side_effect=LlmNotConfiguredError("LLM not configured"),
    ):
        result = generate_pipeline_assist(
            {
                "goal": "Score customer support tickets by urgency and route escalations",
                "domain": "generic",
                "agent_creation_mode": "suggest",
            }
        )
    assert result["validation"]["valid"] is True
    assert len(result["suggested_agents"]) >= 1
    assert result["agent_creation_mode"] == "suggest"
    assert any("new agent(s) suggested" in warning for warning in result["warnings"])


def test_existing_only_skips_suggestions() -> None:
    from ratatoskr.designer.llm_client import LlmNotConfiguredError
    from ratatoskr.pipelines.assist import generate_pipeline_assist

    with patch(
        "ratatoskr.designer.llm_client.chat_completion_json",
        side_effect=LlmNotConfiguredError("LLM not configured"),
    ):
        result = generate_pipeline_assist(
            {
                "goal": "Score customer support tickets by urgency and route escalations",
                "domain": "generic",
                "agent_creation_mode": "existing_only",
            }
        )
    assert result["suggested_agents"] == []


def test_build_pipeline_assist_applies_overrides() -> None:
    from ratatoskr.pipelines.assist import build_pipeline_assist, generate_pipeline_assist

    generated = generate_pipeline_assist(
        {
            "goal": "Score customer support tickets by urgency and route escalations",
            "domain": "generic",
            "agent_creation_mode": "suggest",
            "use_llm": False,
        }
    )

    suggestion = generated["suggested_agents"][0]
    with patch(
        "ratatoskr.pipelines.agent_factory.publish_approved_suggestions",
        return_value={suggestion["suggestion_id"]: "custom_ticket_router"},
    ):
        built = build_pipeline_assist(
            {
                "goal": "Score customer support tickets by urgency and route escalations",
                "domain": "generic",
                "agent_creation_mode": "suggest",
                "use_llm": False,
                "approved_suggestions": [suggestion],
            }
        )

    agents = [n["agent"] for n in built["pipeline"]["nodes"] if n.get("kind") == "agent"]
    assert "custom_ticket_router" in agents
    assert built["created_agents"][0]["manifest"] == "custom_ticket_router"


def test_pipeline_assist_build_api(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ.pop("RATATOSKR_API_KEY", None)
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.pipelines.assist import generate_pipeline_assist

    generated = generate_pipeline_assist(
        {
            "goal": "Score customer support tickets by urgency and route escalations",
            "domain": "generic",
            "agent_creation_mode": "suggest",
            "use_llm": False,
        }
    )
    suggestion = generated["suggested_agents"][0]

    monkeypatch.setattr(
        "ratatoskr.pipelines.agent_factory.publish_approved_suggestions",
        lambda approved, root=None: {suggestion["suggestion_id"]: "custom_ticket_router"},
    )

    client = TestClient(create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1)))
    spec = client.get("/openapi.json").json()
    assert "/v1/pipelines/assist/build" in spec["paths"]

    response = client.post(
        "/v1/pipelines/assist/build",
        json={
            "goal": "Score customer support tickets by urgency and route escalations",
            "domain": "generic",
            "agent_creation_mode": "suggest",
            "use_llm": False,
            "approved_suggestions": [suggestion],
        },
    )
    assert response.status_code == 200
    body = response.json()
    agents = [n["agent"] for n in body["pipeline"]["nodes"] if n.get("kind") == "agent"]
    assert "custom_ticket_router" in agents


def test_normalize_intent_kafka_forces_windowing() -> None:
    from ratatoskr.pipelines.assist import normalize_intent

    intent = normalize_intent({"goal": "Stream alerts", "source_type": "kafka", "use_windowing": False})
    assert intent["use_windowing"] is True


def test_build_baseline_kafka_injects_dynamic_window() -> None:
    from ratatoskr.pipelines.assist import build_baseline_pipeline, normalize_intent

    intent = normalize_intent(
        {
            "goal": "Double values from Kafka and classify",
            "domain": "numeric_transform",
            "source_type": "kafka",
            "source_topic": "workflow.test.input",
        }
    )
    pipeline = build_baseline_pipeline(intent)
    kinds = [n["kind"] for n in pipeline["nodes"]]
    assert "window" in kinds
    window = next(n for n in pipeline["nodes"] if n["kind"] == "window")
    assert window["config"]["window_type"] == "dynamic_session"
    edges = {(e["source"], e["target"]) for e in pipeline["edges"]}
    assert ("src1", "win1") in edges or any(
        pipeline["nodes"][0]["id"] == src and any(n["kind"] == "window" and n["id"] == tgt for n in pipeline["nodes"])
        for src, tgt in edges
    )


def test_ensure_kafka_dynamic_window_preserves_existing() -> None:
    from ratatoskr.pipelines.assist import ensure_kafka_dynamic_window
    from ratatoskr.pipelines.service import yggdrasil_event_pipeline_template

    pipeline = yggdrasil_event_pipeline_template()
    updated, injected = ensure_kafka_dynamic_window(pipeline)
    assert injected is False
    assert any(n["kind"] == "window" for n in updated["nodes"])
