#!/usr/bin/env python3
"""Agent definition compile/codegen tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_compile_double_value_definition() -> None:
    from ratatoskr.designer.definitions.compile import compile_agent_definition
    from ratatoskr.designer.definitions.models import agent_definition_from_dict
    from ratatoskr.designer.definitions.seed import double_value_definition_payload
    from ratatoskr.designer.definitions.service import (
        AgentDefinitionService,
        reset_agent_definition_service_for_tests,
    )
    from ratatoskr.designer.definitions.store import AgentDefinitionStore

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db = root / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        store = AgentDefinitionStore(db)
        service = AgentDefinitionService(store)
        service.create_from_payload(double_value_definition_payload())

        definition = store.get("def_double_value_v1")
        assert definition is not None

        result = compile_agent_definition(definition, root=root, write_files=True)
        assert result.agent_slug == "workflow_counter"
        assert result.class_name == "DoubleValueAgent"
        assert (root / ".ratatoskr" / "agents" / "def_double_value_v1" / "agent.py").is_file()

        agent_py = next(a for a in result.files if a.path == "agent.py").content
        assert "class DoubleValueAgent" in agent_py
        assert "def double(value: int)" in agent_py
        assert "def process(event: Event" in agent_py
        assert '"doubled": result' in agent_py
        assert '"agent": "workflow_counter"' in agent_py

        agent_yaml = next(a for a in result.files if a.path == "agent.yaml").content
        assert "name: workflow_counter" in agent_yaml
        assert "listen_to: [_input_event]" in agent_yaml or "listen_to: [input]" in agent_yaml

        compile_via_service = service.compile("def_double_value_v1", root=root)
        assert compile_via_service["status"] == "compiled"
        assert compile_via_service["definition"]["status"] == "compiled"
        assert len(compile_via_service["files"]) == 5

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_compile_api_endpoint() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.definitions.seed import DOUBLE_VALUE_ID
    from ratatoskr.designer.definitions.service import reset_agent_definition_service_for_tests

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db = root / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)

        client = TestClient(create_app(ApiSettings(api_key=None)))
        client.get("/v1/agent-definitions")

        resp = client.post(f"/v1/agent-definitions/{DOUBLE_VALUE_ID}/compile")
        assert resp.status_code == 200
        body = resp.json()
        assert body["class_name"] == "DoubleValueAgent"
        assert body["status"] == "compiled"
        paths = {item["path"] for item in body["files"]}
        assert "agent.py" in paths
        assert "agent.yaml" in paths

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_compile_react_definition() -> None:
    from ratatoskr.designer.definitions.compile import compile_agent_definition
    from ratatoskr.designer.definitions.models import agent_definition_from_dict

    definition = agent_definition_from_dict(
        {
            "id": "def_react_test",
            "name": "Test ReAct",
            "type": "react",
            "version": 1,
            "description": "ReAct test agent",
            "status": "draft",
            "input_schema": {
                "type": "object",
                "required": ["message"],
                "properties": {"message": {"type": "string"}},
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "message": {"type": "string"},
                    "result": {"type": "string"},
                    "agent": {"type": "string"},
                },
            },
            "nodes": [
                {
                    "id": "in1",
                    "kind": "input_event",
                    "name": "InputEvent",
                    "config": {"event_type": "_input_event"},
                },
                {
                    "id": "act1",
                    "kind": "action",
                    "name": "process",
                    "config": {"listens_to": ["_input_event"]},
                },
                {
                    "id": "prompt1",
                    "kind": "prompt",
                    "name": "prompt",
                    "config": {"system": "Double the value.", "user": "{message}"},
                },
                {
                    "id": "llm1",
                    "kind": "llm_call",
                    "name": "llm",
                    "config": {"use_platform_llm": True},
                },
                {
                    "id": "out1",
                    "kind": "output_event",
                    "name": "OutputEvent",
                    "config": {"event_type": "_output_event"},
                },
            ],
            "edges": [
                {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
                {"id": "e2", "source": "act1", "target": "prompt1", "kind": "calls"},
                {"id": "e3", "source": "act1", "target": "llm1", "kind": "calls"},
                {"id": "e4", "source": "act1", "target": "out1", "kind": "emits"},
            ],
        }
    )

    result = compile_agent_definition(definition, write_files=False)
    assert result.class_name == "TestReactAgent"
    paths = {artifact.path for artifact in result.files}
    assert paths == {"agent.py", "agent_logic.py", "manifest_snippet.yaml", "run_local.py"}

    agent_py = next(a for a in result.files if a.path == "agent.py").content
    assert "class TestReactAgent" in agent_py
    assert "def process(event: Event" in agent_py

    logic_py = next(a for a in result.files if a.path == "agent_logic.py").content
    assert "SYSTEM_PROMPT = 'Double the value." in logic_py
    assert "Respond with a single JSON object only" in logic_py
    assert "def run_react(message: str" in logic_py
    assert "chat_completion_json" in logic_py

    manifest = next(a for a in result.files if a.path == "manifest_snippet.yaml").content
    assert "type: react" in manifest


def test_compile_rejects_invalid_definition() -> None:
    from ratatoskr.designer.definitions.compile import CompileError, compile_agent_definition
    from ratatoskr.designer.definitions.models import agent_definition_from_dict

    definition = agent_definition_from_dict(
        {
            "id": "def_bad",
            "name": "Bad",
            "type": "workflow",
            "version": 1,
            "description": "",
            "status": "draft",
            "nodes": [
                {"id": "in1", "kind": "input_event", "name": "InputEvent"},
                {"id": "out1", "kind": "output_event", "name": "OutputEvent"},
            ],
            "edges": [],
        }
    )
    try:
        compile_agent_definition(definition, write_files=False)
        raise AssertionError("expected CompileError")
    except CompileError as exc:
        assert "invalid" in str(exc).lower()


if __name__ == "__main__":
    test_compile_double_value_definition()
    print("OK  compile double value")
    test_compile_react_definition()
    print("OK  compile react")
    test_compile_rejects_invalid_definition()
    print("OK  compile rejects invalid")
    test_compile_api_endpoint()
    print("OK  compile API")
    print("PASS")
