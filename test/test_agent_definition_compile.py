#!/usr/bin/env python3
"""Agent definition compile/codegen tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_compile_double_value_definition() -> None:
    from apemosyne.designer.definitions.compile import compile_agent_definition
    from apemosyne.designer.definitions.models import agent_definition_from_dict
    from apemosyne.designer.definitions.seed import double_value_definition_payload
    from apemosyne.designer.definitions.service import (
        AgentDefinitionService,
        reset_agent_definition_service_for_tests,
    )
    from apemosyne.designer.definitions.store import AgentDefinitionStore

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db = root / "designer.db"
        os.environ["APEMOSYNE_DESIGNER_DB"] = str(db)
        store = AgentDefinitionStore(db)
        service = AgentDefinitionService(store)
        service.create_from_payload(double_value_definition_payload())

        definition = store.get("def_double_value_v1")
        assert definition is not None

        result = compile_agent_definition(definition, root=root, write_files=True)
        assert result.agent_slug == "workflow_counter"
        assert result.class_name == "DoubleValueAgent"
        assert (root / ".apemosyne" / "agents" / "def_double_value_v1" / "agent.py").is_file()

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

        os.environ.pop("APEMOSYNE_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_compile_api_endpoint() -> None:
    from fastapi.testclient import TestClient

    from apemosyne.api.app import create_app
    from apemosyne.api.config import ApiSettings
    from apemosyne.designer.definitions.seed import DOUBLE_VALUE_ID
    from apemosyne.designer.definitions.service import reset_agent_definition_service_for_tests

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db = root / "designer.db"
        os.environ["APEMOSYNE_DESIGNER_DB"] = str(db)

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

        os.environ.pop("APEMOSYNE_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_compile_rejects_invalid_definition() -> None:
    from apemosyne.designer.definitions.compile import CompileError, compile_agent_definition
    from apemosyne.designer.definitions.models import agent_definition_from_dict

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
    test_compile_rejects_invalid_definition()
    print("OK  compile rejects invalid")
    test_compile_api_endpoint()
    print("OK  compile API")
    print("PASS")
