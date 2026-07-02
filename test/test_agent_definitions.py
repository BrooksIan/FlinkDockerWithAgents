#!/usr/bin/env python3
"""Agent definition store, validation, and API tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_double_value_seed_and_validate() -> None:
    from ratatoskr.designer.definitions.models import agent_definition_from_dict
    from ratatoskr.designer.definitions.seed import (
        DOUBLE_VALUE_ID,
        double_value_definition_payload,
    )
    from ratatoskr.designer.definitions.service import (
        AgentDefinitionService,
        reset_agent_definition_service_for_tests,
    )
    from ratatoskr.designer.definitions.store import AgentDefinitionStore
    from ratatoskr.designer.definitions.validate import validate_agent_definition

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        store = AgentDefinitionStore(db)
        service = AgentDefinitionService(store)

        seeded = service.seed_double_value()
        assert seeded["id"] == DOUBLE_VALUE_ID
        assert seeded["name"] == "Double Value"
        assert len(seeded["nodes"]) == 4
        assert len(seeded["edges"]) == 3

        again = service.seed_double_value()
        assert again["id"] == DOUBLE_VALUE_ID

        validation = service.validate(DOUBLE_VALUE_ID)
        assert validation["valid"] is True
        assert not validation["errors"]

        payload = double_value_definition_payload()
        direct = agent_definition_from_dict(payload)
        assert validate_agent_definition(direct)["valid"] is True

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_agent_definitions_api_crud() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.designer.definitions.seed import DOUBLE_VALUE_ID
    from ratatoskr.designer.definitions.service import reset_agent_definition_service_for_tests

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        listed = client.get("/v1/agent-definitions")
        assert listed.status_code == 200
        body = listed.json()
        assert len(body) >= 1
        assert any(item["id"] == DOUBLE_VALUE_ID for item in body)

        detail = client.get(f"/v1/agent-definitions/{DOUBLE_VALUE_ID}")
        assert detail.status_code == 200
        assert detail.json()["manifest_name"] == "workflow_counter"

        validate = client.post(f"/v1/agent-definitions/{DOUBLE_VALUE_ID}/validate")
        assert validate.status_code == 200
        assert validate.json()["valid"] is True

        created = client.post(
            "/v1/agent-definitions",
            json={"name": "Empty workflow", "type": "workflow"},
        )
        assert created.status_code == 200
        new_id = created.json()["id"]
        assert created.json()["status"] == "draft"

        invalid = client.post(f"/v1/agent-definitions/{new_id}/validate")
        assert invalid.status_code == 200
        assert invalid.json()["valid"] is False

        updated = client.put(
            f"/v1/agent-definitions/{new_id}",
            json={"description": "Updated description"},
        )
        assert updated.status_code == 200
        assert updated.json()["description"] == "Updated description"

        deleted = client.delete(f"/v1/agent-definitions/{new_id}")
        assert deleted.status_code == 200
        assert client.get(f"/v1/agent-definitions/{new_id}").status_code == 404

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


def test_validation_detects_missing_action() -> None:
    from ratatoskr.designer.definitions.models import agent_definition_from_dict
    from ratatoskr.designer.definitions.validate import validate_agent_definition

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
    result = validate_agent_definition(definition)
    assert result["valid"] is False
    assert any("action" in err for err in result["errors"])
    assert "issues" in result
    assert any(issue["level"] == "error" for issue in result["issues"])


def test_validation_issues_include_node_ids() -> None:
    from ratatoskr.designer.definitions.models import agent_definition_from_dict
    from ratatoskr.designer.definitions.validate import validate_agent_definition

    definition = agent_definition_from_dict(
        {
            "id": "def_mcp",
            "name": "MCP bad",
            "type": "workflow",
            "version": 1,
            "description": "",
            "status": "draft",
            "nodes": [
                {"id": "in1", "kind": "input_event", "name": "InputEvent"},
                {
                    "id": "act1",
                    "kind": "action",
                    "name": "process",
                    "config": {"listens_to": ["_input_event"]},
                },
                {
                    "id": "mcp1",
                    "kind": "mcp_tool",
                    "name": "check_ip",
                    "config": {"server_ref": "", "tool_name": ""},
                },
                {"id": "out1", "kind": "output_event", "name": "OutputEvent"},
            ],
            "edges": [
                {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
                {"id": "e2", "source": "act1", "target": "mcp1", "kind": "calls"},
                {"id": "e3", "source": "act1", "target": "out1", "kind": "emits"},
            ],
        }
    )
    result = validate_agent_definition(definition)
    assert result["valid"] is False
    mcp_issues = [issue for issue in result["issues"] if issue.get("node_id") == "mcp1"]
    assert mcp_issues
    assert any("MCP tool" in issue["message"] for issue in mcp_issues)


def test_normalize_records_for_runtime() -> None:
    from ratatoskr.designer.definitions.service import AgentDefinitionService

    normalize = AgentDefinitionService._normalize_records_for_runtime

    # Records already carrying value/v are left untouched.
    assert normalize([{"key": "1", "value": 3}]) == [{"key": "1", "value": 3}]
    assert normalize([{"k": "1", "v": 9}]) == [{"k": "1", "v": 9}]

    # Domain-only fields are wrapped under value, preserving the key.
    assert normalize([{"key": "1", "message": "hi"}]) == [
        {"key": "1", "value": {"message": "hi"}}
    ]

    # Non-dict records become a value payload.
    assert normalize([5]) == [{"value": 5}]


def test_default_sample_records_shape() -> None:
    from ratatoskr.designer.definitions.models import agent_definition_from_dict
    from ratatoskr.designer.definitions.service import AgentDefinitionService

    workflow = agent_definition_from_dict(
        {
            "id": "def_wf",
            "name": "wf",
            "type": "workflow",
            "version": 1,
            "description": "",
            "status": "draft",
            "input_schema": {"required": ["value"], "properties": {"value": {}}},
            "nodes": [],
            "edges": [],
        }
    )
    wf_records = AgentDefinitionService._default_sample_records(workflow)
    assert all("value" in record for record in wf_records)

    react = agent_definition_from_dict(
        {
            "id": "def_rx",
            "name": "rx",
            "type": "react",
            "version": 1,
            "description": "",
            "status": "draft",
            "input_schema": {"required": ["message"], "properties": {"message": {}}},
            "nodes": [],
            "edges": [],
        }
    )
    rx_records = AgentDefinitionService._default_sample_records(react)
    assert all("message" in record for record in rx_records)


if __name__ == "__main__":
    test_double_value_seed_and_validate()
    print("OK  seed + validate")
    test_agent_definitions_api_crud()
    print("OK  API CRUD")
    test_validation_detects_missing_action()
    print("OK  validation errors")
    print("PASS")
