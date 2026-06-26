#!/usr/bin/env python3
"""MCP catalog, instances, and API tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def test_mcp_catalog_loads() -> None:
    from ratatoskr.mcp.catalog import load_mcp_catalog, mcp_catalog_response

    catalog = load_mcp_catalog()
    assert len(catalog.categories) >= 1
    servers = [s for c in catalog.categories for s in c.servers]
    assert any(s.id == "abuseipdb" for s in servers)

    payload = mcp_catalog_response()
    assert payload["categories"][0]["servers"][0]["default_instance_id"] == "inst_abuseipdb"


def test_mcp_instances_store_and_api() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.mcp.instances import reset_mcp_store_for_tests

    reset_mcp_store_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        listed = client.get("/v1/designer/mcp-instances").json()
        assert "instances" in listed
        assert any(item["catalog_id"] == "abuseipdb" for item in listed["instances"])

        saved = client.put(
            "/v1/designer/mcp-instances/abuseipdb",
            json={
                "enabled": True,
                "secrets": {"ABUSEIPDB_API_KEY": "test-key-1234"},
            },
        )
        assert saved.status_code == 200
        body = saved.json()
        assert body["enabled"] is True
        assert body["configured"] is True
        assert body["secrets"]["ABUSEIPDB_API_KEY"]["set"] is True

        resp = client.post("/v1/designer/mcp-instances/abuseipdb/test", json={})
        assert resp.status_code == 200
        test_body = resp.json()
        assert test_body["ok"] is True
        assert test_body["tool"] == "check_ip"

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_mcp_store_for_tests()


def test_agent_definition_mcp_validation() -> None:
    from ratatoskr.designer.definitions.models import (
        AgentDefinition,
        AgentDefinitionEdge,
        AgentDefinitionNode,
    )
    from ratatoskr.designer.definitions.validate import validate_agent_definition

    definition = AgentDefinition(
        id="def_test_mcp",
        name="MCP Test",
        type="workflow",
        version=1,
        description="",
        status="draft",
        nodes=[
            AgentDefinitionNode("in1", "input_event", "InputEvent", {"event_type": "_input_event"}),
            AgentDefinitionNode("act1", "action", "process", {"listens_to": ["_input_event"]}),
            AgentDefinitionNode(
                "mcp1",
                "mcp_tool",
                "check_ip",
                {
                    "server_ref": "inst_abuseipdb",
                    "tool_name": "check_ip",
                    "arg_name": "ip",
                },
            ),
            AgentDefinitionNode("out1", "output_event", "OutputEvent", {"event_type": "_output_event"}),
        ],
        edges=[
            AgentDefinitionEdge("e1", "in1", "act1", "listens_to"),
            AgentDefinitionEdge("e2", "act1", "mcp1", "calls"),
            AgentDefinitionEdge("e3", "act1", "out1", "emits"),
        ],
        mcp_servers=["inst_other"],
        input_schema={"type": "object", "properties": {"src_ip": {"type": "string"}}},
        output_schema={"type": "object", "properties": {"result": {}}},
    )
    result = validate_agent_definition(definition)
    assert result["valid"] is False
    assert any("not attached" in err for err in result["errors"])

    definition.mcp_servers = ["inst_abuseipdb"]
    result = validate_agent_definition(definition)
    assert result["valid"] is True


def test_mcp_tool_compile() -> None:
    from ratatoskr.designer.definitions.compile import compile_agent_definition
    from ratatoskr.designer.definitions.models import (
        AgentDefinition,
        AgentDefinitionEdge,
        AgentDefinitionNode,
    )

    definition = AgentDefinition(
        id="def_compile_mcp",
        name="IP Check",
        type="workflow",
        version=1,
        description="Check IP via MCP",
        status="draft",
        nodes=[
            AgentDefinitionNode("in1", "input_event", "InputEvent", {"event_type": "_input_event"}),
            AgentDefinitionNode("act1", "action", "process", {"listens_to": ["_input_event"]}),
            AgentDefinitionNode(
                "mcp1",
                "mcp_tool",
                "check_ip",
                {
                    "server_ref": "inst_abuseipdb",
                    "tool_name": "check_ip",
                    "arg_name": "ip",
                },
            ),
            AgentDefinitionNode("out1", "output_event", "OutputEvent", {"event_type": "_output_event"}),
        ],
        edges=[
            AgentDefinitionEdge("e1", "in1", "act1", "listens_to"),
            AgentDefinitionEdge("e2", "act1", "mcp1", "calls"),
            AgentDefinitionEdge("e3", "act1", "out1", "emits"),
        ],
        mcp_servers=["inst_abuseipdb"],
        input_schema={
            "type": "object",
            "required": ["src_ip"],
            "properties": {"src_ip": {"type": "string"}},
        },
        output_schema={"type": "object", "properties": {"result": {}}},
    )
    result = compile_agent_definition(definition, write_files=False)
    agent_py = next(f for f in result.files if f.path == "agent.py").content
    assert "invoke_tool" in agent_py
    assert "inst_abuseipdb" in agent_py
    assert "_str_from_input" in agent_py


def test_add_custom_mcp_catalog_server() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings
    from ratatoskr.mcp.instances import reset_mcp_store_for_tests

    reset_mcp_store_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "designer.db"
        os.environ["RATATOSKR_DESIGNER_DB"] = str(db)
        client = TestClient(create_app(ApiSettings(api_key=None)))

        created = client.post(
            "/v1/mcp/catalog/servers",
            json={
                "display_name": "My Lookup API",
                "description": "Custom enrichment server",
                "transport": "http",
                "tool_name": "lookup",
                "secret_name": "MY_LOOKUP_API_KEY",
                "secret_label": "Lookup API key",
            },
        )
        assert created.status_code == 200
        body = created.json()
        assert body["source"] == "custom"
        assert body["id"] == "my_lookup_api"
        assert body["tools"][0]["name"] == "lookup"

        catalog = client.get("/v1/mcp/catalog").json()
        custom = next(c for c in catalog["categories"] if c["id"] == "custom")
        assert any(s["id"] == "my_lookup_api" for s in custom["servers"])

        listed = client.get("/v1/designer/mcp-instances").json()
        assert any(item["catalog_id"] == "my_lookup_api" for item in listed["instances"])

        os.environ.pop("RATATOSKR_DESIGNER_DB", None)
        reset_mcp_store_for_tests()


if __name__ == "__main__":
    test_mcp_catalog_loads()
    print("OK  MCP catalog")
    test_mcp_instances_store_and_api()
    print("OK  MCP instances API")
    test_agent_definition_mcp_validation()
    print("OK  MCP validation")
    test_mcp_tool_compile()
    print("OK  MCP compile")
    test_add_custom_mcp_catalog_server()
    print("OK  add custom MCP server")
    print("PASS")
