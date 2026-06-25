#!/usr/bin/env python3
"""Agent catalog tests."""

from __future__ import annotations


def test_load_agent_catalog() -> None:
    from apemosyne.agents.catalog import load_agent_catalog

    catalog = load_agent_catalog()
    assert len(catalog.categories) >= 2
    workflow = next(c for c in catalog.categories if c.id == "workflow")
    transform = next(s for s in workflow.subcategories if s.id == "transform")
    double = next(a for a in transform.agents if a.id == "double_value")
    assert double.manifest == "workflow_counter"
    assert double.display_name == "Double Value"
    assert "value" in double.input_schema.get("properties", {})


def test_catalog_api_route() -> None:
    from fastapi.testclient import TestClient

    from apemosyne.api.app import create_app
    from apemosyne.api.config import ApiSettings

    client = TestClient(create_app(ApiSettings(api_key=None)))
    resp = client.get("/v1/agents/catalog")
    assert resp.status_code == 200
    body = resp.json()
    assert "categories" in body
    manifests = {
        agent["manifest"]
        for cat in body["categories"]
        for sub in cat["subcategories"]
        for agent in sub["agents"]
    }
    assert "workflow_counter" in manifests
    double = next(
        a
        for cat in body["categories"]
        for sub in cat["subcategories"]
        for a in sub["agents"]
        if a["id"] == "double_value"
    )
    assert double["display_name"] == "Double Value"
    assert double["subcategory_label"] == "Transform"


def test_list_agents_includes_catalog_metadata() -> None:
    from apemosyne.api.services import list_agents

    agents = {a["name"]: a for a in list_agents()}
    wc = agents["workflow_counter"]
    assert wc.get("catalog_id") == "double_value"
    assert wc.get("display_name") == "Double Value"


if __name__ == "__main__":
    test_load_agent_catalog()
    print("OK  load catalog")
    test_catalog_api_route()
    print("OK  catalog API")
    test_list_agents_includes_catalog_metadata()
    print("OK  list agents metadata")
    print("PASS")
