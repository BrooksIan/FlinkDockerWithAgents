#!/usr/bin/env python3
"""Publish designer agent to catalog tests."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path


def _setup_repo(root: Path) -> None:
    agents = root / "examples" / "agents"
    agents.mkdir(parents=True)
    src = Path(__file__).resolve().parents[1] / "examples" / "agents"
    for name in ("agent-manifest.yaml", "agent-catalog.yaml", "__init__.py"):
        shutil.copy2(src / name, agents / name)
    shims = agents / "published_shims"
    shims.mkdir(exist_ok=True)
    (shims / "__init__.py").write_text('"""Published designer agent shims."""\n', encoding="utf-8")


def test_publish_workflow_definition() -> None:
    from apemosyne.designer.definitions.service import (
        AgentDefinitionService,
        reset_agent_definition_service_for_tests,
    )
    from apemosyne.designer.definitions.store import AgentDefinitionStore

    reset_agent_definition_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _setup_repo(root)
        db = root / "designer.db"
        os.environ["APEMOSYNE_DESIGNER_DB"] = str(db)

        store = AgentDefinitionStore(db)
        service = AgentDefinitionService(store)
        created = service.create(
            "Publish Test Agent",
            agent_type="workflow",
            description="Test publish flow",
            catalog_category_id="workflow",
            catalog_subcategory_id="transform",
            catalog_tags=["test"],
            nodes=[
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
                    "id": "tool1",
                    "kind": "tool",
                    "name": "double",
                    "config": {"tool_ref": "double", "expression": "value * 2"},
                },
                {
                    "id": "out1",
                    "kind": "output_event",
                    "name": "OutputEvent",
                    "config": {"event_type": "_output_event"},
                },
            ],
            edges=[
                {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
                {"id": "e2", "source": "act1", "target": "tool1", "kind": "calls"},
                {"id": "e3", "source": "act1", "target": "out1", "kind": "emits"},
            ],
            input_schema={
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "integer"}},
            },
            output_schema={
                "type": "object",
                "properties": {
                    "input": {"type": "integer"},
                    "doubled": {"type": "integer"},
                    "agent": {"type": "string"},
                },
            },
        )
        definition_id = created["id"]
        definition = store.get(definition_id)
        assert definition is not None

        published = service.publish(definition_id, root=root)
        assert published["manifest_name"] == "publish_test_agent"
        assert published["status"] == "published"
        assert published["definition"]["status"] == "published"
        assert published["definition"]["manifest_name"] == "publish_test_agent"
        assert (root / published["shim_path"]).is_file()

        from apemosyne.agents.registry import load_agent_registry

        registry = load_agent_registry(root=root, validate=False)
        assert "publish_test_agent" in registry.agents

        from apemosyne.agents.catalog import load_agent_catalog

        catalog = load_agent_catalog(root=root, validate=True)
        manifests = {
            entry.manifest
            for category in catalog.categories
            for sub in category.subcategories
            for entry in sub.agents
        }
        assert "publish_test_agent" in manifests

        from apemosyne.agents.published_copy import published_agent_artifact_pairs
        from apemosyne.agents.registry import load_agent_registry

        registry = load_agent_registry(root=root, validate=False)
        spec = registry.agents["publish_test_agent"]
        artifact_pairs = published_agent_artifact_pairs(root, spec)
        assert any(".apemosyne/agents/" in remote for _, remote in artifact_pairs)

        republished = service.publish(definition_id, root=root)
        assert republished["manifest_name"] == "publish_test_agent"

        os.environ.pop("APEMOSYNE_DESIGNER_DB", None)
        reset_agent_definition_service_for_tests()


if __name__ == "__main__":
    test_publish_workflow_definition()
    print("OK  publish workflow definition")
    print("PASS")
