#!/usr/bin/env python3
"""Designer skills catalog and compile tests."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def test_skill_catalog_lists_math_calculator() -> None:
    from ratatoskr.designer.skills_catalog import list_skill_catalog

    entries = list_skill_catalog(root=_ROOT)
    ids = {entry.id for entry in entries}
    assert "math-calculator" in ids
    entry = next(item for item in entries if item.id == "math-calculator")
    assert "bc" in entry.default_allowed_commands


def test_react_llm_config_defaults_commands() -> None:
    from ratatoskr.designer.definitions.models import AgentDefinitionNode
    from ratatoskr.designer.skills_catalog import react_llm_config

    node = AgentDefinitionNode(
        id="llm1",
        kind="llm_call",
        name="llm",
        config={"mode": "flink_skills", "skills": ["math-calculator"]},
    )
    config = react_llm_config(node)
    assert config["mode"] == "flink_skills"
    assert config["skills"] == ["math-calculator"]
    assert "echo" in config["allowed_commands"]
    assert "bc" in config["allowed_commands"]


def test_compile_react_skills_definition() -> None:
    from ratatoskr.designer.definitions.compile import compile_agent_definition
    from ratatoskr.designer.definitions.models import agent_definition_from_dict

    definition = agent_definition_from_dict(
        {
            "id": "def_react_skills_test",
            "name": "Skills Test",
            "type": "react",
            "version": 1,
            "description": "Skills compile test",
            "status": "draft",
            "input_schema": {
                "type": "object",
                "required": ["message"],
                "properties": {"message": {"type": "string"}},
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "answer": {"type": "string"},
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
                    "config": {
                        "system": "Use math-calculator skill.",
                        "user": "{message}",
                    },
                },
                {
                    "id": "llm1",
                    "kind": "llm_call",
                    "name": "llm",
                    "config": {
                        "mode": "flink_skills",
                        "skills": ["math-calculator"],
                        "allowed_commands": ["echo", "bc"],
                    },
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

    result = compile_agent_definition(definition, root=_ROOT, write_files=False)
    paths = {artifact.path for artifact in result.files}
    assert paths == {"agent.py", "manifest_snippet.yaml", "run_local.py"}

    agent_py = next(item for item in result.files if item.path == "agent.py").content
    assert "class SkillsTestAgent" in agent_py
    assert "@skills" in agent_py
    assert "@chat_model_setup" in agent_py
    assert "Skills.from_local_dir" in agent_py
    assert "math-calculator" in agent_py
    assert "ChatRequestEvent" in agent_py
    assert "agent_logic.py" not in paths


def test_designer_skills_api() -> None:
    from fastapi.testclient import TestClient

    from ratatoskr.api.app import create_app
    from ratatoskr.api.config import ApiSettings

    client = TestClient(create_app(ApiSettings(api_key=None)))
    resp = client.get("/v1/designer/skills")
    assert resp.status_code == 200
    body = resp.json()
    assert any(item["id"] == "math-calculator" for item in body)


def test_create_and_delete_user_skill(tmp_path: Path) -> None:
    from ratatoskr.designer.skills_catalog import (
        create_user_skill,
        delete_user_skill,
        list_skill_catalog,
    )

    content = (
        "---\n"
        "name: greeter\n"
        "description: Say hello using echo.\n"
        "compatibility: Requires bash with echo\n"
        "---\n\n"
        "# Greeter\n\nRun `echo hello`.\n"
    )

    created = create_user_skill(content, root=tmp_path)
    assert created["id"] == "greeter"
    assert created["source"] == "user"
    assert (tmp_path / "data" / "skills" / "greeter" / "SKILL.md").is_file()

    ids = {entry.id for entry in list_skill_catalog(root=tmp_path)}
    assert "greeter" in ids

    # Duplicate name is rejected.
    try:
        create_user_skill(content, root=tmp_path)
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for duplicate skill")

    assert delete_user_skill("greeter", root=tmp_path) is True
    assert "greeter" not in {e.id for e in list_skill_catalog(root=tmp_path)}


def test_create_user_skill_rejects_invalid_frontmatter(tmp_path: Path) -> None:
    from ratatoskr.designer.skills_catalog import create_user_skill

    for bad in ("no frontmatter here", "---\ndescription: missing name\n---\nbody"):
        try:
            create_user_skill(bad, root=tmp_path)
        except ValueError:
            continue
        raise AssertionError("expected ValueError for invalid SKILL.md")


if __name__ == "__main__":
    test_skill_catalog_lists_math_calculator()
    print("OK  skill catalog")
    test_react_llm_config_defaults_commands()
    print("OK  react llm config")
    test_compile_react_skills_definition()
    print("OK  compile react skills")
    test_designer_skills_api()
    print("OK  skills API")
    print("PASS")
