#!/usr/bin/env python3
"""Tests for react_skills_demo agent (no flink_agents required)."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def test_skills_dir_and_skill_file() -> None:
    from examples.agents.react_skills_paths import examples_skills_dir

    skills_dir = examples_skills_dir()
    assert skills_dir.is_dir()
    skill_md = skills_dir / "math-calculator" / "SKILL.md"
    assert skill_md.is_file()
    content = skill_md.read_text(encoding="utf-8")
    assert "name: math-calculator" in content
    assert "bc" in content


def test_registry_and_catalog() -> None:
    from apemosyne.agents.catalog import load_agent_catalog
    from apemosyne.agents.registry import load_agent_registry

    registry = load_agent_registry()
    assert "react_skills_demo" in registry.agents
    spec = registry.agents["react_skills_demo"]
    assert spec.type == "react"
    assert spec.runner.endswith("run_react_skills_demo_local.py")
    assert spec.cluster_script.endswith("run_react_skills_demo_cluster.py")

    catalog = load_agent_catalog()
    react = next(c for c in catalog.categories if c.id == "react")
    numeric = next(s for s in react.subcategories if s.id == "numeric")
    entry = next(a for a in numeric.agents if a.id == "react_skills_demo")
    assert entry.manifest == "react_skills_demo"
    assert entry.display_name == "ReAct Skills Demo"
    assert "skills" in entry.tags


def test_flink_llm_requires_settings() -> None:
    from apemosyne.designer.flink_llm import require_react_llm_settings
    from apemosyne.designer.llm_client import LlmNotConfiguredError
    from apemosyne.designer.llm_settings import reset_designer_store_for_tests

    reset_designer_store_for_tests()
    db_path = Path(__file__).resolve().parents[1] / ".apemosyne" / "designer.db"
    if db_path.is_file():
        db_path.unlink()

    import os

    saved = {
        key: os.environ.pop(key, None)
        for key in (
            "APEMOSYNE_LLM_ENDPOINT_URL",
            "APEMOSYNE_LLM_MODEL_ID",
            "APEMOSYNE_LLM_API_KEY",
            "CLOUDERA_AI_BASE_URL",
            "CLOUDERA_MODEL_ID",
            "CLOUDERA_JWT_TOKEN",
            "OPENAI_BASE_URL",
            "OPENAI_MODEL",
            "OPENAI_API_KEY",
        )
    }
    try:
        try:
            require_react_llm_settings()
        except LlmNotConfiguredError:
            pass
        else:
            raise AssertionError("expected LlmNotConfiguredError when settings unset")
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value
        reset_designer_store_for_tests()


if __name__ == "__main__":
    test_skills_dir_and_skill_file()
    print("OK  skills dir + SKILL.md")
    test_registry_and_catalog()
    print("OK  registry + catalog")
    test_flink_llm_requires_settings()
    print("OK  flink_llm settings guard")
    print("PASS")
