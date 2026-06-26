#!/usr/bin/env python3
"""Tests for react_double_value agent (no flink_agents required)."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def test_double_value_fallback_from_message() -> None:
    from examples.agents.react_double_value_logic import double_value_from_message

    result = double_value_from_message("Please double input value 7")
    assert result["input"] == 7
    assert result["doubled"] == 14
    assert result["mode"] == "fallback"


def test_double_value_fallback_from_hint() -> None:
    from examples.agents.react_double_value_logic import double_value_from_message

    result = double_value_from_message("process this", value_hint=21)
    assert result["input"] == 21
    assert result["doubled"] == 42


def test_registry_and_catalog() -> None:
    from ratatoskr.agents.catalog import load_agent_catalog
    from ratatoskr.agents.registry import load_agent_registry

    registry = load_agent_registry()
    assert "react_double_value" in registry.agents
    assert registry.agents["react_double_value"].type == "react"

    catalog = load_agent_catalog()
    react = next(c for c in catalog.categories if c.id == "react")
    numeric = next(s for s in react.subcategories if s.id == "numeric")
    entry = next(a for a in numeric.agents if a.id == "react_double_value")
    assert entry.manifest == "react_double_value"
    assert entry.display_name == "ReAct Double Value"


def test_prompt_placeholders() -> None:
    from examples.agents.react_double_value_prompt import DOUBLE_VALUE_SYSTEM, DOUBLE_VALUE_USER

    assert "{message}" in DOUBLE_VALUE_USER
    assert "{value}" in DOUBLE_VALUE_USER
    assert "doubled" in DOUBLE_VALUE_SYSTEM
    # Single-brace JSON examples (not {{ }} — per Flink Agents brace handling docs)
    assert "{{" not in DOUBLE_VALUE_SYSTEM


if __name__ == "__main__":
    test_double_value_fallback_from_message()
    print("OK  fallback from message")
    test_double_value_fallback_from_hint()
    print("OK  fallback from hint")
    test_registry_and_catalog()
    print("OK  registry + catalog")
    test_prompt_placeholders()
    print("OK  prompt placeholders")
    print("PASS")
