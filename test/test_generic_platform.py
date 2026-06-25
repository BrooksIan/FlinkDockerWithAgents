#!/usr/bin/env python3
"""Generic platform tests — no honeypot, no Docker required."""

from __future__ import annotations

import sys
from pathlib import Path


def test_paths_and_manifests() -> None:
    root = Path(__file__).resolve().parents[1]
    from apemosyne.paths import agents_dir, configure_runtime_sys_path, honeypot_available, runtime_dir

    assert runtime_dir(root).is_dir()
    assert agents_dir(root).is_dir()
    configure_runtime_sys_path(root, include_honeypot=False)
    # Honeypot may exist in this repo but must not be required.
    _ = honeypot_available(root)


def test_agent_registry() -> None:
    from apemosyne.agents.registry import load_agent_registry

    registry = load_agent_registry()
    assert "workflow_counter" in registry.agents
    assert "react_echo" in registry.agents
    wf = registry.agents["workflow_counter"]
    assert wf.type == "workflow"
    assert wf.runner.endswith("run_workflow_local.py")


def test_generic_validate_paths() -> None:
    from apemosyne.commands.test_cmd import _generic_validate_paths

    root = Path(__file__).resolve().parents[1]
    missing = [p for p in _generic_validate_paths(root) if not (root / p).is_file()]
    assert not missing, f"missing generic paths: {missing}"


def test_api_factory() -> None:
    from apemosyne.api.app import create_app

    app = create_app()
    assert app.title == "Apemosyne Control API"


def main() -> int:
    print("=" * 60)
    print("Generic Flink Agents platform tests")
    print("=" * 60)
    test_paths_and_manifests()
    print("OK  paths")
    test_agent_registry()
    print("OK  agent registry")
    test_generic_validate_paths()
    print("OK  generic validate paths")
    test_api_factory()
    print("OK  api factory")
    print("=" * 60)
    print("PASS")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
