"""Repository and subproject path resolution."""

from __future__ import annotations

import os
import sys
from pathlib import Path

MANIFESTS_DIR_NAME = "manifests"
EXAMPLES_DIR_NAME = "examples"
HONEYPOT_DIR_NAME = "honeypot"

_RUNTIME_SUBDIRS = (
    "core",
    "pipeline",
    "traps",
    "react",
    "integrations",
    "cluster",
    "services",
)


def project_root() -> Path:
    """Return the workspace repository root (directory containing ``apemosyne/``)."""
    return Path(__file__).resolve().parent.parent


def workspace_dir(root: Path | None = None) -> Path:
    """Return the ``apemosyne/`` package directory."""
    return (root or project_root()) / "apemosyne"


def honeypot_dir(root: Path | None = None) -> Path:
    """Return the Cowrie honeypot subproject root (may not exist)."""
    return (root or project_root()) / HONEYPOT_DIR_NAME


def examples_dir(root: Path | None = None) -> Path:
    """Generic Flink Agents examples (no honeypot dependencies)."""
    return (root or project_root()) / EXAMPLES_DIR_NAME


def manifests_dir(root: Path | None = None) -> Path:
    """Primary CLI manifests directory (``apemosyne/manifests/``)."""
    return workspace_dir(root) / MANIFESTS_DIR_NAME


def honeypot_manifests_dir(root: Path | None = None) -> Path:
    """Honeypot subproject manifests (optional)."""
    return honeypot_dir(root) / MANIFESTS_DIR_NAME


def runtime_src_paths(root: Path | None = None) -> list[Path]:
    """Directories added to ``sys.path`` for honeypot runtime modules."""
    repo = root or project_root()
    hp = honeypot_dir(repo)
    paths: list[Path] = []
    src = hp / "src"
    if src.is_dir():
        for name in _RUNTIME_SUBDIRS:
            path = src / name
            if path.is_dir():
                paths.append(path)
    tools = hp / "tools"
    if tools.is_dir():
        paths.append(tools)
    if hp.is_dir():
        paths.append(hp)
    return paths


def configure_runtime_sys_path(root: Path | None = None) -> None:
    """Prepend honeypot ``src/*`` trees to ``sys.path`` when present."""
    for path in runtime_src_paths(root):
        s = str(path)
        if s not in sys.path:
            sys.path.insert(0, s)


def runtime_module_path(filename: str, root: Path | None = None) -> Path:
    """Resolve a bare module filename (e.g. ``cowrie_policy.py``) under ``honeypot/``."""
    name = filename if filename.endswith(".py") else f"{filename}.py"
    for directory in runtime_src_paths(root):
        candidate = directory / name
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"runtime module not found: {name}")


def loaded_env_files(root: Path | None = None) -> list[str]:
    """Return ``.env`` files that exist at the workspace root (for doctor display)."""
    repo = root or project_root()
    found: list[str] = []
    for name in (".env", ".env.flink", ".env.cowrie", ".env.llm"):
        path = repo / name
        if path.is_file():
            found.append(name)
    return found
