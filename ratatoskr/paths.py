"""Repository and subproject path resolution."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from ratatoskr.constants import DEFAULT_PROFILE, FULL_PROFILE

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
    """Return the workspace repository root (directory containing ``ratatoskr/``)."""
    return Path(__file__).resolve().parent.parent


def workspace_dir(root: Path | None = None) -> Path:
    """Return the ``ratatoskr/`` package directory."""
    return (root or project_root()) / "ratatoskr"


def runtime_dir(root: Path | None = None) -> Path:
    """Generic Flink Agents runtime helpers (``ratatoskr/runtime/``)."""
    return workspace_dir(root) / "runtime"


def honeypot_dir(root: Path | None = None) -> Path:
    """Return the Cowrie honeypot subproject root (may not exist)."""
    return (root or project_root()) / HONEYPOT_DIR_NAME


def examples_dir(root: Path | None = None) -> Path:
    """Generic Flink Agents examples (no honeypot dependencies)."""
    return (root or project_root()) / EXAMPLES_DIR_NAME


def agents_dir(root: Path | None = None) -> Path:
    """Registered Flink Agents example agents."""
    return examples_dir(root) / "agents"


def manifests_dir(root: Path | None = None) -> Path:
    """Primary CLI manifests directory (``ratatoskr/manifests/``)."""
    return workspace_dir(root) / MANIFESTS_DIR_NAME


def honeypot_manifests_dir(root: Path | None = None) -> Path:
    """Honeypot subproject manifests (optional)."""
    return honeypot_dir(root) / MANIFESTS_DIR_NAME


def honeypot_available(root: Path | None = None) -> bool:
    """True when the optional honeypot subproject is present."""
    return honeypot_dir(root).is_dir()


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


def configure_runtime_sys_path(
    root: Path | None = None,
    *,
    profile: str | None = None,
    include_honeypot: bool | None = None,
) -> None:
    """
    Prepend workspace paths needed at runtime.

  By default honeypot modules are loaded only for the ``full`` compose profile.
    """
    repo = root or project_root()
    for path in (repo, workspace_dir(repo)):
        s = str(path)
        if s not in sys.path:
            sys.path.insert(0, s)

    if include_honeypot is None:
        active = (profile or os.environ.get("RATATOSKR_PROFILE", DEFAULT_PROFILE)).strip().lower()
        include_honeypot = active in (FULL_PROFILE, "honeypot", "cowrie", "full")

    if include_honeypot and honeypot_available(repo):
        for path in runtime_src_paths(repo):
            s = str(path)
            if s not in sys.path:
                sys.path.insert(0, s)


def runtime_module_path(filename: str, root: Path | None = None) -> Path:
    """Resolve a bare module filename under honeypot ``src/*`` trees."""
    name = filename if filename.endswith(".py") else f"{filename}.py"
    for directory in runtime_src_paths(root):
        candidate = directory / name
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"runtime module not found: {name}")


def honeypot_module_rel(filename: str, root: Path | None = None) -> str:
    """Repo-relative path to a honeypot runtime module."""
    repo = root or project_root()
    return str(runtime_module_path(filename, repo).relative_to(repo))


def loaded_env_files(root: Path | None = None) -> list[str]:
    """Return ``.env`` files that exist at the workspace root (for doctor display)."""
    repo = root or project_root()
    found: list[str] = []
    for name in (".env", ".env.flink", ".env.cowrie", ".env.llm"):
        path = repo / name
        if path.is_file():
            found.append(name)
    return found
