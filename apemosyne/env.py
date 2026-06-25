"""Load workspace ``.env`` files into the process environment."""

from __future__ import annotations

from pathlib import Path

from apemosyne.constants import ENV_FILE_NAMES
from apemosyne.paths import project_root


def load_workspace_env(root: Path | None = None) -> list[str]:
    """Load repo-root env files (first wins for each key unless already set)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return []

    repo = root or project_root()
    loaded: list[str] = []
    for name in ENV_FILE_NAMES:
        path = repo / name
        if path.is_file():
            load_dotenv(path, override=False)
            loaded.append(name)
    return loaded
