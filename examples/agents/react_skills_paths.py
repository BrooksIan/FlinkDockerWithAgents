"""Resolve bundled example skills (no flink_agents import)."""

from __future__ import annotations

from pathlib import Path


def examples_skills_dir() -> Path:
    """Return the examples/skills directory (repo or Flink container mount)."""
    candidates = (
        Path(__file__).resolve().parents[1] / "skills",
        Path("/opt/flink/examples/skills"),
    )
    for path in candidates:
        if path.is_dir():
            return path
    return candidates[0]
