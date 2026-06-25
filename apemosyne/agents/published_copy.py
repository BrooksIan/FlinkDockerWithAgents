"""Copy published designer agent artifacts into Flink containers."""

from __future__ import annotations

from pathlib import Path

from apemosyne.agents.registry import AgentSpec


def is_published_agent_spec(spec: AgentSpec) -> bool:
    return spec.runner.startswith(".apemosyne/") or "published_shims" in spec.module


def published_agent_artifact_pairs(root: Path, spec: AgentSpec) -> list[tuple[str, str]]:
    """Host → JobManager paths for compiled designer agents under ``.apemosyne/agents/``."""
    if not spec.runner.startswith(".apemosyne/"):
        return []

    pairs: list[tuple[str, str]] = []
    runner = root / spec.runner
    if not runner.is_file():
        return pairs

    agent_dir = runner.parent
    if not agent_dir.is_dir():
        return pairs

    for path in sorted(agent_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix not in {".py", ".yaml"}:
            continue
        rel = path.relative_to(root).as_posix()
        pairs.append((str(path), f"/opt/flink/{rel}"))

    shims_dir = root / "examples" / "agents" / "published_shims"
    slug = spec.module.rsplit(".", 1)[-1] if "published_shims" in spec.module else spec.name
    shim = shims_dir / f"{slug}.py"
    if shim.is_file():
        rel = shim.relative_to(root).as_posix()
        pairs.append((str(shim), f"/opt/flink/{rel}"))

    return pairs
