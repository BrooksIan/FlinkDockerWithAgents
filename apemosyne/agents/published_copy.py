"""Copy published designer agent artifacts into Flink containers."""

from __future__ import annotations

from pathlib import Path

from apemosyne.agents.registry import AgentSpec


def is_published_agent_spec(spec: AgentSpec) -> bool:
    return spec.runner.startswith(".apemosyne/") or "published_shims" in spec.module


def published_cluster_module_name(definition_id: str) -> str:
    return f"apemosyne_published_{definition_id}"


def published_cluster_module_remote(definition_id: str) -> str:
    name = published_cluster_module_name(definition_id)
    return f"/opt/flink/pythonpath/agent-site-packages/{name}.py"


def write_published_cluster_import_module(
    root: Path,
    definition_id: str,
    class_name: str,
) -> Path:
    """Write a PYTHONPATH module Flink taskmanagers import by name."""
    agent_dir = root / ".apemosyne" / "agents" / definition_id
    agent_dir.mkdir(parents=True, exist_ok=True)
    path = agent_dir / "cluster_import.py"
    path.write_text(
        _cluster_import_module_source(definition_id, class_name),
        encoding="utf-8",
    )
    return path


def _cluster_import_module_source(definition_id: str, class_name: str) -> str:
    return f'''\
"""Flink cluster worker import for published agent {definition_id}. Auto-generated."""

from __future__ import annotations

from pathlib import Path

_DEFINITION_ID = {definition_id!r}
_CLASS_NAME = {class_name!r}


def _agent_module_path() -> Path:
    flink_root = Path("/opt/flink")
    if flink_root.is_dir():
        path = flink_root / ".apemosyne" / "agents" / _DEFINITION_ID / "agent.py"
        if path.is_file():
            return path
    repo = Path(__file__).resolve().parents[3]
    return repo / ".apemosyne" / "agents" / _DEFINITION_ID / "agent.py"


_agent_path = _agent_module_path()
_namespace = {{"__name__": __name__, "__file__": str(_agent_path)}}
exec(compile(_agent_path.read_text(encoding="utf-8"), str(_agent_path), "exec"), _namespace)
globals()[_CLASS_NAME] = _namespace[_CLASS_NAME]
'''


def published_cluster_import_line(spec: AgentSpec) -> str:
    """Import statement for cluster runners (matches Flink worker module names)."""
    if spec.runner.startswith(".apemosyne/"):
        definition_id = Path(spec.runner).parent.name
        module = published_cluster_module_name(definition_id)
        return f"from {module} import {spec.class_name}"
    return f"from {spec.module} import {spec.class_name}"


def published_agent_artifact_pairs(root: Path, spec: AgentSpec) -> list[tuple[str, str]]:
    """Host → cluster paths for compiled designer agents under ``.apemosyne/agents/``."""
    if not spec.runner.startswith(".apemosyne/"):
        return []

    pairs: list[tuple[str, str]] = []
    runner = root / spec.runner
    if not runner.is_file():
        return pairs

    agent_dir = runner.parent
    if not agent_dir.is_dir():
        return pairs

    definition_id = agent_dir.name
    cluster_import = write_published_cluster_import_module(
        root,
        definition_id,
        spec.class_name,
    )
    pairs.append(
        (
            str(cluster_import),
            published_cluster_module_remote(definition_id),
        )
    )

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
