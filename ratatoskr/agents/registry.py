"""Load agent definitions from ``examples/agents/agent-manifest.yaml``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

from ratatoskr.paths import agents_dir, project_root, published_agent_dir, resolve_repo_rel_path


class AgentRegistryError(Exception):
    """Invalid or missing agent manifest."""


@dataclass(frozen=True)
class AgentSpec:
    name: str
    type: str
    entry: str
    module: str
    class_name: str
    runner: str
    cluster_script: str
    description: str = ""
    flink_yaml: str = ""


@dataclass(frozen=True)
class AgentManifest:
    agents: Dict[str, AgentSpec]


def _parse_entry(entry: str) -> tuple[str, str]:
    if ":" not in entry:
        raise AgentRegistryError(f"Agent entry must be module:Class, got {entry!r}")
    module, class_name = entry.rsplit(":", 1)
    return module.strip(), class_name.strip()


def _parse_agent(name: str, raw: Mapping[str, Any]) -> AgentSpec:
    entry = str(raw.get("entry", "")).strip()
    if not entry:
        raise AgentRegistryError(f"Agent {name!r} missing 'entry'")
    module, class_name = _parse_entry(entry)
    return AgentSpec(
        name=name,
        type=str(raw.get("type", "workflow")).strip().lower(),
        entry=entry,
        module=module,
        class_name=class_name,
        runner=str(raw.get("runner", "")).strip(),
        cluster_script=str(raw.get("cluster_script", "")).strip(),
        description=str(raw.get("description", "")).strip(),
        flink_yaml=str(raw.get("flink_yaml", "")).strip(),
    )


def agent_manifest_path(root: Optional[Path] = None) -> Path:
    return agents_dir(root) / "agent-manifest.yaml"


def _validate_spec(repo: Path, spec: AgentSpec) -> None:
    """Validate that a single agent's referenced artifacts resolve on disk."""
    runner_path = resolve_repo_rel_path(repo, spec.runner) if spec.runner else None
    cluster_path = (
        resolve_repo_rel_path(repo, spec.cluster_script) if spec.cluster_script else None
    )
    flink_yaml_path = (
        resolve_repo_rel_path(repo, spec.flink_yaml) if spec.flink_yaml else None
    )
    if spec.runner:
        published = "published_shims" in spec.module
        agent_dir = published_agent_dir(repo, spec.runner) if published else None
        runner_ok = runner_path is not None and runner_path.is_file()
        if not runner_ok and not (published and agent_dir is not None):
            raise AgentRegistryError(f"Agent {spec.name!r} runner missing: {spec.runner}")
    if spec.cluster_script and cluster_path is None:
        raise AgentRegistryError(
            f"Agent {spec.name!r} cluster script missing: {spec.cluster_script}"
        )
    if spec.flink_yaml and flink_yaml_path is None:
        raise AgentRegistryError(
            f"Agent {spec.name!r} flink_yaml missing: {spec.flink_yaml}"
        )


def load_agent_registry(
    *,
    root: Optional[Path] = None,
    validate: bool = True,
) -> AgentManifest:
    repo = root or project_root()
    path = agent_manifest_path(repo)
    if not path.is_file():
        raise AgentRegistryError(f"Agent manifest not found: {path}")

    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, Mapping):
        raise AgentRegistryError(f"Agent manifest root must be a mapping: {path}")

    raw_agents = data.get("agents")
    if not isinstance(raw_agents, Mapping):
        raise AgentRegistryError(f"{path} must define an 'agents' mapping")

    agents: Dict[str, AgentSpec] = {}
    for name, raw in raw_agents.items():
        if not isinstance(raw, Mapping):
            raise AgentRegistryError(f"Agent {name!r} must be a mapping")
        spec = _parse_agent(str(name), raw)
        if validate:
            _validate_spec(repo, spec)
        agents[str(name)] = spec

    return AgentManifest(agents=agents)


def list_agent_names(*, root: Optional[Path] = None) -> List[str]:
    return sorted(load_agent_registry(root=root, validate=False).agents.keys())


def get_agent_spec(name: str, *, root: Optional[Path] = None) -> AgentSpec:
    repo = root or project_root()
    registry = load_agent_registry(root=repo, validate=False)
    if name not in registry.agents:
        known = ", ".join(sorted(registry.agents))
        raise AgentRegistryError(f"Unknown agent {name!r}. Known: {known}")
    spec = registry.agents[name]
    _validate_spec(repo, spec)
    return spec
