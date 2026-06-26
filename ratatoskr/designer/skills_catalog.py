"""Discover bundled Flink Agent skills for the designer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from ratatoskr.paths import project_root

if TYPE_CHECKING:
    from ratatoskr.designer.definitions.models import AgentDefinition, AgentDefinitionNode

DEFAULT_ALLOWED_COMMANDS: dict[str, list[str]] = {
    "math-calculator": ["echo", "bc"],
}


@dataclass(frozen=True)
class SkillCatalogEntry:
    id: str
    name: str
    description: str
    compatibility: str
    default_allowed_commands: tuple[str, ...]
    path: str


def examples_skills_root(*, root: Path | None = None) -> Path:
    repo = root or project_root()
    candidates = (
        repo / "examples" / "skills",
        Path("/opt/flink/examples/skills"),
    )
    for path in candidates:
        if path.is_dir():
            return path
    return candidates[0]


def _parse_skill_frontmatter(skill_md: Path) -> dict[str, Any]:
    text = skill_md.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    payload = yaml.safe_load(parts[1])
    return payload if isinstance(payload, dict) else {}


def list_skill_catalog(*, root: Path | None = None) -> list[SkillCatalogEntry]:
    skills_root = examples_skills_root(root=root)
    entries: list[SkillCatalogEntry] = []
    if not skills_root.is_dir():
        return entries

    for skill_dir in sorted(skills_root.iterdir()):
        if not skill_dir.is_dir():
            continue
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.is_file():
            continue
        meta = _parse_skill_frontmatter(skill_md)
        skill_id = str(meta.get("name") or skill_dir.name).strip()
        if not skill_id:
            continue
        default_commands = tuple(DEFAULT_ALLOWED_COMMANDS.get(skill_id, ["echo"]))
        entries.append(
            SkillCatalogEntry(
                id=skill_id,
                name=skill_id,
                description=str(meta.get("description") or "").strip(),
                compatibility=str(meta.get("compatibility") or "").strip(),
                default_allowed_commands=default_commands,
                path=str(skill_dir.relative_to(root or project_root())),
            )
        )
    return entries


def skill_catalog_for_api(*, root: Path | None = None) -> list[dict[str, Any]]:
    return [
        {
            "id": entry.id,
            "name": entry.name,
            "description": entry.description,
            "compatibility": entry.compatibility,
            "default_allowed_commands": list(entry.default_allowed_commands),
            "path": entry.path,
        }
        for entry in list_skill_catalog(root=root)
    ]


def default_allowed_commands_for_skills(
    skill_names: list[str],
    *,
    root: Path | None = None,
) -> list[str]:
    by_id = {entry.id: entry for entry in list_skill_catalog(root=root)}
    commands: list[str] = []
    for name in skill_names:
        entry = by_id.get(name)
        if entry is None:
            continue
        for command in entry.default_allowed_commands:
            if command not in commands:
                commands.append(command)
    return commands


def definition_uses_flink_skills(definition: "AgentDefinition") -> bool:
    llm_nodes = [node for node in definition.nodes if node.kind == "llm_call"]
    if not llm_nodes:
        return False
    config = llm_nodes[0].config or {}
    return str(config.get("mode") or "simple").strip() == "flink_skills"


def react_llm_config(llm_node: "AgentDefinitionNode | None") -> dict[str, Any]:
    config = (llm_node.config or {}) if llm_node else {}
    mode = str(config.get("mode") or "simple").strip()
    skills_raw = config.get("skills") or []
    commands_raw = config.get("allowed_commands") or []
    skills = [str(item).strip() for item in skills_raw if str(item).strip()] if isinstance(skills_raw, list) else []
    allowed_commands = (
        [str(item).strip() for item in commands_raw if str(item).strip()]
        if isinstance(commands_raw, list)
        else []
    )
    if mode == "flink_skills" and skills and not allowed_commands:
        allowed_commands = default_allowed_commands_for_skills(skills)
    return {
        "mode": mode,
        "skills": skills,
        "allowed_commands": allowed_commands,
        "use_platform_llm": bool(config.get("use_platform_llm", True)),
    }


def compiled_agent_uses_skills(agent_dir: Path) -> bool:
    agent_py = agent_dir / "agent.py"
    if not agent_py.is_file():
        return False
    return "Skills.from_local_dir" in agent_py.read_text(encoding="utf-8")


def skills_copy_pairs(root: Path | None = None, *, rel_dir: str = "examples/skills") -> list[tuple[str, str]]:
    from ratatoskr.designer.runtime_env import skills_copy_pairs as _pairs

    return _pairs(root, rel_dir=rel_dir)
