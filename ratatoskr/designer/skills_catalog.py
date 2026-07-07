"""Discover bundled Flink Agent skills for the designer."""

from __future__ import annotations

import re
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

# Source labels distinguishing version-controlled example skills from
# user-authored skills pasted through the designer.
SOURCE_BUILTIN = "builtin"
SOURCE_USER = "user"

USER_SKILLS_REL = "data/skills"


@dataclass(frozen=True)
class SkillCatalogEntry:
    id: str
    name: str
    description: str
    compatibility: str
    default_allowed_commands: tuple[str, ...]
    path: str
    source: str = SOURCE_BUILTIN


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


def user_skills_root(*, root: Path | None = None) -> Path:
    """Writable directory holding skills pasted through the designer."""
    repo = root or project_root()
    return repo / USER_SKILLS_REL


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return slug or "skill"


def _frontmatter_from_text(text: str) -> dict[str, Any]:
    if not text.startswith("---"):
        return {}
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    payload = yaml.safe_load(parts[1])
    return payload if isinstance(payload, dict) else {}


def _parse_skill_frontmatter(skill_md: Path) -> dict[str, Any]:
    return _frontmatter_from_text(skill_md.read_text(encoding="utf-8"))


def _skill_entry_from_dir(
    skill_dir: Path,
    *,
    source: str,
    rel_base: Path,
) -> SkillCatalogEntry | None:
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.is_file():
        return None
    meta = _parse_skill_frontmatter(skill_md)
    skill_id = str(meta.get("name") or skill_dir.name).strip()
    if not skill_id:
        return None
    default_commands = tuple(DEFAULT_ALLOWED_COMMANDS.get(skill_id, ["echo"]))
    try:
        rel_path = str(skill_dir.relative_to(rel_base))
    except ValueError:
        rel_path = str(skill_dir)
    return SkillCatalogEntry(
        id=skill_id,
        name=skill_id,
        description=str(meta.get("description") or "").strip(),
        compatibility=str(meta.get("compatibility") or "").strip(),
        default_allowed_commands=default_commands,
        path=rel_path,
        source=source,
    )


def list_skill_catalog(*, root: Path | None = None) -> list[SkillCatalogEntry]:
    rel_base = root or project_root()
    scan_roots: list[tuple[Path, str]] = [
        (examples_skills_root(root=root), SOURCE_BUILTIN),
        (user_skills_root(root=root), SOURCE_USER),
    ]

    entries: list[SkillCatalogEntry] = []
    seen: set[str] = set()
    for skills_root, source in scan_roots:
        if not skills_root.is_dir():
            continue
        for skill_dir in sorted(skills_root.iterdir()):
            if not skill_dir.is_dir():
                continue
            entry = _skill_entry_from_dir(skill_dir, source=source, rel_base=rel_base)
            if entry is None or entry.id in seen:
                continue
            seen.add(entry.id)
            entries.append(entry)
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
            "source": entry.source,
        }
        for entry in list_skill_catalog(root=root)
    ]


def create_user_skill(content: str, *, root: Path | None = None) -> dict[str, Any]:
    """Persist a pasted ``SKILL.md`` under the writable user skills directory.

    Returns the created catalog entry (API shape). Raises ``ValueError`` for
    invalid frontmatter or name collisions.
    """
    text = (content or "").strip()
    if not text.startswith("---"):
        raise ValueError("SKILL.md must start with a YAML frontmatter block (---).")
    meta = _frontmatter_from_text(text)
    if not meta:
        raise ValueError("Could not parse SKILL.md frontmatter.")
    name = str(meta.get("name") or "").strip()
    if not name:
        raise ValueError("SKILL.md frontmatter must include a 'name'.")
    if not str(meta.get("description") or "").strip():
        raise ValueError("SKILL.md frontmatter must include a 'description'.")

    existing = {entry.id for entry in list_skill_catalog(root=root)}
    if name in existing:
        raise ValueError(f"A skill named '{name}' already exists.")

    user_root = user_skills_root(root=root)
    slug = _slugify(name)
    dest_dir = user_root / slug
    if dest_dir.exists():
        raise ValueError(f"A skill directory '{slug}' already exists.")

    dest_dir.mkdir(parents=True, exist_ok=True)
    body = text if text.endswith("\n") else text + "\n"
    (dest_dir / "SKILL.md").write_text(body, encoding="utf-8")

    entry = _skill_entry_from_dir(
        dest_dir, source=SOURCE_USER, rel_base=root or project_root()
    )
    if entry is None:  # pragma: no cover - just written above
        raise ValueError("Failed to read back the created skill.")
    return {
        "id": entry.id,
        "name": entry.name,
        "description": entry.description,
        "compatibility": entry.compatibility,
        "default_allowed_commands": list(entry.default_allowed_commands),
        "path": entry.path,
        "source": entry.source,
    }


def delete_user_skill(skill_id: str, *, root: Path | None = None) -> bool:
    """Delete a user-authored skill by id. Built-in skills cannot be deleted."""
    import shutil

    target = (skill_id or "").strip()
    if not target:
        raise ValueError("skill id is required.")

    user_root = user_skills_root(root=root)
    if not user_root.is_dir():
        return False

    for skill_dir in user_root.iterdir():
        if not skill_dir.is_dir():
            continue
        entry = _skill_entry_from_dir(
            skill_dir, source=SOURCE_USER, rel_base=root or project_root()
        )
        if entry is not None and entry.id == target:
            shutil.rmtree(skill_dir)
            return True
    return False


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
