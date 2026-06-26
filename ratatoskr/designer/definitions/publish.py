"""Publish compiled designer agents to manifest + catalog."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ratatoskr.agents.catalog import agent_catalog_path
from ratatoskr.agents.published_copy import write_published_cluster_import_module
from ratatoskr.agents.registry import agent_manifest_path, load_agent_registry
from ratatoskr.designer.definitions.compile import (
    CompileError,
    _class_name,
    compile_agent_definition,
    compiled_agents_dir,
)
from ratatoskr.designer.definitions.models import AgentDefinition
from ratatoskr.designer.definitions.validate import validate_agent_definition
from ratatoskr.paths import agents_dir, project_root


class PublishError(ValueError):
    """Agent definition cannot be published."""


@dataclass(frozen=True)
class PublishResult:
    definition_id: str
    manifest_name: str
    catalog_id: str
    manifest_path: str
    catalog_path: str
    shim_path: str


def _slugify_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9_]+", "_", name.lower()).strip("_")
    return slug or "agent"


def _resolve_manifest_slug(definition: AgentDefinition, root: Path) -> str:
    if definition.manifest_name:
        return definition.manifest_name

    registry = load_agent_registry(root=root, validate=False)
    base = _slugify_name(definition.name)
    if base and base not in registry.agents:
        return base

    fallback = f"gen_{definition.id.removeprefix('def_')[:12]}"
    if fallback not in registry.agents:
        return fallback

    suffix = definition.id.removeprefix("def_")[:8]
    return f"{base}_{suffix}" if base else f"gen_{suffix}"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PublishError(f"Missing YAML file: {path}")
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise PublishError(f"Invalid YAML root in {path}")
    return data


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False, default_flow_style=False)


def _shim_path(root: Path, manifest_slug: str) -> Path:
    return agents_dir(root) / "published_shims" / f"{manifest_slug}.py"


def _write_published_shim(
    root: Path,
    manifest_slug: str,
    definition: AgentDefinition,
    class_name: str,
) -> Path:
    shims_dir = agents_dir(root) / "published_shims"
    shims_dir.mkdir(parents=True, exist_ok=True)
    init_py = shims_dir / "__init__.py"
    if not init_py.is_file():
        init_py.write_text('"""Published designer agent shims."""\n', encoding="utf-8")

    path = shims_dir / f"{manifest_slug}.py"
    path.write_text(
        f'''\
"""Published designer agent — {definition.name}. Auto-generated; do not edit."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_DEFINITION_ID = "{definition.id}"
_MODULE_NAME = f"ratatoskr_published_{{_DEFINITION_ID}}"


def _load_class():
    repo = Path(__file__).resolve().parents[3]
    module_path = repo / ".ratatoskr" / "agents" / _DEFINITION_ID / "agent.py"
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load published agent from {{module_path}}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return getattr(module, "{class_name}")


{class_name} = _load_class()
''',
        encoding="utf-8",
    )
    return path


def _merge_manifest(
    root: Path,
    manifest_slug: str,
    definition: AgentDefinition,
    class_name: str,
) -> Path:
    path = agent_manifest_path(root)
    data = _load_yaml(path)
    agents = data.setdefault("agents", {})
    if not isinstance(agents, dict):
        raise PublishError(f"{path} agents mapping is invalid")

    entry: dict[str, Any] = {
        "type": definition.type,
        "description": definition.description or definition.name,
        "entry": f"examples.agents.published_shims.{manifest_slug}:{class_name}",
        "runner": f".ratatoskr/agents/{definition.id}/run_local.py",
    }
    agent_yaml = compiled_agents_dir(root) / definition.id / "agent.yaml"
    if agent_yaml.is_file():
        entry["flink_yaml"] = str(agent_yaml.relative_to(root))

    agents[manifest_slug] = entry
    _write_yaml(path, data)
    return path


def _catalog_entry(definition: AgentDefinition, manifest_slug: str) -> dict[str, Any]:
    tags = list(definition.catalog_tags or [])
    if "custom" not in tags:
        tags.append("custom")
    if "designer" not in tags:
        tags.append("designer")
    return {
        "id": manifest_slug,
        "manifest": manifest_slug,
        "display_name": definition.name,
        "description": definition.description or definition.name,
        "tags": tags,
        "input_schema": definition.input_schema or {},
        "output_schema": definition.output_schema or {},
    }


def _merge_catalog(root: Path, manifest_slug: str, definition: AgentDefinition) -> Path:
    path = agent_catalog_path(root)
    data = _load_yaml(path)
    categories = data.get("categories")
    if not isinstance(categories, list):
        raise PublishError(f"{path} categories list is invalid")

    category_id = definition.catalog_category_id or (
        "react" if definition.type == "react" else "workflow"
    )
    subcategory_id = definition.catalog_subcategory_id or (
        "numeric" if definition.type == "react" else "transform"
    )

    category = next(
        (item for item in categories if isinstance(item, dict) and item.get("id") == category_id),
        None,
    )
    if category is None:
        raise PublishError(
            f"Catalog category {category_id!r} not found — set catalog_category_id on the definition"
        )

    subcategories = category.setdefault("subcategories", [])
    if not isinstance(subcategories, list):
        raise PublishError(f"Category {category_id!r} subcategories is invalid")

    subcategory = next(
        (item for item in subcategories if isinstance(item, dict) and item.get("id") == subcategory_id),
        None,
    )
    if subcategory is None:
        subcategory = {
            "id": subcategory_id,
            "label": subcategory_id.replace("_", " ").title(),
            "description": "",
            "agents": [],
        }
        subcategories.append(subcategory)

    agents = subcategory.setdefault("agents", [])
    if not isinstance(agents, list):
        raise PublishError(f"Subcategory {subcategory_id!r} agents list is invalid")

    entry = _catalog_entry(definition, manifest_slug)
    replaced = False
    for index, item in enumerate(agents):
        if not isinstance(item, dict):
            continue
        if item.get("manifest") == manifest_slug or item.get("id") == manifest_slug:
            agents[index] = entry
            replaced = True
            break
    if not replaced:
        agents.append(entry)

    _write_yaml(path, data)
    return path


def publish_agent_definition(
    definition: AgentDefinition,
    *,
    root: Path | None = None,
    compile_first: bool = True,
) -> PublishResult:
    """Compile (optional), register manifest + catalog entries, write import shim."""
    repo = root or project_root()
    validation = validate_agent_definition(definition)
    if not validation["valid"]:
        raise PublishError(
            "Definition is invalid: " + "; ".join(validation.get("errors") or [])
        )

    if compile_first:
        try:
            compile_agent_definition(definition, root=repo, write_files=True)
        except CompileError as exc:
            raise PublishError(str(exc)) from exc

    output_dir = compiled_agents_dir(repo) / definition.id
    if not (output_dir / "agent.py").is_file():
        raise PublishError(
            f"Compiled agent not found at {output_dir / 'agent.py'} — compile first"
        )
    if not (output_dir / "run_local.py").is_file():
        raise PublishError(
            f"Compiled runner not found at {output_dir / 'run_local.py'} — compile first"
        )

    manifest_slug = _resolve_manifest_slug(definition, repo)
    class_name = _class_name(definition.name)
    shim = _write_published_shim(repo, manifest_slug, definition, class_name)
    write_published_cluster_import_module(repo, definition.id, class_name)
    manifest_path = _merge_manifest(repo, manifest_slug, definition, class_name)
    catalog_path = _merge_catalog(repo, manifest_slug, definition)

    return PublishResult(
        definition_id=definition.id,
        manifest_name=manifest_slug,
        catalog_id=manifest_slug,
        manifest_path=str(manifest_path.relative_to(repo)),
        catalog_path=str(catalog_path.relative_to(repo)),
        shim_path=str(shim.relative_to(repo)),
    )


def publish_result_to_dict(result: PublishResult) -> dict[str, Any]:
    return {
        "definition_id": result.definition_id,
        "manifest_name": result.manifest_name,
        "catalog_id": result.catalog_id,
        "manifest_path": result.manifest_path,
        "catalog_path": result.catalog_path,
        "shim_path": result.shim_path,
        "status": "published",
    }
