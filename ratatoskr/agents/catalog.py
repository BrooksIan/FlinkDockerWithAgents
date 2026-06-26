"""Load agent catalog for dashboard grouping and Studio palette."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml

from ratatoskr.agents.registry import AgentRegistryError, load_agent_registry
from ratatoskr.paths import agents_dir, project_root


class AgentCatalogError(Exception):
    """Invalid or missing agent catalog."""


@dataclass(frozen=True)
class CatalogAgentEntry:
    id: str
    manifest: str
    display_name: str
    description: str = ""
    tags: tuple[str, ...] = ()
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogSubcategory:
    id: str
    label: str
    description: str = ""
    agents: tuple[CatalogAgentEntry, ...] = ()


@dataclass(frozen=True)
class CatalogCategory:
    id: str
    label: str
    description: str = ""
    subcategories: tuple[CatalogSubcategory, ...] = ()
    llm_required: bool = False


@dataclass(frozen=True)
class AgentCatalog:
    categories: tuple[CatalogCategory, ...]


def agent_catalog_path(root: Optional[Path] = None) -> Path:
    return agents_dir(root) / "agent-catalog.yaml"


def _parse_agent_entry(raw: Mapping[str, Any]) -> CatalogAgentEntry:
    catalog_id = str(raw.get("id", "")).strip()
    manifest = str(raw.get("manifest", "")).strip()
    if not catalog_id:
        raise AgentCatalogError("Catalog agent missing 'id'")
    if not manifest:
        raise AgentCatalogError(f"Catalog agent {catalog_id!r} missing 'manifest'")
    tags_raw = raw.get("tags") or []
    tags = tuple(str(t).strip() for t in tags_raw if str(t).strip()) if isinstance(tags_raw, list) else ()
    input_schema = raw.get("input_schema") if isinstance(raw.get("input_schema"), dict) else {}
    output_schema = raw.get("output_schema") if isinstance(raw.get("output_schema"), dict) else {}
    return CatalogAgentEntry(
        id=catalog_id,
        manifest=manifest,
        display_name=str(raw.get("display_name") or catalog_id).strip(),
        description=str(raw.get("description", "")).strip(),
        tags=tags,
        input_schema=dict(input_schema),
        output_schema=dict(output_schema),
    )


def _parse_subcategory(raw: Mapping[str, Any]) -> CatalogSubcategory:
    sub_id = str(raw.get("id", "")).strip()
    if not sub_id:
        raise AgentCatalogError("Catalog subcategory missing 'id'")
    agents_raw = raw.get("agents") or []
    if not isinstance(agents_raw, list):
        raise AgentCatalogError(f"Subcategory {sub_id!r} 'agents' must be a list")
    agents = tuple(_parse_agent_entry(item) for item in agents_raw if isinstance(item, Mapping))
    return CatalogSubcategory(
        id=sub_id,
        label=str(raw.get("label") or sub_id).strip(),
        description=str(raw.get("description", "")).strip(),
        agents=agents,
    )


def _parse_category(raw: Mapping[str, Any]) -> CatalogCategory:
    cat_id = str(raw.get("id", "")).strip()
    if not cat_id:
        raise AgentCatalogError("Catalog category missing 'id'")
    subs_raw = raw.get("subcategories") or []
    if not isinstance(subs_raw, list):
        raise AgentCatalogError(f"Category {cat_id!r} 'subcategories' must be a list")
    subcategories = tuple(_parse_subcategory(item) for item in subs_raw if isinstance(item, Mapping))
    return CatalogCategory(
        id=cat_id,
        label=str(raw.get("label") or cat_id).strip(),
        description=str(raw.get("description", "")).strip(),
        subcategories=subcategories,
        llm_required=bool(raw.get("llm_required")),
    )


def load_agent_catalog(*, root: Optional[Path] = None, validate: bool = True) -> AgentCatalog:
    repo = root or project_root()
    path = agent_catalog_path(repo)
    if not path.is_file():
        raise AgentCatalogError(f"Agent catalog not found: {path}")

    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, Mapping):
        raise AgentCatalogError(f"Agent catalog root must be a mapping: {path}")

    categories_raw = data.get("categories")
    if not isinstance(categories_raw, list):
        raise AgentCatalogError(f"{path} must define a 'categories' list")

    categories = tuple(_parse_category(item) for item in categories_raw if isinstance(item, Mapping))

    if validate:
        registry = load_agent_registry(root=repo, validate=False)
        for category in categories:
            for sub in category.subcategories:
                for entry in sub.agents:
                    if entry.manifest not in registry.agents:
                        raise AgentCatalogError(
                            f"Catalog entry {entry.id!r} references unknown manifest "
                            f"{entry.manifest!r}"
                        )
    return AgentCatalog(categories=categories)


def catalog_index(*, root: Optional[Path] = None) -> dict[str, CatalogAgentEntry]:
    """Map manifest name → catalog entry (first match wins)."""
    try:
        catalog = load_agent_catalog(root=root, validate=False)
    except AgentCatalogError:
        return {}

    index: dict[str, CatalogAgentEntry] = {}
    for category in catalog.categories:
        for sub in category.subcategories:
            for entry in sub.agents:
                index.setdefault(entry.manifest, entry)
    return index


def catalog_entry_for_manifest(name: str, *, root: Optional[Path] = None) -> CatalogAgentEntry | None:
    return catalog_index(root=root).get(name)


def catalog_agent_payload(entry: CatalogAgentEntry, *, manifest_spec: Any | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": entry.id,
        "manifest": entry.manifest,
        "display_name": entry.display_name,
        "description": entry.description or (getattr(manifest_spec, "description", "") or ""),
        "tags": list(entry.tags),
        "input_schema": entry.input_schema,
        "output_schema": entry.output_schema,
    }
    if manifest_spec is not None:
        payload["type"] = getattr(manifest_spec, "type", "")
        payload["entry"] = getattr(manifest_spec, "entry", "")
        payload["runner"] = getattr(manifest_spec, "runner", "")
        payload["cluster_script"] = getattr(manifest_spec, "cluster_script", "")
        payload["flink_yaml"] = getattr(manifest_spec, "flink_yaml", "") or None
    return payload


def agent_catalog_response(*, root: Optional[Path] = None) -> dict[str, Any]:
    catalog = load_agent_catalog(root=root)
    registry = load_agent_registry(root=root, validate=False)
    categories: list[dict[str, Any]] = []

    for category in catalog.categories:
        subcategories: list[dict[str, Any]] = []
        for sub in category.subcategories:
            agents: list[dict[str, Any]] = []
            for entry in sub.agents:
                spec = registry.agents.get(entry.manifest)
                if spec is None:
                    raise AgentRegistryError(f"Unknown manifest {entry.manifest!r} in catalog")
                agents.append(
                    {
                        **catalog_agent_payload(entry, manifest_spec=spec),
                        "category_id": category.id,
                        "category_label": category.label,
                        "subcategory_id": sub.id,
                        "subcategory_label": sub.label,
                    }
                )
            subcategories.append(
                {
                    "id": sub.id,
                    "label": sub.label,
                    "description": sub.description,
                    "agents": agents,
                }
            )
        categories.append(
            {
                "id": category.id,
                "label": category.label,
                "description": category.description,
                "llm_required": category.llm_required,
                "subcategories": subcategories,
            }
        )

    payload: dict[str, Any] = {"categories": categories}
    try:
        from ratatoskr.designer.llm_settings import llm_settings_for_api

        payload["react_llm_defaults"] = llm_settings_for_api(root=root)
    except Exception:
        pass
    return payload
