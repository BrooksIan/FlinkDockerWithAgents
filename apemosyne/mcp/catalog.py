"""Load MCP server catalog for Settings and Designer."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml

from apemosyne.paths import examples_dir, project_root


class McpCatalogError(Exception):
    """Invalid or missing MCP server catalog."""


@dataclass(frozen=True)
class McpToolEntry:
    name: str
    description: str = ""
    input_schema: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class McpSecretSpec:
    name: str
    label: str = ""


@dataclass(frozen=True)
class McpServerEntry:
    id: str
    display_name: str
    description: str = ""
    transport: str = "http"
    docs_url: str = ""
    tags: tuple[str, ...] = ()
    tools: tuple[McpToolEntry, ...] = ()
    required_secrets: tuple[McpSecretSpec, ...] = ()
    config_schema: dict[str, Any] = field(default_factory=dict)
    category_id: str = ""
    category_label: str = ""


@dataclass(frozen=True)
class McpCatalogCategory:
    id: str
    label: str
    description: str = ""
    servers: tuple[McpServerEntry, ...] = ()


@dataclass(frozen=True)
class McpCatalog:
    categories: tuple[McpCatalogCategory, ...]


def mcp_catalog_path(root: Optional[Path] = None) -> Path:
    return examples_dir(root) / "mcp" / "mcp-server-catalog.yaml"


def _parse_tool(raw: Mapping[str, Any]) -> McpToolEntry:
    name = str(raw.get("name") or "").strip()
    if not name:
        raise McpCatalogError("MCP tool missing 'name'")
    input_schema = raw.get("input_schema") if isinstance(raw.get("input_schema"), dict) else {}
    return McpToolEntry(
        name=name,
        description=str(raw.get("description") or "").strip(),
        input_schema=dict(input_schema),
    )


def _parse_secret(raw: Mapping[str, Any]) -> McpSecretSpec:
    name = str(raw.get("name") or "").strip()
    if not name:
        raise McpCatalogError("MCP secret spec missing 'name'")
    return McpSecretSpec(name=name, label=str(raw.get("label") or name).strip())


def _parse_server(raw: Mapping[str, Any], *, category_id: str, category_label: str) -> McpServerEntry:
    server_id = str(raw.get("id") or "").strip()
    if not server_id:
        raise McpCatalogError("MCP server missing 'id'")
    tools_raw = raw.get("tools") or []
    if not isinstance(tools_raw, list):
        raise McpCatalogError(f"MCP server {server_id!r} tools must be a list")
    secrets_raw = raw.get("required_secrets") or []
    if not isinstance(secrets_raw, list):
        raise McpCatalogError(f"MCP server {server_id!r} required_secrets must be a list")
    tags_raw = raw.get("tags") or []
    tags = tuple(str(t).strip() for t in tags_raw if str(t).strip()) if isinstance(tags_raw, list) else ()
    config_schema = raw.get("config_schema") if isinstance(raw.get("config_schema"), dict) else {}
    return McpServerEntry(
        id=server_id,
        display_name=str(raw.get("display_name") or server_id).strip(),
        description=str(raw.get("description") or "").strip(),
        transport=str(raw.get("transport") or "http").strip().lower(),
        docs_url=str(raw.get("docs_url") or "").strip(),
        tags=tags,
        tools=tuple(_parse_tool(item) for item in tools_raw if isinstance(item, Mapping)),
        required_secrets=tuple(_parse_secret(item) for item in secrets_raw if isinstance(item, Mapping)),
        config_schema=dict(config_schema),
        category_id=category_id,
        category_label=category_label,
    )


def _parse_category(raw: Mapping[str, Any]) -> McpCatalogCategory:
    cat_id = str(raw.get("id") or "").strip()
    if not cat_id:
        raise McpCatalogError("MCP catalog category missing 'id'")
    label = str(raw.get("label") or cat_id).strip()
    servers_raw = raw.get("servers") or []
    if not isinstance(servers_raw, list):
        raise McpCatalogError(f"MCP category {cat_id!r} servers must be a list")
    return McpCatalogCategory(
        id=cat_id,
        label=label,
        description=str(raw.get("description") or "").strip(),
        servers=tuple(
            _parse_server(item, category_id=cat_id, category_label=label)
            for item in servers_raw
            if isinstance(item, Mapping)
        ),
    )


def load_mcp_catalog(*, root: Optional[Path] = None) -> McpCatalog:
    repo = root or project_root()
    path = mcp_catalog_path(repo)
    if not path.is_file():
        raise McpCatalogError(f"MCP server catalog not found: {path}")

    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, Mapping):
        raise McpCatalogError(f"MCP catalog root must be a mapping: {path}")

    categories_raw = data.get("categories")
    if not isinstance(categories_raw, list):
        raise McpCatalogError(f"{path} must define a 'categories' list")

    categories = tuple(_parse_category(item) for item in categories_raw if isinstance(item, Mapping))
    return McpCatalog(categories=categories)


def catalog_server_index(*, root: Optional[Path] = None) -> dict[str, McpServerEntry]:
    try:
        catalog = load_mcp_catalog(root=root)
    except McpCatalogError:
        return {}
    index: dict[str, McpServerEntry] = {}
    for category in catalog.categories:
        for server in category.servers:
            index[server.id] = server
    return index


def catalog_server(server_id: str, *, root: Optional[Path] = None) -> McpServerEntry | None:
    return catalog_server_index(root=root).get(server_id)


def default_instance_id(catalog_id: str) -> str:
    return f"inst_{catalog_id}"


def mcp_catalog_response(*, root: Optional[Path] = None) -> dict[str, Any]:
    catalog = load_mcp_catalog(root=root)
    categories: list[dict[str, Any]] = []
    for category in catalog.categories:
        servers: list[dict[str, Any]] = []
        for server in category.servers:
            servers.append(
                {
                    "id": server.id,
                    "display_name": server.display_name,
                    "description": server.description,
                    "transport": server.transport,
                    "docs_url": server.docs_url or None,
                    "tags": list(server.tags),
                    "tools": [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.input_schema,
                        }
                        for tool in server.tools
                    ],
                    "required_secrets": [
                        {"name": secret.name, "label": secret.label or secret.name}
                        for secret in server.required_secrets
                    ],
                    "config_schema": server.config_schema,
                    "default_instance_id": default_instance_id(server.id),
                    "category_id": category.id,
                    "category_label": category.label,
                }
            )
        categories.append(
            {
                "id": category.id,
                "label": category.label,
                "description": category.description,
                "servers": servers,
            }
        )
    return {"categories": categories}
