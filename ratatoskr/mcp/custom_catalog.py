"""User-added MCP servers stored in designer.db (merged with YAML catalog)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from ratatoskr.designer.store import DesignerStore, designer_db_path
from ratatoskr.mcp.catalog import (
    McpServerEntry,
    _parse_server,
    load_builtin_mcp_catalog,
)

MCP_CUSTOM_CATALOG_KEY = "mcp_custom_catalog"

_default_store: DesignerStore | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_designer_store(root=None) -> DesignerStore:
    global _default_store
    if _default_store is None or root is not None:
        _default_store = DesignerStore(designer_db_path(root))
    return _default_store


def reset_custom_mcp_catalog_for_tests() -> None:
    global _default_store
    _default_store = None


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")
    return slug or "mcp_server"


def _load_custom_servers(store: DesignerStore) -> list[dict[str, Any]]:
    raw = store.get_json(MCP_CUSTOM_CATALOG_KEY)
    if not raw:
        return []
    servers = raw.get("servers")
    if not isinstance(servers, list):
        return []
    return [item for item in servers if isinstance(item, dict)]


def _save_custom_servers(store: DesignerStore, servers: list[dict[str, Any]]) -> None:
    store.set_json(
        MCP_CUSTOM_CATALOG_KEY,
        {"servers": servers, "updated_at": _utc_now()},
        updated_at=_utc_now(),
    )


def custom_servers_as_entries(*, root=None) -> tuple[McpServerEntry, ...]:
    store = default_designer_store(root)
    custom_raw = _load_custom_servers(store)
    return tuple(
        _parse_server(item, category_id="custom", category_label="Custom")
        for item in custom_raw
    )


def _builtin_server_ids(*, root=None) -> set[str]:
    try:
        builtin = load_builtin_mcp_catalog(root=root)
    except Exception:
        return set()
    return {server.id for category in builtin.categories for server in category.servers}


def add_mcp_catalog_server(body: dict[str, Any], *, root=None) -> dict[str, Any]:
    display_name = str(body.get("display_name") or "").strip()
    if not display_name:
        raise ValueError("display_name is required")

    requested_id = str(body.get("id") or "").strip()
    server_id = _slugify(requested_id or display_name)

    tools_raw = body.get("tools") or []
    if not isinstance(tools_raw, list) or not tools_raw:
        tool_name = str(body.get("tool_name") or "").strip()
        if not tool_name:
            raise ValueError("At least one tool is required (tools or tool_name)")
        tools_raw = [
            {
                "name": tool_name,
                "description": str(body.get("tool_description") or f"{tool_name} tool").strip(),
            }
        ]

    secrets_raw = body.get("required_secrets") or []
    if not secrets_raw:
        secret_name = str(body.get("secret_name") or "").strip()
        if secret_name:
            secrets_raw = [
                {
                    "name": secret_name,
                    "label": str(body.get("secret_label") or secret_name).strip(),
                }
            ]

    store = default_designer_store(root)
    existing = _load_custom_servers(store)
    builtin_ids = _builtin_server_ids(root=root)
    taken = builtin_ids | {str(item.get("id") or "") for item in existing}
    if server_id in taken:
        base = server_id
        suffix = 2
        while f"{base}_{suffix}" in taken:
            suffix += 1
        server_id = f"{base}_{suffix}"

    record: dict[str, Any] = {
        "id": server_id,
        "display_name": display_name,
        "description": str(body.get("description") or "").strip(),
        "transport": str(body.get("transport") or "http").strip().lower(),
        "docs_url": str(body.get("docs_url") or "").strip(),
        "tags": list(body.get("tags") or ["custom"]),
        "tools": tools_raw,
        "required_secrets": secrets_raw,
        "source": "custom",
        "created_at": _utc_now(),
    }
    if isinstance(body.get("config_schema"), dict):
        record["config_schema"] = body["config_schema"]

    existing.append(record)
    _save_custom_servers(store, existing)

    server = _parse_server(record, category_id="custom", category_label="Custom")
    from ratatoskr.mcp.catalog import default_instance_id

    return {
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
        "category_id": "custom",
        "category_label": "Custom",
        "source": "custom",
    }
