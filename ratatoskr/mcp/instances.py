"""Platform MCP server instances (designer.db)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from ratatoskr.designer.store import DesignerStore, designer_db_path
from ratatoskr.mcp.catalog import catalog_server, default_instance_id

MCP_INSTANCES_KEY = "mcp_instances"

_default_store: DesignerStore | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_designer_store(root=None) -> DesignerStore:
    global _default_store
    if _default_store is None or root is not None:
        _default_store = DesignerStore(designer_db_path(root))
    return _default_store


def reset_mcp_store_for_tests() -> None:
    global _default_store
    _default_store = None
    from ratatoskr.mcp.custom_catalog import reset_custom_mcp_catalog_for_tests

    reset_custom_mcp_catalog_for_tests()


def _load_instances_doc(store: DesignerStore) -> dict[str, Any]:
    raw = store.get_json(MCP_INSTANCES_KEY)
    if not raw:
        return {"instances": []}
    instances = raw.get("instances")
    if not isinstance(instances, list):
        return {"instances": []}
    return {"instances": instances}


def _save_instances_doc(store: DesignerStore, instances: list[dict[str, Any]]) -> None:
    store.set_json(MCP_INSTANCES_KEY, {"instances": instances}, updated_at=_utc_now())


def _secret_hint(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return f"{'*' * (len(value) - 4)}{value[-4:]}"


def _env_secrets_for_server(catalog_id: str) -> dict[str, str]:
    server = catalog_server(catalog_id)
    if server is None:
        return {}
    resolved: dict[str, str] = {}
    for spec in server.required_secrets:
        env_val = os.environ.get(spec.name, "").strip()
        if env_val:
            resolved[spec.name] = env_val
    return resolved


def _instance_for_api(instance: dict[str, Any], *, server: Any | None) -> dict[str, Any]:
    secrets = instance.get("secrets") if isinstance(instance.get("secrets"), dict) else {}
    secret_status: dict[str, Any] = {}
    for key, value in secrets.items():
        secret_status[key] = {
            "set": bool(str(value).strip()),
            "hint": _secret_hint(str(value)) if str(value).strip() else None,
        }
    if server is not None:
        for spec in server.required_secrets:
            if spec.name not in secret_status:
                env_val = os.environ.get(spec.name, "").strip()
                secret_status[spec.name] = {
                    "set": bool(env_val),
                    "hint": _secret_hint(env_val) if env_val else None,
                    "source": "environment" if env_val else None,
                }
    configured = True
    if server is not None:
        for spec in server.required_secrets:
            status = secret_status.get(spec.name) or {}
            if not status.get("set"):
                configured = False
                break
    return {
        "instance_id": instance.get("instance_id"),
        "catalog_id": instance.get("catalog_id"),
        "enabled": bool(instance.get("enabled")),
        "config": instance.get("config") or {},
        "secrets": secret_status,
        "configured": configured and bool(instance.get("enabled")),
        "updated_at": instance.get("updated_at"),
    }


def list_mcp_instances_api(*, root=None) -> dict[str, Any]:
    store = default_designer_store(root)
    doc = _load_instances_doc(store)
    index = {item.get("catalog_id"): item for item in doc["instances"] if isinstance(item, dict)}
    instances: list[dict[str, Any]] = []
    for server in _all_catalog_servers(root):
        stored = index.get(server.id)
        if stored is None:
            instances.append(
                {
                    "instance_id": default_instance_id(server.id),
                    "catalog_id": server.id,
                    "display_name": server.display_name,
                    "description": server.description,
                    "enabled": False,
                    "config": {},
                    "secrets": {
                        spec.name: {
                            "set": bool(os.environ.get(spec.name, "").strip()),
                            "hint": _secret_hint(os.environ.get(spec.name, ""))
                            if os.environ.get(spec.name, "").strip()
                            else None,
                            "source": "environment"
                            if os.environ.get(spec.name, "").strip()
                            else None,
                        }
                        for spec in server.required_secrets
                    },
                    "configured": False,
                    "updated_at": None,
                }
            )
        else:
            payload = _instance_for_api(stored, server=server)
            payload["display_name"] = server.display_name
            payload["description"] = server.description
            instances.append(payload)
    return {"instances": instances}


def _all_catalog_servers(root=None):
    from ratatoskr.mcp.catalog import load_mcp_catalog

    catalog = load_mcp_catalog(root=root)
    for category in catalog.categories:
        for server in category.servers:
            yield server


def get_mcp_instance(instance_id: str, *, root=None) -> dict[str, Any] | None:
    store = default_designer_store(root)
    doc = _load_instances_doc(store)
    for item in doc["instances"]:
        if not isinstance(item, dict):
            continue
        if str(item.get("instance_id") or "") == instance_id:
            server = catalog_server(str(item.get("catalog_id") or ""), root=root)
            return _resolve_instance(item, server=server, root=root)
    for server in _all_catalog_servers(root):
        if default_instance_id(server.id) == instance_id:
            stored = _find_by_catalog(store, server.id)
            if stored:
                return _resolve_instance(stored, server=server, root=root)
            env_secrets = _env_secrets_for_server(server.id)
            if env_secrets:
                return {
                    "instance_id": instance_id,
                    "catalog_id": server.id,
                    "enabled": True,
                    "config": {},
                    "secrets": env_secrets,
                }
    return None


def _find_by_catalog(store: DesignerStore, catalog_id: str) -> dict[str, Any] | None:
    doc = _load_instances_doc(store)
    for item in doc["instances"]:
        if isinstance(item, dict) and str(item.get("catalog_id") or "") == catalog_id:
            return item
    return None


def _resolve_instance(
    instance: dict[str, Any],
    *,
    server: Any | None,
    root=None,
) -> dict[str, Any]:
    catalog_id = str(instance.get("catalog_id") or "")
    secrets = dict(instance.get("secrets") or {})
    env_secrets = _env_secrets_for_server(catalog_id)
    for key, value in env_secrets.items():
        secrets.setdefault(key, value)
    return {
        "instance_id": str(instance.get("instance_id") or default_instance_id(catalog_id)),
        "catalog_id": catalog_id,
        "enabled": bool(instance.get("enabled")),
        "config": dict(instance.get("config") or {}),
        "secrets": secrets,
        "server": server,
    }


def upsert_mcp_instance(
    catalog_id: str,
    *,
    enabled: bool,
    secrets: dict[str, str] | None = None,
    config: dict[str, Any] | None = None,
    root=None,
) -> dict[str, Any]:
    server = catalog_server(catalog_id, root=root)
    if server is None:
        raise ValueError(f"Unknown MCP catalog server {catalog_id!r}")

    store = default_designer_store(root)
    doc = _load_instances_doc(store)
    instance_id = default_instance_id(catalog_id)
    existing = _find_by_catalog(store, catalog_id)
    merged_secrets = dict((existing or {}).get("secrets") or {})
    if secrets:
        for key, value in secrets.items():
            if value is None:
                continue
            trimmed = str(value).strip()
            if trimmed:
                merged_secrets[key] = trimmed
    merged_config = dict((existing or {}).get("config") or {})
    if config is not None:
        merged_config.update(config)

    record = {
        "instance_id": instance_id,
        "catalog_id": catalog_id,
        "enabled": enabled,
        "secrets": merged_secrets,
        "config": merged_config,
        "updated_at": _utc_now(),
    }

    remaining = [
        item
        for item in doc["instances"]
        if isinstance(item, dict) and str(item.get("catalog_id") or "") != catalog_id
    ]
    remaining.append(record)
    _save_instances_doc(store, remaining)

    payload = _instance_for_api(record, server=server)
    payload["display_name"] = server.display_name
    payload["description"] = server.description
    return payload


def test_mcp_instance(catalog_id: str, *, secrets: dict[str, str] | None = None, root=None) -> dict[str, Any]:
    from ratatoskr.mcp.client import invoke_tool

    server = catalog_server(catalog_id, root=root)
    if server is None:
        raise ValueError(f"Unknown MCP catalog server {catalog_id!r}")

    instance_id = default_instance_id(catalog_id)
    if secrets:
        upsert_mcp_instance(
            catalog_id,
            enabled=True,
            secrets=secrets,
            root=root,
        )

    stored = get_mcp_instance(instance_id, root=root)
    if stored is None or not stored.get("enabled"):
        missing = [spec.label or spec.name for spec in server.required_secrets]
        raise ValueError(
            f"MCP server {server.display_name!r} is not enabled. "
            f"Provide secrets: {', '.join(missing) or 'none'}"
        )

    if catalog_id == "abuseipdb":
        result = invoke_tool(instance_id, "check_ip", {"ip": "8.8.8.8"}, root=root)
        ok = isinstance(result, dict) and "abuseConfidenceScore" in result
        return {
            "ok": ok,
            "catalog_id": catalog_id,
            "instance_id": instance_id,
            "tool": "check_ip",
            "message": "AbuseIPDB connection verified (lookup 8.8.8.8)."
            if ok
            else "AbuseIPDB responded but result was unexpected.",
            "result": result,
        }

    if not server.tools:
        return {
            "ok": True,
            "catalog_id": catalog_id,
            "instance_id": instance_id,
            "message": f"{server.display_name} enabled (no test tool defined).",
            "result": {},
        }

    tool = server.tools[0]
    sample_args = _sample_args_for_tool(tool)
    result = invoke_tool(instance_id, tool.name, sample_args, root=root)
    return {
        "ok": True,
        "catalog_id": catalog_id,
        "instance_id": instance_id,
        "tool": tool.name,
        "message": f"{server.display_name} tool {tool.name!r} invoked successfully.",
        "result": result,
    }


def _sample_args_for_tool(tool: Any) -> dict[str, Any]:
    schema = tool.input_schema or {}
    props = schema.get("properties") or {}
    if "ip" in props:
        return {"ip": "8.8.8.8"}
    required = schema.get("required") or []
    if required:
        key = str(required[0])
        return {key: "test"}
    if props:
        key = next(iter(props))
        return {key: "test"}
    return {}
