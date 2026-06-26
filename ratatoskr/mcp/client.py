"""Runtime bridge for catalog MCP tools (HTTP adapters)."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from ratatoskr.mcp.catalog import catalog_server
from ratatoskr.mcp.instances import get_mcp_instance


class McpToolError(RuntimeError):
    """MCP tool invocation failed."""


def invoke_tool(
    instance_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    *,
    root=None,
) -> dict[str, Any]:
    """Call a catalog MCP tool via its platform instance."""
    instance = get_mcp_instance(instance_id, root=root)
    if instance is None:
        raise McpToolError(f"Unknown MCP instance {instance_id!r}")
    if not instance.get("enabled"):
        raise McpToolError(f"MCP instance {instance_id!r} is not enabled")

    catalog_id = str(instance.get("catalog_id") or "")
    server = catalog_server(catalog_id, root=root)
    if server is None:
        raise McpToolError(f"Unknown MCP catalog server for instance {instance_id!r}")

    tool_names = {tool.name for tool in server.tools}
    if tool_name not in tool_names:
        raise McpToolError(
            f"Tool {tool_name!r} is not defined on MCP server {catalog_id!r}"
        )

    if catalog_id == "abuseipdb":
        return _abuseipdb_check_ip(instance, arguments)

    raise McpToolError(f"No runtime adapter for MCP server {catalog_id!r}")


def _abuseipdb_check_ip(instance: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    ip = str(arguments.get("ip") or "").strip()
    if not ip:
        raise McpToolError("check_ip requires 'ip' argument")

    secrets = instance.get("secrets") or {}
    api_key = str(secrets.get("ABUSEIPDB_API_KEY") or "").strip()
    if not api_key:
        return _mock_abuseipdb(ip, reason="ABUSEIPDB_API_KEY not configured")

    config = instance.get("config") or {}
    max_age = int(config.get("max_age_days") or 90)
    query = urllib.parse.urlencode({"ipAddress": ip, "maxAgeInDays": max_age})
    url = f"https://api.abuseipdb.com/api/v2/check?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Key": api_key,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return _mock_abuseipdb(ip, reason=f"AbuseIPDB auth failed ({exc.code})")
        body = exc.read().decode("utf-8", errors="replace")
        raise McpToolError(f"AbuseIPDB HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        return _mock_abuseipdb(ip, reason=f"AbuseIPDB unreachable: {exc.reason}")

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise McpToolError("AbuseIPDB returned unexpected payload")

    return {
        "ip": ip,
        "abuseConfidenceScore": data.get("abuseConfidenceScore"),
        "totalReports": data.get("totalReports"),
        "countryCode": data.get("countryCode"),
        "usageType": data.get("usageType"),
        "isp": data.get("isp"),
        "source": "abuseipdb",
    }


def _mock_abuseipdb(ip: str, *, reason: str) -> dict[str, Any]:
    return {
        "ip": ip,
        "abuseConfidenceScore": 0,
        "totalReports": 0,
        "countryCode": None,
        "usageType": "mock",
        "isp": None,
        "source": "mock",
        "fallback_reason": reason,
    }
