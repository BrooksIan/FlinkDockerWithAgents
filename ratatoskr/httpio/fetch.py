"""Minimal JSON HTTP fetch helper (stdlib only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def http_fetch_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout_seconds: float = 15,
) -> dict[str, Any]:
    """Fetch a URL and return {ok, status_code, url, data, error, raw_text}."""
    normalized = method.strip().upper() or "GET"
    if normalized not in ("GET", "POST"):
        raise ValueError(f"Unsupported HTTP method {method!r}")

    payload_bytes: bytes | None = None
    request_headers = dict(headers or {})
    if body is not None:
        payload_bytes = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(
        url,
        data=payload_bytes,
        headers=request_headers,
        method=normalized,
    )
    started_url = url
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8", errors="replace")
            status_code = int(getattr(response, "status", 200))
    except urllib.error.HTTPError as exc:
        status_code = int(exc.code)
        raw = exc.read().decode("utf-8", errors="replace")
        parsed = _parse_json_or_text(raw)
        return {
            "ok": False,
            "status_code": status_code,
            "url": started_url,
            "data": parsed if isinstance(parsed, dict) else {"body": parsed},
            "error": f"HTTP {status_code}",
            "raw_text": raw[:4000],
        }
    except urllib.error.URLError as exc:
        return {
            "ok": False,
            "status_code": 0,
            "url": started_url,
            "data": {},
            "error": str(exc.reason or exc),
            "raw_text": "",
        }

    parsed = _parse_json_or_text(raw)
    data = parsed if isinstance(parsed, (dict, list)) else {"body": parsed}
    return {
        "ok": 200 <= status_code < 300,
        "status_code": status_code,
        "url": started_url,
        "data": data,
        "error": None if 200 <= status_code < 300 else f"HTTP {status_code}",
        "raw_text": raw[:4000],
    }


def _parse_json_or_text(raw: str) -> Any:
    text = raw.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def append_query(url: str, params: dict[str, Any]) -> str:
    """Append query parameters to a URL."""
    filtered = {k: v for k, v in params.items() if v is not None and str(v).strip()}
    if not filtered:
        return url
    parts = urllib.parse.urlsplit(url)
    existing = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    merged = existing + [(str(k), str(v)) for k, v in filtered.items()]
    query = urllib.parse.urlencode(merged)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


__all__ = ["append_query", "http_fetch_json"]
