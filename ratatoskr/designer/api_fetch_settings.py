"""Platform HTTP API fetch settings for workflow agents."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from ratatoskr.designer.models import ApiFetchSettings
from ratatoskr.designer.store import API_FETCH_KEY, DesignerStore, designer_db_path
from ratatoskr.httpio.fetch import append_query, http_fetch_json

_default_store: DesignerStore | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_designer_store(root: Path | None = None) -> DesignerStore:
    global _default_store
    if _default_store is None or root is not None:
        _default_store = DesignerStore(designer_db_path(root))
    return _default_store


def reset_api_fetch_store_for_tests() -> None:
    global _default_store
    _default_store = None


def _env_api_fetch_settings() -> ApiFetchSettings:
    endpoint = (os.environ.get("RATATOSKR_API_FETCH_ENDPOINT_URL") or "").strip()
    method = (os.environ.get("RATATOSKR_API_FETCH_HTTP_METHOD") or "GET").strip()
    api_key = (os.environ.get("RATATOSKR_API_FETCH_API_KEY") or "").strip()
    auth_header = (os.environ.get("RATATOSKR_API_FETCH_AUTH_HEADER") or "Authorization").strip()
    auth_prefix = (os.environ.get("RATATOSKR_API_FETCH_AUTH_PREFIX") or "Bearer").strip()
    timeout_raw = (os.environ.get("RATATOSKR_API_FETCH_TIMEOUT_SECONDS") or "15").strip()
    try:
        timeout_seconds = max(1, int(timeout_raw))
    except ValueError:
        timeout_seconds = 15
    return ApiFetchSettings(
        endpoint_url=endpoint,
        http_method=method,
        api_key=api_key,
        auth_header=auth_header or "Authorization",
        auth_prefix=auth_prefix,
        timeout_seconds=timeout_seconds,
    )


def _stored_api_fetch_settings(store: DesignerStore) -> ApiFetchSettings | None:
    raw = store.get_json(API_FETCH_KEY)
    if not raw:
        return None
    timeout_raw = raw.get("timeout_seconds", 15)
    try:
        timeout_seconds = max(1, int(timeout_raw))
    except (TypeError, ValueError):
        timeout_seconds = 15
    return ApiFetchSettings(
        endpoint_url=str(raw.get("endpoint_url") or "").strip(),
        http_method=str(raw.get("http_method") or "GET").strip(),
        api_key=str(raw.get("api_key") or "").strip(),
        auth_header=str(raw.get("auth_header") or "Authorization").strip() or "Authorization",
        auth_prefix=str(raw.get("auth_prefix") or "Bearer").strip(),
        timeout_seconds=timeout_seconds,
    )


def get_api_fetch_settings(*, root: Path | None = None) -> ApiFetchSettings:
    store = default_designer_store(root)
    stored = _stored_api_fetch_settings(store)
    if stored is not None:
        env = _env_api_fetch_settings()
        return ApiFetchSettings(
            endpoint_url=stored.endpoint_url or env.endpoint_url,
            http_method=stored.http_method or env.http_method,
            api_key=stored.api_key or env.api_key,
            auth_header=stored.auth_header or env.auth_header,
            auth_prefix=stored.auth_prefix if stored.auth_prefix is not None else env.auth_prefix,
            timeout_seconds=stored.timeout_seconds or env.timeout_seconds,
        )
    return _env_api_fetch_settings()


def auth_headers(settings: ApiFetchSettings) -> dict[str, str]:
    if not settings.api_key.strip():
        return {}
    header = settings.auth_header.strip() or "Authorization"
    prefix = settings.auth_prefix.strip()
    value = f"{prefix} {settings.api_key}".strip() if prefix else settings.api_key
    return {header: value}


def build_fetch_url(settings: ApiFetchSettings, input_payload: dict[str, Any] | None = None) -> str:
    base = settings.endpoint_url.strip()
    if not base:
        raise ValueError("API fetch endpoint URL is not configured")

    payload = input_payload or {}
    path_suffix = str(payload.get("path") or payload.get("path_suffix") or "").strip()
    url = urljoin(base if base.endswith("/") else f"{base}/", path_suffix.lstrip("/")) if path_suffix else base

    query = payload.get("query")
    if isinstance(query, dict):
        return append_query(url, query)
    if settings.normalized_method() == "GET" and payload:
        skip = {"path", "path_suffix", "query", "body"}
        params = {k: v for k, v in payload.items() if k not in skip}
        return append_query(url, params)
    return url


def fetch_with_settings(
    settings: ApiFetchSettings,
    *,
    input_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not settings.is_complete():
        raise ValueError(
            "API fetch endpoint is not configured. Set it in Settings or "
            "RATATOSKR_API_FETCH_ENDPOINT_URL."
        )

    payload = input_payload or {}
    url = build_fetch_url(settings, payload)
    body = payload.get("body") if isinstance(payload.get("body"), dict) else None
    if settings.normalized_method() == "POST" and body is None and payload:
        skip = {"path", "path_suffix", "query"}
        body = {k: v for k, v in payload.items() if k not in skip} or None

    return http_fetch_json(
        url,
        method=settings.normalized_method(),
        headers=auth_headers(settings),
        body=body if settings.normalized_method() == "POST" else None,
        timeout_seconds=float(settings.timeout_seconds),
    )


def _api_key_hint(api_key: str) -> str | None:
    if not api_key:
        return None
    if len(api_key) <= 4:
        return "****"
    return f"{'*' * (len(api_key) - 4)}{api_key[-4:]}"


def api_fetch_settings_for_api(*, root: Path | None = None) -> dict[str, Any]:
    store = default_designer_store(root)
    stored = _stored_api_fetch_settings(store)
    env = _env_api_fetch_settings()
    effective = get_api_fetch_settings(root=root)
    source = "designer" if stored is not None else ("environment" if env.is_complete() else "unset")
    return {
        "scope": "workflow_api_fetch",
        "endpoint_url": effective.endpoint_url,
        "http_method": effective.normalized_method(),
        "auth_header": effective.auth_header,
        "auth_prefix": effective.auth_prefix,
        "timeout_seconds": effective.timeout_seconds,
        "api_key_set": bool(effective.api_key),
        "api_key_hint": _api_key_hint(effective.api_key),
        "configured": effective.is_complete(),
        "source": source,
        "env_fallback": {
            "endpoint_url": env.endpoint_url or None,
            "http_method": env.normalized_method(),
            "api_key_set": bool(env.api_key),
        },
    }


def update_api_fetch_settings(
    *,
    endpoint_url: str,
    http_method: str = "GET",
    api_key: str | None = None,
    auth_header: str = "Authorization",
    auth_prefix: str = "Bearer",
    timeout_seconds: int = 15,
    root: Path | None = None,
) -> dict[str, Any]:
    store = default_designer_store(root)
    current = _stored_api_fetch_settings(store) or _env_api_fetch_settings()
    resolved_key = (api_key if api_key is not None else current.api_key).strip()
    payload = {
        "endpoint_url": endpoint_url.strip(),
        "http_method": http_method.strip().upper() or "GET",
        "api_key": resolved_key,
        "auth_header": auth_header.strip() or "Authorization",
        "auth_prefix": auth_prefix.strip(),
        "timeout_seconds": max(1, int(timeout_seconds)),
    }
    store.set_json(API_FETCH_KEY, payload, updated_at=_utc_now())
    return api_fetch_settings_for_api(root=root)


def resolve_api_fetch_settings_from_body(
    body: dict[str, Any] | None,
    *,
    root: Path | None = None,
) -> ApiFetchSettings:
    current = get_api_fetch_settings(root=root)
    raw = body or {}
    endpoint_url = str(raw.get("endpoint_url") or current.endpoint_url or "").strip()
    http_method = str(raw.get("http_method") or current.http_method or "GET").strip()
    auth_header = str(raw.get("auth_header") or current.auth_header or "Authorization").strip()
    auth_prefix = str(raw.get("auth_prefix") if raw.get("auth_prefix") is not None else current.auth_prefix)
    timeout_raw = raw.get("timeout_seconds", current.timeout_seconds)
    try:
        timeout_seconds = max(1, int(timeout_raw))
    except (TypeError, ValueError):
        timeout_seconds = current.timeout_seconds
    api_key_raw = raw.get("api_key")
    api_key = current.api_key if api_key_raw is None else str(api_key_raw).strip()
    return ApiFetchSettings(
        endpoint_url=endpoint_url,
        http_method=http_method,
        api_key=api_key,
        auth_header=auth_header or "Authorization",
        auth_prefix=auth_prefix,
        timeout_seconds=timeout_seconds,
    )


def test_api_fetch_settings(
    body: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    settings = resolve_api_fetch_settings_from_body(body, root=root)
    if not settings.is_complete():
        raise ValueError(
            "API fetch endpoint URL is required. Configure it in Settings or via "
            "RATATOSKR_API_FETCH_ENDPOINT_URL."
        )

    started = time.perf_counter()
    result = fetch_with_settings(settings)
    duration_ms = int((time.perf_counter() - started) * 1000)
    ok = bool(result.get("ok"))
    preview = result.get("data")
    if isinstance(preview, dict):
        preview_keys = list(preview.keys())[:8]
    elif isinstance(preview, list):
        preview_keys = [f"list[{len(preview)}]"]
    else:
        preview_keys = []
    return {
        "ok": ok,
        "duration_ms": duration_ms,
        "endpoint_url": settings.endpoint_url,
        "http_method": settings.normalized_method(),
        "status_code": result.get("status_code"),
        "url": result.get("url"),
        "preview_keys": preview_keys,
        "message": (
            f"Fetched {result.get('url')} — HTTP {result.get('status_code')}."
            if ok
            else f"Fetch failed: {result.get('error') or result.get('status_code')}"
        ),
    }


__all__ = [
    "api_fetch_settings_for_api",
    "auth_headers",
    "build_fetch_url",
    "fetch_with_settings",
    "get_api_fetch_settings",
    "reset_api_fetch_store_for_tests",
    "resolve_api_fetch_settings_from_body",
    "test_api_fetch_settings",
    "update_api_fetch_settings",
]
