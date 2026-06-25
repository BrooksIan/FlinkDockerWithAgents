"""Optional API key authentication."""

from __future__ import annotations

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from apemosyne.api.config import ApiSettings, load_settings

_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    request: Request,
    api_key: str | None = Security(_HEADER),
    settings: ApiSettings | None = None,
) -> None:
    cfg = settings or getattr(request.app.state, "settings", None) or load_settings()
    if not cfg.api_key:
        return
    if api_key != cfg.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
