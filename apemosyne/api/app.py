"""FastAPI application factory."""

from __future__ import annotations

import os

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from apemosyne import __version__
from apemosyne.api.config import ApiSettings, load_settings
from apemosyne.api.observability import configure_logging, metrics_payload
from apemosyne.api.routes import router


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    from apemosyne.env import load_workspace_env

    load_workspace_env()
    cfg = settings or load_settings()
    # Keep Flink REST helpers aligned with API settings.
    os.environ.setdefault("FLINK_REST_ADDRESS", cfg.flink_rest_host)
    os.environ.setdefault("FLINK_REST_PORT", str(cfg.flink_rest_port))
    os.environ.setdefault("APEMOSYNE_PROFILE", cfg.default_profile)
    configure_logging(json_logs=cfg.log_json)

    app = FastAPI(
        title="Apemosyne Control API",
        description="Read-only Flink Agents control plane for dashboards and automation.",
        version=__version__,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    app.state.settings = cfg

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        import time

        from apemosyne.api.observability import track_request

        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start
        path = request.url.path
        if path.startswith("/v1/jobs/") and path != "/v1/jobs":
            path = "/v1/jobs/{id}"
        elif path.startswith("/v1/agents/") and not path.endswith("/submit"):
            path = "/v1/agents/{name}"
        track_request(request.method, path, response.status_code, duration)
        return response

    app.include_router(router)

    @app.get("/", tags=["meta"])
    def root() -> dict[str, str]:
        return {
            "service": "apemosyne-api",
            "version": __version__,
            "docs": "/docs",
            "health": "/v1/health",
        }

    @app.get("/metrics", tags=["observability"])
    def metrics() -> Response:
        body, content_type = metrics_payload()
        return Response(content=body, media_type=content_type)

    return app
