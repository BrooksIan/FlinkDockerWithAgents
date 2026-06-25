"""Control API server commands."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import typer

from apemosyne.api.config import load_settings

app = typer.Typer(help="Apemosyne control API (for dashboards).")


@app.command("start")
def api_start(
    host: str = typer.Option("", "--host", help="Bind host (default: APEMOSYNE_API_HOST)"),
    port: int = typer.Option(0, "--port", help="Bind port (default: APEMOSYNE_API_PORT)"),
    reload: bool = typer.Option(False, "--reload", help="Dev auto-reload"),
) -> None:
    """Start the FastAPI control API with uvicorn."""
    settings = load_settings()
    bind_host = host or settings.host
    bind_port = port or settings.port
    typer.echo(f"Starting Apemosyne API on http://{bind_host}:{bind_port}")
    typer.echo(f"  Docs:    http://{bind_host}:{bind_port}/docs")
    typer.echo(f"  Health:  http://{bind_host}:{bind_port}/v1/health")
    typer.echo(f"  Metrics: http://{bind_host}:{bind_port}/metrics")

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "apemosyne.api.app:create_app",
        "--factory",
        "--host",
        bind_host,
        "--port",
        str(bind_port),
    ]
    if reload:
        cmd.append("--reload")
    raise typer.Exit(subprocess.run(cmd).returncode)


@app.command("url")
def api_url() -> None:
    """Print the configured API base URL."""
    settings = load_settings()
    typer.echo(settings.base_url)


@app.command("openapi")
def api_openapi(
    output: Path = typer.Option(
        "",
        "--output",
        "-o",
        help="Write OpenAPI JSON to file (default: stdout)",
    ),
) -> None:
    """Dump the OpenAPI schema."""
    from apemosyne.api.app import create_app

    schema = create_app().openapi()
    text = json.dumps(schema, indent=2)
    if output:
        output.write_text(text, encoding="utf-8")
        typer.echo(f"Wrote {output}")
    else:
        typer.echo(text)


@app.command("check")
def api_check() -> None:
    """Probe /v1/health (API must already be running)."""
    import urllib.error
    import urllib.request

    settings = load_settings()
    url = f"{settings.base_url}/v1/health"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError) as exc:
        typer.echo(f"API not reachable at {url}: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(body, indent=2))
