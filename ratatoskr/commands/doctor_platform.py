"""Platform doctor — Flink Agents stack preflight (no honeypot)."""

from __future__ import annotations

import typer

from ratatoskr.agents.registry import load_agent_registry
from ratatoskr.api.config import load_settings
from ratatoskr.api.services import pipeline_health
from ratatoskr.constants import DEFAULT_PROFILE
from ratatoskr.docker_utils import compose_available, container_id, image_exists

app = typer.Typer(help="Platform health checks (Flink Agents, no honeypot).")


def _line(label: str, level: str, detail: str = "") -> None:
    suffix = f" — {detail}" if detail else ""
    typer.echo(f"  [{level}] {label}{suffix}")


@app.command("platform")
def doctor_platform(
    fix: bool = typer.Option(False, "--fix", help="Attempt auto-fixes where supported"),
) -> None:
    """Check generic Flink Agents platform readiness."""
    del fix  # reserved for future auto-fix hooks
    settings = load_settings()
    critical_fail = False

    typer.echo("Ratatoskr platform doctor")
    typer.echo("")

    typer.echo("Workspace")
    try:
        load_agent_registry()
        _line("agent manifest", "OK")
    except Exception as exc:
        critical_fail = True
        _line("agent manifest", "FAIL", str(exc))

    typer.echo("")
    typer.echo("Docker")
    docker_ok = compose_available()
    if docker_ok:
        _line("docker compose", "OK")
    else:
        _line("docker compose", "WARN", "not available")

    image_ok = image_exists()
    if image_ok:
        _line("agent_flink_image", "OK")
    else:
        _line("agent_flink_image", "WARN", "run: ratatoskr build")

    jm = tm = None
    if docker_ok:
        jm = container_id("jobmanager", profile=DEFAULT_PROFILE)
        tm = container_id("taskmanager", profile=DEFAULT_PROFILE)
        _line("jobmanager container", "OK" if jm else "WARN", "not running")
        _line("taskmanager container", "OK" if tm else "WARN", "not running")

    typer.echo("")
    typer.echo("Flink REST")
    health = pipeline_health(settings)
    flink = health.get("flink") or {}
    flink_up = bool(flink.get("reachable"))
    if flink_up:
        _line("flink reachable", "OK", settings.flink_rest_url)
        _line(
            "taskmanager slots",
            "OK" if int(flink.get("slots_total") or 0) > 0 else "WARN",
            f"{flink.get('slots_free', 0)} free / {flink.get('slots_total', 0)} total",
        )
    else:
        if docker_ok and not jm:
            _line("flink reachable", "WARN", flink.get("error", "start: ratatoskr up"))
        else:
            critical_fail = True
            _line("flink reachable", "FAIL", flink.get("error", settings.flink_rest_url))

    typer.echo("")
    typer.echo("API")
    _line("api settings", "OK", settings.base_url)
    if settings.api_key:
        _line("api auth", "OK", "RATATOSKR_API_KEY configured")
    else:
        _line("api auth", "WARN", "RATATOSKR_API_KEY not set — protected routes are open")

    try:
        import urllib.request

        with urllib.request.urlopen(f"{settings.base_url}/v1/health", timeout=2) as resp:
            live = resp.status == 200
        _line("api process", "OK" if live else "WARN", "running" if live else "not running")
    except Exception:
        _line("api process", "WARN", "start with: ratatoskr api start")

    typer.echo("")
    if critical_fail:
        typer.echo("Platform doctor: FAIL", err=True)
        raise typer.Exit(1)
    typer.echo("Platform doctor: PASS")
