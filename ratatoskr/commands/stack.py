"""Docker Compose stack management."""

from __future__ import annotations

import subprocess
import time

import typer

from ratatoskr.docker_utils import (
    compose_cmd,
    compose_file,
    container_id,
    docker_exec,
    image_exists,
    project_root,
    run_compose,
)
from ratatoskr.flink_rest import flink_web_ui_url

app = typer.Typer(help="Manage the Docker Compose stack.")

PROFILE_HELP = (
    "Stack profile: 'full' (honeypot + Kafka + dashboard), "
    "'minimal' (Flink only), or 'kafka' (Studio Kafka only)"
)

_ENSURE_PIPELINE_CMD = (
    "cd /opt/flink && "
    "export PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
    "/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-src.zip && "
    "export FLINK_REST_ADDRESS=jobmanager FLINK_REST_PORT=8081 && "
    "python3 /opt/flink/cowrie_flink_pipeline_supervisor.py --once"
)

_ENSURE_KAFKA_TOPICS_CMD = _ENSURE_PIPELINE_CMD
_ENSURE_FLINK_JOBS_CMD = _ENSURE_PIPELINE_CMD


def _pipeline_service_container(profile: str, *services: str) -> str | None:
    for service in services:
        cid = container_id(service, profile=profile)
        if cid:
            return cid
    return None


def ensure_kafka_topics(profile: str = "full") -> None:
    """Create all honeypot pipeline Kafka topics if missing (full stack only)."""
    if profile != "full":
        typer.echo("Kafka pipeline topics require --profile full.", err=True)
        raise typer.Exit(1)

    cid = _pipeline_service_container(profile, "flink-pipeline-supervisor")
    if not cid:
        typer.echo(
            "flink-pipeline-supervisor not running. Start the stack first:\n"
            "  ratatoskr up --profile full",
            err=True,
        )
        raise typer.Exit(1)

    typer.echo("Ensuring honeypot pipeline Kafka topics exist...")
    rc = docker_exec(cid, _ENSURE_KAFKA_TOPICS_CMD, interactive=False)
    if rc != 0:
        typer.echo(
            "Failed to ensure Kafka topics. Check: ratatoskr logs flink-pipeline-supervisor",
            err=True,
        )
        raise typer.Exit(rc)
    typer.echo("Kafka topics OK")


def ensure_flink_jobs(profile: str = "full") -> None:
    """Submit Phase 1 / 1.5 / 2 Flink jobs if they are missing (full stack only)."""
    if profile != "full":
        typer.echo("Flink pipeline jobs require --profile full.", err=True)
        raise typer.Exit(1)

    cid = container_id("flink-pipeline-supervisor", profile=profile)
    if not cid:
        typer.echo(
            "flink-pipeline-supervisor not running. Start the stack first:\n"
            "  ratatoskr up --profile full",
            err=True,
        )
        raise typer.Exit(1)

    typer.echo("Ensuring honeypot Phase 1 / 1.5 / 2 Flink jobs are RUNNING...")
    rc = docker_exec(cid, _ENSURE_FLINK_JOBS_CMD, interactive=False)
    if rc != 0:
        typer.echo(
            "Failed to ensure Flink jobs. Check: ratatoskr logs flink-pipeline-supervisor",
            err=True,
        )
        raise typer.Exit(rc)
    typer.echo("Flink jobs OK — see " + flink_web_ui_url("full"))


@app.command("ensure-kafka-topics")
def ensure_kafka_topics_cmd(
    profile: str = typer.Option("full", "--profile", "-p", help=PROFILE_HELP),
) -> None:
    """Ensure all honeypot pipeline Kafka topics exist."""
    ensure_kafka_topics(profile=profile)


@app.command("ensure-flink-jobs")
def ensure_flink_jobs_cmd(
    profile: str = typer.Option("full", "--profile", "-p", help=PROFILE_HELP),
) -> None:
    """Ensure Phase 1 / 1.5 / 2 Flink streaming jobs are RUNNING on the cluster."""
    ensure_flink_jobs(profile=profile)


@app.command("up")
def up(
    profile: str = typer.Option("minimal", "--profile", "-p", help=PROFILE_HELP),
    build_image: bool = typer.Option(
        False, "--build-image", help="Build Docker image before starting if missing"
    ),
    wait: int = typer.Option(10, "--wait", help="Seconds to wait after start"),
    ensure_kafka: bool = typer.Option(
        True,
        "--ensure-kafka-topics/--no-ensure-kafka-topics",
        help="After start (full profile), verify pipeline Kafka topics exist",
    ),
    ensure_flink: bool = typer.Option(
        True,
        "--ensure-flink-jobs/--no-ensure-flink-jobs",
        help="After start (full profile), verify Phase 1/1.5/2 Flink jobs are RUNNING",
    ),
) -> None:
    """Start the stack (``docker compose up -d``)."""
    root = project_root()

    if profile == "full":
        hp = root / "honeypot"
        (hp / "cowrie-logs").mkdir(parents=True, exist_ok=True)
        (hp / "cowrie-data").mkdir(parents=True, exist_ok=True)

    if build_image and not image_exists():
        typer.echo("Flink image not found — building first...")
        from ratatoskr.commands.build import build_image as do_build

        do_build()

    typer.echo(f"Starting stack ({profile}) with {compose_file(profile).name}...")
    run_compose("up", "-d", "--remove-orphans", profile=profile)

    if wait > 0:
        typer.echo(f"Waiting {wait}s for services...")
        time.sleep(wait)

    if profile == "full" and ensure_kafka:
        try:
            ensure_kafka_topics(profile=profile)
        except typer.Exit:
            typer.echo(
                "Stack started, but Kafka topics are not ready yet. "
                "Supervisor will retry; or run: ratatoskr ensure-kafka-topics",
                err=True,
            )

    if profile == "full" and ensure_flink:
        try:
            ensure_flink_jobs(profile=profile)
        except typer.Exit:
            typer.echo(
                "Stack started, but Flink jobs are not ready yet. "
                "Sidecars will retry; or run: ratatoskr ensure-flink-jobs",
                err=True,
            )

    typer.echo("")
    typer.echo("Stack started.")
    if profile == "full":
        typer.echo("  Dashboard:  http://localhost:8501")
        typer.echo(f"  Flink UI:   {flink_web_ui_url('full')}")
        typer.echo("  Cowrie SSH: localhost:2222")
        typer.echo("  Cowrie Tel: localhost:2223")
    else:
        typer.echo(f"  Flink UI:   {flink_web_ui_url('minimal')}")


@app.command("down")
def down(
    profile: str = typer.Option("minimal", "--profile", "-p", help=PROFILE_HELP),
    volumes: bool = typer.Option(False, "--volumes", "-v", help="Remove volumes"),
) -> None:
    """Stop the stack."""
    args = ["down"]
    if volumes:
        args.append("-v")
    run_compose(*args, profile=profile)
    typer.echo("Stack stopped.")


@app.command("status")
def status(
    profile: str = typer.Option("minimal", "--profile", "-p", help=PROFILE_HELP),
) -> None:
    """Show running services."""
    run_compose("ps", profile=profile)


@app.command("logs")
def logs(
    service: str = typer.Argument("", help="Service name (omit for all services)"),
    profile: str = typer.Option("minimal", "--profile", "-p", help=PROFILE_HELP),
    follow: bool = typer.Option(False, "--follow", "-f", help="Follow log output"),
) -> None:
    """Tail service logs."""
    args = ["logs"]
    if follow:
        args.append("-f")
    if service:
        args.append(service)
    subprocess.run(compose_cmd(*args, profile=profile), cwd=project_root(), check=False)
