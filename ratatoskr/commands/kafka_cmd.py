"""Studio Kafka stack (independent of honeypot)."""

from __future__ import annotations

import time

import typer

from ratatoskr.constants import KAFKA_PROFILE
from ratatoskr.docker_utils import compose_file, run_compose
from ratatoskr.kafka_sources import STUDIO_KAFKA_EXTERNAL_PORT, kafka_reachable, sample_topic_records

app = typer.Typer(help="Studio Kafka — Zookeeper + broker for pipeline sources/sinks.")


@app.command("up")
def kafka_up(
    wait: int = typer.Option(15, "--wait", help="Seconds to wait for broker health"),
) -> None:
    """Start the Studio Kafka stack (``deploy/docker-compose.kafka.yml``)."""
    typer.echo(f"Starting Studio Kafka ({compose_file(KAFKA_PROFILE).name})...")
    run_compose("up", "-d", "--remove-orphans", profile=KAFKA_PROFILE)

    if wait > 0:
        typer.echo(f"Waiting {wait}s for Kafka...")
        time.sleep(wait)

    if kafka_reachable():
        typer.echo("Studio Kafka is ready.")
    else:
        typer.echo(
            "Kafka started but broker not reachable yet — retry in a few seconds.",
            err=True,
        )

    typer.echo(f"  Bootstrap:  localhost:{STUDIO_KAFKA_EXTERNAL_PORT}")
    typer.echo("  Topics:     workflow.test.input, workflow.test.output")
    typer.echo("")
    typer.echo("Set in .env:")
    typer.echo(f"  KAFKA_BOOTSTRAP_SERVERS=localhost:{STUDIO_KAFKA_EXTERNAL_PORT}")


@app.command("down")
def kafka_down(
    volumes: bool = typer.Option(False, "--volumes", "-v", help="Remove volumes"),
) -> None:
    """Stop the Studio Kafka stack."""
    args = ["down"]
    if volumes:
        args.append("-v")
    run_compose(*args, profile=KAFKA_PROFILE)
    typer.echo("Studio Kafka stopped.")


@app.command("status")
def kafka_status() -> None:
    """Show Studio Kafka services."""
    run_compose("ps", profile=KAFKA_PROFILE)
    if kafka_reachable():
        typer.echo(f"Broker reachable at localhost:{STUDIO_KAFKA_EXTERNAL_PORT}")
    else:
        typer.echo("Broker not reachable from host.", err=True)


@app.command("consume")
def kafka_consume(
    topic: str = typer.Argument(..., help="Topic name (e.g. workflow.test.output)"),
    limit: int = typer.Option(10, "--limit", "-n", help="Max messages to print"),
) -> None:
    """Print recent messages from a Studio Kafka topic."""
    import json

    if not kafka_reachable():
        typer.echo(
            f"Kafka not reachable at localhost:{STUDIO_KAFKA_EXTERNAL_PORT} — run: ratatoskr kafka up",
            err=True,
        )
        raise typer.Exit(1)

    try:
        records = sample_topic_records(topic, limit=limit)
    except Exception as exc:
        typer.echo(f"Failed to read {topic!r}: {exc}", err=True)
        raise typer.Exit(1) from exc

    if not records:
        typer.echo(f"No messages on {topic!r}.")
        return

    typer.echo(f"{topic} ({len(records)} message(s)):")
    for record in records:
        typer.echo(json.dumps(record, default=str))


@app.command("logs")
def kafka_logs(
    service: str = typer.Argument("", help="Service name (kafka, zookeeper; omit for all)"),
    follow: bool = typer.Option(False, "--follow", "-f", help="Follow log output"),
) -> None:
    """Tail Studio Kafka compose logs."""
    args = ["logs"]
    if follow:
        args.append("-f")
    if service:
        args.append(service)
    run_compose(*args, profile=KAFKA_PROFILE)
