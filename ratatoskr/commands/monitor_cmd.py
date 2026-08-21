"""Continuous monitoring agents (NiFi / Kafka poll loops)."""

from __future__ import annotations

import json

import typer

from ratatoskr.monitor_mode import DEFAULT_MONITOR_INTERVAL_SEC
from ratatoskr.monitor_runtime import start_monitors, status_dict, stop_monitors

app = typer.Typer(
    help="Turn NiFi/Kafka monitoring agents on as continuous queries.",
    no_args_is_help=True,
)


@app.command("start")
def monitor_start(
    nifi: bool = typer.Option(True, "--nifi/--no-nifi", help="Start workflow_nifi_monitor"),
    kafka: bool = typer.Option(True, "--kafka/--no-kafka", help="Start workflow_kafka_monitor"),
    interval: float = typer.Option(
        DEFAULT_MONITOR_INTERVAL_SEC,
        "--interval",
        "-i",
        help="Seconds between polls",
    ),
    phase: str = typer.Option(
        "monitor",
        "--phase",
        help="Heal phase for both agents (monitor|safe|lab)",
    ),
    foreground: bool = typer.Option(
        False,
        "--foreground",
        "-f",
        help="Run in this terminal until Ctrl-C (default: background)",
    ),
) -> None:
    """Start continuous host monitors (MONITOR_MODE=continuous)."""
    try:
        state = start_monitors(
            nifi=nifi,
            kafka=kafka,
            interval=interval,
            phase=phase,
            foreground=foreground,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    if foreground:
        return

    typer.echo("Continuous monitors started (background):")
    for proc in state.processes:
        typer.echo(f"  {proc.agent:28} pid={proc.pid}  log={proc.log}")
    typer.echo(f"interval={state.interval}s  phase={state.phase}")
    typer.echo("Use: ratatoskr monitor status | ratatoskr monitor stop")


@app.command("stop")
def monitor_stop() -> None:
    """Stop background continuous monitors."""
    stopped = stop_monitors()
    if not stopped:
        typer.echo("No continuous monitors were running.")
        return
    typer.echo(f"Stopped {len(stopped)} process(es): {', '.join(str(p) for p in stopped)}")


@app.command("status")
def monitor_status(
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Show whether continuous monitors are running."""
    info = status_dict()
    if as_json:
        typer.echo(json.dumps(info, indent=2))
        return
    if not info.get("running"):
        typer.echo("Continuous monitors: off")
        return
    typer.echo(
        f"Continuous monitors: on  interval={info.get('interval')}s  "
        f"phase={info.get('phase')}  since={info.get('started_at')}"
    )
    for proc in info.get("processes") or []:
        alive = "alive" if proc.get("alive") else "dead"
        typer.echo(
            f"  {proc.get('agent'):28} pid={proc.get('pid')}  {alive}  log={proc.get('log')}"
        )
