"""Continuous monitoring / healing agents (host or Flink cluster)."""

from __future__ import annotations

import json
from typing import Optional

import typer

from ratatoskr.constants import NIFI_PROFILE, PROFILE_HELP
from ratatoskr.monitor_mode import DEFAULT_MONITOR_INTERVAL_SEC
from ratatoskr.monitor_runtime import (
    start_cluster_monitors,
    start_monitors,
    status_dict,
    stop_monitors,
)

app = typer.Typer(
    help="Turn NiFi/Kafka/CM monitoring (and healing) agents on as continuous queries.",
    no_args_is_help=True,
)


@app.command("start")
def monitor_start(
    agent: Optional[list[str]] = typer.Option(
        None,
        "--agent",
        help="Monitor agent(s): nifi, kafka, cm (repeatable or comma-separated). "
        "Overrides --nifi/--kafka/--cm when set.",
    ),
    nifi: bool = typer.Option(True, "--nifi/--no-nifi", help="Start workflow_nifi_monitor"),
    kafka: bool = typer.Option(True, "--kafka/--no-kafka", help="Start workflow_kafka_monitor"),
    cm: bool = typer.Option(False, "--cm/--no-cm", help="Start workflow_cm_monitor (recommend-only)"),
    interval: float = typer.Option(
        DEFAULT_MONITOR_INTERVAL_SEC,
        "--interval",
        "-i",
        help="Seconds between polls",
    ),
    phase: str = typer.Option(
        "monitor",
        "--phase",
        help="Heal phase for NiFi/Kafka (monitor|safe|lab) — CM is always recommend-only",
    ),
    cluster: bool = typer.Option(
        False,
        "--cluster/--local",
        help="Deploy continuous Flink jobs (default: host background processes)",
    ),
    profile: str = typer.Option(
        NIFI_PROFILE,
        "--profile",
        "-p",
        help=PROFILE_HELP,
    ),
    foreground: bool = typer.Option(
        False,
        "--foreground",
        "-f",
        help="Host only: run in this terminal until Ctrl-C",
    ),
) -> None:
    """Start continuous monitors (host) or deploy them to the Flink cluster."""
    if cluster and foreground:
        typer.echo("--foreground is only valid with --local", err=True)
        raise typer.Exit(2)

    try:
        if cluster:
            state = start_cluster_monitors(
                keys=agent,
                nifi=nifi,
                kafka=kafka,
                cm=cm,
                interval=interval,
                phase=phase,
                profile=profile,
            )
        else:
            state = start_monitors(
                keys=agent,
                nifi=nifi,
                kafka=kafka,
                cm=cm,
                interval=interval,
                phase=phase,
                foreground=foreground,
            )
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    if foreground:
        return

    if cluster:
        typer.echo("Continuous monitors deployed to Flink cluster:")
        for proc in state.processes:
            jid = proc.flink_job_id or "(pending)"
            typer.echo(f"  {proc.agent:28} job={jid}")
        typer.echo(
            f"mode=cluster  profile={state.profile}  "
            f"interval={state.interval}s  phase={state.phase}"
        )
        typer.echo("Flink UI: http://localhost:8082  |  ratatoskr monitor status | stop")
        return

    typer.echo("Continuous monitors started (host background):")
    for proc in state.processes:
        typer.echo(f"  {proc.agent:28} pid={proc.pid}  log={proc.log}")
    typer.echo(f"mode=host  interval={state.interval}s  phase={state.phase}")
    typer.echo("Use: ratatoskr monitor status | ratatoskr monitor stop")


@app.command("stop")
def monitor_stop() -> None:
    """Stop host monitors or cancel Flink monitor jobs tracked by monitor start."""
    result = stop_monitors()
    mode = result.get("mode")
    if mode is None:
        typer.echo("No continuous monitors were running.")
        return
    if mode == "cluster":
        jobs = result.get("canceled_jobs") or []
        if not jobs:
            typer.echo("Cleared cluster monitor state (no job ids to cancel).")
            return
        typer.echo(f"Canceled {len(jobs)} Flink job(s): {', '.join(jobs)}")
        return
    pids = result.get("stopped_pids") or []
    if not pids:
        typer.echo("No host monitor processes were running (state cleared).")
        return
    typer.echo(f"Stopped {len(pids)} process(es): {', '.join(str(p) for p in pids)}")


@app.command("status")
def monitor_status(
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Show whether continuous monitors are running (host or cluster)."""
    info = status_dict()
    if as_json:
        typer.echo(json.dumps(info, indent=2))
        return
    if not info.get("running"):
        typer.echo("Continuous monitors: off")
        return
    mode = info.get("mode") or "host"
    typer.echo(
        f"Continuous monitors: on  mode={mode}  interval={info.get('interval')}s  "
        f"phase={info.get('phase')}  since={info.get('started_at')}"
    )
    if mode == "cluster" and info.get("profile"):
        typer.echo(f"  profile={info.get('profile')}")
    for proc in info.get("processes") or []:
        if mode == "cluster":
            typer.echo(
                f"  {proc.get('agent'):28} job={proc.get('flink_job_id')}  "
                f"state={proc.get('flink_state')}"
            )
        else:
            alive = "alive" if proc.get("alive") else "dead"
            typer.echo(
                f"  {proc.get('agent'):28} pid={proc.get('pid')}  {alive}  "
                f"log={proc.get('log')}"
            )
