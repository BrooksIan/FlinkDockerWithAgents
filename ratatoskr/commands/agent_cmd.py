"""Flink Agents lifecycle commands."""

from __future__ import annotations

import json
import os

import typer

from ratatoskr.agents.registry import AgentRegistryError, list_agent_names, load_agent_registry
from ratatoskr.agents.submit import (
    describe_agent,
    run_agent_local,
    submit_agent_cluster,
)
from ratatoskr.constants import DEFAULT_PROFILE, PROFILE_HELP, normalize_profile
from ratatoskr.runtime import flink_cluster_submit

app = typer.Typer(help="Manage Flink Agents (list, run, submit, status).")


def _resolve_profile(profile: str | None) -> str:
    return normalize_profile(profile or os.environ.get("RATATOSKR_PROFILE", DEFAULT_PROFILE))


@app.command("list")
def agent_list() -> None:
    """List registered agents from examples/agents/agent-manifest.yaml."""
    try:
        names = list_agent_names()
    except AgentRegistryError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    for name in names:
        spec = load_agent_registry(validate=False).agents[name]
        typer.echo(f"{name:20} {spec.type:10} {spec.description or spec.entry}")


@app.command("describe")
def agent_describe(name: str = typer.Argument(..., help="Agent name")) -> None:
    """Show agent metadata and entry class."""
    try:
        info = describe_agent(name)
    except (AgentRegistryError, ImportError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(info, indent=2))


@app.command("run")
def agent_run(
    name: str = typer.Argument(..., help="Agent name"),
    local: bool = typer.Option(True, "--local/--cluster", help="Local runner or cluster submit"),
    profile: str | None = typer.Option(None, "--profile", "-p", help=PROFILE_HELP),
    continuous: bool = typer.Option(
        False,
        "--continuous",
        "-c",
        help="Continuous queries: forever host polls, or unbounded cluster Kafka ticks",
    ),
    interval: float | None = typer.Option(
        None,
        "--interval",
        "-i",
        help="Poll interval seconds (local continuous; default MONITOR_INTERVAL_SEC or 10)",
    ),
    phase: str | None = typer.Option(
        None,
        "--phase",
        help="Heal phase override (monitor|safe|lab) for NiFi/Kafka monitors",
    ),
) -> None:
    """Run an agent locally or submit its cluster job."""
    from ratatoskr.monitor_mode import DEFAULT_MONITOR_INTERVAL_SEC, monitor_interval_sec

    active_profile = _resolve_profile(profile)
    if phase:
        if name.startswith("workflow_nifi") or name == "workflow_nifi_monitor":
            os.environ["NIFI_HEAL_PHASE"] = phase
        if name.startswith("workflow_kafka") or name == "workflow_kafka_monitor":
            os.environ["KAFKA_HEAL_PHASE"] = phase
        if "cross" in name or "correlate" in name:
            os.environ["CROSS_HEAL_PHASE"] = phase

    try:
        if local:
            extra: list[str] = []
            monitor_like = name in (
                "workflow_nifi_monitor",
                "workflow_kafka_monitor",
                "workflow_cross_stack_heal",
                "workflow_signal_correlate",
            )
            if continuous and monitor_like:
                os.environ["MONITOR_MODE"] = "continuous"
                extra.append("--continuous")
                sec = interval if interval is not None else monitor_interval_sec()
                extra.extend(["--interval", str(sec)])
            elif continuous and not monitor_like:
                typer.echo(
                    f"--continuous is for monitor agents "
                    f"(workflow_nifi_monitor / workflow_kafka_monitor); "
                    f"ignoring for {name!r}",
                    err=True,
                )
            elif interval is not None and monitor_like:
                extra.extend(["--interval", str(interval)])
            result = run_agent_local(name, extra_args=extra or None)
            rc = result.return_code
            typer.echo(f"Run {result.run_id} finished (exit {rc}).")
        else:
            env_extra: dict[str, str] = {}
            if continuous:
                env_extra["MONITOR_MODE"] = "continuous"
                env_extra["NIFI_MONITOR_POLLS"] = "0"
                env_extra["KAFKA_MONITOR_POLLS"] = "0"
                if interval is not None:
                    env_extra["MONITOR_INTERVAL_SEC"] = str(interval)
                else:
                    env_extra.setdefault(
                        "MONITOR_INTERVAL_SEC",
                        str(monitor_interval_sec(DEFAULT_MONITOR_INTERVAL_SEC)),
                    )
            submit = submit_agent_cluster(
                name, profile=active_profile, env_extra=env_extra or None
            )
            rc = submit.return_code
            if rc == 0:
                msg = f"Run {submit.run_id} submitted."
                if submit.flink_job_id:
                    msg += f" Flink job {submit.flink_job_id}"
                if continuous:
                    msg += " (continuous — in-job interval ticks; no publisher needed)"
                typer.echo(msg)
    except (AgentRegistryError, RuntimeError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    if rc != 0:
        raise typer.Exit(rc)


@app.command("submit")
def agent_submit(
    name: str = typer.Argument(..., help="Agent name"),
    profile: str | None = typer.Option(None, "--profile", "-p", help=PROFILE_HELP),
) -> None:
    """Submit an agent job to the Flink cluster."""
    try:
        submit = submit_agent_cluster(name, profile=_resolve_profile(profile))
    except (AgentRegistryError, RuntimeError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    if submit.return_code != 0:
        raise typer.Exit(submit.return_code)
    msg = f"Submitted agent {name!r} — run {submit.run_id}"
    if submit.flink_job_id:
        msg += f", job {submit.flink_job_id}"
    typer.echo(f"{msg}.")


@app.command("status")
def agent_status() -> None:
    """Show Flink jobs from the JobManager REST API."""
    try:
        overview = flink_cluster_submit.fetch_json("/jobs/overview")
    except Exception as exc:
        typer.echo(f"Flink REST unavailable: {exc}", err=True)
        raise typer.Exit(1) from exc
    jobs = overview.get("jobs", [])
    if not jobs:
        typer.echo("No Flink jobs.")
        return
    for job in jobs:
        typer.echo(
            f"{job.get('jid', '?'):32}  {job.get('state', '?'):12}  {job.get('name', '')}"
        )


@app.command("cancel")
def agent_cancel(
    job_id: str = typer.Argument(..., help="Flink job id"),
) -> None:
    """Cancel a Flink job by id."""
    try:
        flink_cluster_submit.cancel_job(job_id)
    except Exception as exc:
        typer.echo(f"Cancel failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Canceled job {job_id}")
