"""Flink Agents lifecycle commands."""

from __future__ import annotations

import json

import typer

from apemosyne.agents.registry import AgentRegistryError, list_agent_names, load_agent_registry
from apemosyne.agents.submit import describe_agent, run_agent_local, submit_agent_cluster
from apemosyne.constants import DEFAULT_PROFILE, PROFILE_HELP
from apemosyne.runtime import flink_cluster_submit

app = typer.Typer(help="Manage Flink Agents (list, run, submit, status).")


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
    profile: str = typer.Option(DEFAULT_PROFILE, "--profile", "-p", help=PROFILE_HELP),
) -> None:
    """Run an agent locally or submit its cluster job."""
    try:
        rc = (
            run_agent_local(name)
            if local
            else submit_agent_cluster(name, profile=profile)
        )
    except (AgentRegistryError, RuntimeError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    if rc != 0:
        raise typer.Exit(rc)


@app.command("submit")
def agent_submit(
    name: str = typer.Argument(..., help="Agent name"),
    profile: str = typer.Option(DEFAULT_PROFILE, "--profile", "-p", help=PROFILE_HELP),
) -> None:
    """Submit an agent job to the Flink cluster."""
    try:
        rc = submit_agent_cluster(name, profile=profile)
    except (AgentRegistryError, RuntimeError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    if rc != 0:
        raise typer.Exit(rc)
    typer.echo(f"Submitted agent {name!r}.")


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
