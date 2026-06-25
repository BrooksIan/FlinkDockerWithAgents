"""Main CLI entry point for Apemosyne."""

from __future__ import annotations

import typer

from apemosyne import __version__
from apemosyne.commands import agent_cmd, api_cmd, build, doctor_platform, process, stack, test_cmd, verify_cmd
from apemosyne.commands.build import build_image
from apemosyne.constants import DEFAULT_PROFILE, PROFILE_HELP, STARTUP_MODE_HELP
from apemosyne.startup_modes import resolve_up_options

app = typer.Typer(
    name="apemosyne",
    help="CLI for Apache Flink Agents — build, run, and verify agent workflows on Docker.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"apemosyne {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """Apemosyne — Flink Agents control plane."""


@app.command("build")
def cli_build(
    git_ref: str = typer.Argument("stable", help="Flink Agents git ref"),
) -> None:
    """Build agent_flink_image from the workspace Dockerfile."""
    build_image(git_ref)


@app.command("up")
def cli_up(
    mode: str = typer.Option(
        "",
        "--mode",
        "-m",
        help=STARTUP_MODE_HELP,
    ),
    profile: str = typer.Option(
        DEFAULT_PROFILE,
        "--profile",
        "-p",
        help=PROFILE_HELP,
    ),
    build_image_flag: bool = typer.Option(False, "--build-image"),
    wait: int = typer.Option(10, "--wait"),
    ensure_kafka: bool = typer.Option(True, "--ensure-kafka-topics/--no-ensure-kafka-topics"),
    ensure_flink: bool = typer.Option(True, "--ensure-flink-jobs/--no-ensure-flink-jobs"),
) -> None:
    """Start the Docker Compose stack."""
    opts = resolve_up_options(mode=mode or None, profile=profile)
    stack.up(
        profile=opts.profile,
        build_image=build_image_flag or opts.build_image,
        wait=opts.wait if wait == 10 else wait,
        ensure_kafka=ensure_kafka and opts.ensure_kafka,
        ensure_flink=ensure_flink and opts.ensure_flink,
    )


app.add_typer(stack.app, name="stack")
app.add_typer(test_cmd.app, name="test")
app.add_typer(verify_cmd.app, name="verify")
app.add_typer(process.app, name="process")
app.add_typer(agent_cmd.app, name="agent")
app.add_typer(api_cmd.app, name="api")
app.add_typer(doctor_platform.app, name="doctor-platform")

# Optional bytecode-backed commands (demo, doctor, dashboard, …)
try:
    from apemosyne._bootstrap import install_aliases

    install_aliases()
    from apemosyne.commands import (
        config_cmd,
        dashboard,
        demo,
        demo_ready_cmd,
        modes_cmd,
        sync,
        sync_env_cmd,
        utils,
    )

    app.command("config")(config_cmd.config)
    app.command("dashboard")(dashboard.dashboard)
    app.command("demo")(demo.demo)
    app.command("demo-ready")(demo_ready_cmd.demo_ready)
    app.command("modes")(modes_cmd.modes)
    app.command("sync")(sync.sync)
    app.command("sync-env")(sync_env_cmd.sync_env)
    app.add_typer(utils.app, name="utils")
except Exception:
    pass

# Flatten common stack commands at top level (bytecode CLI compatibility).
app.command("down")(stack.down)
app.command("status")(stack.status)
app.command("logs")(stack.logs)
app.command("ensure-kafka-topics")(stack.ensure_kafka_topics_cmd)
app.command("ensure-flink-jobs")(stack.ensure_flink_jobs_cmd)
app.command("doctor")(doctor_platform.doctor_platform)
