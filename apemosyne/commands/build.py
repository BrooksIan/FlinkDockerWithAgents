"""Docker image build command."""

from __future__ import annotations

import subprocess

import typer

from apemosyne.docker_utils import IMAGE_NAME, run_cmd
from apemosyne.paths import project_root

app = typer.Typer(help="Build the Flink Agents Docker image.")


def build_image(version: str = "stable") -> None:
    """Build ``agent_flink_image`` with the given Flink Agents version."""
    if version == "stable":
        version = "release-0.3"

    root = project_root()
    typer.echo("=" * 80)
    typer.echo("Building Flink Agents Docker Image")
    typer.echo("=" * 80)
    typer.echo(f"Flink Agents Version: {version}")
    typer.echo(f"Project root: {root}")
    typer.echo("")

    cmd = [
        "docker",
        "build",
        "--build-arg",
        f"FLINK_AGENTS_VERSION={version}",
        "--tag",
        f"{IMAGE_NAME}:latest",
        "--tag",
        f"{IMAGE_NAME}:{version}",
        ".",
    ]
    try:
        run_cmd(cmd, cwd=root, check=True)
    except subprocess.CalledProcessError:
        typer.echo("Build failed.", err=True)
        raise typer.Exit(1)

    typer.echo("")
    typer.echo("Build successful.")
    typer.echo(f"  - {IMAGE_NAME}:latest")
    typer.echo(f"  - {IMAGE_NAME}:{version}")


@app.callback(invoke_without_command=True)
def build(
    ctx: typer.Context,
    version: str = typer.Argument(
        "stable",
        help="Flink Agents git ref (main, stable→release-0.3, release-0.2, ...)",
    ),
) -> None:
    """Build the Docker image."""
    if ctx.invoked_subcommand is not None:
        return
    build_image(version)
