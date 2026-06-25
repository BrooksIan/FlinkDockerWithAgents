"""Build and smoke-test Flink Agents."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import typer

from apemosyne.commands.build import build_image
from apemosyne.constants import DEFAULT_PROFILE, FULL_PROFILE, PROFILE_HELP
from apemosyne.docker_utils import (
    IMAGE_NAME,
    IMAGE_TAG,
    compose_cmd,
    container_id,
    docker_exec,
    project_root,
)
from apemosyne.paths import honeypot_available, honeypot_module_rel, project_root

app = typer.Typer(help="Build and smoke-test Flink Agents.")

DEFAULT_IMAGE = f"{IMAGE_NAME}:{IMAGE_TAG}"
LAUNCH_TEST = "test/test_launch_flink_agents.py"
CONTAINER_TEST_PATH = "/opt/flink/test_launch_flink_agents.py"
CONTAINER_PRODUCTION_TEST = "/opt/flink/test_production_pipeline.py"


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _generic_validate_paths(root: Path) -> list[str]:
    return [
        "Dockerfile",
        LAUNCH_TEST,
        "examples/demo_datastream_local.py",
        "apemosyne/runtime/cluster_launch_test.py",
        "apemosyne/runtime/flink_cluster_submit.py",
        "apemosyne/manifests/test-launch.yaml",
        "examples/agents/agent-manifest.yaml",
        "examples/agents/workflow_counter/agent.yaml",
        "examples/agents/workflow_counter_actions.py",
        "apemosyne/api/app.py",
        "dashboard/package.json",
        "pyproject.toml",
    ]


def _honeypot_validate_paths(root: Path) -> list[str]:
    if not honeypot_available(root):
        return []
    return [
        "test/test_production_pipeline.py",
        honeypot_module_rel("cowrie_kafka_normalize_job.py", root),
        honeypot_module_rel("cowrie_normalize.py", root),
        honeypot_module_rel("flink_cluster_submit.py", root),
        honeypot_module_rel("cowrie_phase2_workflow_job.py", root),
        honeypot_module_rel("cowrie_phase2_agent.py", root),
        honeypot_module_rel("cowrie_workflow_detect.py", root),
        honeypot_module_rel("cowrie_phase3_react_augmentor.py", root),
        honeypot_module_rel("cowrie_pipeline.py", root),
        honeypot_module_rel("cowrie_log_processor.py", root),
        "honeypot/manifests/test-production.yaml",
    ]


def _required_validate_paths(root: Path, *, profile: str = DEFAULT_PROFILE) -> list[str]:
    paths = list(_generic_validate_paths(root))
    if profile == FULL_PROFILE:
        paths.extend(_honeypot_validate_paths(root))
    return paths


def _run_launch_test_in_image(
    image: str = DEFAULT_IMAGE,
    extra_args: Optional[List[str]] = None,
) -> int:
    root = project_root()
    test_file = root / LAUNCH_TEST
    if not test_file.is_file():
        typer.echo(f"Test file not found: {test_file}", err=True)
        raise typer.Exit(1)

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{test_file}:{CONTAINER_TEST_PATH}:ro",
        image,
        "python3",
        CONTAINER_TEST_PATH,
    ]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, cwd=root).returncode


def _bootstrap_cluster_containers(profile: str = DEFAULT_PROFILE) -> None:
    bootstrap_cmd = (
        "cd /opt/flink && "
        "export PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
        "/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-src.zip && "
        "python3 -c 'from apemosyne.runtime.cluster_launch_test import bootstrap_runtime; "
        "bootstrap_runtime()'"
    )
    for service in ("jobmanager", "taskmanager"):
        cid = container_id(service, profile=profile)
        if cid:
            docker_exec(cid, bootstrap_cmd, interactive=False)


def _run_launch_test_in_container(
    extra_args: Optional[List[str]] = None,
    profile: str = DEFAULT_PROFILE,
) -> int:
    from apemosyne.copy_manifest import copy_manifest_to_cluster

    cluster_requested = bool(extra_args and "--cluster" in extra_args)
    service = "jobmanager" if cluster_requested else "taskmanager"
    cid = container_id(service, profile=profile)
    if not cid:
        typer.echo(
            f"{service} not running. Start the stack first:\n"
            f"  apemosyne up --profile {profile}",
            err=True,
        )
        raise typer.Exit(1)

    root = project_root()
    test_file = root / LAUNCH_TEST
    if not test_file.is_file():
        typer.echo(f"Test file not found: {test_file}", err=True)
        raise typer.Exit(1)

    copy_manifest_to_cluster("test-launch", profile)

    if cluster_requested:
        typer.echo("Bootstrapping Flink cluster runtime (JARs + python)...")
        _bootstrap_cluster_containers(profile)
        typer.echo("Submitting Flink Agents job to JobManager (visible in Web UI)...")

    args = " ".join(extra_args or [])
    exports = (
        "export FLINK_REST_ADDRESS=localhost FLINK_REST_PORT=8081 && "
        if cluster_requested
        else ""
    )
    command = (
        "cd /opt/flink && "
        f"{exports}"
        "export PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
        "/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-src.zip && "
        f"python3 test_launch_flink_agents.py {args}"
    ).strip()
    return docker_exec(cid, command, interactive=False)


@app.command("launch")
def test_launch(
    build: bool = typer.Option(False, "--build", help="Build image before testing"),
    version: str = typer.Option(
        "main",
        "--version",
        help="Flink Agents git ref for --build",
    ),
    image: str = typer.Option(DEFAULT_IMAGE, "--image", help="Docker image to test"),
    cluster: bool = typer.Option(
        False,
        "--cluster",
        help="Also submit a PyFlink job (requires running compose stack)",
    ),
    in_container: bool = typer.Option(
        False,
        "--in-container",
        help="Run inside TaskManager instead of docker run",
    ),
    profile: str = typer.Option(
        DEFAULT_PROFILE,
        "--profile",
        "-p",
        help=PROFILE_HELP,
    ),
) -> None:
    """Build (optional) and verify Flink Agents launch and execute."""
    if not _docker_available():
        typer.echo("Docker is not running.", err=True)
        raise typer.Exit(1)

    if build:
        typer.echo("Building Docker image...")
        build_image(version if version != "main" else "stable")

    if not build:
        inspect = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
        )
        if inspect.returncode != 0:
            typer.echo(
                f"Image {image} not found. Build first:\n"
                "  apemosyne test launch --build",
                err=True,
            )
            raise typer.Exit(1)

    extra = ["--cluster"] if cluster else []
    typer.echo("Running Flink Agents launch test...")

    if cluster or in_container:
        if not container_id("taskmanager", profile=profile):
            typer.echo(f"Starting {profile} stack for cluster test...")
            subprocess.run(
                compose_cmd("up", "-d", profile=profile),
                cwd=project_root(),
                check=True,
            )
        rc = _run_launch_test_in_container(extra, profile=profile)
    else:
        rc = _run_launch_test_in_image(image, extra)

    if rc != 0:
        typer.echo("Launch test failed.", err=True)
        raise typer.Exit(rc)
    typer.echo("Launch test passed.")


@app.command("validate")
def test_validate(
    profile: str = typer.Option(
        DEFAULT_PROFILE,
        "--profile",
        "-p",
        help="Include honeypot paths when 'full' or when honeypot/ exists",
    ),
) -> None:
    """Validate Dockerfile and required workspace files (no Docker build)."""
    root = project_root()
    required = _required_validate_paths(root, profile=profile)
    missing = [rel for rel in required if not (root / rel).is_file()]

    typer.echo("Validating project files...")
    for rel in required:
        mark = "MISSING" if rel in missing else "OK"
        typer.echo(f"  [{mark}] {rel}")

    if missing:
        typer.echo(f"{len(missing)} required file(s) missing.", err=True)
        raise typer.Exit(1)

    if _docker_available():
        typer.echo("Docker is running.")
    else:
        typer.echo("Docker is not running (optional for validate).")
    typer.echo("Validation passed.")


@app.command("local")
def test_local(
    cluster: bool = typer.Option(
        False,
        "--cluster",
        help="Pass --cluster to the launch smoke script",
    ),
) -> None:
    """Run the launch test on the host (requires flink_agents on PYTHONPATH)."""
    root = project_root()
    test_file = root / LAUNCH_TEST
    args = [sys.executable, str(test_file)]
    if cluster:
        args.append("--cluster")
    rc = subprocess.run(args, cwd=root).returncode
    raise typer.Exit(rc)


def _load_legacy_test_cmd():
    import marshal
    import types

    qualified = "apemosyne.commands._test_cmd_legacy"
    if qualified in sys.modules:
        return sys.modules[qualified]

    pyc = Path(__file__).parent / "_legacy_test_cmd.pyc"
    code = marshal.loads(pyc.read_bytes()[16:])
    mod = types.ModuleType(qualified)
    mod.__file__ = str(pyc)
    sys.modules[qualified] = mod
    sys.modules["flink_cowrie.commands._test_cmd_legacy"] = mod
    exec(code, mod.__dict__)  # noqa: S102
    return mod


def _patch_legacy_paths(mod) -> None:
    root = project_root()
    rel = lambda name: honeypot_module_rel(name, root)  # noqa: E731

    mod.LAUNCH_TEST = LAUNCH_TEST
    mod.CONTAINER_TEST_PATH = CONTAINER_TEST_PATH
    mod.CONTAINER_PRODUCTION_TEST = CONTAINER_PRODUCTION_TEST
    mod.PRODUCTION_TEST = "test/test_production_pipeline.py"
    mod.PHASE1_JOB = rel("cowrie_kafka_normalize_job.py")
    mod.PHASE1_NORMALIZE = rel("cowrie_normalize.py")
    mod.FLINK_SUBMIT = rel("flink_cluster_submit.py")
    mod.PHASE2_JOB = rel("cowrie_phase2_workflow_job.py")
    mod.PHASE2_AGENT = rel("cowrie_phase2_agent.py")
    mod.PHASE2_DETECT = rel("cowrie_workflow_detect.py")
    mod.PHASE3_AUGMENTOR = rel("cowrie_phase3_react_augmentor.py")
    mod.COWRIE_PIPELINE = rel("cowrie_pipeline.py")
    mod.PHASE2_DEMO = "honeypot/demo/demo_cowrie_response.py"
    mod.PHASE1_TEST = "test/test_production_pipeline.py"
    mod.PHASE2_TEST = "test/test_production_pipeline.py"
    mod.PHASE3_TEST = "test/test_production_pipeline.py"
    mod.CONTAINER_PHASE1_TEST = CONTAINER_PRODUCTION_TEST
    mod.CONTAINER_PHASE2_TEST = CONTAINER_PRODUCTION_TEST
    mod.CONTAINER_PHASE3_TEST = CONTAINER_PRODUCTION_TEST


_LEGACY_REGISTERED = False


def register_legacy_commands() -> None:
    """Attach honeypot cluster/e2e test commands when the subproject is present."""
    global _LEGACY_REGISTERED
    if _LEGACY_REGISTERED:
        return
    if not honeypot_available():
        return
    _LEGACY_REGISTERED = True

    legacy = _load_legacy_test_cmd()
    _patch_legacy_paths(legacy)
    app.command("phase1")(legacy.test_phase1)
    app.command("phase2")(legacy.test_phase2)
    app.command("actor-classify")(legacy.test_actor_classify)
    app.command("phase3")(legacy.test_phase3)
    app.command("react")(legacy.test_react)
    app.command("production")(legacy.test_production)
