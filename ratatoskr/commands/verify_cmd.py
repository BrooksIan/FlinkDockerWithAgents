"""Run verification tiers defined in manifests/verify-tiers.yaml."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, List, Mapping, Optional

import typer

from ratatoskr.constants import DEFAULT_PROFILE, VERIFY_TIERS, normalize_verify_profile
from ratatoskr.docker_utils import IMAGE_NAME, IMAGE_TAG, project_root
from ratatoskr.manifests import VerifyStep, get_verify_tier, load_verify_tiers
from ratatoskr.paths import runtime_src_paths

app = typer.Typer(help="Run verification tiers (quick, standard, full, nightly).")

DOCKER_STEP_TYPES = frozenset({"docker", "docker_run"})
SKIPPED_WITHOUT_DOCKER = DOCKER_STEP_TYPES | frozenset({"cli"})

TIER_ALIASES = {
    "launch": "quick",
    "sprint-c": "standard",
}


def _expand(value: str) -> str:
    pattern = re.compile(r"\$\{([^}:]+)(?::-([^}]*))?\}")

    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        default = match.group(2) or ""
        return os.environ.get(key, default)

    return pattern.sub(repl, value)


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _run(
    cmd: List[str],
    cwd: Path,
    env: Optional[Mapping[str, str]] = None,
) -> int:
    typer.echo(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    return result.returncode


def _step_label(step: VerifyStep, index: int) -> str:
    description = step.options.get("description")
    if description:
        return f"[{index}] {step.type}: {description}"
    if step.type == "python":
        paths = step.options.get("paths") or []
        return f"[{index}] python ({len(paths)} files)"
    if step.type == "shell":
        script = step.options.get("script", "")
        return f"[{index}] shell {script}"
    if step.type == "cli":
        command = step.options.get("command", "")
        args = step.options.get("args") or []
        return f"[{index}] cli {command} {' '.join(str(a) for a in args)}".strip()
    return f"[{index}] {step.type}"


def _resolve_image(options: Mapping[str, Any]) -> str:
    raw = str(options.get("image") or f"{IMAGE_NAME}:{IMAGE_TAG}")
    return _expand(raw)


def _run_python_step(step: VerifyStep, root: Path) -> int:
    paths = step.options.get("paths")
    if not isinstance(paths, list):
        typer.echo("python step requires a list of paths", err=True)
        return 1

    env = os.environ.copy()
    path_parts = [str(p) for p in runtime_src_paths(root)]
    path_parts.append(str(root))
    existing = env.get("PYTHONPATH", "")
    if existing:
        path_parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(path_parts)

    rc = 0
    for rel in paths:
        path = root / str(rel)
        if not path.is_file():
            typer.echo(f"  missing test file: {path}", err=True)
            return 1
        typer.echo(f"  >> {Path(sys.executable).name} {rel}")
        result = subprocess.run([sys.executable, str(path)], cwd=root, env=env)
        if result.returncode != 0:
            rc = result.returncode
    return rc


def _run_shell_step(step: VerifyStep, root: Path) -> int:
    script = step.options.get("script")
    if not script:
        typer.echo("shell step requires script", err=True)
        return 1

    script_path = root / str(script)
    if not script_path.is_file():
        typer.echo(f"  missing script: {script_path}", err=True)
        return 1

    extra_globs = step.options.get("extra_globs") or []
    cmd = ["bash", str(script_path)]
    if isinstance(extra_globs, list):
        cmd.extend(str(item) for item in extra_globs)
    return _run(cmd, root)


def _run_grep_present(step: VerifyStep, root: Path) -> int:
    patterns = step.options.get("patterns") or []
    if not isinstance(patterns, list):
        return 1

    for item in patterns:
        if not isinstance(item, Mapping):
            continue
        pattern = str(item.get("pattern", ""))
        rel = str(item.get("path", ""))
        path = root / rel
        if not path.is_file():
            typer.echo(f"  missing file for grep_present: {rel}", err=True)
            return 1
        text = path.read_text(encoding="utf-8")
        if pattern not in text:
            typer.echo(
                f"  FAIL grep_present: {pattern!r} not in {rel}",
                err=True,
            )
            return 1
        typer.echo(f"  OK  grep_present {pattern!r} in {rel}")
    return 0


def _run_grep_absent(step: VerifyStep, root: Path) -> int:
    paths = step.options.get("paths") or []
    pattern = str(step.options.get("pattern", ""))
    if not pattern or not isinstance(paths, list):
        return 1

    for rel in paths:
        path = root / str(rel)
        if not path.is_file():
            typer.echo(f"  missing file for grep_absent: {rel}", err=True)
            return 1
        text = path.read_text(encoding="utf-8")
        if re.search(pattern, text):
            typer.echo(
                f"  FAIL grep_absent: {pattern!r} found in {rel}",
                err=True,
            )
            return 1
        typer.echo(f"  OK  grep_absent {pattern!r} in {rel}")
    return 0


def _run_file_exists(step: VerifyStep, root: Path) -> int:
    paths = step.options.get("paths") or []
    if not isinstance(paths, list):
        return 1

    missing = [str(rel) for rel in paths if not (root / str(rel)).is_file()]
    if missing:
        typer.echo("  FAIL file_exists — missing deliverables:", err=True)
        for rel in missing:
            typer.echo(f"    - {rel}", err=True)
        return 1

    for rel in paths:
        typer.echo(f"  OK  exists {rel}")
    return 0


def _docker_run_cmd(step: VerifyStep, root: Path) -> List[str]:
    options = step.options
    inline_check = options.get("inline_check")
    if inline_check == "durable_api_d0":
        script = root / "scripts/checks/durable_api_d0.sh"
        if not script.is_file():
            typer.echo(f"  missing check script: {script}", err=True)
            return []
        return ["bash", str(script)]

    image = _resolve_image(options)
    cmd: List[str] = ["docker", "run", "--rm"]

    entrypoint = options.get("entrypoint")
    if entrypoint:
        cmd.extend(["--entrypoint", str(entrypoint)])

    env_map = options.get("env") or {}
    if isinstance(env_map, Mapping):
        for key, value in env_map.items():
            cmd.extend(["-e", f"{key}={_expand(str(value))}"])

    if options.get("mount_project"):
        cmd.extend(
            [
                "-v",
                f"{root}:/opt/flink/src:ro",
                "-w",
                str(options.get("workdir", "/opt/flink/src")),
            ]
        )
    else:
        manifest_name = options.get("mounts_manifest")
        if manifest_name:
            from ratatoskr.manifests import load_manifest

            manifest = load_manifest(str(manifest_name), validate=True)
            for item in manifest.files:
                cmd.extend(
                    ["-v", f"{item.local}:{item.remote}:ro"]
                )

    cmd.append(image)
    command = str(options.get("command", ""))
    if command:
        cmd.extend(command.split())
    return cmd


def _run_docker_image_step(step: VerifyStep) -> int:
    image = _resolve_image(step.options)
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
    )
    if result.returncode != 0:
        typer.echo(f"  FAIL docker image not found: {image}", err=True)
        return 1
    typer.echo(f"  OK  docker image {image}")
    return 0


def _run_docker_step(step: VerifyStep, root: Path) -> int:
    cmd = _docker_run_cmd(step, root)
    if not cmd:
        return 1
    return _run(cmd, root)


def _run_cli_step(step: VerifyStep, root: Path) -> int:
    command = str(step.options.get("command", ""))
    args = step.options.get("args") or []
    profile = step.options.get("profile")

    env = os.environ.copy()
    env_map = step.options.get("env") or {}
    if isinstance(env_map, Mapping):
        env.update({str(k): _expand(str(v)) for k, v in env_map.items()})

    if command == "validate":
        cli_args = [sys.executable, "-m", "ratatoskr.main", "config", "validate"]
        if profile:
            cli_args.extend(["--profile", str(profile)])
        else:
            cli_args.extend(["--profile", DEFAULT_PROFILE])
        return _run(cli_args, root, env=env)

    base = [sys.executable, "-m", "ratatoskr.main", command]
    if isinstance(args, list):
        base.extend(str(arg) for arg in args)
    if profile and "--profile" not in base and "-p" not in base:
        base.extend(["--profile", str(profile)])
    return _run(base, root, env=env)


def _execute_step(step: VerifyStep, root: Path, *, skip_docker: bool) -> int:
    if skip_docker and step.type in SKIPPED_WITHOUT_DOCKER:
        if step.type == "cli":
            args = step.options.get("args") or []
            command = step.options.get("command")
            if command == "test":
                typer.echo("  skip cli test step (--skip-docker)")
                return 0
        if step.type in DOCKER_STEP_TYPES:
            typer.echo("  skip docker step (--skip-docker)")
            return 0

    if step.type == "python":
        return _run_python_step(step, root)
    if step.type == "shell":
        return _run_shell_step(step, root)
    if step.type == "grep_present":
        return _run_grep_present(step, root)
    if step.type == "grep_absent":
        return _run_grep_absent(step, root)
    if step.type == "file_exists":
        return _run_file_exists(step, root)
    if step.type == "docker":
        if step.options.get("check") == "image":
            if not _docker_available():
                typer.echo("  FAIL docker not available", err=True)
                return 1
            return _run_docker_image_step(step)
        typer.echo(f"  unsupported docker check: {step.options.get('check')}", err=True)
        return 1
    if step.type == "docker_run":
        if not _docker_available():
            typer.echo("  FAIL docker not available", err=True)
            return 1
        return _run_docker_step(step, root)
    if step.type == "cli":
        return _run_cli_step(step, root)

    typer.echo(f"  unsupported step type: {step.type}", err=True)
    return 1


def _run_tier(
    tier_name: str,
    *,
    skip_docker: bool = False,
    list_only: bool = False,
    profile: Optional[str] = None,
) -> int:
    tier = get_verify_tier(tier_name, profile=profile)
    header = f"=== verify tier: {tier_name} ({len(tier.steps)} steps)"
    if profile:
        header += f", profile={profile}"
    typer.echo(f"{header} ===")

    if list_only:
        for index, step in enumerate(tier.steps, start=1):
            typer.echo(_step_label(step, index))
        return 0

    if (
        not skip_docker
        and any(s.type in DOCKER_STEP_TYPES for s in tier.steps)
        and not _docker_available()
    ):
        typer.echo("Docker is not running but this tier requires Docker.", err=True)
        typer.echo("Use --skip-docker to run local steps only.", err=True)
        return 1

    failures = 0
    for index, step in enumerate(tier.steps, start=1):
        typer.echo(_step_label(step, index))
        rc = _execute_step(step, project_root(), skip_docker=skip_docker)
        if rc != 0:
            typer.echo(
                f"  FAIL step {index} ({step.type}) exit={rc}",
                err=True,
            )
            failures += 1
            break
        typer.echo(f"  PASS step {index}")

    if failures:
        typer.echo(f"=== verify tier {tier_name} FAIL ===", err=True)
        return 1

    typer.echo(f"=== verify tier {tier_name} PASS ===")
    if tier_name == "full":
        typer.echo(
            "Cluster e2e still required before sign-off (flags off):\n"
            "  COWRIE_DURABLE_TOOLS=0 ratatoskr verify --tier nightly --profile honeypot"
        )
    return 0


@app.callback(invoke_without_command=True)
def verify(
    ctx: typer.Context,
    tier: str = typer.Option(
        "quick",
        "--tier",
        "-t",
        help=f"Verification tier: {', '.join(VERIFY_TIERS)} (also: launch, sprint-c)",
    ),
    skip_docker: bool = typer.Option(
        False,
        "--skip-docker",
        help="Skip Docker and cluster e2e steps",
    ),
    list_steps: bool = typer.Option(
        False,
        "--list",
        help="List steps for the tier and exit",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Merge honeypot verify steps (honeypot, full, cowrie)",
    ),
) -> None:
    """Run verification tiers defined in manifests/verify-tiers.yaml."""
    if ctx.invoked_subcommand is not None:
        return

    verify_profile = normalize_verify_profile(profile)
    tier = TIER_ALIASES.get(tier, tier)
    known = set(load_verify_tiers(profile=verify_profile).keys())
    if tier not in known:
        typer.echo(
            f"Unknown tier {tier!r}. Known: {', '.join(sorted(known))}",
            err=True,
        )
        raise typer.Exit(1)

    rc = _run_tier(
        tier,
        skip_docker=skip_docker,
        list_only=list_steps,
        profile=verify_profile,
    )
    raise typer.Exit(rc)
