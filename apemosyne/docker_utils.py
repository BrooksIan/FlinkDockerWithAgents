"""Docker and Docker Compose helpers."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Sequence

from apemosyne.constants import DEFAULT_PROFILE, FULL_PROFILE, KAFKA_PROFILE
from apemosyne.paths import project_root

IMAGE_NAME = "agent_flink_image"
IMAGE_TAG = "latest"
COMPOSE_MINIMAL = "docker-compose.yml"
COMPOSE_FULL = "honeypot/docker-compose.yml"
COMPOSE_KAFKA = "docker-compose.kafka.yml"
PYFLINK_PYTHONPATH = (
    "/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
    "/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j"
)


def compose_available() -> bool:
    try:
        _compose_base_cmd()
    except RuntimeError:
        return False
    return True


def compose_file(profile: str = DEFAULT_PROFILE) -> Path:
    root = project_root()
    if profile == KAFKA_PROFILE:
        return root / COMPOSE_KAFKA
    name = COMPOSE_MINIMAL if profile == DEFAULT_PROFILE else COMPOSE_FULL
    return root / name


def _compose_base_cmd() -> List[str]:
    if shutil.which("docker"):
        try:
            subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True,
                check=True,
            )
            return ["docker", "compose"]
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    raise RuntimeError("Neither 'docker compose' nor 'docker-compose' found in PATH")


def compose_cmd(
    *args: str,
    profile: str = DEFAULT_PROFILE,
    cwd: Optional[Path] = None,
) -> List[str]:
    cmd = _compose_base_cmd()
    cmd.extend(["-f", str(compose_file(profile))])
    cmd.extend(args)
    return cmd


def run_compose(
    *args: str,
    profile: str = DEFAULT_PROFILE,
    check: bool = True,
    cwd: Optional[Path] = None,
) -> subprocess.CompletedProcess:
    return run_cmd(compose_cmd(*args, profile=profile), cwd=cwd, check=check)


def run_cmd(
    cmd: Sequence[str],
    *,
    cwd: Optional[Path] = None,
    check: bool = True,
    interactive: bool = False,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(cmd),
        cwd=cwd or project_root(),
        check=check,
        stdin=None if interactive else subprocess.DEVNULL,
    )


def container_id(service: str = "taskmanager", profile: str = DEFAULT_PROFILE) -> Optional[str]:
    result = subprocess.run(
        compose_cmd("ps", "-q", service, profile=profile),
        cwd=project_root(),
        capture_output=True,
        text=True,
    )
    ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return ids[0] if ids else None


def require_container(service: str = "taskmanager", profile: str = DEFAULT_PROFILE) -> str:
    cid = container_id(service, profile=profile)
    if not cid:
        raise RuntimeError(f"Container not running for service: {service}")
    return cid


def docker_cp(local: Path, container: str, remote: str) -> bool:
    remote_parent = str(Path(remote).parent).replace("\\", "/")
    if remote_parent not in (".", "/"):
        subprocess.run(
            ["docker", "exec", "-u", "root", container, "mkdir", "-p", remote_parent],
            cwd=project_root(),
            capture_output=True,
            text=True,
        )
    result = subprocess.run(
        ["docker", "cp", str(local), f"{container}:{remote}"],
        cwd=project_root(),
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and remote_parent not in (".", "/"):
        subprocess.run(
            ["docker", "exec", "-u", "root", container, "chown", "flink:flink", remote],
            cwd=project_root(),
            capture_output=True,
            text=True,
        )
    return result.returncode == 0


def docker_exec(
    container: str,
    command: str,
    *,
    interactive: bool = True,
    workdir: str = "/opt/flink",
) -> int:
    rc, _, _ = docker_exec_output(container, command, interactive=interactive, workdir=workdir)
    return rc


def docker_exec_output(
    container: str,
    command: str,
    *,
    interactive: bool = False,
    workdir: str = "/opt/flink",
) -> tuple[int, str, str]:
    flags = ["-it"] if interactive and sys.stdin.isatty() else []
    cmd = ["docker", "exec", *flags, container, "bash", "-c", command]
    result = subprocess.run(
        cmd,
        cwd=project_root(),
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout or "", result.stderr or ""


def pyflink_python_cmd(script: str) -> str:
    return (
        f"export PYTHONPATH={PYFLINK_PYTHONPATH} && "
        f"python3 {script}"
    )


def image_exists(name: str = IMAGE_NAME, tag: str = IMAGE_TAG) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", f"{name}:{tag}"],
        capture_output=True,
    )
    return result.returncode == 0
