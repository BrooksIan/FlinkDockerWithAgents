"""Flink cluster readiness checks for the control API and dashboard."""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from typing import Any

from ratatoskr.api.config import ApiSettings
from ratatoskr.api import flink_client
from ratatoskr.constants import DEFAULT_PROFILE
from ratatoskr.flink_rest import studio_flink_rest_port
from ratatoskr.docker_utils import (
    IMAGE_NAME,
    IMAGE_TAG,
    compose_available,
    compose_file,
    container_id,
    image_exists,
)


def _check(
    check_id: str,
    label: str,
    status: str,
    detail: str,
    *,
    required: bool = True,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "detail": detail,
        "required": required,
    }


def _container_running(service: str, profile: str) -> tuple[bool, str | None]:
    cid = container_id(service, profile=profile)
    if not cid:
        return False, None
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", cid],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False, cid
    running = result.stdout.strip().lower() == "true"
    return running, cid


def _agents_jars_in_container(container: str) -> bool:
    script = (
        "test -f /opt/flink/pythonpath/agent-site-packages/flink_agents/lib/common/"
        "flink-agents-dist-common-*.jar 2>/dev/null || "
        "ls /opt/flink/pythonpath/agent-site-packages/flink_agents/lib/common/"
        "flink-agents-dist-common-*.jar >/dev/null 2>&1"
    )
    result = subprocess.run(
        ["docker", "exec", container, "bash", "-c", script],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def cluster_readiness(settings: ApiSettings) -> dict[str, Any]:
    """Return Flink cluster status and pass/warn/fail checks for job submission."""
    profile = DEFAULT_PROFILE
    studio_port = studio_flink_rest_port()
    studio_flink_url = f"http://{settings.flink_rest_host}:{studio_port}"
    checks: list[dict[str, Any]] = []

    docker_ok = compose_available()
    checks.append(
        _check(
            "docker_compose",
            "Docker Compose",
            "ok" if docker_ok else "fail",
            "docker compose available" if docker_ok else "Install Docker Desktop or docker compose",
        )
    )

    image_ok = image_exists(IMAGE_NAME, IMAGE_TAG)
    checks.append(
        _check(
            "docker_image",
            "Flink Agents image",
            "ok" if image_ok else "warn",
            f"{IMAGE_NAME}:{IMAGE_TAG} built"
            if image_ok
            else f"Build image: ratatoskr build",
            required=False,
        )
    )

    jm_running, jm_id = _container_running("jobmanager", profile) if docker_ok else (False, None)
    checks.append(
        _check(
            "jobmanager",
            "JobManager container",
            "ok" if jm_running else ("warn" if not docker_ok else "fail"),
            "running" if jm_running else "Start stack: ratatoskr up",
        )
    )

    tm_running, tm_id = _container_running("taskmanager", profile) if docker_ok else (False, None)
    checks.append(
        _check(
            "taskmanager",
            "TaskManager container",
            "ok" if tm_running else ("warn" if not docker_ok else "fail"),
            "running" if tm_running else "TaskManagers execute jobs — run: ratatoskr up",
        )
    )

    flink_block: dict[str, Any] = {
        "reachable": False,
        "url": studio_flink_url,
    }
    flink_up = False
    slots_total = 0
    slots_free = 0
    try:
        overview = flink_client.cluster_overview(rest_port=studio_port)
        tm = flink_client.taskmanager_summary(rest_port=studio_port)
        flink_up = True
        slots_total = int(tm["slots_total"])
        slots_free = int(tm["slots_free"])
        flink_block.update(
            {
                "reachable": True,
                "flink_version": overview.get("flink-version"),
                "taskmanagers": tm["count"],
                "slots_total": slots_total,
                "slots_free": slots_free,
                "jobs_running": overview.get("jobs-running"),
                "jobs_finished": overview.get("jobs-finished"),
                "jobs_failed": overview.get("jobs-failed"),
            }
        )
        checks.append(
            _check(
                "flink_rest",
                "Flink REST API",
                "ok",
                f"reachable at {studio_flink_url}",
            )
        )
        if tm["count"] == 0:
            checks.append(
                _check(
                    "taskmanager_registered",
                    "TaskManagers registered",
                    "fail",
                    "No taskmanagers connected to JobManager",
                )
            )
        else:
            checks.append(
                _check(
                    "taskmanager_registered",
                    "TaskManagers registered",
                    "ok",
                    f"{tm['count']} taskmanager(s) connected",
                )
            )
        if slots_total <= 0:
            checks.append(
                _check(
                    "task_slots",
                    "Task slots available",
                    "fail",
                    "No task slots reported by Flink",
                )
            )
        elif slots_free <= 0:
            checks.append(
                _check(
                    "task_slots",
                    "Task slots available",
                    "warn",
                    f"0 free / {slots_total} total — wait for running jobs to finish",
                )
            )
        else:
            checks.append(
                _check(
                    "task_slots",
                    "Task slots available",
                    "ok",
                    f"{slots_free} free / {slots_total} total",
                )
            )
    except flink_client.FlinkUnavailableError as exc:
        flink_block["error"] = str(exc)
        level = "warn" if docker_ok and not jm_running else "fail"
        checks.append(
            _check(
                "flink_rest",
                "Flink REST API",
                level,
                str(exc) if str(exc) else f"Cannot reach {studio_flink_url}",
            )
        )

    if jm_running and jm_id:
        jars_ok = _agents_jars_in_container(jm_id)
        checks.append(
            _check(
                "flink_agents_jars",
                "Flink Agents JARs (JobManager)",
                "ok" if jars_ok else "warn",
                "dist JARs present in container"
                if jars_ok
                else "Submit once or run ratatoskr test launch --cluster to bootstrap",
                required=False,
            )
        )

    required_failed = any(
        c["required"] and c["status"] == "fail" for c in checks
    )
    ready = (
        flink_up
        and jm_running
        and tm_running
        and slots_total > 0
        and slots_free > 0
        and not required_failed
    )

    return {
        "ready": ready,
        "profile": profile,
        "compose_file": compose_file(profile).name,
        "flink_rest_url": studio_flink_url,
        "flink": flink_block,
        "containers": {
            "jobmanager": {"running": jm_running, "id": jm_id},
            "taskmanager": {"running": tm_running, "id": tm_id},
        },
        "image": {"name": IMAGE_NAME, "tag": IMAGE_TAG, "exists": image_ok},
        "checks": checks,
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }
