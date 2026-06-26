"""Sync Studio runtime code into minimal Flink JM/TM and wait for readiness."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from apemosyne.constants import DEFAULT_PROFILE
from apemosyne.copy_manifest import CopyStats, copy_pairs_to_cluster
from apemosyne.docker_utils import project_root
from apemosyne.flink_rest import studio_flink_rest_port
from apemosyne.runtime import flink_cluster_submit


def studio_cluster_copy_pairs(*, root: Path | None = None) -> list[tuple[str, str]]:
    """Files required for Studio pipeline cluster submit and smoke jobs."""
    repo = root or project_root()
    pairs: list[tuple[str, str]] = []

    def add(rel: str) -> None:
        local = repo / rel
        if local.is_file():
            pairs.append((str(local), f"/opt/flink/{rel}"))

    for rel in (
        "apemosyne/__init__.py",
        "apemosyne/constants.py",
        "apemosyne/flink_rest.py",
        "apemosyne/paths.py",
        "apemosyne/env.py",
        "apemosyne/manifests.py",
        "apemosyne/copy_manifest.py",
        "apemosyne/docker_utils.py",
        "apemosyne/kafka_sources.py",
        "apemosyne/agents/__init__.py",
        "apemosyne/agents/registry.py",
        "apemosyne/agents/published_copy.py",
        "apemosyne/agents/submit.py",
        "apemosyne/pipelines/__init__.py",
        "apemosyne/pipelines/models.py",
        "apemosyne/pipelines/validate.py",
        "apemosyne/pipelines/validate_cluster.py",
        "apemosyne/pipelines/executor.py",
        "apemosyne/pipelines/cluster_codegen.py",
        "apemosyne/pipelines/cluster_submit.py",
        "apemosyne/pipelines/generated/__init__.py",
        "apemosyne/pipelines/cluster_kafka_sink.py",
        "apemosyne/pipelines/docker_runner.py",
        "apemosyne/pipelines/container_run.py",
        "apemosyne/runs/__init__.py",
        "apemosyne/runs/models.py",
        "apemosyne/runs/plan.py",
        "apemosyne/runs/store.py",
        "apemosyne/runs/service.py",
        "apemosyne/designer/__init__.py",
        "apemosyne/designer/models.py",
        "apemosyne/designer/store.py",
        "apemosyne/designer/llm_settings.py",
        "apemosyne/designer/llm_client.py",
        "apemosyne/designer/runtime_env.py",
        "apemosyne/runtime/__init__.py",
        "apemosyne/runtime/flink_cluster_submit.py",
        "apemosyne/runtime/flink_agents_bootstrap.py",
        "apemosyne/runtime/cluster_launch_test.py",
        "apemosyne/runtime/cluster_launch_agent.py",
        "apemosyne/runtime/studio_cluster_sync.py",
        "test/test_launch_flink_agents.py",
        "examples/agents/__init__.py",
    ):
        add(rel)

    agents_root = repo / "examples" / "agents"
    if agents_root.is_dir():
        for path in agents_root.rglob("*.py"):
            rel = path.relative_to(repo).as_posix()
            pairs.append((str(path), f"/opt/flink/{rel}"))
        for path in agents_root.rglob("*.yaml"):
            rel = path.relative_to(repo).as_posix()
            pairs.append((str(path), f"/opt/flink/{rel}"))

    examples_init = repo / "examples" / "__init__.py"
    if examples_init.is_file():
        pairs.append((str(examples_init), "/opt/flink/examples/__init__.py"))
    elif (agents_root / "__init__.py").is_file():
        pairs.append((str(agents_root / "__init__.py"), "/opt/flink/examples/__init__.py"))

    pipelines_root = repo / ".apemosyne" / "pipelines"
    if pipelines_root.is_dir():
        for path in pipelines_root.rglob("run_cluster.py"):
            rel = path.relative_to(repo).as_posix()
            pairs.append((str(path), f"/opt/flink/{rel}"))

    generated_root = repo / "apemosyne" / "pipelines" / "generated"
    if generated_root.is_dir():
        for path in generated_root.rglob("*.py"):
            rel = path.relative_to(repo).as_posix()
            pairs.append((str(path), f"/opt/flink/{rel}"))

    agents_pub = repo / ".apemosyne" / "agents"
    if agents_pub.is_dir():
        for path in agents_pub.rglob("*.py"):
            rel = path.relative_to(repo).as_posix()
            pairs.append((str(path), f"/opt/flink/{rel}"))

    seen: set[tuple[str, str]] = set()
    unique: list[tuple[str, str]] = []
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        unique.append(pair)
    return unique


def wait_for_taskmanagers(
    *,
    min_slots: int = 1,
    timeout_sec: float = 120,
    poll_interval_sec: float = 2.0,
    rest_port: int | None = None,
) -> dict[str, Any]:
    from apemosyne.api import flink_client

    port = rest_port if rest_port is not None else studio_flink_rest_port()
    deadline = time.time() + timeout_sec
    last: dict[str, Any] = {"count": 0, "slots_total": 0, "slots_free": 0}
    while time.time() < deadline:
        try:
            last = flink_client.taskmanager_summary(rest_port=port)
            if last["count"] > 0 and int(last.get("slots_total") or 0) >= min_slots:
                return last
        except Exception:
            pass
        time.sleep(poll_interval_sec)
    raise TimeoutError(
        f"No taskmanager with >={min_slots} slot(s) on port {port} after {timeout_sec}s "
        f"(last: {last})"
    )


def sync_studio_cluster_code(*, profile: str = DEFAULT_PROFILE) -> CopyStats:
    pairs = studio_cluster_copy_pairs()
    return copy_pairs_to_cluster(pairs, profile=profile)


def ensure_cluster_python_embed_libs(*, profile: str | None = None) -> None:
    """Install libpython on JM/TM (Pemja needs libpython3.10.so at runtime)."""
    import subprocess

    from apemosyne.constants import DEFAULT_PROFILE
    from apemosyne.docker_utils import container_id

    active_profile = profile or DEFAULT_PROFILE
    install_cmd = (
        "dpkg -s libpython3.10 >/dev/null 2>&1 || "
        "(apt-get update -qq && apt-get install -y -qq libpython3.10 libpython3.10-dev g++ gcc)"
    )
    for service in ("jobmanager", "taskmanager"):
        cid = container_id(service, profile=active_profile)
        if cid:
            subprocess.run(
                ["docker", "exec", "-u", "root", cid, "bash", "-c", install_cmd],
                check=False,
            )


def bootstrap_studio_cluster(*, profile: str = DEFAULT_PROFILE) -> None:
    ensure_cluster_python_embed_libs(profile=profile)
    flink_cluster_submit.bootstrap_cluster_containers(profile=profile)


def restart_taskmanager(*, profile: str = DEFAULT_PROFILE) -> None:
    """Restart TaskManager to clear stale PemJa / child classloader state."""
    import subprocess

    from apemosyne.docker_utils import container_id

    cid = container_id("taskmanager", profile=profile)
    if not cid:
        return
    subprocess.run(["docker", "restart", cid], check=False)
    wait_for_taskmanagers(timeout_sec=120, rest_port=studio_flink_rest_port())


def restart_studio_cluster(
    *,
    profile: str = DEFAULT_PROFILE,
    smoke: bool = False,
    wait_rest_sec: float = 180,
    wait_tm_sec: float = 120,
) -> dict[str, Any]:
    """Wait for Flink, sync code, bootstrap Pemja/JARs; optionally run smoke job."""
    port = studio_flink_rest_port()
    print(f"Waiting for Flink REST on :{port}...")
    flink_cluster_submit.wait_for_flink_rest(timeout_sec=wait_rest_sec)

    print("Waiting for taskmanager registration...")
    tm = wait_for_taskmanagers(timeout_sec=wait_tm_sec, rest_port=port)
    print(f"  {tm['count']} taskmanager(s), {tm['slots_total']} slot(s)")

    print("Copying Studio runtime into JobManager + TaskManager...")
    stats = sync_studio_cluster_code(profile=profile)
    print(f"  copied={stats.copied} skipped={stats.skipped} failed={stats.failed}")
    if stats.failed:
        raise RuntimeError(f"Failed to copy {stats.failed} file(s) to cluster")

    print("Bootstrapping Flink Agents JARs + Python on cluster...")
    bootstrap_studio_cluster(profile=profile)

    print("Restarting TaskManager (clears PemJa classloader state)...")
    restart_taskmanager(profile=profile)

    result: dict[str, Any] = {
        "flink_rest_port": port,
        "flink_ui_url": f"http://localhost:{port}",
        "copy": {"copied": stats.copied, "skipped": stats.skipped, "failed": stats.failed},
        "taskmanagers": tm,
        "smoke": None,
    }

    if smoke:
        print("Running cluster launch smoke job...")
        from apemosyne.runtime.cluster_launch_test import run_cluster_launch

        run_cluster_launch()
        result["smoke"] = "ok"

    return result
