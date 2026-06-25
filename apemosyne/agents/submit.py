"""Run and submit registered Flink Agents."""

from __future__ import annotations

import importlib
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Tuple

from apemosyne.agents.registry import AgentSpec, get_agent_spec
from apemosyne.constants import DEFAULT_PROFILE
from apemosyne.copy_manifest import copy_pairs_to_cluster
from apemosyne.designer.runtime_env import (
    designer_copy_pairs,
    react_llm_shell_prefix,
    sync_designer_db_to_cluster,
)
from apemosyne.docker_utils import container_id, docker_exec, project_root
from apemosyne.paths import agents_dir, configure_runtime_sys_path, project_root
from apemosyne.runs.plan import find_flink_job_for_agent, flink_job_state

if False:  # TYPE_CHECKING
    from apemosyne.runs.service import RunService


@dataclass(frozen=True)
class LocalRunResult:
    run_id: str
    return_code: int


@dataclass(frozen=True)
class ClusterSubmitResult:
    run_id: str
    return_code: int
    flink_job_id: str | None = None


def _run_service(root: Optional[Path] = None, runs: Optional["RunService"] = None) -> "RunService":
    if runs is not None:
        return runs
    from apemosyne.runs.service import default_run_service

    return default_run_service(root)


def _sync_cluster_run_status(service: "RunService", run_id: str, job_id: str | None) -> None:
    if not job_id:
        return
    state = flink_job_state(job_id)
    if state in ("FINISHED", "SUCCEEDED"):
        service.finish_run(run_id, status="finished", flink_job_id=job_id)
    elif state in ("FAILED", "CANCELED", "CANCELLED"):
        service.finish_run(run_id, status="failed", flink_job_id=job_id, error=f"Flink job {state}")


def _import_agent_class(spec: AgentSpec) -> type:
    configure_runtime_sys_path(include_honeypot=False)
    module = importlib.import_module(spec.module)
    agent_cls = getattr(module, spec.class_name, None)
    if agent_cls is None:
        raise ImportError(f"{spec.entry} not found")
    return agent_cls


def _agent_copy_pairs(spec: AgentSpec, *, root: Path) -> List[Tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    cluster = root / spec.cluster_script
    pairs.append((str(cluster), f"/opt/flink/{spec.cluster_script}"))

    module_path = root / spec.module.replace(".", "/")
    if not str(module_path).endswith(".py"):
        module_path = Path(str(module_path) + ".py")
    pairs.append((str(module_path), f"/opt/flink/{spec.module.replace('.', '/')}.py"))

    init_py = agents_dir(root) / "__init__.py"
    pairs.append((str(init_py), "/opt/flink/examples/agents/__init__.py"))

    for rel in (
        "apemosyne/__init__.py",
        "apemosyne/runtime/__init__.py",
        "apemosyne/runtime/flink_cluster_submit.py",
        "apemosyne/runtime/cluster_launch_test.py",
        "apemosyne/runtime/cluster_launch_agent.py",
    ):
        local = root / rel
        if local.is_file():
            pairs.append((str(local), f"/opt/flink/{rel}"))

    examples_init = root / "examples" / "__init__.py"
    if not examples_init.is_file():
        # Ensure examples package import works in container.
        pairs.append((str(agents_dir(root) / "__init__.py"), "/opt/flink/examples/__init__.py"))
    else:
        pairs.append((str(examples_init), "/opt/flink/examples/__init__.py"))

    if spec.type == "react" or spec.name == "react_double_value":
        pairs.extend(designer_copy_pairs(root=root))
        for path in (
            root / "examples/agents/react_double_value_logic.py",
            root / "examples/agents/react_double_value_prompt.py",
        ):
            if path.is_file():
                rel = path.relative_to(root).as_posix()
                remote = f"/opt/flink/{rel}"
                if (str(path), remote) not in pairs:
                    pairs.append((str(path), remote))

    return pairs


def run_agent_local(
    name: str,
    *,
    root: Optional[Path] = None,
    runs: Optional["RunService"] = None,
) -> LocalRunResult:
    """Execute an agent via its local runner script and record a run."""
    spec = get_agent_spec(name, root=root)
    if not spec.runner:
        raise ValueError(f"Agent {name!r} has no local runner configured")
    repo = root or project_root()
    runner = repo / spec.runner
    service = _run_service(repo, runs)
    run_id = service.create_run(name, kind="local", status="running")
    rc = subprocess.run([sys.executable, str(runner)], cwd=repo).returncode
    if rc == 0:
        service.finish_run(run_id, status="finished")
    else:
        service.finish_run(run_id, status="failed", error=f"exit code {rc}")
    return LocalRunResult(run_id=run_id, return_code=rc)


def submit_agent_cluster(
    name: str,
    *,
    root: Optional[Path] = None,
    profile: str = DEFAULT_PROFILE,
    runs: Optional["RunService"] = None,
    flink_job_id: str | None = None,
) -> ClusterSubmitResult:
    """Submit an agent cluster job to JobManager via ``flink run``."""
    spec = get_agent_spec(name, root=root)
    if not spec.cluster_script:
        raise ValueError(f"Agent {name!r} has no cluster script configured")

    if not container_id("jobmanager", profile=profile):
        raise RuntimeError(
            f"jobmanager not running. Start stack: apemosyne up --profile {profile}"
        )

    repo = root or project_root()
    service = _run_service(repo, runs)
    run_id = service.create_run(name, kind="cluster", status="starting")

    pairs = _agent_copy_pairs(spec, root=repo)
    stats = copy_pairs_to_cluster(pairs, profile=profile)
    if stats.failed:
        service.finish_run(run_id, status="failed", error=f"copy failed: {stats.failed} file(s)")
        raise RuntimeError(f"Failed to copy {stats.failed} file(s) to cluster")

    remote_designer_db = sync_designer_db_to_cluster(root=repo, profile=profile)
    llm_env = react_llm_shell_prefix(root=repo, remote_designer_db=remote_designer_db)

    remote = f"/opt/flink/{spec.cluster_script}"
    command = (
        f"{llm_env}"
        "cd /opt/flink && "
        "export PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
        "/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-src.zip && "
        "export FLINK_REST_ADDRESS=localhost FLINK_REST_PORT=8081 && "
        "python3 -c \""
        "from pathlib import Path; "
        "from apemosyne.runtime.flink_cluster_submit import bootstrap_cluster_runtime, flink_run_py; "
        "bootstrap_cluster_runtime(); "
        f"job_id, out = flink_run_py(Path('{remote}')); "
        "print('Submitted job', job_id)\""
    )
    rc = docker_exec(
        container_id("jobmanager", profile=profile) or "",
        command,
        interactive=False,
    )
    job_id = flink_job_id
    if rc == 0:
        if not job_id:
            job_id = find_flink_job_for_agent(name)
        service.set_running(run_id, flink_job_id=job_id)
        _sync_cluster_run_status(service, run_id, job_id)
    else:
        service.finish_run(run_id, status="failed", error=f"submit exit code {rc}")
    return ClusterSubmitResult(run_id=run_id, return_code=rc, flink_job_id=job_id)


def describe_agent(name: str, *, root: Optional[Path] = None) -> dict[str, Any]:
    spec = get_agent_spec(name, root=root)
    info: dict[str, Any] = {
        "name": spec.name,
        "type": spec.type,
        "entry": spec.entry,
        "description": spec.description,
        "runner": spec.runner,
        "cluster_script": spec.cluster_script,
    }
    try:
        agent_cls = _import_agent_class(spec)
        info["class"] = agent_cls.__name__
        info["members"] = sorted(
            attr
            for attr in dir(agent_cls)
            if not attr.startswith("_") and callable(getattr(agent_cls, attr, None))
        )[:20]
    except ImportError as exc:
        info["class"] = spec.class_name
        info["import_note"] = (
            f"Install flink_agents (apemosyne build) to introspect class: {exc}"
        )
    return info
