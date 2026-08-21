"""Run and submit registered Flink Agents."""

from __future__ import annotations

import importlib
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Tuple

from ratatoskr.agents.published_copy import is_published_agent_spec, published_agent_artifact_pairs
from ratatoskr.agents.registry import AgentSpec, get_agent_spec
from ratatoskr.constants import DEFAULT_PROFILE
from ratatoskr.copy_manifest import copy_pairs_to_cluster
from ratatoskr.designer.runtime_env import (
    designer_copy_pairs,
    react_llm_shell_prefix,
    sync_designer_db_to_cluster,
)
from ratatoskr.docker_utils import container_id, docker_exec, project_root
from ratatoskr.paths import agents_dir, configure_runtime_sys_path, project_root
from ratatoskr.runs.plan import find_flink_job_for_agent, flink_job_state

if False:  # TYPE_CHECKING
    from ratatoskr.runs.service import RunService


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
    from ratatoskr.runs.service import default_run_service

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


def _skills_copy_pairs(root: Path, *, rel_dir: str = "examples/skills") -> List[Tuple[str, str]]:
    from ratatoskr.designer.skills_catalog import skills_copy_pairs

    return skills_copy_pairs(root, rel_dir=rel_dir)


def _agent_copy_pairs(spec: AgentSpec, *, root: Path) -> List[Tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    if spec.cluster_script:
        cluster = root / spec.cluster_script
        if cluster.is_file():
            pairs.append((str(cluster), f"/opt/flink/{spec.cluster_script}"))

    module_path = root / spec.module.replace(".", "/")
    if not str(module_path).endswith(".py"):
        module_path = Path(str(module_path) + ".py")
    if module_path.is_file():
        pairs.append((str(module_path), f"/opt/flink/{spec.module.replace('.', '/')}.py"))

    pairs.extend(published_agent_artifact_pairs(root, spec))

    init_py = agents_dir(root) / "__init__.py"
    pairs.append((str(init_py), "/opt/flink/examples/agents/__init__.py"))

    for rel in (
        "ratatoskr/__init__.py",
        "ratatoskr/constants.py",
        "ratatoskr/flink_rest.py",
        "ratatoskr/agents/published_copy.py",
        "ratatoskr/runtime/__init__.py",
        "ratatoskr/runtime/flink_cluster_submit.py",
        "ratatoskr/runtime/cluster_launch_test.py",
        "ratatoskr/runtime/cluster_launch_agent.py",
        "ratatoskr/runtime/kafka_jars.py",
        "ratatoskr/runtime/flink_agents_bootstrap.py",
        "ratatoskr/kafka_sources.py",
        "ratatoskr/paths.py",
        "ratatoskr/docker_utils.py",
        "ratatoskr/nifi/__init__.py",
        "ratatoskr/nifi/client.py",
        "ratatoskr/nifi/policy.py",
        "ratatoskr/nifi/env.py",
        "ratatoskr/kafka/__init__.py",
        "ratatoskr/kafka/client.py",
        "ratatoskr/kafka/policy.py",
        "ratatoskr/kafka/env.py",
    ):
        local = root / rel
        if local.is_file():
            pairs.append((str(local), f"/opt/flink/{rel}"))

    if spec.name == "workflow_nifi_monitor":
        # Ensure package marker dirs exist for import on cluster.
        for rel in (
            "ratatoskr/nifi/__init__.py",
            "ratatoskr/nifi/client.py",
            "ratatoskr/nifi/policy.py",
            "ratatoskr/nifi/env.py",
        ):
            local = root / rel
            remote = f"/opt/flink/{rel}"
            if local.is_file() and (str(local), remote) not in pairs:
                pairs.append((str(local), remote))

    if spec.name == "workflow_kafka_monitor":
        for rel in (
            "ratatoskr/kafka/__init__.py",
            "ratatoskr/kafka/client.py",
            "ratatoskr/kafka/policy.py",
            "ratatoskr/kafka/env.py",
        ):
            local = root / rel
            remote = f"/opt/flink/{rel}"
            if local.is_file() and (str(local), remote) not in pairs:
                pairs.append((str(local), remote))

    examples_init = root / "examples" / "__init__.py"
    if not examples_init.is_file():
        # Ensure examples package import works in container.
        pairs.append((str(agents_dir(root) / "__init__.py"), "/opt/flink/examples/__init__.py"))
    else:
        pairs.append((str(examples_init), "/opt/flink/examples/__init__.py"))

    if spec.type == "react" or spec.name == "react_double_value" or is_published_agent_spec(spec):
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

    if spec.name == "react_skills_demo":
        for path in (
            root / "examples/agents/react_skills_paths.py",
            root / "ratatoskr/designer/flink_llm.py",
        ):
            if path.is_file():
                rel = path.relative_to(root).as_posix()
                remote = f"/opt/flink/{rel}"
                if (str(path), remote) not in pairs:
                    pairs.append((str(path), remote))
        for skill_pair in _skills_copy_pairs(root):
            if skill_pair not in pairs:
                pairs.append(skill_pair)

    if spec.name == "session_detect":
        for path in (
            root / "examples/agents/session_detect_logic.py",
            root / "examples/agents/session_detect_actions.py",
            root / "examples/agents/session_window_policy.py",
            root / "examples/agents/session_window_ops.py",
            root / "examples/agents/session_window_fixtures.py",
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
            f"jobmanager not running. Start stack: ratatoskr up --profile {profile}"
        )

    repo = root or project_root()
    service = _run_service(repo, runs)
    run_id = service.create_run(name, kind="cluster", status="starting")

    pairs = _agent_copy_pairs(spec, root=repo)
    stats = copy_pairs_to_cluster(pairs, profile=profile)
    if stats.failed:
        service.finish_run(run_id, status="failed", error=f"copy failed: {stats.failed} file(s)")
        raise RuntimeError(f"Failed to copy {stats.failed} file(s) to cluster")

    from ratatoskr.runtime.flink_cluster_submit import bootstrap_cluster_containers

    bootstrap_cluster_containers(profile=profile)

    remote_designer_db = sync_designer_db_to_cluster(root=repo, profile=profile)
    llm_env = react_llm_shell_prefix(root=repo, remote_designer_db=remote_designer_db)

    remote = f"/opt/flink/{spec.cluster_script}"
    command = (
        f"{llm_env}"
        "cd /opt/flink && "
        "export PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
        "/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j && "
        "export FLINK_REST_ADDRESS=localhost FLINK_REST_PORT=8081 && "
        "python3 -c \""
        "from pathlib import Path; "
        "from ratatoskr.runtime.flink_cluster_submit import bootstrap_cluster_runtime, flink_run_py; "
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
            f"Install flink_agents (ratatoskr build) to introspect class: {exc}"
        )
    return info
