"""Run and submit registered Flink Agents."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path
from typing import Any, List, Optional, Tuple

from apemosyne.agents.registry import AgentSpec, get_agent_spec
from apemosyne.constants import DEFAULT_PROFILE
from apemosyne.copy_manifest import copy_pairs_to_cluster
from apemosyne.docker_utils import container_id, docker_exec, project_root
from apemosyne.paths import agents_dir, configure_runtime_sys_path, project_root


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

    return pairs


def run_agent_local(name: str, *, root: Optional[Path] = None) -> int:
    """Execute an agent via its local runner script."""
    spec = get_agent_spec(name, root=root)
    if not spec.runner:
        raise ValueError(f"Agent {name!r} has no local runner configured")
    repo = root or project_root()
    runner = repo / spec.runner
    return subprocess.run([sys.executable, str(runner)], cwd=repo).returncode


def submit_agent_cluster(
    name: str,
    *,
    root: Optional[Path] = None,
    profile: str = DEFAULT_PROFILE,
) -> int:
    """Submit an agent cluster job to JobManager via ``flink run``."""
    spec = get_agent_spec(name, root=root)
    if not spec.cluster_script:
        raise ValueError(f"Agent {name!r} has no cluster script configured")

    if not container_id("jobmanager", profile=profile):
        raise RuntimeError(
            f"jobmanager not running. Start stack: apemosyne up --profile {profile}"
        )

    repo = root or project_root()
    pairs = _agent_copy_pairs(spec, root=repo)
    stats = copy_pairs_to_cluster(pairs, profile=profile)
    if stats.failed:
        raise RuntimeError(f"Failed to copy {stats.failed} file(s) to cluster")

    remote = f"/opt/flink/{spec.cluster_script}"
    command = (
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
    return docker_exec(
        container_id("jobmanager", profile=profile) or "",
        command,
        interactive=False,
    )


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
