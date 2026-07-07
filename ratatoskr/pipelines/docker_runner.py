"""Execute pipelines inside the Flink JobManager when flink_agents is not on the host."""

from __future__ import annotations

import json
import subprocess
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ratatoskr.agents.published_copy import is_published_agent_spec, published_agent_artifact_pairs
from ratatoskr.constants import DEFAULT_PROFILE
from ratatoskr.copy_manifest import copy_pairs_to_cluster
from ratatoskr.designer.runtime_env import designer_copy_pairs, react_llm_shell_prefix, sync_designer_db_to_cluster
from ratatoskr.docker_utils import PYFLINK_PYTHONPATH, container_id, docker_cp, project_root
from ratatoskr.pipelines.models import AgentStepResult, Pipeline


def _pipeline_copy_pairs(root: Path, pipeline: Pipeline) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for rel in (
        "ratatoskr/__init__.py",
        "ratatoskr/constants.py",
        "ratatoskr/paths.py",
        "ratatoskr/flink_rest.py",
        "ratatoskr/paths.py",
        "ratatoskr/agents/__init__.py",
        "ratatoskr/agents/registry.py",
        "ratatoskr/agents/published_copy.py",
        "ratatoskr/agents/submit.py",
        "ratatoskr/copy_manifest.py",
        "ratatoskr/docker_utils.py",
        "ratatoskr/kafka_sources.py",
        "ratatoskr/manifests.py",
        "ratatoskr/runs/__init__.py",
        "ratatoskr/runs/models.py",
        "ratatoskr/runs/plan.py",
        "ratatoskr/runs/store.py",
        "ratatoskr/runs/service.py",
        "ratatoskr/pipelines/__init__.py",
        "ratatoskr/pipelines/models.py",
        "ratatoskr/pipelines/validate.py",
        "ratatoskr/pipelines/executor.py",
        "ratatoskr/pipelines/agent_settings.py",
        "ratatoskr/pipelines/window_config.py",
        "ratatoskr/pipelines/window_policies.py",
        "ratatoskr/pipelines/window_ops.py",
        "ratatoskr/pipelines/window_local.py",
        "ratatoskr/pipelines/window_codegen.py",
        "ratatoskr/pipelines/validate_cluster.py",
        "ratatoskr/pipelines/cluster_codegen.py",
        "ratatoskr/pipelines/container_run.py",
        "ratatoskr/designer/__init__.py",
        "ratatoskr/designer/models.py",
        "ratatoskr/designer/store.py",
        "ratatoskr/designer/llm_settings.py",
        "ratatoskr/designer/llm_client.py",
        "ratatoskr/designer/api_fetch_settings.py",
        "ratatoskr/designer/runtime_env.py",
        "ratatoskr/httpio/__init__.py",
        "ratatoskr/httpio/fetch.py",
        "examples/agents/__init__.py",
    ):
        local = root / rel
        if local.is_file():
            pairs.append((str(local), f"/opt/flink/{rel}"))

    agents_root = root / "examples" / "agents"
    if agents_root.is_dir():
        for path in agents_root.rglob("*.py"):
            rel = path.relative_to(root)
            pairs.append((str(path), f"/opt/flink/{rel.as_posix()}"))
        manifest = agents_root / "agent-manifest.yaml"
        if manifest.is_file():
            pairs.append((str(manifest), "/opt/flink/examples/agents/agent-manifest.yaml"))
        for path in agents_root.rglob("*.yaml"):
            rel = path.relative_to(root)
            pairs.append((str(path), f"/opt/flink/{rel.as_posix()}"))

    examples_init = root / "examples" / "__init__.py"
    if not examples_init.is_file() and (agents_root / "__init__.py").is_file():
        pairs.append((str(agents_root / "__init__.py"), "/opt/flink/examples/__init__.py"))

    for node in pipeline.nodes:
        if node.kind != "agent" or not node.agent:
            continue
        from ratatoskr.agents.registry import get_agent_spec

        spec = get_agent_spec(node.agent, root=root)
        module_path = root / spec.module.replace(".", "/")
        if not str(module_path).endswith(".py"):
            module_path = Path(str(module_path) + ".py")
        if module_path.is_file():
            pairs.append((str(module_path), f"/opt/flink/{spec.module.replace('.', '/')}.py"))
        pairs.extend(published_agent_artifact_pairs(root, spec))
        if is_published_agent_spec(spec) and spec.type == "react":
            pairs.extend(designer_copy_pairs(root=root))

    # Deduplicate while preserving order.
    seen: set[tuple[str, str]] = set()
    unique: list[tuple[str, str]] = []
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        unique.append(pair)
    return unique


def _container_run_error(stderr: str, stdout: str) -> str:
    """Prefer traceback error line over Flink INFO log noise."""
    for block in (stderr, stdout):
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        for line in reversed(lines):
            if line.startswith(("Traceback", "  File ", "During handling")):
                continue
            if any(
                token in line
                for token in (
                    "Error:",
                    "Exception:",
                    "ModuleNotFoundError",
                    "RuntimeError",
                    "ValueError",
                    "ImportError",
                )
            ):
                return line
        if lines:
            return lines[-1]
    return ""


def run_pipeline_in_container(
    pipeline: Pipeline,
    *,
    input_override: list[dict[str, Any]] | None = None,
    profile: str = DEFAULT_PROFILE,
) -> tuple[list[dict[str, Any]], list[AgentStepResult]]:
    """Run pipeline agents via JobManager container (flink_agents in Docker image)."""
    cid = container_id("jobmanager", profile=profile)
    if not cid:
        raise RuntimeError(
            "flink_agents is not installed on the host and JobManager is not running. "
            "Run: ratatoskr up && ratatoskr build"
        )

    root = project_root()
    stats = copy_pairs_to_cluster(_pipeline_copy_pairs(root, pipeline), profile=profile)
    if stats.failed:
        raise RuntimeError(f"Failed to copy {stats.failed} pipeline file(s) to cluster")

    payload = {
        "pipeline": {
            "id": pipeline.id,
            "name": pipeline.name,
            "nodes": [asdict(n) for n in pipeline.nodes],
            "edges": [asdict(e) for e in pipeline.edges],
            "layout": pipeline.layout,
            "created_at": pipeline.created_at,
            "updated_at": pipeline.updated_at,
        },
        "input_override": input_override,
    }

    remote_payload = "/tmp/ratatoskr_pipeline_payload.json"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
        json.dump(payload, handle)
        local_payload = Path(handle.name)

    try:
        if not docker_cp(local_payload, cid, remote_payload):
            raise RuntimeError("Failed to copy pipeline payload to JobManager")

        remote_designer_db = sync_designer_db_to_cluster(root=root, profile=profile)
        llm_env = react_llm_shell_prefix(root=root, remote_designer_db=remote_designer_db)

        command = (
            f"{llm_env}"
            f"export PYTHONPATH={PYFLINK_PYTHONPATH} && "
            "python3 /opt/flink/ratatoskr/pipelines/container_run.py "
            f"{remote_payload}"
        )
        result = subprocess.run(
            ["docker", "exec", cid, "bash", "-c", command],
            cwd=root,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            detail = _container_run_error(result.stderr or "", result.stdout or "")
            raise RuntimeError(
                detail or f"Pipeline container run failed with exit code {result.returncode}"
            )

        stdout = result.stdout.strip()
        if not stdout:
            raise RuntimeError("Pipeline container run returned no output")

        body = json.loads(stdout)
        steps = [
            AgentStepResult(
                agent=step["agent"],
                duration_ms=int(step["duration_ms"]),
                input_data=step.get("input_data"),
                output_data=step.get("output_data"),
            )
            for step in body.get("steps") or []
        ]
        output = body.get("output") or []
        return output, steps
    finally:
        local_payload.unlink(missing_ok=True)
