"""Submit Studio pipelines as Flink cluster jobs (PR1: batch records → capture)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from apemosyne.constants import DEFAULT_PROFILE
from apemosyne.copy_manifest import copy_pairs_to_cluster
from apemosyne.designer.runtime_env import react_llm_shell_prefix, sync_designer_db_to_cluster
from apemosyne.docker_utils import container_id, docker_exec, project_root
from apemosyne.pipelines.cluster_codegen import (
    cluster_job_name,
    cluster_runner_relpath,
    pipeline_execution_plan,
    write_cluster_runner,
)
from apemosyne.pipelines.cluster_kafka_sink import deliver_pipeline_kafka_sink
from apemosyne.pipelines.docker_runner import _pipeline_copy_pairs
from apemosyne.pipelines.models import Pipeline
from apemosyne.pipelines.validate_cluster import validate_pipeline_cluster
from apemosyne.runs.plan import find_flink_job_for_pipeline

if TYPE_CHECKING:
    from apemosyne.runs.service import RunService


def _sync_cluster_run_status(service: "RunService", run_id: str, job_id: str | None) -> None:
    if not job_id:
        return
    from apemosyne.runs.plan import flink_job_state

    state = flink_job_state(job_id)
    if state in ("FINISHED", "SUCCEEDED"):
        service.finish_run(run_id, status="finished", flink_job_id=job_id)
    elif state in ("FAILED", "CANCELED", "CANCELLED"):
        service.finish_run(run_id, status="failed", flink_job_id=job_id, error=f"Flink job {state}")


@dataclass(frozen=True)
class PipelineClusterSubmitResult:
    run_id: str
    return_code: int
    flink_job_id: str | None = None
    validation: dict[str, Any] | None = None


def _run_service(root: Path | None = None, runs: Optional["RunService"] = None) -> "RunService":
    if runs is not None:
        return runs
    from apemosyne.runs.service import default_run_service

    return default_run_service(root)


def _cluster_copy_pairs(root: Path, pipeline: Pipeline, runner_path: Path) -> list[tuple[str, str]]:
    rel = runner_path.relative_to(root).as_posix()
    pairs = _pipeline_copy_pairs(root, pipeline)
    pairs.append((str(runner_path), f"/opt/flink/{rel}"))
    generated_init = root / "apemosyne" / "pipelines" / "generated" / "__init__.py"
    if generated_init.is_file():
        pairs.append((str(generated_init), "/opt/flink/apemosyne/pipelines/generated/__init__.py"))
    for runtime_rel in (
        "apemosyne/constants.py",
        "apemosyne/flink_rest.py",
        "apemosyne/runtime/__init__.py",
        "apemosyne/runtime/flink_cluster_submit.py",
        "apemosyne/runtime/cluster_launch_test.py",
        "apemosyne/runtime/cluster_launch_agent.py",
        "apemosyne/kafka_sources.py",
        "apemosyne/pipelines/cluster_kafka_sink.py",
        "apemosyne/pipelines/cluster_codegen.py",
    ):
        local = root / runtime_rel
        if local.is_file():
            pairs.append((str(local), f"/opt/flink/{runtime_rel}"))

    seen: set[tuple[str, str]] = set()
    unique: list[tuple[str, str]] = []
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        unique.append(pair)
    return unique


def submit_pipeline_cluster(
    pipeline: Pipeline,
    *,
    root: Path | None = None,
    profile: str = DEFAULT_PROFILE,
    runs: Optional["RunService"] = None,
    flink_job_id: str | None = None,
) -> PipelineClusterSubmitResult:
    """Validate, codegen, copy artifacts, and ``flink run`` a batch pipeline."""
    validation = validate_pipeline_cluster(pipeline)
    if not validation["valid"]:
        raise ValueError("; ".join(validation["errors"]))

    if not container_id("jobmanager", profile=profile):
        raise RuntimeError(
            f"jobmanager not running. Start stack: apemosyne up --profile {profile}"
        )

    repo = root or project_root()
    service = _run_service(repo, runs)
    run_id = service.create_pipeline_run(
        f"pipeline:{pipeline.name}",
        kind="cluster",
        status="starting",
    )

    try:
        runner_path = write_cluster_runner(pipeline, root=repo)
        rel = cluster_runner_relpath(pipeline.id)
        remote = f"/opt/flink/{rel}"

        pairs = _cluster_copy_pairs(repo, pipeline, runner_path)
        stats = copy_pairs_to_cluster(pairs, profile=profile)
        if stats.failed:
            service.finish_run(
                run_id,
                status="failed",
                error=f"copy failed: {stats.failed} file(s)",
            )
            raise RuntimeError(f"Failed to copy {stats.failed} file(s) to cluster")

        from apemosyne.runtime.flink_cluster_submit import bootstrap_cluster_containers

        bootstrap_cluster_containers(profile=profile)

        remote_designer_db = sync_designer_db_to_cluster(root=repo, profile=profile)
        llm_env = react_llm_shell_prefix(root=repo, remote_designer_db=remote_designer_db)

        kafka_env = ""
        sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
        if sink_node is not None:
            sink_type = str(sink_node.config.get("sink_type") or "capture").strip().lower()
            if sink_type == "kafka":
                from apemosyne.kafka_sources import cluster_kafka_bootstrap_servers

                bootstrap = cluster_kafka_bootstrap_servers()
                kafka_env = f'export KAFKA_BOOTSTRAP_SERVERS="{bootstrap}" && '

        command = (
            f"{llm_env}"
            f"{kafka_env}"
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
                job_id = find_flink_job_for_pipeline(pipeline)
            try:
                deliver_pipeline_kafka_sink(pipeline, root=repo, profile=profile)
            except Exception as exc:
                service.finish_run(
                    run_id,
                    status="failed",
                    flink_job_id=job_id,
                    error=f"Kafka sink delivery failed: {exc}",
                )
                return PipelineClusterSubmitResult(
                    run_id=run_id,
                    return_code=1,
                    flink_job_id=job_id,
                    validation=validation,
                )
            service.set_running(run_id, flink_job_id=job_id)
            _sync_cluster_run_status(service, run_id, job_id)
        else:
            service.finish_run(run_id, status="failed", error=f"submit exit code {rc}")

        return PipelineClusterSubmitResult(
            run_id=run_id,
            return_code=rc,
            flink_job_id=job_id,
            validation=validation,
        )
    except Exception as exc:
        service.finish_run(run_id, status="failed", error=str(exc))
        raise


def submit_result_to_dict(
    result: PipelineClusterSubmitResult,
    *,
    pipeline: Pipeline,
) -> dict[str, Any]:
    import os

    from apemosyne.flink_rest import studio_flink_rest_port

    status = "submitted" if result.return_code == 0 else "failed"
    host = os.environ.get("FLINK_REST_ADDRESS", "localhost").strip() or "localhost"
    flink_ui_url = f"http://{host}:{studio_flink_rest_port()}"
    job_url = (
        f"{flink_ui_url}/#/job/completed/{result.flink_job_id}/exceptions"
        if result.flink_job_id
        else None
    )
    return {
        "pipeline_id": pipeline.id,
        "pipeline_name": pipeline.name,
        "job_name": cluster_job_name(pipeline),
        "status": status,
        "run_id": result.run_id,
        "flink_job_id": result.flink_job_id,
        "flink_ui_url": flink_ui_url,
        "flink_job_url": job_url,
        "validation": result.validation,
        "plan": pipeline_execution_plan(pipeline),
    }
