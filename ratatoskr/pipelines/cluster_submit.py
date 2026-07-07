"""Submit Studio pipelines as Flink cluster jobs (PR1: batch records → capture)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from ratatoskr.constants import DEFAULT_PROFILE
from ratatoskr.copy_manifest import copy_pairs_to_cluster
from ratatoskr.designer.runtime_env import react_llm_shell_prefix, sync_designer_db_to_cluster
from ratatoskr.docker_utils import PYFLINK_PYTHONPATH, container_id, docker_exec_output, project_root
from ratatoskr.pipelines.cluster_codegen import (
    cluster_job_name,
    cluster_runner_relpath,
    pipeline_execution_plan,
    write_cluster_runner,
)
from ratatoskr.pipelines.cluster_kafka_sink import deliver_pipeline_kafka_sink
from ratatoskr.pipelines.docker_runner import _pipeline_copy_pairs
from ratatoskr.pipelines.models import Pipeline
from ratatoskr.pipelines.validate_cluster import validate_pipeline_cluster
from ratatoskr.runs.plan import find_flink_job_for_pipeline

if TYPE_CHECKING:
    from ratatoskr.runs.service import RunService


def _sync_cluster_run_status(
    service: "RunService",
    run_id: str,
    job_id: str | None,
    *,
    record_count: int | None = None,
) -> None:
    if not job_id:
        return
    from ratatoskr.runs.plan import flink_job_state

    state = flink_job_state(job_id)
    if state in ("FINISHED", "SUCCEEDED"):
        service.finish_run(
            run_id, status="finished", flink_job_id=job_id, record_count=record_count
        )
    elif state in ("FAILED", "CANCELED", "CANCELLED"):
        service.finish_run(run_id, status="failed", flink_job_id=job_id, error=f"Flink job {state}")


def _record_cluster_sink_span(
    service: "RunService",
    run_id: str,
    pipeline: Pipeline,
    sink_output: list[dict[str, Any]] | None,
) -> int | None:
    """Persist the delivered Kafka sink output as a run span (parity with local runs)."""
    if sink_output is None:
        return None

    sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
    if sink_node is None:
        return None

    sink_type = str(sink_node.config.get("sink_type") or "capture").strip().lower()
    topic = _resolved_kafka_sink_topic(sink_node.config) if sink_type == "kafka" else ""
    service.append_span(
        run_id,
        kind="sink",
        name=topic or "capture",
        output_data=sink_output,
        input_data={"sink_type": sink_type, "topic": topic or sink_node.config.get("topic")},
    )
    # Persist the count now so the "Records" stat is correct even if the Flink
    # job is still running when the run detail is next fetched.
    record_count = len(sink_output)
    service.set_record_count(run_id, record_count)
    return record_count


def _resolved_kafka_sink_topic(config: dict[str, Any]) -> str:
    topic = str(config.get("topic") or "").strip()
    if topic:
        return topic

    from ratatoskr.kafka_sources import DEFAULT_KAFKA_OUTPUT_TOPIC

    return DEFAULT_KAFKA_OUTPUT_TOPIC


def _sample_cluster_kafka_sink_output(pipeline: Pipeline) -> list[dict[str, Any]] | None:
    """Best-effort snapshot for sinks that publish from the running Flink job."""
    sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
    if sink_node is None:
        return None

    sink_type = str(sink_node.config.get("sink_type") or "capture").strip().lower()
    if sink_type != "kafka":
        return None

    from ratatoskr.kafka_sources import sample_topic_records

    topic = _resolved_kafka_sink_topic(sink_node.config)
    limit = int(sink_node.config.get("sample_limit") or sink_node.config.get("max_records") or 10)
    bootstrap = sink_node.config.get("bootstrap")
    try:
        return sample_topic_records(
            topic,
            limit=limit,
            bootstrap=str(bootstrap) if bootstrap else None,
            timeout_ms=8000,
        )
    except Exception:
        return None


@dataclass(frozen=True)
class PipelineClusterSubmitResult:
    run_id: str
    return_code: int
    flink_job_id: str | None = None
    validation: dict[str, Any] | None = None


def _run_service(root: Path | None = None, runs: Optional["RunService"] = None) -> "RunService":
    if runs is not None:
        return runs
    from ratatoskr.runs.service import default_run_service

    return default_run_service(root)


def _cluster_copy_pairs(root: Path, pipeline: Pipeline, runner_path: Path) -> list[tuple[str, str]]:
    rel = runner_path.relative_to(root).as_posix()
    pairs = _pipeline_copy_pairs(root, pipeline)
    pairs.append((str(runner_path), f"/opt/flink/{rel}"))
    generated_init = root / "ratatoskr" / "pipelines" / "generated" / "__init__.py"
    if generated_init.is_file():
        pairs.append((str(generated_init), "/opt/flink/ratatoskr/pipelines/generated/__init__.py"))
    for runtime_rel in (
        "ratatoskr/constants.py",
        "ratatoskr/flink_rest.py",
        "ratatoskr/runtime/__init__.py",
        "ratatoskr/runtime/flink_cluster_submit.py",
        "ratatoskr/runtime/cluster_launch_test.py",
        "ratatoskr/runtime/cluster_launch_agent.py",
        "ratatoskr/runtime/flink_agents_bootstrap.py",
        "ratatoskr/runtime/kafka_jars.py",
        "ratatoskr/kafka_sources.py",
        "ratatoskr/pipelines/cluster_kafka_sink.py",
        "ratatoskr/pipelines/cluster_codegen.py",
        "ratatoskr/pipelines/agent_settings.py",
        "ratatoskr/pipelines/window_config.py",
        "ratatoskr/pipelines/window_policies.py",
        "ratatoskr/pipelines/window_ops.py",
        "ratatoskr/pipelines/window_local.py",
        "ratatoskr/pipelines/window_codegen.py",
        "ratatoskr/pipelines/validate_cluster.py",
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
            f"jobmanager not running. Start stack: ratatoskr up --profile {profile}"
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
        import ast

        try:
            ast.parse(runner_path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            raise RuntimeError(f"Generated cluster runner has a syntax error: {exc}") from exc

        rel = cluster_runner_relpath(pipeline.id)
        remote = f"/opt/flink/{rel}"

        pairs = _cluster_copy_pairs(repo, pipeline, runner_path)
        from ratatoskr.pipelines.window_config import EXECUTION_AGENT_BRIDGE, parse_window_config, pipeline_window_node

        window_node = pipeline_window_node(pipeline)
        if window_node and parse_window_config(window_node.config).execution_mode == EXECUTION_AGENT_BRIDGE:
            bridge_dir = runner_path.parent
            for name in ("run_cluster_window.py", "run_cluster_agent.py"):
                local = bridge_dir / name
                if local.is_file():
                    pairs.append((str(local), f"/opt/flink/.ratatoskr/pipelines/{pipeline.id}/{name}"))
        stats = copy_pairs_to_cluster(pairs, profile=profile)
        if stats.failed:
            service.finish_run(
                run_id,
                status="failed",
                error=f"copy failed: {stats.failed} file(s)",
            )
            raise RuntimeError(f"Failed to copy {stats.failed} file(s) to cluster")

        from ratatoskr.runtime.flink_cluster_submit import bootstrap_cluster_containers

        bootstrap_cluster_containers(profile=profile)

        remote_designer_db = sync_designer_db_to_cluster(root=repo, profile=profile)
        llm_env = react_llm_shell_prefix(root=repo, remote_designer_db=remote_designer_db)

        kafka_env = ""
        sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
        if sink_node is not None:
            sink_type = str(sink_node.config.get("sink_type") or "capture").strip().lower()
            if sink_type == "kafka":
                from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers

                bootstrap = cluster_kafka_bootstrap_servers()
                kafka_env = f'export KAFKA_BOOTSTRAP_SERVERS="{bootstrap}" && '

        command = (
            f"{llm_env}"
            f"{kafka_env}"
            "cd /opt/flink && "
            f"export PYTHONPATH={PYFLINK_PYTHONPATH} && "
            "export FLINK_REST_ADDRESS=localhost FLINK_REST_PORT=8081 && "
            "python3 -c \""
            "from pathlib import Path; "
            "from ratatoskr.runtime.flink_cluster_submit import bootstrap_cluster_runtime, flink_run_py; "
            "bootstrap_cluster_runtime(); "
            f"job_id, out = flink_run_py(Path('{remote}')); "
            "print('Submitted job', job_id)\""
        )
        rc, stdout, stderr = docker_exec_output(
            container_id("jobmanager", profile=profile) or "",
            command,
            interactive=False,
        )
        output = stdout + stderr

        job_id = flink_job_id
        if rc == 0:
            if not job_id:
                for line in output.splitlines():
                    if line.startswith("Submitted job "):
                        job_id = line.split("Submitted job ", 1)[1].strip()
                        break
            if not job_id:
                job_id = find_flink_job_for_pipeline(pipeline)
            try:
                sink_output = deliver_pipeline_kafka_sink(pipeline, root=repo, profile=profile)
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
            if sink_output is None:
                sink_output = _sample_cluster_kafka_sink_output(pipeline)
            record_count = _record_cluster_sink_span(service, run_id, pipeline, sink_output)
            service.set_running(run_id, flink_job_id=job_id)
            _sync_cluster_run_status(service, run_id, job_id, record_count=record_count)
        else:
            detail = (stderr or stdout or "").strip()
            service.finish_run(
                run_id,
                status="failed",
                error=detail or f"submit exit code {rc}",
            )

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

    from ratatoskr.flink_rest import studio_flink_rest_port

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
