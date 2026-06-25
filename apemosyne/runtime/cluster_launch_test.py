"""Bootstrap Flink cluster runtime and run the launch smoke job."""

from __future__ import annotations

from pathlib import Path


def bootstrap_runtime() -> None:
    from apemosyne.runtime import flink_cluster_submit

    flink_cluster_submit.bootstrap_cluster_runtime(
        download_kafka_jars=False,
        install_agents_jars=True,
    )


def run_cluster_launch() -> int:
    from apemosyne.runtime import flink_cluster_submit

    bootstrap_runtime()
    script = Path(__file__).resolve().parent / "cluster_launch_agent.py"
    job_id, output = flink_cluster_submit.flink_run_py(script)
    print(f"OK  submitted launch smoke job {job_id}")
    if output.strip():
        print(output.strip())
    flink_cluster_submit.wait_for_job(
        job_id, accept={"FINISHED", "RUNNING"}, timeout_sec=180
    )
    print("OK  launch smoke job reached FINISHED or RUNNING")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_cluster_launch())
