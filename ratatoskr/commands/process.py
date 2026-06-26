"""Log processor and Kafka pipeline service runners."""

from __future__ import annotations

import subprocess
import sys

import typer

from ratatoskr.docker_utils import project_root

app = typer.Typer(help="Run processing services locally.")

SERVICES = {
    "log-processor": "cowrie_log_processor.py",
    "kafka-shipper": "cowrie_kafka_shipper.py",
    "flink-pipeline-supervisor": "cowrie_flink_pipeline_supervisor.py",
    "kafka-normalizer": "cowrie_kafka_normalizer.py",
    "workflow": "cowrie_phase2_workflow_processor.py",
    "react-augmentor": "cowrie_phase3_react_augmentor.py",
    "flink-job": "cowrie_kafka_flink_job.py",
    "kafka-alerts-to-dashboard": "kafka_alerts_to_dashboard.py",
    "alerts-to-dashboard": "kafka_alerts_to_dashboard.py",
    "phase1-verify": "cowrie_phase1_verify.py",
}


def _run_script(script: str, extra_args: list[str]) -> None:
    from ratatoskr.paths import configure_runtime_sys_path, runtime_module_path

    configure_runtime_sys_path()
    try:
        path = runtime_module_path(script)
    except FileNotFoundError:
        root = project_root()
        path = root / script
    if not path.is_file():
        typer.echo(f"Script not found: {path}", err=True)
        raise typer.Exit(1)
    cmd = [sys.executable, str(path), *extra_args]
    try:
        subprocess.run(cmd, cwd=project_root(), check=True)
    except subprocess.CalledProcessError as exc:
        raise typer.Exit(exc.returncode)


@app.command("logs")
def process_logs(
    log_file: str = typer.Option(
        None,
        "--log-file",
        help="Honeypot JSON log path (default: auto-detect)",
    ),
    dashboard_file: str = typer.Option(
        None,
        "--dashboard-file",
        help="Dashboard JSON output path (default: auto-detect)",
    ),
) -> None:
    """Watch honeypot logs and update the dashboard (cowrie_log_processor.py)."""
    args: list[str] = []
    if log_file:
        args.extend(["--log-file", log_file])
    if dashboard_file:
        args.extend(["--dashboard-file", dashboard_file])
    _run_script("cowrie_log_processor.py", args)


@app.command("service")
def process_service(
    name: str = typer.Argument(..., help=f"Service: {', '.join(SERVICES)}"),
) -> None:
    """Run a Kafka pipeline or auxiliary service script."""
    if name not in SERVICES:
        typer.echo(f"Unknown service: {name}", err=True)
        typer.echo(f"Available: {', '.join(SERVICES)}", err=True)
        raise typer.Exit(1)
    if name == "flink-job":
        typer.echo(
            "Deprecated: cowrie_kafka_flink_job.py is superseded by Phase 1 normalize + "
            "Phase 2 workflow. Use compose profile 'legacy' only for comparison. "
            "See docs/SPRINT_ROADMAP.md (Sprint C3).",
            err=True,
        )
    _run_script(SERVICES[name], [])
