"""
Shared helpers for submitting PyFlink jobs to the Docker Compose Flink cluster.

Jobs must be submitted with ``flink run`` from the JobManager container so they
appear in the Web UI (http://localhost:8081).
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set

FLINK_LIB = Path("/opt/flink/lib")
FLINK_BIN = Path("/opt/flink/bin/flink")
SITE_PACKAGES = Path("/opt/flink/pythonpath/agent-site-packages")
FLINK_AGENTS_SRC = Path("/opt/flink/flink-agents")

DEFAULT_PYTHONPATH = (
    "/opt/flink:"
    "/opt/flink/pythonpath/agent-site-packages:"
    "/opt/flink/opt/python/pyflink.zip:"
    "/opt/flink/opt/python/py4j-src.zip"
)


def ensure_python_symlink() -> None:
    python3 = Path("/usr/bin/python3")
    python = Path("/usr/bin/python")
    if python3.is_file() and not python.exists():
        subprocess.run(["ln", "-sf", str(python3), str(python)], check=True)


def flink_major_version() -> str:
    jars = list(FLINK_LIB.glob("flink-dist-*.jar"))
    if not jars:
        raise FileNotFoundError(f"No flink-dist jar in {FLINK_LIB}")
    version = jars[0].name.removeprefix("flink-dist-").removesuffix(".jar")
    parts = version.split(".")
    return ".".join(parts[:2])


def ensure_flink_agents_jars() -> None:
    """Copy Flink Agents dist JARs into the Python package lib tree for workers."""
    import shutil

    if not FLINK_AGENTS_SRC.is_dir():
        return

    lib_root = SITE_PACKAGES / "flink_agents" / "lib"
    flink_major = flink_major_version()
    common_dir = lib_root / "common"
    version_dir = lib_root / f"flink-{flink_major}"

    common_jar = next(
        (FLINK_AGENTS_SRC / "dist/common/target").glob("flink-agents-dist-common-*.jar"),
        None,
    )
    thin_jars = list(
        (FLINK_AGENTS_SRC / f"dist/flink-{flink_major}/target").glob(
            f"flink-agents-dist-flink-{flink_major}-*-thin.jar"
        )
    )
    if not common_jar or not thin_jars:
        return

    for directory in (lib_root, common_dir, version_dir):
        directory.mkdir(parents=True, exist_ok=True)
        init_py = directory / "__init__.py"
        if not init_py.exists():
            init_py.touch()

    shutil.copy2(common_jar, common_dir / common_jar.name)
    shutil.copy2(thin_jars[0], version_dir / thin_jars[0].name)


def bootstrap_cluster_runtime(
    *,
    download_kafka_jars: bool = False,
    install_agents_jars: bool = True,
) -> None:
    """Prepare Python workers and optional Flink Agents JARs."""
    ensure_python_symlink()
    if install_agents_jars:
        ensure_flink_agents_jars()


def rest_base() -> str:
    host = os.environ.get("FLINK_REST_ADDRESS", "localhost").strip()
    port = int(os.environ.get("FLINK_REST_PORT", "8081").strip())
    return f"http://{host}:{port}"


def fetch_json(path: str) -> dict:
    with urllib.request.urlopen(f"{rest_base()}{path}", timeout=10) as resp:
        return json.loads(resp.read().decode())


def wait_for_flink_rest(
    *,
    timeout_sec: float = 300,
    poll_interval_sec: float = 2.0,
) -> None:
    deadline = time.time() + timeout_sec
    last_error: Optional[str] = None
    while time.time() < deadline:
        try:
            fetch_json("/overview")
            print(f"Flink REST ready at {rest_base()}")
            return
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(poll_interval_sec)
    raise TimeoutError(
        f"Flink REST not available at {rest_base()} after {timeout_sec}s: {last_error}"
    )


def parse_submitted_job_id(output: str) -> str:
    for line in output.splitlines():
        marker = "Job has been submitted with JobID "
        if marker in line:
            return line.split(marker, 1)[1].strip()
    raise RuntimeError(f"Could not parse JobID from flink CLI output:\n{output}")


def wait_for_job(
    job_id: str,
    *,
    accept: Optional[Set[str]] = None,
    reject: Optional[Set[str]] = None,
    timeout_sec: int = 120,
    poll_interval_sec: float = 1.0,
) -> str:
    accept = accept or {"FINISHED"}
    reject = reject or {"FAILED", "CANCELED"}
    deadline = time.time() + timeout_sec
    last_state = "UNKNOWN"

    while time.time() < deadline:
        try:
            detail = fetch_json(f"/jobs/{job_id}")
            last_state = detail.get("state", last_state)
            if last_state in accept:
                return job_id
            if last_state in reject:
                raise RuntimeError(
                    f"Flink job {job_id} ended with state {last_state}"
                )
        except urllib.error.URLError:
            pass
        time.sleep(poll_interval_sec)

    raise TimeoutError(
        f"Timed out waiting for job {job_id} (last state: {last_state})"
    )


def find_running_jobs(job_name: str) -> List[str]:
    try:
        overview = fetch_json("/jobs/overview")
    except urllib.error.URLError:
        return []
    out: List[str] = []
    for job in overview.get("jobs", []):
        if job.get("name") != job_name:
            continue
        if job.get("state") == "RUNNING":
            jid = job.get("jid")
            if jid:
                out.append(str(jid))
    return out


def cancel_job(job_id: str) -> None:
    """Request cancellation of a Flink job via REST API."""
    url = f"{rest_base()}/jobs/{job_id}?mode=cancel"
    req = urllib.request.Request(url, method="PATCH")
    with urllib.request.urlopen(req, timeout=10):
        return


def flink_run_py(
    entry_script: Path,
    *,
    pyfiles: Optional[Sequence[Path]] = None,
    detached: bool = True,
    extra_args: Optional[Iterable[str]] = None,
    env: Optional[dict[str, str]] = None,
) -> tuple[str, str]:
    """Submit a PyFlink script via ``flink run``. Returns ``(job_id, cli_output)``."""
    if not FLINK_BIN.is_file():
        raise FileNotFoundError(f"Flink CLI not found at {FLINK_BIN}")
    if not entry_script.is_file():
        raise FileNotFoundError(f"Entry script not found: {entry_script}")

    cmd = [str(FLINK_BIN), "run"]
    host = os.environ.get("FLINK_REST_ADDRESS", "localhost").strip() or "localhost"
    port = os.environ.get("FLINK_REST_PORT", "8081").strip() or "8081"
    cmd.extend(["-m", f"{host}:{port}"])
    if detached:
        cmd.append("-d")
    if pyfiles:
        uris = ",".join(f"file://{p.resolve()}" for p in pyfiles)
        cmd.extend(["-pyFiles", uris])
    cmd.extend(["-py", str(entry_script.resolve())])
    if extra_args:
        cmd.extend(extra_args)

    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    run_env.setdefault("PYTHONPATH", DEFAULT_PYTHONPATH)

    result = subprocess.run(
        cmd,
        cwd="/opt/flink",
        env=run_env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(f"flink run failed (exit {result.returncode}):\n{output}")

    return parse_submitted_job_id(output), output
