"""
Shared helpers for submitting PyFlink jobs to the Docker Compose Flink cluster.

Jobs must be submitted with ``flink run`` from the JobManager container so they
appear in the Web UI (http://localhost:8081). Running ``python3 job.py`` in a
sidecar uses an embedded mini-cluster and does not register with JobManager.
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
PHASE1_JOB_NAME = "Cowrie Phase1 Normalize (Kafka)"
PHASE15_JOB_NAME = "Cowrie Phase1.5 Actor Classify (Kafka)"
PHASE2_JOB_NAME = "Cowrie Phase2 Workflow (Kafka)"

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


def kafka_connector_version(flink_major: Optional[str] = None) -> str:
    """Map Flink distro version to a compatible flink-connector-kafka artifact."""
    major = flink_major or flink_major_version()
    parts = major.split(".")
    if len(parts) >= 2 and parts[0] == "2":
        minor = int(parts[1]) if parts[1].isdigit() else 0
        if minor >= 2:
            return f"5.0.0-2.{minor}"
        return "4.0.1-2.0"
    if len(parts) >= 2 and parts[0] == "1":
        minor = int(parts[1]) if parts[1].isdigit() else 0
        if minor >= 20:
            return f"3.4.0-1.{minor}"
        if minor >= 19:
            return f"3.2.0-1.{minor}"
        if minor >= 18:
            return f"3.2.0-1.{minor}"
    return major


def kafka_clients_version(connector_version: Optional[str] = None) -> str:
    """Kafka client version bundled with the chosen Flink Kafka connector."""
    cv = connector_version or kafka_connector_version()
    if cv.startswith("5.0.0-"):
        return "4.2.0"
    if cv.startswith("4.0."):
        return "3.9.1"
    if cv.startswith("3.4."):
        return "3.9.0"
    return "3.9.1"


def _download_maven_jar(group_path: str, artifact: str, version: str) -> Path:
    jar_path = FLINK_LIB / f"{artifact}-{version}.jar"
    url = (
        f"https://repo1.maven.org/maven2/{group_path}/"
        f"{artifact}/{version}/{artifact}-{version}.jar"
    )
    FLINK_LIB.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl", "-fsSL", url, "-o", str(jar_path)], check=True)
    return jar_path


def ensure_kafka_connector_jar(
    *,
    download: bool = True,
) -> Path:
    """Return path to a Kafka connector jar (SQL fat jar preferred), downloading if needed."""
    version = kafka_connector_version()
    sql_jar = FLINK_LIB / f"flink-sql-connector-kafka-{version}.jar"
    thin_jar = FLINK_LIB / f"flink-connector-kafka-{version}.jar"
    clients_jar = FLINK_LIB / f"kafka-clients-{kafka_clients_version(version)}.jar"

    for stale in FLINK_LIB.glob("flink-*connector-kafka-*.jar"):
        if stale not in (sql_jar, thin_jar) and stale.is_file():
            stale.unlink()
    if clients_jar.is_file():
        clients_jar.unlink()

    if sql_jar.is_file():
        return sql_jar
    if not download:
        raise FileNotFoundError(f"Kafka connector jar not found: {sql_jar}")

    try:
        return _download_maven_jar(
            "org/apache/flink", "flink-sql-connector-kafka", version
        )
    except subprocess.CalledProcessError:
        if thin_jar.is_file():
            return thin_jar
        thin = _download_maven_jar(
            "org/apache/flink", "flink-connector-kafka", version
        )
        _download_maven_jar("org/apache/kafka", "kafka-clients", kafka_clients_version(version))
        return thin


def ensure_kafka_clients_jar(
    *,
    download: bool = True,
) -> Path:
    """Return path to kafka-clients jar (required on the pipeline classpath)."""
    connector_version = kafka_connector_version()
    version = kafka_clients_version(connector_version)
    jar_path = FLINK_LIB / f"kafka-clients-{version}.jar"
    for stale in FLINK_LIB.glob("kafka-clients-*.jar"):
        if stale != jar_path and stale.is_file():
            stale.unlink()
    if jar_path.is_file():
        return jar_path
    if not download:
        raise FileNotFoundError(f"kafka-clients jar not found: {jar_path}")
    return _download_maven_jar("org/apache/kafka", "kafka-clients", version)


def ensure_kafka_jars(*, download: bool = True) -> list[Path]:
    """Ensure Flink Kafka connector jar is present (pipeline classpath only)."""
    return [ensure_kafka_connector_jar(download=download)]


def kafka_jar_uris() -> list[str]:
    jars = ensure_kafka_jars()
    return [f"file://{p.resolve()}" for p in jars]


def attach_kafka_jars(stream_env) -> None:
    _attach_jar_uris(stream_env, kafka_jar_uris())


def ensure_flink_agents_jars() -> None:
    """Copy Flink Agents dist JARs into the Python package lib tree for workers."""
    import shutil

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
        raise FileNotFoundError(
            f"Flink Agents JARs not found for Flink {flink_major}"
        )

    for directory in (lib_root, common_dir, version_dir):
        directory.mkdir(parents=True, exist_ok=True)
        init_py = directory / "__init__.py"
        if not init_py.exists():
            init_py.touch()

    shutil.copy2(common_jar, common_dir / common_jar.name)
    shutil.copy2(thin_jars[0], version_dir / thin_jars[0].name)


def flink_agents_jar_uris() -> list[str]:
    flink_major = flink_major_version()
    common_dir = SITE_PACKAGES / "flink_agents" / "lib" / "common"
    version_dir = SITE_PACKAGES / "flink_agents" / "lib" / f"flink-{flink_major}"
    jars = sorted(common_dir.glob("*.jar")) + sorted(version_dir.glob("*-thin.jar"))
    return [f"file://{jar.resolve()}" for jar in jars]


def attach_flink_agents_jars(stream_env) -> None:
    _attach_jar_uris(stream_env, flink_agents_jar_uris())


def attach_pipeline_jars(
    stream_env,
    *,
    kafka: bool = True,
    agents: bool = False,
) -> None:
    """Attach Kafka and/or Flink Agents JARs to a PyFlink pipeline."""
    uris: list[str] = []
    if kafka:
        uris.extend(kafka_jar_uris())
    if agents:
        uris.extend(flink_agents_jar_uris())
    _attach_jar_uris(stream_env, uris)


def _attach_jar_uris(stream_env, jar_uris: list[str]) -> None:
    if not jar_uris:
        return
    joined = ";".join(jar_uris)
    try:
        stream_env.get_config().set("pipeline.jars", joined)
    except Exception:
        pass
    try:
        stream_env.add_jars(*jar_uris)
    except Exception:
        pass


def bootstrap_cluster_runtime(*, download_kafka_jars: bool = True) -> None:
    """Prepare Python workers, Kafka connector JARs, and Flink Agents JARs."""
    ensure_python_symlink()
    ensure_kafka_jars(download=download_kafka_jars)
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
    """Block until the JobManager REST API responds."""
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


def wait_for_taskmanager_slots(
    *,
    min_slots: int = 1,
    min_free_slots: int = 1,
    timeout_sec: float = 300,
    poll_interval_sec: float = 2.0,
) -> int:
    """Block until at least ``min_slots`` TaskManager slots exist with capacity."""
    deadline = time.time() + timeout_sec
    last_totals = (0, 0)
    while time.time() < deadline:
        try:
            data = fetch_json("/taskmanagers")
            managers = data.get("taskmanagers") or []
            total = sum(int(tm.get("slotsNumber") or 0) for tm in managers)
            free = sum(int(tm.get("freeSlots") or 0) for tm in managers)
            last_totals = (total, free)
            if total >= min_slots and free >= min_free_slots:
                print(f"Flink TaskManagers ready: {total} slot(s), {free} free")
                return free
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            pass
        time.sleep(poll_interval_sec)
    total, free = last_totals
    raise TimeoutError(
        f"Timed out waiting for TaskManager slots (need {min_slots}, saw {total}; "
        f"need {min_free_slots} free, saw {free})"
    )


def wait_for_flink_cluster(
    *,
    min_slots: int = 1,
    timeout_sec: float = 300,
) -> None:
    """Wait for JobManager REST and TaskManager slot registration."""
    wait_for_flink_rest(timeout_sec=timeout_sec)
    wait_for_taskmanager_slots(min_slots=min_slots, timeout_sec=timeout_sec)


def job_state_by_name(job_name: str) -> Optional[str]:
    """Return the best-known state for a job name (prefers RUNNING over terminal states)."""
    priority = {
        "RUNNING": 5,
        "CREATED": 4,
        "RESTARTING": 4,
        "INITIALIZING": 3,
        "RECONCILING": 3,
        "FAILED": 2,
        "CANCELED": 1,
        "FINISHED": 1,
    }
    best: Optional[tuple[int, str]] = None
    try:
        overview = fetch_json("/jobs/overview")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None
    for job in overview.get("jobs", []):
        if job.get("name") != job_name:
            continue
        state = str(job.get("state") or "UNKNOWN")
        score = priority.get(state, 0)
        if best is None or score > best[0]:
            best = (score, state)
    return best[1] if best else None


def parse_submitted_job_id(output: str) -> str:
    for line in output.splitlines():
        marker = "Job has been submitted with JobID "
        if marker in line:
            return line.split(marker, 1)[1].strip()
    raise RuntimeError(f"Could not parse JobID from flink CLI output:\n{output}")


def cancel_job(job_id: str) -> None:
    """Request cancellation of a Flink job via REST API."""
    url = f"{rest_base()}/jobs/{job_id}?mode=cancel"
    req = urllib.request.Request(url, method="PATCH")
    with urllib.request.urlopen(req, timeout=15):
        pass


def cancel_jobs_by_name(
    job_name: str,
    *,
    exclude_job_id: Optional[str] = None,
) -> list[str]:
    """
    Cancel active Flink jobs matching ``job_name``.

    Returns the list of canceled job IDs.
    """
    active = {
        "CREATED",
        "RUNNING",
        "RESTARTING",
        "RECONCILING",
        "INITIALIZING",
        "CANCELLING",
    }
    canceled: list[str] = []
    try:
        overview = fetch_json("/jobs/overview")
    except urllib.error.URLError:
        return canceled

    for job in overview.get("jobs", []):
        if job.get("name") != job_name:
            continue
        jid = job.get("jid")
        if not jid or jid == exclude_job_id:
            continue
        if job.get("state") not in active:
            continue
        try:
            cancel_job(jid)
            canceled.append(jid)
        except urllib.error.URLError:
            pass
    return canceled


def wait_for_job_stable(
    job_id: str,
    *,
    stable_sec: float = 5.0,
    timeout_sec: int = 120,
) -> str:
    """Wait until a job stays RUNNING (or FINISHED) for ``stable_sec`` seconds."""
    accept = {"RUNNING", "FINISHED"}
    reject = {"FAILED", "CANCELED"}
    deadline = time.time() + timeout_sec
    stable_since: Optional[float] = None
    last_state = "UNKNOWN"

    while time.time() < deadline:
        try:
            detail = fetch_json(f"/jobs/{job_id}")
            last_state = detail.get("state", last_state)
            if last_state in reject:
                raise RuntimeError(
                    f"Flink job {job_id} ended with state {last_state}"
                )
            if last_state in accept:
                if stable_since is None:
                    stable_since = time.time()
                elif time.time() - stable_since >= stable_sec:
                    return job_id
            else:
                stable_since = None
        except urllib.error.URLError:
            stable_since = None
        time.sleep(1.0)

    raise TimeoutError(
        f"Timed out waiting for job {job_id} to stabilize (last state: {last_state})"
    )


def wait_for_job(
    job_id: str,
    *,
    accept: Optional[Set[str]] = None,
    reject: Optional[Set[str]] = None,
    timeout_sec: int = 120,
    poll_interval_sec: float = 1.0,
) -> str:
    """Poll JobManager REST until the job reaches an accepted terminal or steady state."""
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
    """Return job ids for all RUNNING jobs with ``job_name``."""
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


def find_running_job(job_name: str) -> Optional[str]:
    """Return job id if a RUNNING job with ``job_name`` exists."""
    running = find_running_jobs(job_name)
    return running[0] if running else None


def wait_for_running_job_name(
    job_name: str,
    *,
    timeout_sec: float = 300,
    poll_interval_sec: float = 2.0,
) -> str:
    """Block until a Flink job with ``job_name`` is RUNNING (used for compose startup ordering)."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        job_id = find_running_job(job_name)
        if job_id:
            print(f"Flink job {job_name!r} is RUNNING ({job_id})")
            return job_id
        time.sleep(poll_interval_sec)
    raise TimeoutError(f"Timed out waiting for RUNNING Flink job {job_name!r}")


def ensure_remote_job(
    job_name: str,
    submit_fn,
    *,
    wait: bool = True,
    wait_for_running: bool = True,
    wait_for_cluster: bool = True,
    cluster_timeout_sec: float = 300,
) -> str:
    """
    Submit a cluster job unless an identically named job is already RUNNING.

    ``submit_fn`` must return a new job id (typically via ``flink_run_py``).
    """
    if wait_for_cluster:
        wait_for_flink_cluster(timeout_sec=cluster_timeout_sec)

    running = find_running_jobs(job_name)
    if len(running) == 1:
        print(f"Reusing RUNNING Flink job {running[0]} ({job_name})")
        return running[0]
    if len(running) > 1:
        print(f"Found {len(running)} duplicate RUNNING jobs for {job_name!r}; keeping {running[0]}")
        for extra in running[1:]:
            try:
                cancel_job(extra)
            except urllib.error.URLError:
                pass
        return running[0]

    canceled = cancel_jobs_by_name(job_name)
    if canceled:
        print(f"Canceled {len(canceled)} prior job(s): {', '.join(canceled)}")

    job_id = submit_fn()
    if wait:
        if wait_for_running:
            wait_for_job(job_id, accept={"RUNNING"}, timeout_sec=180)
            wait_for_job_stable(job_id, stable_sec=3.0, timeout_sec=60)
        else:
            wait_for_job(job_id, accept={"FINISHED", "RUNNING"}, timeout_sec=180)
    return job_id


def ensure_running_job_name(
    job_name: str,
    submit_fn,
    *,
    max_attempts: int = 5,
    retry_delay_sec: float = 5.0,
) -> str:
    """Submit or reuse a job, retrying transient cluster startup failures."""
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return ensure_remote_job(
                job_name,
                submit_fn,
                wait=True,
                wait_for_running=True,
            )
        except (RuntimeError, TimeoutError, urllib.error.URLError) as exc:
            last_error = exc
            print(
                f"ensure_running_job_name attempt {attempt}/{max_attempts} "
                f"failed for {job_name!r}: {exc}",
                flush=True,
            )
            if attempt < max_attempts:
                time.sleep(retry_delay_sec)
    raise RuntimeError(
        f"Could not ensure RUNNING Flink job {job_name!r} after {max_attempts} attempts"
    ) from last_error


def flink_run_py(
    entry_script: Path,
    *,
    pyfiles: Optional[Sequence[Path]] = None,
    detached: bool = True,
    extra_args: Optional[Iterable[str]] = None,
    env: Optional[dict[str, str]] = None,
) -> tuple[str, str]:
    """
    Submit a PyFlink script via ``flink run``.

    Returns ``(job_id, combined_cli_output)``.
    """
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
