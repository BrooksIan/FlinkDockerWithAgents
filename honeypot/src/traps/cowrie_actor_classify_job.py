"""Phase 1.5 Flink job: Kafka normalized → actor classify → enriched + session_actor.

Submit to the compose Flink cluster (visible in Web UI)::

    flink run -d \\
      -pyFiles file:///opt/flink/cowrie_actor_classify_agent.py,... \\
      -py /opt/flink/cowrie_actor_classify_job.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Optional

DEFAULT_JOB_NAME = "Cowrie Phase1.5 Actor Classify (Kafka)"


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or default).strip()


def _safe_loads(line: str) -> Optional[dict]:
    try:
        obj = json.loads(line)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def is_enriched_line(line: str) -> bool:
    obj = _safe_loads(line)
    return bool(obj and obj.get("_stream") == "enriched")


def is_session_actor_line(line: str) -> bool:
    obj = _safe_loads(line)
    return bool(obj and obj.get("_stream") == "session_actor")


def strip_stream_tag(line: str) -> str:
    obj = _safe_loads(line)
    if not obj:
        return line
    obj.pop("_stream", None)
    return json.dumps(obj)


def bootstrap_runtime() -> None:
    """Prepare Python workers and Kafka connector JAR for cluster submission."""
    from flink_cluster_submit import ensure_kafka_jars, ensure_python_symlink

    ensure_kafka_jars()
    ensure_python_symlink()


def _phase15_pyfiles(root: Path) -> list[Path]:
    modules = [
        "cowrie_actor_classify_agent.py",
        "cowrie_actor_classify.py",
        "cowrie_injection_catalog.py",
        "cowrie_trap_state.py",
        "cowrie_trap_disinfo.py",
        "cowrie_trap_compliance.py",
        "cowrie_disinfo_publish.py",
        "cowrie_pipeline.py",
        "_pyc_shim.py",
    ]
    out: list[Path] = []
    for name in modules:
        p = root / name
        if p.is_file():
            out.append(p)
    return out


def build_actor_classify_pipeline():
    """Build the Phase 1.5 streaming pipeline (does not execute)."""
    if "/opt/flink" not in sys.path:
        sys.path.insert(0, "/opt/flink")

    from pyflink.common import WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )

    from cowrie_actor_classify_agent import actor_classify_flat_map_line
    from flink_cluster_submit import attach_kafka_jars

    kafka_bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    in_topic = _env("KAFKA_NORMALIZED_TOPIC", "cowrie.normalized")
    enriched_topic = _env("KAFKA_NORMALIZED_ENRICHED_TOPIC", "cowrie.normalized.enriched")
    session_topic = _env("KAFKA_SESSION_ACTOR_TOPIC", "cowrie.session_actor")
    group_id = _env("KAFKA_GROUP_ID", "cowrie-actor-classify-flink")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(_env("COWRIE_FLINK_PARALLELISM", "1")))
    attach_kafka_jars(env)

    offsets = _env("KAFKA_AUTO_OFFSET_RESET", "latest").lower()
    starting = (
        KafkaOffsetsInitializer.earliest()
        if offsets in ("earliest", "beginning")
        else KafkaOffsetsInitializer.latest()
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(in_topic)
        .set_group_id(group_id)
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(starting)
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "cowrie.normalized")
    classified = ds.flat_map(actor_classify_flat_map_line, output_type=Types.STRING())

    enriched_ds = (
        classified.filter(is_enriched_line)
        .map(strip_stream_tag, output_type=Types.STRING())
    )
    session_ds = (
        classified.filter(is_session_actor_line)
        .map(strip_stream_tag, output_type=Types.STRING())
    )

    def _sink(topic: str) -> Any:
        record_serializer = (
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(kafka_bootstrap)
            .set_record_serializer(record_serializer)
            .build()
        )

    enriched_ds.sink_to(_sink(enriched_topic))
    session_ds.sink_to(_sink(session_topic))
    return env


def submit_remote_job(
    *,
    detached: bool = True,
    wait: bool = True,
    wait_for_running: bool = True,
) -> str:
    """Submit Phase 1.5 actor classify job via ``flink run`` from JobManager."""
    from flink_cluster_submit import DEFAULT_PYTHONPATH, ensure_remote_job, flink_run_py

    bootstrap_runtime()
    script = Path(__file__).resolve()
    root = script.parent
    # Flat /opt/flink mounts in compose
    if (Path("/opt/flink") / "cowrie_actor_classify_agent.py").is_file():
        root = Path("/opt/flink")

    pyfiles = _phase15_pyfiles(root)
    if not any(p.name == "cowrie_actor_classify_agent.py" for p in pyfiles):
        raise FileNotFoundError(
            f"Missing actor classify agent module under {root}"
        )

    job_name = _env("FLINK_JOB_NAME", DEFAULT_JOB_NAME)

    def _submit() -> str:
        job_id, _output = flink_run_py(
            script,
            pyfiles=pyfiles,
            detached=detached,
            env={
                "PYTHONPATH": DEFAULT_PYTHONPATH,
                "KAFKA_BOOTSTRAP_SERVERS": _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                "KAFKA_NORMALIZED_TOPIC": _env("KAFKA_NORMALIZED_TOPIC", "cowrie.normalized"),
                "KAFKA_NORMALIZED_ENRICHED_TOPIC": _env(
                    "KAFKA_NORMALIZED_ENRICHED_TOPIC", "cowrie.normalized.enriched"
                ),
                "KAFKA_SESSION_ACTOR_TOPIC": _env(
                    "KAFKA_SESSION_ACTOR_TOPIC", "cowrie.session_actor"
                ),
                "KAFKA_GROUP_ID": _env("KAFKA_GROUP_ID", "cowrie-actor-classify-flink"),
                "KAFKA_AUTO_OFFSET_RESET": _env("KAFKA_AUTO_OFFSET_RESET", "latest"),
                "COWRIE_ACTOR_CLASSIFY": _env("COWRIE_ACTOR_CLASSIFY", "1"),
                "COWRIE_ACTOR_TIMING_THRESHOLD_SEC": _env(
                    "COWRIE_ACTOR_TIMING_THRESHOLD_SEC", "1.7"
                ),
                "COWRIE_INJECTION_CATALOG": _env(
                    "COWRIE_INJECTION_CATALOG",
                    "/opt/flink/cowrie-config/injections/catalog.json",
                ),
            },
        )
        return job_id

    return ensure_remote_job(
        job_name,
        _submit,
        wait=wait,
        wait_for_running=wait_for_running,
    )


def main() -> None:
    """Entry point for ``flink run -py cowrie_actor_classify_job.py``."""
    bootstrap_runtime()
    env = build_actor_classify_pipeline()
    env.execute(_env("FLINK_JOB_NAME", DEFAULT_JOB_NAME))


if __name__ == "__main__":
    main()
