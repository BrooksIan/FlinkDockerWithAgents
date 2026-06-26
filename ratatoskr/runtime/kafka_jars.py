"""Flink Kafka connector JAR helpers for PyFlink cluster jobs."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional

from ratatoskr.runtime.flink_cluster_submit import FLINK_LIB, flink_major_version


def kafka_connector_version(flink_major: Optional[str] = None) -> str:
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


def _download_maven_jar(group_path: str, artifact: str, version: str) -> Path:
    jar_path = FLINK_LIB / f"{artifact}-{version}.jar"
    url = (
        f"https://repo1.maven.org/maven2/{group_path}/"
        f"{artifact}/{version}/{artifact}-{version}.jar"
    )
    FLINK_LIB.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl", "-fsSL", url, "-o", str(jar_path)], check=True)
    return jar_path


def ensure_kafka_connector_jar(*, download: bool = True) -> Path:
    version = kafka_connector_version()
    sql_jar = FLINK_LIB / f"flink-sql-connector-kafka-{version}.jar"
    thin_jar = FLINK_LIB / f"flink-connector-kafka-{version}.jar"

    for stale in FLINK_LIB.glob("flink-*connector-kafka-*.jar"):
        if stale not in (sql_jar, thin_jar) and stale.is_file():
            stale.unlink()

    if sql_jar.is_file():
        return sql_jar
    if not download:
        raise FileNotFoundError(f"Kafka connector jar not found: {sql_jar}")

    try:
        return _download_maven_jar("org/apache/flink", "flink-sql-connector-kafka", version)
    except subprocess.CalledProcessError:
        if thin_jar.is_file():
            return thin_jar
        return _download_maven_jar("org/apache/flink", "flink-connector-kafka", version)


def kafka_jar_uris() -> list[str]:
    jar = ensure_kafka_connector_jar()
    return [f"file://{jar.resolve()}"]


def attach_kafka_jars(stream_env) -> None:
    jar_uris = kafka_jar_uris()
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
