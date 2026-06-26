#!/usr/bin/env python3
"""Tests for Studio Kafka compose wiring."""

from __future__ import annotations

from pathlib import Path


def test_compose_file_kafka_profile() -> None:
    from apemosyne.constants import KAFKA_PROFILE
    from apemosyne.docker_utils import COMPOSE_KAFKA, compose_file, project_root

    path = compose_file(KAFKA_PROFILE)
    assert path == project_root() / COMPOSE_KAFKA
    assert path.is_file()


def test_studio_kafka_port_default() -> None:
    from apemosyne.kafka_sources import STUDIO_KAFKA_EXTERNAL_PORT, kafka_bootstrap_candidates

    assert STUDIO_KAFKA_EXTERNAL_PORT == 9094
    assert kafka_bootstrap_candidates()[0] == "localhost:9094"


def test_kafka_compose_declares_broker_port() -> None:
    text = (Path(__file__).resolve().parents[1] / "docker-compose.kafka.yml").read_text()
    assert "9094" in text
    assert "workflow.test.input" in text
    assert "workflow.test.output" in text


if __name__ == "__main__":
    test_compose_file_kafka_profile()
    test_studio_kafka_port_default()
    test_kafka_compose_declares_broker_port()
    print("OK")
