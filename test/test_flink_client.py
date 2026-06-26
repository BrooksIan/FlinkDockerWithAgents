#!/usr/bin/env python3
"""Flink REST client port fallback tests."""

from __future__ import annotations

import os


def test_rest_ports_prefers_studio_before_env() -> None:
    os.environ["FLINK_REST_PORT"] = "8081"
    from apemosyne.api import flink_client

    ports = flink_client._rest_ports_to_try()
    assert ports[0] == 8082
    assert 8081 in ports


def test_get_job_falls_back_to_studio_port(monkeypatch) -> None:
    os.environ["FLINK_REST_PORT"] = "8081"
    from apemosyne.api import flink_client

    calls: list[int] = []

    def fake_fetch(path: str, *, rest_port: int) -> dict:
        calls.append(rest_port)
        if rest_port == 8081:
            raise flink_client.FlinkUnavailableError("connection refused")
        if path.endswith("/jobs/abc"):
            return {"jid": "abc", "state": "RUNNING", "name": "test"}
        raise AssertionError(path)

    monkeypatch.setattr(flink_client, "_fetch_one", fake_fetch)
    body = flink_client.get_job("abc")
    assert body["jid"] == "abc"
    assert calls == [8082, 8081] or calls == [8082]


if __name__ == "__main__":
    test_rest_ports_prefers_studio_before_env()
    print("OK")
