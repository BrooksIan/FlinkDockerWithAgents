"""Deterministic workflow agent — monitor Cloudera Manager and recommend fixes."""

from __future__ import annotations

import os
from typing import Any

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

_INPUT_EVENT = InputEvent.EVENT_TYPE


def _payload_from_input(event: Event) -> dict[str, Any]:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        if isinstance(payload.get("value"), dict):
            return payload["value"]
        return payload
    return {}


class CMMonitorAgent(Agent):
    """
    Workflow agent for Cloudera Manager health monitoring.

    Recommend-only: classifies CM health and emits structured fix recommendations.
    No CM mutations are performed.
    """

    @tool
    @staticmethod
    def get_cluster_health_status(cluster_name: str | None = None) -> dict[str, Any]:
        """Poll Cloudera Manager cluster health."""
        from ratatoskr.cm.client import CMClient

        return CMClient().get_cluster_health_snapshot(cluster_name)

    @tool
    @staticmethod
    def classify_and_recommend(health: dict[str, Any]) -> dict[str, Any]:
        """Classify severities and build fix recommendations."""
        from ratatoskr.cm.policy import build_recommendations, classify_health

        classification = classify_health(health)
        recommendations = build_recommendations(health, classification)
        return {
            "classification": classification,
            "recommendations": recommendations,
        }

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.cm.client import CMClient
        from ratatoskr.cm.env import cm_cluster
        from ratatoskr.cm.policy import run_monitor_cycle

        payload = _payload_from_input(event)
        cluster = payload.get("cluster") or os.environ.get("CM_CLUSTER") or cm_cluster()
        client = CMClient(cluster=str(cluster) if cluster else "")
        result = run_monitor_cycle(client, cluster=str(cluster) if cluster else None)
        ctx.send_event(OutputEvent(output=result))
