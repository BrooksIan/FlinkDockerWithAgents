"""Agent internal graph introspection for drill-down views."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from apemosyne.agents.registry import AgentRegistryError, get_agent_spec
from apemosyne.runs.plan import agent_execution_plan


def agent_graph(name: str) -> dict[str, Any]:
    """Return normalized nodes/edges for an agent's internal action/tool graph."""
    try:
        spec = get_agent_spec(name)
    except AgentRegistryError as exc:
        raise AgentRegistryError(str(exc)) from exc

    plan_steps = agent_execution_plan(name)
    if plan_steps:
        return _graph_from_plan(name, plan_steps)

    if spec.flink_yaml:
        graph = _graph_from_flink_yaml(spec.flink_yaml)
        if graph["nodes"]:
            return graph

    return {
        "agent": name,
        "nodes": [],
        "edges": [],
        "note": "No internal graph available for this agent",
    }


def _graph_from_plan(agent: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for step in steps:
        node_id = f"{step['kind']}_{step['name']}"
        nodes.append(
            {
                "id": node_id,
                "kind": step["kind"],
                "name": step["name"],
                "description": step.get("description", ""),
            }
        )
        parent = step.get("parent")
        if parent:
            parent_id = next(
                (f"{s['kind']}_{s['name']}" for s in steps if s["name"] == parent),
                f"action_{parent}",
            )
            edges.append(
                {
                    "id": f"{parent_id}->{node_id}",
                    "source": parent_id,
                    "target": node_id,
                }
            )
    return {"agent": agent, "nodes": nodes, "edges": edges, "source": "plan"}


def _graph_from_flink_yaml(yaml_path: str) -> dict[str, Any]:
    from apemosyne.paths import project_root

    path = Path(yaml_path)
    if not path.is_absolute():
        path = project_root() / yaml_path
    if not path.is_file():
        return {"agent": "", "nodes": [], "edges": [], "note": f"YAML not found: {yaml_path}"}

    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    agents = data.get("agents") or []
    if not agents:
        return {"agent": "", "nodes": [], "edges": []}

    agent_def = agents[0]
    agent_name = agent_def.get("name", "")
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    for action in agent_def.get("actions") or []:
        action_name = action.get("name", "")
        action_id = f"action_{action_name}"
        nodes.append(
            {
                "id": action_id,
                "kind": "action",
                "name": action_name,
                "description": action.get("function", ""),
            }
        )
        for tool in agent_def.get("tools") or []:
            tool_name = tool.get("name", "")
            tool_id = f"tool_{tool_name}"
            if not any(n["id"] == tool_id for n in nodes):
                nodes.append(
                    {
                        "id": tool_id,
                        "kind": "tool",
                        "name": tool_name,
                        "description": tool.get("function", ""),
                    }
                )
            edges.append(
                {
                    "id": f"{action_id}->{tool_id}",
                    "source": action_id,
                    "target": tool_id,
                }
            )

    nodes.append({"id": "output_OutputEvent", "kind": "output", "name": "OutputEvent", "description": ""})
    for action in agent_def.get("actions") or []:
        action_id = f"action_{action.get('name', '')}"
        edges.append(
            {
                "id": f"{action_id}->output_OutputEvent",
                "source": action_id,
                "target": "output_OutputEvent",
            }
        )

    return {"agent": agent_name, "nodes": nodes, "edges": edges, "source": "flink_yaml"}
