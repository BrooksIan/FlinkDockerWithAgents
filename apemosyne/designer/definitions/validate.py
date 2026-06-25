"""Agent definition graph validation."""

from __future__ import annotations

from typing import Any

from apemosyne.designer.definitions.models import AgentDefinition

_VALID_NODE_KINDS = {
    "input_event",
    "action",
    "tool",
    "output_event",
    "prompt",
    "llm_call",
}
_VALID_EDGE_KINDS = {"listens_to", "calls", "emits"}


def validate_agent_definition(definition: AgentDefinition) -> dict[str, Any]:
    """Return {valid, errors, warnings}."""
    errors: list[str] = []
    warnings: list[str] = []

    if not definition.name.strip():
        errors.append("Definition name is required")
    if definition.type not in ("workflow", "react"):
        errors.append(f"Unknown agent type {definition.type!r}")

    node_ids = {n.id for n in definition.nodes}
    if len(node_ids) != len(definition.nodes):
        errors.append("Duplicate node ids in graph")

    for node in definition.nodes:
        if node.kind not in _VALID_NODE_KINDS:
            errors.append(f"Node {node.id!r} has unknown kind {node.kind!r}")
        if not node.name.strip() and node.kind not in ("input_event", "output_event"):
            warnings.append(f"Node {node.id!r} has no name")

    for edge in definition.edges:
        if edge.kind not in _VALID_EDGE_KINDS:
            errors.append(f"Edge {edge.id!r} has unknown kind {edge.kind!r}")
        if edge.source not in node_ids:
            errors.append(f"Edge {edge.id!r} references unknown source {edge.source!r}")
        if edge.target not in node_ids:
            errors.append(f"Edge {edge.id!r} references unknown target {edge.target!r}")

    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}

    inputs = [n for n in definition.nodes if n.kind == "input_event"]
    outputs = [n for n in definition.nodes if n.kind == "output_event"]
    actions = [n for n in definition.nodes if n.kind == "action"]
    tools = [n for n in definition.nodes if n.kind == "tool"]

    if len(inputs) != 1:
        errors.append("Agent must have exactly one input_event node")
    if len(outputs) != 1:
        errors.append("Agent must have exactly one output_event node")
    if len(actions) != 1:
        errors.append("Agent must have exactly one action node")

    if definition.type == "workflow" and not tools:
        warnings.append("Workflow agent has no tool nodes")

    if definition.type == "react":
        prompts = [n for n in definition.nodes if n.kind == "prompt"]
        llm_nodes = [n for n in definition.nodes if n.kind == "llm_call"]
        if not prompts and not llm_nodes:
            warnings.append("ReAct agent has no prompt or llm_call nodes")

    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}

    action = actions[0]
    input_node = inputs[0]
    output_node = outputs[0]

    listens = [
        e for e in definition.edges if e.kind == "listens_to" and e.target == action.id
    ]
    if not listens:
        errors.append(f"Action {action.id!r} must listen_to an input_event")
    elif not any(e.source == input_node.id for e in listens):
        errors.append(f"Action {action.id!r} must listen_to the input_event node")

    emits = [e for e in definition.edges if e.kind == "emits" and e.source == action.id]
    if not emits:
        errors.append(f"Action {action.id!r} must emit to an output_event")
    elif not any(e.target == output_node.id for e in emits):
        errors.append(f"Action {action.id!r} must emit to the output_event node")

    tool_adjacency: dict[str, list[str]] = {n.id: [] for n in definition.nodes}
    for edge in definition.edges:
        if edge.kind == "calls":
            tool_adjacency.setdefault(edge.source, []).append(edge.target)

    if _has_cycle(tool_adjacency, list(node_ids)):
        errors.append("Agent graph contains a cycle")

    for tool in tools:
        callers = [
            e.source
            for e in definition.edges
            if e.kind == "calls" and e.target == tool.id
        ]
        if not callers:
            warnings.append(f"Tool {tool.id!r} is not called by any action")

    if not definition.input_schema:
        warnings.append("input_schema is empty")
    if not definition.output_schema:
        warnings.append("output_schema is empty")

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def _has_cycle(adjacency: dict[str, list[str]], nodes: list[str]) -> bool:
    visited: set[str] = set()
    stack: set[str] = set()

    def visit(node: str) -> bool:
        if node in stack:
            return True
        if node in visited:
            return False
        visited.add(node)
        stack.add(node)
        for nxt in adjacency.get(node, []):
            if visit(nxt):
                return True
        stack.remove(node)
        return False

    return any(visit(n) for n in nodes)
