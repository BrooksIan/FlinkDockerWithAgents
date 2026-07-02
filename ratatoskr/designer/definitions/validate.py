"""Agent definition graph validation."""

from __future__ import annotations

from typing import Any

from ratatoskr.designer.definitions.models import AgentDefinition, AgentDefinitionNode

_VALID_NODE_KINDS = {
    "input_event",
    "action",
    "tool",
    "mcp_tool",
    "output_event",
    "prompt",
    "llm_call",
}
_VALID_EDGE_KINDS = {"listens_to", "calls", "emits"}


def _node_label(node: AgentDefinitionNode) -> str:
    name = node.name.strip()
    return name or node.id


def _issue(
    message: str,
    *,
    level: str = "error",
    node_id: str | None = None,
    edge_id: str | None = None,
) -> dict[str, Any]:
    return {
        "message": message,
        "level": level,
        "node_id": node_id,
        "edge_id": edge_id,
    }


def _finalize(
    errors: list[str],
    warnings: list[str],
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "issues": issues,
    }


def validate_agent_definition(definition: AgentDefinition) -> dict[str, Any]:
    """Return {valid, errors, warnings, issues}."""
    errors: list[str] = []
    warnings: list[str] = []
    issues: list[dict[str, Any]] = []

    def add_error(
        message: str,
        *,
        node_id: str | None = None,
        edge_id: str | None = None,
    ) -> None:
        errors.append(message)
        issues.append(_issue(message, level="error", node_id=node_id, edge_id=edge_id))

    def add_warning(
        message: str,
        *,
        node_id: str | None = None,
        edge_id: str | None = None,
    ) -> None:
        warnings.append(message)
        issues.append(_issue(message, level="warning", node_id=node_id, edge_id=edge_id))

    if not definition.name.strip():
        add_error("Definition name is required")
    if definition.type not in ("workflow", "react"):
        add_error(f"Unknown agent type {definition.type!r}")

    node_ids = {n.id for n in definition.nodes}
    nodes_by_id = {n.id: n for n in definition.nodes}
    if len(node_ids) != len(definition.nodes):
        add_error("Duplicate node ids in graph")

    for node in definition.nodes:
        if node.kind not in _VALID_NODE_KINDS:
            add_error(
                f"{kind_label(node)} has unknown kind {node.kind!r}",
                node_id=node.id,
            )
        if not node.name.strip() and node.kind not in ("input_event", "output_event"):
            add_warning(f"{kind_label(node)} has no name", node_id=node.id)

    for edge in definition.edges:
        if edge.kind not in _VALID_EDGE_KINDS:
            add_error(
                f"Edge {edge.id!r} has unknown kind {edge.kind!r}",
                edge_id=edge.id,
            )
        if edge.source not in node_ids:
            add_error(
                f"Edge references unknown source {edge.source!r}",
                edge_id=edge.id,
            )
        if edge.target not in node_ids:
            add_error(
                f"Edge references unknown target {edge.target!r}",
                edge_id=edge.id,
            )

    if errors:
        return _finalize(errors, warnings, issues)

    inputs = [n for n in definition.nodes if n.kind == "input_event"]
    outputs = [n for n in definition.nodes if n.kind == "output_event"]
    actions = [n for n in definition.nodes if n.kind == "action"]
    tools = [n for n in definition.nodes if n.kind == "tool"]
    mcp_tools = [n for n in definition.nodes if n.kind == "mcp_tool"]

    if len(inputs) != 1:
        add_error("Agent must have exactly one input_event node")
    if len(outputs) != 1:
        add_error("Agent must have exactly one output_event node")
    if len(actions) != 1:
        add_error("Agent must have exactly one action node")

    if definition.type == "workflow" and not tools and not mcp_tools:
        add_warning("Workflow agent has no tool nodes")

    attached = set(definition.mcp_servers or [])
    for mcp_node in mcp_tools:
        config = mcp_node.config or {}
        server_ref = str(config.get("server_ref") or "").strip()
        tool_name = str(config.get("tool_name") or "").strip()
        label = _node_label(mcp_node)
        if not server_ref:
            add_error(f"MCP tool {label!r} requires a server", node_id=mcp_node.id)
        elif attached and server_ref not in attached:
            add_error(
                f"MCP tool {label!r} references {server_ref!r} which is not attached to this agent",
                node_id=mcp_node.id,
            )
        if not tool_name:
            add_error(f"MCP tool {label!r} requires a tool name", node_id=mcp_node.id)

    if definition.type == "react":
        prompts = [n for n in definition.nodes if n.kind == "prompt"]
        llm_nodes = [n for n in definition.nodes if n.kind == "llm_call"]
        if not prompts and not llm_nodes:
            add_warning("ReAct agent has no prompt or llm_call nodes")
        if llm_nodes:
            from ratatoskr.designer.skills_catalog import react_llm_config

            llm_config = react_llm_config(llm_nodes[0])
            llm_id = llm_nodes[0].id
            if llm_config["mode"] == "flink_skills":
                if not llm_config["skills"]:
                    add_error(
                        "Flink skills mode requires at least one skill on the LLM node",
                        node_id=llm_id,
                    )
                if not llm_config["allowed_commands"]:
                    add_error(
                        "Flink skills mode requires allowed_commands on the LLM node "
                        "(select skills to auto-fill defaults)",
                        node_id=llm_id,
                    )
                if llm_config["use_platform_llm"] is False:
                    add_warning(
                        "Flink skills mode ignores use_platform_llm=false; native chat model is required",
                        node_id=llm_id,
                    )

    if errors:
        return _finalize(errors, warnings, issues)

    action = actions[0]
    input_node = inputs[0]
    output_node = outputs[0]
    action_label = _node_label(action)

    listens = [
        e for e in definition.edges if e.kind == "listens_to" and e.target == action.id
    ]
    if not listens:
        add_error(f"Action {action_label!r} must listen_to an input_event", node_id=action.id)
    elif not any(e.source == input_node.id for e in listens):
        add_error(
            f"Action {action_label!r} must listen_to the input_event node",
            node_id=action.id,
        )

    emits = [e for e in definition.edges if e.kind == "emits" and e.source == action.id]
    if not emits:
        add_error(f"Action {action_label!r} must emit to an output_event", node_id=action.id)
    elif not any(e.target == output_node.id for e in emits):
        add_error(
            f"Action {action_label!r} must emit to the output_event node",
            node_id=action.id,
        )

    tool_adjacency: dict[str, list[str]] = {n.id: [] for n in definition.nodes}
    for edge in definition.edges:
        if edge.kind == "calls":
            tool_adjacency.setdefault(edge.source, []).append(edge.target)

    if _has_cycle(tool_adjacency, list(node_ids)):
        add_error("Agent graph contains a cycle")

    for tool in tools + mcp_tools:
        callers = [
            e.source
            for e in definition.edges
            if e.kind == "calls" and e.target == tool.id
        ]
        if not callers:
            add_warning(
                f"Tool {_node_label(tool)!r} is not called by any action",
                node_id=tool.id,
            )

    if not definition.input_schema:
        add_warning("input_schema is empty")
    if not definition.output_schema:
        add_warning("output_schema is empty")

    return _finalize(errors, warnings, issues)


def kind_label(node: AgentDefinitionNode) -> str:
    name = node.name.strip()
    if name:
        return f"{name} ({node.kind})"
    return node.id


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
