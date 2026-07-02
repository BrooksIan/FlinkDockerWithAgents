"""LLM-assisted agent definition generation and refinement."""

from __future__ import annotations

import copy
import json
import uuid
from typing import Any

from ratatoskr.designer.definitions.models import agent_definition_from_dict
from ratatoskr.designer.definitions.seed import double_value_definition_payload
from ratatoskr.designer.definitions.validate import validate_agent_definition
from ratatoskr.designer.llm_client import LlmNotConfiguredError, chat_completion_json
from ratatoskr.designer.skills_catalog import skill_catalog_for_api
from ratatoskr.paths import project_root
from ratatoskr.tools.builtins import list_builtin_tools

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
_BUILTIN_TOOL_NAMES = {tool["name"] for tool in list_builtin_tools()}


def _react_template() -> dict[str, Any]:
    return {
        "name": "New ReAct agent",
        "type": "react",
        "description": "ReAct agent with LLM prompt.",
        "catalog_category_id": "react",
        "catalog_subcategory_id": "numeric",
        "catalog_tags": ["custom", "llm-assisted"],
        "input_schema": {
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": {"type": "string", "description": "User or upstream message"},
            },
        },
        "output_schema": {
            "type": "object",
            "properties": {
                "message": {"type": "string"},
                "result": {"type": "string"},
                "reasoning": {"type": "string"},
                "agent": {"type": "string"},
            },
        },
        "nodes": [
            {"id": "in1", "kind": "input_event", "name": "InputEvent", "config": {"event_type": "_input_event"}},
            {"id": "act1", "kind": "action", "name": "process", "config": {"listens_to": ["_input_event"]}},
            {
                "id": "prompt1",
                "kind": "prompt",
                "name": "prompt",
                "config": {
                    "template": "assist",
                    "system": "You are a helpful agent. Respond with JSON only.",
                    "user": "{message}",
                },
            },
            {"id": "llm1", "kind": "llm_call", "name": "llm", "config": {"use_platform_llm": True, "mode": "simple"}},
            {"id": "out1", "kind": "output_event", "name": "OutputEvent", "config": {"event_type": "_output_event"}},
        ],
        "edges": [
            {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
            {"id": "e2", "source": "act1", "target": "prompt1", "kind": "calls"},
            {"id": "e3", "source": "act1", "target": "llm1", "kind": "calls"},
            {"id": "e4", "source": "act1", "target": "out1", "kind": "emits"},
        ],
        "layout": {
            "in1": {"x": 80, "y": 200},
            "act1": {"x": 320, "y": 200},
            "prompt1": {"x": 560, "y": 120},
            "llm1": {"x": 560, "y": 200},
            "out1": {"x": 560, "y": 280},
        },
    }


def _workflow_template() -> dict[str, Any]:
    payload = double_value_definition_payload()
    payload.pop("id", None)
    payload.pop("manifest_name", None)
    payload.pop("status", None)
    payload["name"] = "New workflow agent"
    payload["description"] = "Deterministic workflow agent."
    payload["catalog_tags"] = ["custom", "llm-assisted"]
    return payload


def assist_design_context(*, root: Any | None = None) -> dict[str, Any]:
    from pathlib import Path

    repo = root or project_root()
    mcp_instances: list[dict[str, Any]] = []
    try:
        from ratatoskr.mcp.instances import list_mcp_instances_api

        mcp_instances = list_mcp_instances_api().get("instances", [])
    except Exception:
        mcp_instances = []

    enabled_mcp = [
        {
            "instance_id": inst.get("instance_id"),
            "display_name": inst.get("display_name"),
            "catalog_id": inst.get("catalog_id"),
        }
        for inst in mcp_instances
        if inst.get("enabled")
    ]

    return {
        "builtin_tools": list_builtin_tools(),
        "skills": skill_catalog_for_api(root=repo if isinstance(repo, Path) else None),
        "mcp_instances": enabled_mcp,
        "node_kinds": sorted(_VALID_NODE_KINDS),
        "edge_kinds": sorted(_VALID_EDGE_KINDS),
        "examples": {
            "workflow": _workflow_template(),
            "react": _react_template(),
        },
    }


def _system_prompt() -> str:
    return (
        "You are an expert Flink Agents designer assistant. "
        "Return a single JSON object with keys: definition, rationale, test_records, warnings. "
        "definition must include: name, type (workflow|react), description, nodes, edges, layout, "
        "input_schema, output_schema, catalog_category_id, catalog_subcategory_id, catalog_tags, mcp_servers. "
        "Each node: id, kind, name, config. Each edge: id, source, target, kind. "
        "Workflow agents need exactly one input_event, one action, one output_event, and at least one tool or mcp_tool. "
        "ReAct agents need input_event, action, output_event, prompt, and llm_call nodes. "
        "Use only allowed node kinds and edge kinds from the provided context. "
        "For workflow tool nodes use tool_ref in double|scale|identity and a safe expression like value * 2. "
        "For ReAct prompt nodes set config.system and config.user with {message} and {value} placeholders. "
        "test_records is an array of sample input objects. warnings is an array of assumption strings. "
        "Respond with JSON only, no markdown fences."
    )


def _choose_base_template(agent_type: str, preference: str | None) -> dict[str, Any]:
    pref = (preference or "auto").strip().lower()
    if pref == "react":
        return copy.deepcopy(_react_template())
    if pref == "workflow":
        return copy.deepcopy(_workflow_template())
    if agent_type == "react":
        return copy.deepcopy(_react_template())
    return copy.deepcopy(_workflow_template())


def _merge_definition(base: dict[str, Any], proposal: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key in (
        "name",
        "type",
        "description",
        "input_schema",
        "output_schema",
        "catalog_category_id",
        "catalog_subcategory_id",
        "catalog_tags",
        "mcp_servers",
    ):
        if key in proposal and proposal[key] is not None:
            merged[key] = proposal[key]

    if proposal.get("nodes"):
        merged["nodes"] = proposal["nodes"]
    if proposal.get("edges"):
        merged["edges"] = proposal["edges"]
    if proposal.get("layout"):
        merged["layout"] = proposal["layout"]
    return merged


def _sanitize_nodes(nodes: list[dict[str, Any]], enabled_mcp: set[str]) -> list[dict[str, Any]]:
    clean: list[dict[str, Any]] = []
    for node in nodes:
        kind = str(node.get("kind") or "").strip()
        if kind not in _VALID_NODE_KINDS:
            continue
        node_id = str(node.get("id") or f"n_{uuid.uuid4().hex[:8]}")
        config = dict(node.get("config") or {})
        if kind == "tool":
            tool_ref = str(config.get("tool_ref") or "double")
            if tool_ref not in _BUILTIN_TOOL_NAMES:
                tool_ref = "double"
            config["tool_ref"] = tool_ref
            config.setdefault("expression", "value * 2")
        if kind == "mcp_tool":
            server_ref = str(config.get("server_ref") or "").strip()
            if enabled_mcp and server_ref not in enabled_mcp:
                config["server_ref"] = next(iter(enabled_mcp))
            config.setdefault("tool_name", str(config.get("tool_name") or "check_ip"))
            config.setdefault("arg_name", "ip")
        if kind == "action":
            listens = config.get("listens_to")
            if not isinstance(listens, list) or not listens:
                config["listens_to"] = ["_input_event"]
        clean.append(
            {
                "id": node_id,
                "kind": kind,
                "name": str(node.get("name") or kind),
                "config": config,
            }
        )
    return clean


def _sanitize_edges(edges: list[dict[str, Any]], node_ids: set[str]) -> list[dict[str, Any]]:
    clean: list[dict[str, Any]] = []
    for edge in edges:
        kind = str(edge.get("kind") or "listens_to")
        if kind not in _VALID_EDGE_KINDS:
            kind = "listens_to"
        source = str(edge.get("source") or "")
        target = str(edge.get("target") or "")
        if source not in node_ids or target not in node_ids:
            continue
        clean.append(
            {
                "id": str(edge.get("id") or f"e_{uuid.uuid4().hex[:8]}"),
                "source": source,
                "target": target,
                "kind": kind,
            }
        )
    return clean


def _default_layout(nodes: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    layout: dict[str, dict[str, float]] = {}
    x = 80
    for index, node in enumerate(nodes):
        layout[node["id"]] = {"x": float(x), "y": 200.0 + float((index % 3) * 80)}
        x += 240
    return layout


def _ensure_graph_minimums(definition: dict[str, Any]) -> dict[str, Any]:
    """Fill missing required nodes/edges using template defaults when LLM output is sparse."""
    nodes = definition.get("nodes") or []
    edges = definition.get("edges") or []
    kinds = {n["kind"] for n in nodes}
    agent_type = definition.get("type") or "workflow"

    template = _react_template() if agent_type == "react" else _workflow_template()
    if "input_event" not in kinds or "action" not in kinds or "output_event" not in kinds:
        return copy.deepcopy(template) | {
            "name": definition.get("name") or template["name"],
            "description": definition.get("description") or template["description"],
            "input_schema": definition.get("input_schema") or template["input_schema"],
            "output_schema": definition.get("output_schema") or template["output_schema"],
        }

    if agent_type == "react" and "prompt" not in kinds:
        prompt = next(n for n in template["nodes"] if n["kind"] == "prompt")
        nodes.append(copy.deepcopy(prompt))
    if agent_type == "react" and "llm_call" not in kinds:
        llm = next(n for n in template["nodes"] if n["kind"] == "llm_call")
        nodes.append(copy.deepcopy(llm))
    if agent_type == "workflow" and "tool" not in kinds and "mcp_tool" not in kinds:
        tool = next(n for n in template["nodes"] if n["kind"] == "tool")
        nodes.append(copy.deepcopy(tool))

    definition["nodes"] = nodes
    node_ids = {n["id"] for n in nodes}
    if not edges:
        tmpl_edges = template["edges"]
        definition["edges"] = [e for e in tmpl_edges if e["source"] in node_ids and e["target"] in node_ids]
    if not definition.get("layout"):
        definition["layout"] = _default_layout(nodes)
    return definition


def normalize_proposal(
    proposal: dict[str, Any],
    *,
    preference: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ctx = context or assist_design_context()
    enabled_mcp = {
        str(inst.get("instance_id"))
        for inst in ctx.get("mcp_instances", [])
        if inst.get("instance_id")
    }

    definition = proposal.get("definition") if "definition" in proposal else proposal
    if not isinstance(definition, dict):
        raise ValueError("LLM response missing definition object")
    if not definition.get("nodes") and not definition.get("name") and not definition.get("type"):
        raise ValueError("LLM response missing definition object")

    agent_type = str(definition.get("type") or "workflow").strip().lower()
    if agent_type not in ("workflow", "react"):
        agent_type = "workflow"

    base = _choose_base_template(agent_type, preference)
    merged = _merge_definition(base, definition)
    merged["type"] = agent_type

    nodes = _sanitize_nodes(list(merged.get("nodes") or []), enabled_mcp)
    if not nodes:
        nodes = _sanitize_nodes(list(base.get("nodes") or []), enabled_mcp)
    merged["nodes"] = nodes

    node_ids = {n["id"] for n in nodes}
    edges = _sanitize_edges(list(merged.get("edges") or []), node_ids)
    merged["edges"] = edges

    mcp_servers = merged.get("mcp_servers") or []
    if isinstance(mcp_servers, list) and enabled_mcp:
        merged["mcp_servers"] = [s for s in mcp_servers if str(s) in enabled_mcp]

    if not merged.get("layout"):
        merged["layout"] = base.get("layout") or _default_layout(nodes)

    merged = _ensure_graph_minimums(merged)
    merged["name"] = str(merged.get("name") or "Untitled agent").strip()[:120] or "Untitled agent"
    merged["description"] = str(merged.get("description") or "").strip()
    merged.setdefault("catalog_tags", ["llm-assisted"])
    merged.setdefault("input_schema", base.get("input_schema") or {})
    merged.setdefault("output_schema", base.get("output_schema") or {})

    test_records = proposal.get("test_records")
    if not isinstance(test_records, list):
        test_records = _default_test_records(merged)

    warnings = proposal.get("warnings")
    if not isinstance(warnings, list):
        warnings = []

    rationale = str(proposal.get("rationale") or "").strip()

    draft_id = f"def_{uuid.uuid4().hex[:12]}"
    full = {
        "id": draft_id,
        "version": 1,
        "status": "draft",
        **merged,
    }
    model = agent_definition_from_dict(full)
    validation = validate_agent_definition(model)

    return {
        "definition": _definition_to_create_payload(full),
        "rationale": rationale,
        "test_records": test_records,
        "warnings": [str(w) for w in warnings],
        "validation": validation,
    }


def _definition_to_create_payload(defn: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": defn.get("name"),
        "type": defn.get("type"),
        "description": defn.get("description"),
        "nodes": defn.get("nodes") or [],
        "edges": defn.get("edges") or [],
        "layout": defn.get("layout") or {},
        "input_schema": defn.get("input_schema") or {},
        "output_schema": defn.get("output_schema") or {},
        "catalog_category_id": defn.get("catalog_category_id"),
        "catalog_subcategory_id": defn.get("catalog_subcategory_id"),
        "catalog_tags": list(defn.get("catalog_tags") or []),
        "mcp_servers": list(defn.get("mcp_servers") or []),
    }


def _default_test_records(defn: dict[str, Any]) -> list[dict[str, Any]]:
    if defn.get("type") == "react":
        props = (defn.get("input_schema") or {}).get("properties") or {}
        if "message" in props:
            return [
                {"key": "1", "message": "Please process value 7", "value": 7},
                {"key": "2", "message": "Double the number 21", "value": 21},
            ]
    return [{"key": "1", "value": 3}, {"key": "2", "value": 10}]


def _user_prompt_for_generate(
    goal: str,
    *,
    preference: str | None,
    constraints: dict[str, Any] | None,
    context: dict[str, Any],
) -> str:
    parts = [
        f"Design a new Flink agent for this goal:\n{goal.strip()}",
        f"Agent type preference: {preference or 'auto'}",
    ]
    if constraints:
        parts.append(f"Constraints: {json.dumps(constraints, indent=2)}")
    parts.append(f"Designer context: {json.dumps(context, indent=2)}")
    return "\n\n".join(parts)


def _user_prompt_for_refine(
    definition: dict[str, Any],
    instruction: str,
    *,
    context: dict[str, Any],
) -> str:
    return (
        f"Refine this existing agent definition according to the instruction.\n\n"
        f"Instruction:\n{instruction.strip()}\n\n"
        f"Current definition:\n{json.dumps(definition, indent=2)}\n\n"
        f"Designer context:\n{json.dumps(context, indent=2)}"
    )


def generate_agent_definition(
    goal: str,
    *,
    agent_type_preference: str | None = None,
    constraints: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not goal.strip():
        raise ValueError("Goal is required")
    context = assist_design_context()
    user = _user_prompt_for_generate(
        goal,
        preference=agent_type_preference,
        constraints=constraints,
        context=context,
    )
    raw = chat_completion_json(system=_system_prompt(), user=user)
    return normalize_proposal(raw, preference=agent_type_preference, context=context)


def refine_agent_definition(
    definition: dict[str, Any],
    instruction: str,
    *,
    agent_type_preference: str | None = None,
) -> dict[str, Any]:
    if not instruction.strip():
        raise ValueError("Refinement instruction is required")
    context = assist_design_context()
    user = _user_prompt_for_refine(definition, instruction, context=context)
    raw = chat_completion_json(system=_system_prompt(), user=user)
    preference = agent_type_preference or definition.get("type")
    return normalize_proposal(raw, preference=preference, context=context)


def assist_result_to_dict(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "definition": result.get("definition"),
        "rationale": result.get("rationale") or "",
        "test_records": result.get("test_records") or [],
        "warnings": result.get("warnings") or [],
        "validation": result.get("validation") or {"valid": False, "errors": [], "warnings": []},
    }


__all__ = [
    "LlmNotConfiguredError",
    "assist_design_context",
    "assist_result_to_dict",
    "generate_agent_definition",
    "normalize_proposal",
    "refine_agent_definition",
]
