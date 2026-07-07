"""Detect missing pipeline agents and draft Designer definitions for user approval."""

from __future__ import annotations

import copy
import re
import uuid
from typing import Any

from ratatoskr.agents.registry import list_agent_names


def _slugify(text: str, *, max_len: int = 36) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return (slug[:max_len] or "custom_agent").strip("_")


def _score_catalog_agent(goal: str, agent: dict[str, Any]) -> int:
    words = {w for w in re.findall(r"[a-z0-9]+", goal.lower()) if len(w) > 3}
    if not words:
        return 0
    haystack = " ".join(
        [
            str(agent.get("display_name") or ""),
            str(agent.get("description") or ""),
            str(agent.get("manifest") or ""),
            " ".join(agent.get("tags") or []),
        ]
    ).lower()
    return sum(1 for word in words if word in haystack)


def _agent_type_for_manifest(manifest: str, context: dict[str, Any]) -> str:
    for agent in context.get("agents") or []:
        if agent.get("manifest") == manifest:
            category = str(agent.get("category") or "")
            return "react" if category == "react" else "workflow"
    return "workflow"


def _reused_agents_from_pipeline(pipeline: dict[str, Any]) -> list[str]:
    return [
        str(node.get("agent"))
        for node in pipeline.get("nodes") or []
        if node.get("kind") == "agent" and node.get("agent")
    ]


def _derive_agent_name(goal: str, *, role: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", goal)
    chunk = " ".join(words[:6]).strip()
    if not chunk:
        chunk = "Custom agent"
    prefix = "ReAct" if role == "react" else "Workflow"
    return f"{prefix}: {chunk[:72]}"


def _draft_workflow_definition(goal: str, *, role: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.assist import _workflow_template

    draft = copy.deepcopy(_workflow_template())
    draft["name"] = _derive_agent_name(goal, role=role)
    draft["description"] = goal[:240]
    draft["catalog_category_id"] = "workflow"
    draft["catalog_subcategory_id"] = "transform"
    draft["catalog_tags"] = ["llm-assisted", "pipeline-assist", "custom"]
    return draft


def _draft_react_definition(goal: str) -> dict[str, Any]:
    from ratatoskr.designer.definitions.assist import _react_template

    draft = copy.deepcopy(_react_template())
    draft["name"] = _derive_agent_name(goal, role="react")
    draft["description"] = goal[:240]
    draft["catalog_category_id"] = "react"
    draft["catalog_subcategory_id"] = "text"
    draft["catalog_tags"] = ["llm-assisted", "pipeline-assist", "custom"]
    return draft


def _validate_definition_draft(definition: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.definitions.models import agent_definition_from_dict
    from ratatoskr.designer.definitions.validate import validate_agent_definition

    model = agent_definition_from_dict(
        {
            "id": f"def_{uuid.uuid4().hex[:12]}",
            "version": 1,
            "status": "draft",
            **definition,
        }
    )
    return validate_agent_definition(model)


def _draft_agent_definition(
    goal: str,
    *,
    role: str,
    intent: dict[str, Any],
    root: Any | None = None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    warnings: list[str] = []
    definition = _draft_react_definition(goal) if role == "react" else _draft_workflow_definition(goal, role=role)

    if intent.get("use_llm"):
        try:
            from ratatoskr.designer.definitions.assist import generate_agent_definition

            preference = "react" if role == "react" else "workflow"
            generated = generate_agent_definition(
                f"Create a {role} agent for this pipeline step: {goal}",
                agent_type_preference=preference,
            )
            definition = generated.get("definition") or definition
            warnings.extend(str(w) for w in generated.get("warnings") or [])
        except Exception as exc:
            warnings.append(f"Used rule-based agent draft ({exc}).")

    validation = _validate_definition_draft(definition)
    return definition, validation, warnings


def _workflow_needs_custom_agent(intent: dict[str, Any], assigned: str, context: dict[str, Any]) -> bool:
    if intent.get("workflow_agent") != "auto":
        return False
    if intent.get("domain") in ("cowrie_security", "numeric_transform"):
        return False

    goal = intent["goal"]
    goal_lower = goal.lower()
    if any(word in goal_lower for word in ("cowrie", "honeypot", "session detect", "brute")):
        return False
    if any(word in goal_lower for word in ("double", "counter", "numeric", "multiply")):
        return False

    workflow_agents = [a for a in context.get("agents") or [] if a.get("category") == "workflow"]
    if not workflow_agents:
        return True

    assigned_entry = next((a for a in workflow_agents if a.get("manifest") == assigned), None)
    assigned_score = _score_catalog_agent(goal, assigned_entry) if assigned_entry else 0
    best_score = max(_score_catalog_agent(goal, agent) for agent in workflow_agents)
    return best_score < 2 or assigned_score < max(best_score, 1)


def _react_needs_custom_agent(intent: dict[str, Any], assigned: str, context: dict[str, Any]) -> bool:
    if not intent.get("use_react_enrichment"):
        return False
    if intent.get("react_agent") != "auto":
        return False

    goal = intent["goal"]
    react_agents = [a for a in context.get("agents") or [] if a.get("category") == "react"]
    if not react_agents:
        return True

    assigned_entry = next((a for a in react_agents if a.get("manifest") == assigned), None)
    assigned_score = _score_catalog_agent(goal, assigned_entry) if assigned_entry else 0
    best_score = max(_score_catalog_agent(goal, agent) for agent in react_agents)

    wants_llm = intent.get("preference") == "deep" or any(
        word in goal.lower() for word in ("llm", "reason", "explain", "analyze")
    )
    if wants_llm and assigned == "react_echo":
        return True
    return best_score < 1 or assigned_score < best_score


def _agent_node_slots(pipeline: dict[str, Any]) -> list[dict[str, str]]:
    slots: list[dict[str, str]] = []
    for node in pipeline.get("nodes") or []:
        if node.get("kind") == "agent" and node.get("agent"):
            manifest = str(node["agent"])
            agent_type = "react" if manifest.startswith("react") else "workflow"
            slots.append(
                {
                    "node_id": str(node["id"]),
                    "role": agent_type,
                    "manifest": manifest,
                }
            )
    return slots


def suggest_missing_agents(
    intent: dict[str, Any],
    pipeline: dict[str, Any],
    context: dict[str, Any],
    *,
    root: Any | None = None,
) -> list[dict[str, Any]]:
    if intent.get("agent_creation_mode") == "existing_only":
        return []

    suggestions: list[dict[str, Any]] = []
    seen_roles: set[str] = set()

    for slot in _agent_node_slots(pipeline):
        role = slot["role"]
        if role in seen_roles:
            continue

        needs_custom = (
            _workflow_needs_custom_agent(intent, slot["manifest"], context)
            if role == "workflow"
            else _react_needs_custom_agent(intent, slot["manifest"], context)
        )
        if not needs_custom:
            continue

        seen_roles.add(role)
        definition, validation, draft_warnings = _draft_agent_definition(
            intent["goal"],
            role=role,
            intent=intent,
            root=root,
        )
        proposed_manifest = _slugify(definition.get("name") or intent["goal"])
        suggestions.append(
            {
                "suggestion_id": f"sug_{uuid.uuid4().hex[:10]}",
                "role": role,
                "pipeline_node_id": slot["node_id"],
                "replaces_manifest": slot["manifest"],
                "proposed_manifest": proposed_manifest,
                "display_name": definition.get("name") or proposed_manifest,
                "reason": (
                    f"No catalog {role} agent is a strong match for this goal; "
                    f"propose creating {proposed_manifest!r} instead of {slot['manifest']!r}."
                ),
                "definition": definition,
                "validation": validation,
                "warnings": draft_warnings,
                "selected_by_default": True,
            }
        )

    return suggestions


def publish_approved_suggestions(
    approved: list[dict[str, Any]],
    *,
    root: Any | None = None,
) -> dict[str, str]:
    """Publish approved suggestions. Returns suggestion_id -> manifest_name."""
    from ratatoskr.designer.definitions.service import default_agent_definition_service
    from ratatoskr.paths import project_root

    repo = root or project_root()
    service = default_agent_definition_service(root=repo)
    mapping: dict[str, str] = {}

    for suggestion in approved:
        suggestion_id = str(suggestion.get("suggestion_id") or "").strip()
        definition = suggestion.get("definition")
        if not suggestion_id or not isinstance(definition, dict):
            continue

        validation = suggestion.get("validation") or _validate_definition_draft(definition)
        if not validation.get("valid"):
            raise ValueError(
                f"Suggested agent {suggestion.get('display_name') or suggestion_id} failed validation"
            )

        created = service.create_from_payload(definition)
        service.compile(created["id"])
        published = service.publish(created["id"], root=repo)
        mapping[suggestion_id] = str(published["manifest_name"])

    return mapping


def apply_agent_overrides(
    pipeline: dict[str, Any],
    approved: list[dict[str, Any]],
    manifest_by_suggestion: dict[str, str],
) -> dict[str, Any]:
    updated = copy.deepcopy(pipeline)
    node_overrides: dict[str, str] = {}
    for suggestion in approved:
        suggestion_id = str(suggestion.get("suggestion_id") or "")
        node_id = str(suggestion.get("pipeline_node_id") or "")
        manifest = manifest_by_suggestion.get(suggestion_id)
        if node_id and manifest:
            node_overrides[node_id] = manifest

    if not node_overrides:
        return updated

    for node in updated.get("nodes") or []:
        if node.get("kind") == "agent" and node.get("id") in node_overrides:
            node["agent"] = node_overrides[node["id"]]

    return updated


__all__ = [
    "apply_agent_overrides",
    "publish_approved_suggestions",
    "reused_agents_from_pipeline",
    "suggest_missing_agents",
]

# Public alias used by assist module
reused_agents_from_pipeline = _reused_agents_from_pipeline
