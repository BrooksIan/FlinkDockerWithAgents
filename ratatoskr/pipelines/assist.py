"""LLM-assisted Studio pipeline generation from structured user intent."""

from __future__ import annotations

import copy
import json
import uuid
from typing import Any

from ratatoskr.agents.registry import list_agent_names
from ratatoskr.pipelines.models import Pipeline, pipeline_from_dict
from ratatoskr.pipelines.service import yggdrasil_event_pipeline_template
from ratatoskr.pipelines.validate import validate_pipeline

_AGENT_EDGE_MAPPINGS: dict[tuple[str, str], dict[str, str]] = {
    ("workflow_counter", "react_echo"): {"message": "$.doubled"},
    ("session_detect", "react_echo"): {"message": "$.severity"},
}

_VALID_NODE_KINDS = {"source", "window", "agent", "sink"}
_MAX_NODES = 6


def pipeline_assist_context(*, root: Any | None = None) -> dict[str, Any]:
    from pathlib import Path

    from ratatoskr.agents.catalog import load_agent_catalog
    from ratatoskr.kafka_sources import known_pipeline_topics, list_kafka_sources
    from ratatoskr.paths import project_root

    repo = root or project_root()
    catalog = load_agent_catalog(root=repo if isinstance(repo, Path) else None, validate=False)

    agents: list[dict[str, Any]] = []
    for category in catalog.categories:
        for sub in category.subcategories:
            for entry in sub.agents:
                agents.append(
                    {
                        "manifest": entry.manifest,
                        "display_name": entry.display_name,
                        "description": entry.description,
                        "category": category.id,
                        "input_schema": entry.input_schema,
                        "output_schema": entry.output_schema,
                        "tags": list(entry.tags),
                    }
                )

    kafka = list_kafka_sources()
    return {
        "agents": agents,
        "registered_agents": list_agent_names(root=repo if isinstance(repo, Path) else None),
        "kafka_topics": known_pipeline_topics(),
        "kafka_reachable": kafka.get("reachable"),
        "templates": {
            "counter_then_echo": _counter_echo_template(),
            "yggdrasil_event_pipeline": yggdrasil_event_pipeline_template(),
            "session_window": _session_window_template(),
            "session_detect_cowrie": _session_detect_template(),
        },
        "edge_mappings": {
            f"{src}->{tgt}": mapping for (src, tgt), mapping in _AGENT_EDGE_MAPPINGS.items()
        },
        "rules": {
            "linear_chain_only": True,
            "max_nodes": _MAX_NODES,
            "exactly_one_source_and_sink": True,
            "at_most_one_window": True,
            "node_kinds": sorted(_VALID_NODE_KINDS),
        },
    }


def normalize_intent(body: dict[str, Any]) -> dict[str, Any]:
    goal = str(body.get("goal") or "").strip()
    if not goal:
        raise ValueError("Goal is required")

    domain = str(body.get("domain") or "auto").strip().lower()
    if domain not in ("auto", "cowrie_security", "numeric_transform", "generic"):
        domain = "auto"

    source_type = str(body.get("source_type") or "records").strip().lower()
    if source_type not in ("records", "kafka"):
        source_type = "records"

    sink_type = str(body.get("sink_type") or "capture").strip().lower()
    if sink_type not in ("capture", "kafka"):
        sink_type = "capture"

    preference = str(body.get("preference") or "balanced").strip().lower()
    if preference not in ("fast", "balanced", "deep"):
        preference = "balanced"

    react_policy = str(body.get("react_policy") or "none").strip().lower()
    if react_policy not in ("none", "all", "high_severity_only"):
        react_policy = "none"

    use_windowing = bool(body.get("use_windowing"))
    if source_type == "kafka":
        use_windowing = True
    use_react = bool(body.get("use_react_enrichment")) or react_policy != "none"

    return {
        "goal": goal,
        "pipeline_name": str(body.get("pipeline_name") or "").strip(),
        "domain": domain,
        "source_type": source_type,
        "source_topic": str(body.get("source_topic") or "").strip(),
        "use_windowing": use_windowing,
        "window_key_field": str(body.get("window_key_field") or "key").strip() or "key",
        "window_gap_policy": str(body.get("window_gap_policy") or "default").strip() or "default",
        "workflow_agent": str(body.get("workflow_agent") or "auto").strip() or "auto",
        "use_react_enrichment": use_react,
        "react_agent": str(body.get("react_agent") or "auto").strip() or "auto",
        "react_policy": react_policy if use_react else "none",
        "sink_type": sink_type,
        "sink_topic": str(body.get("sink_topic") or "").strip(),
        "preference": preference,
        "use_llm": body.get("use_llm", True) is not False,
        "agent_creation_mode": _normalize_agent_creation_mode(body.get("agent_creation_mode")),
    }


def _normalize_agent_creation_mode(raw: Any) -> str:
    mode = str(raw or "suggest").strip().lower()
    if mode in ("existing_only", "suggest", "auto_create"):
        return mode
    return "suggest"


def _counter_echo_template() -> dict[str, Any]:
    return {
        "name": "Counter then Echo",
        "nodes": [
            {
                "id": "src1",
                "kind": "source",
                "config": {
                    "source_type": "records",
                    "records": [{"key": "1", "value": 3}, {"key": "2", "value": 10}],
                },
            },
            {"id": "agent_wc", "kind": "agent", "agent": "workflow_counter"},
            {"id": "agent_re", "kind": "agent", "agent": "react_echo"},
            {"id": "sink1", "kind": "sink", "config": {"sink_type": "capture"}},
        ],
        "edges": [
            {"id": "e1", "source": "src1", "target": "agent_wc"},
            {"id": "e2", "source": "agent_wc", "target": "agent_re", "mapping": {"message": "$.doubled"}},
            {"id": "e3", "source": "agent_re", "target": "sink1"},
        ],
        "layout": {
            "src1": {"x": 80, "y": 200},
            "agent_wc": {"x": 320, "y": 200},
            "agent_re": {"x": 560, "y": 200},
            "sink1": {"x": 800, "y": 200},
        },
    }


def _session_window_template() -> dict[str, Any]:
    return {
        "name": "Session window",
        "nodes": [
            {
                "id": "src1",
                "kind": "source",
                "config": {
                    "source_type": "records",
                    "records": [
                        {"key": "user-a", "value": 1, "timestamp": 100},
                        {"key": "user-a", "value": 2, "timestamp": 101},
                        {"key": "user-b", "value": 10, "timestamp": 200},
                    ],
                },
            },
            {
                "id": "win1",
                "kind": "window",
                "config": {
                    "window_type": "dynamic_session",
                    "key_field": "key",
                    "gap_policy": "default",
                    "gap_ms": 1000,
                    "time_mode": "processing",
                    "execution_mode": "logic",
                },
            },
            {"id": "agent_wc", "kind": "agent", "agent": "workflow_counter"},
            {"id": "sink1", "kind": "sink", "config": {"sink_type": "capture"}},
        ],
        "edges": [
            {"id": "e1", "source": "src1", "target": "win1"},
            {"id": "e2", "source": "win1", "target": "agent_wc"},
            {"id": "e3", "source": "agent_wc", "target": "sink1"},
        ],
        "layout": {
            "src1": {"x": 80, "y": 200},
            "win1": {"x": 280, "y": 200},
            "agent_wc": {"x": 480, "y": 200},
            "sink1": {"x": 680, "y": 200},
        },
    }


def _session_detect_template() -> dict[str, Any]:
    template = copy.deepcopy(yggdrasil_event_pipeline_template())
    template["nodes"] = [n for n in template["nodes"] if n.get("agent") != "react_echo"]
    template["edges"] = [
        e for e in template["edges"] if e.get("target") != "agent_re" and e.get("source") != "agent_re"
    ]
    template["edges"][-1]["source"] = "agent_sd"
    template["name"] = "Session detect (Cowrie)"
    template["layout"].pop("agent_re", None)
    return template


def _resolve_agent(name: str, *, fallback: str, known: set[str]) -> str:
    choice = (name or "auto").strip()
    if choice == "auto" or choice not in known:
        return fallback
    return choice


def _apply_intent_to_template(template: dict[str, Any], intent: dict[str, Any], *, known: set[str]) -> dict[str, Any]:
    pipeline = copy.deepcopy(template)
    if intent.get("pipeline_name"):
        pipeline["name"] = intent["pipeline_name"]
    elif intent.get("goal"):
        pipeline["name"] = intent["goal"][:120]

    for node in pipeline["nodes"]:
        if node.get("kind") == "source":
            config = dict(node.get("config") or {})
            if intent["source_type"] == "kafka":
                config["source_type"] = "kafka"
                config["topic"] = intent["source_topic"] or config.get("topic") or "workflow.test.input"
                config.setdefault("max_records", 10)
                config.pop("records", None)
            else:
                config["source_type"] = "records"
                config.setdefault(
                    "records",
                    [{"key": "1", "value": 3}, {"key": "2", "value": 10}],
                )
            node["config"] = config
        elif node.get("kind") == "window":
            config = dict(node.get("config") or {})
            config["key_field"] = intent["window_key_field"]
            config["gap_policy"] = intent["window_gap_policy"]
            node["config"] = config
        elif node.get("kind") == "agent":
            if node.get("agent") in ("session_detect", "workflow_counter") and intent["workflow_agent"] != "auto":
                node["agent"] = _resolve_agent(
                    intent["workflow_agent"],
                    fallback=node["agent"],
                    known=known,
                )
            if node.get("agent") == "react_echo" and intent["react_agent"] != "auto":
                node["agent"] = _resolve_agent(
                    intent["react_agent"],
                    fallback="react_echo",
                    known=known,
                )
        elif node.get("kind") == "sink":
            config = dict(node.get("config") or {})
            if intent["sink_type"] == "kafka":
                config["sink_type"] = "kafka"
                config["topic"] = intent["sink_topic"] or config.get("topic") or "workflow.test.output"
            else:
                config["sink_type"] = "capture"
                config.pop("topic", None)
            node["config"] = config

    agent_names = [n["agent"] for n in pipeline["nodes"] if n.get("kind") == "agent" and n.get("agent")]
    for index in range(len(agent_names) - 1):
        src_agent = agent_names[index]
        tgt_agent = agent_names[index + 1]
        mapping = _AGENT_EDGE_MAPPINGS.get((src_agent, tgt_agent))
        if not mapping:
            continue
        for edge in pipeline["edges"]:
            src_id = next(n["id"] for n in pipeline["nodes"] if n.get("agent") == src_agent)
            tgt_id = next(n["id"] for n in pipeline["nodes"] if n.get("agent") == tgt_agent)
            if edge.get("source") == src_id and edge.get("target") == tgt_id:
                edge["mapping"] = mapping

    return pipeline


def build_baseline_pipeline(intent: dict[str, Any], *, root: Any | None = None) -> dict[str, Any]:
    from ratatoskr.paths import project_root

    repo = root or project_root()
    known = set(list_agent_names(root=repo))
    context = pipeline_assist_context(root=repo)
    templates = context["templates"]

    domain = intent["domain"]
    goal_lower = intent["goal"].lower()

    if domain == "auto":
        if any(word in goal_lower for word in ("cowrie", "session", "security", "honeypot", "alert")):
            domain = "cowrie_security"
        elif any(word in goal_lower for word in ("double", "numeric", "counter", "transform", "echo")):
            domain = "numeric_transform"

    if domain == "cowrie_security" or (
        intent["use_windowing"] and intent["window_key_field"] in ("src_ip", "session", "key")
    ):
        if intent["use_react_enrichment"]:
            template = templates["yggdrasil_event_pipeline"]
        else:
            template = templates["session_detect_cowrie"]
    elif domain == "numeric_transform" or intent["use_react_enrichment"]:
        template = templates["counter_then_echo"]
    elif intent["use_windowing"]:
        template = templates["session_window"]
    else:
        template = {
            "name": intent.get("pipeline_name") or "New pipeline",
            "nodes": [
                {
                    "id": "src1",
                    "kind": "source",
                    "config": {
                        "source_type": intent["source_type"],
                        **(
                            {"topic": intent["source_topic"] or "workflow.test.input", "max_records": 10}
                            if intent["source_type"] == "kafka"
                            else {"records": [{"key": "1", "value": 3}]}
                        ),
                    },
                },
                {
                    "id": "agent1",
                    "kind": "agent",
                    "agent": _resolve_agent(intent["workflow_agent"], fallback="workflow_counter", known=known),
                },
                {
                    "id": "sink1",
                    "kind": "sink",
                    "config": (
                        {"sink_type": "kafka", "topic": intent["sink_topic"] or "workflow.test.output"}
                        if intent["sink_type"] == "kafka"
                        else {"sink_type": "capture"}
                    ),
                },
            ],
            "edges": [
                {"id": "e1", "source": "src1", "target": "agent1"},
                {"id": "e2", "source": "agent1", "target": "sink1"},
            ],
            "layout": {
                "src1": {"x": 80, "y": 200},
                "agent1": {"x": 320, "y": 200},
                "sink1": {"x": 560, "y": 200},
            },
        }

    pipeline = _apply_intent_to_template(template, intent, known=known)
    pipeline, _ = ensure_kafka_dynamic_window(pipeline, intent=intent)
    return pipeline


def _is_kafka_source_node(node: dict[str, Any]) -> bool:
    if node.get("kind") != "source":
        return False
    config = node.get("config") or {}
    return str(config.get("source_type") or "").strip().lower() == "kafka"


def ensure_kafka_dynamic_window(
    pipeline: dict[str, Any],
    *,
    intent: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    """Insert a dynamic session window after a Kafka source when one is not wired in."""
    from ratatoskr.pipelines.window_config import default_window_config

    nodes = list(pipeline.get("nodes") or [])
    edges = list(pipeline.get("edges") or [])
    layout = dict(pipeline.get("layout") or {})

    sources = [n for n in nodes if _is_kafka_source_node(n)]
    if not sources:
        return pipeline, False

    source = sources[0]
    source_id = str(source["id"])
    by_id = {str(n["id"]): n for n in nodes}
    outgoing = {str(e["source"]): str(e["target"]) for e in edges}

    target_id = outgoing.get(source_id)
    if target_id and by_id.get(target_id, {}).get("kind") == "window":
        window = by_id[target_id]
        config = {**default_window_config(), **dict(window.get("config") or {})}
        if intent:
            config["key_field"] = intent.get("window_key_field") or config["key_field"]
            config["gap_policy"] = intent.get("window_gap_policy") or config["gap_policy"]
        window["config"] = config
        return {**pipeline, "nodes": nodes, "edges": edges, "layout": layout}, False

    window_config = default_window_config()
    if intent:
        window_config["key_field"] = intent.get("window_key_field") or window_config["key_field"]
        window_config["gap_policy"] = intent.get("window_gap_policy") or window_config["gap_policy"]
        if intent.get("domain") == "cowrie_security":
            window_config["key_field"] = intent.get("window_key_field") or "src_ip"
            window_config["gap_policy"] = "session_detect"

    existing_windows = [n for n in nodes if n.get("kind") == "window"]
    if existing_windows:
        win_id = str(existing_windows[0]["id"])
        existing_windows[0]["config"] = {**window_config, **dict(existing_windows[0].get("config") or {})}
    else:
        win_id = "win1" if "win1" not in by_id else f"win_{uuid.uuid4().hex[:6]}"
        nodes.append({"id": win_id, "kind": "window", "config": window_config})

    if not target_id:
        for kind in ("agent", "sink"):
            candidates = [n for n in nodes if n.get("kind") == kind]
            if candidates:
                target_id = str(candidates[0]["id"])
                break

    if not target_id or target_id == win_id:
        return {**pipeline, "nodes": nodes, "edges": edges, "layout": layout}, False

    new_edges = [e for e in edges if str(e.get("source")) != source_id]
    new_edges.append({"id": f"e_{uuid.uuid4().hex[:8]}", "source": source_id, "target": win_id})
    if outgoing.get(win_id) != target_id:
        new_edges = [e for e in new_edges if str(e.get("source")) != win_id]
        new_edges.append({"id": f"e_{uuid.uuid4().hex[:8]}", "source": win_id, "target": target_id})

    src_layout = layout.get(source_id, {"x": 80.0, "y": 200.0})
    tgt_layout = layout.get(target_id, {"x": 320.0, "y": 200.0})
    layout[win_id] = {
        "x": (float(src_layout.get("x", 80.0)) + float(tgt_layout.get("x", 320.0))) / 2.0,
        "y": float(src_layout.get("y", 200.0)),
    }

    return {**pipeline, "nodes": nodes, "edges": new_edges, "layout": layout}, True


def _default_layout(nodes: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    layout: dict[str, dict[str, float]] = {}
    x = 80.0
    for node in nodes:
        layout[node["id"]] = {"x": x, "y": 200.0}
        x += 220.0
    return layout


def _sanitize_pipeline(pipeline: dict[str, Any], *, known: set[str]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    for node in pipeline.get("nodes") or []:
        kind = str(node.get("kind") or "").strip()
        if kind not in _VALID_NODE_KINDS:
            continue
        node_id = str(node.get("id") or f"n_{uuid.uuid4().hex[:8]}")
        entry: dict[str, Any] = {"id": node_id, "kind": kind, "config": dict(node.get("config") or {})}
        if kind == "agent":
            agent = str(node.get("agent") or "").strip()
            if agent not in known:
                continue
            entry["agent"] = agent
        nodes.append(entry)

    if len(nodes) > _MAX_NODES:
        nodes = nodes[:_MAX_NODES]

    node_ids = {n["id"] for n in nodes}
    edges: list[dict[str, Any]] = []
    for edge in pipeline.get("edges") or []:
        source = str(edge.get("source") or "")
        target = str(edge.get("target") or "")
        if source not in node_ids or target not in node_ids:
            continue
        mapping = edge.get("mapping") if isinstance(edge.get("mapping"), dict) else {}
        edges.append(
            {
                "id": str(edge.get("id") or f"e_{uuid.uuid4().hex[:8]}"),
                "source": source,
                "target": target,
                "mapping": mapping,
            }
        )

    layout = dict(pipeline.get("layout") or {})
    for node in nodes:
        layout.setdefault(node["id"], {"x": 80.0, "y": 200.0})
    if not layout:
        layout = _default_layout(nodes)

    name = str(pipeline.get("name") or "Untitled pipeline").strip()[:120] or "Untitled pipeline"
    return {"name": name, "nodes": nodes, "edges": edges, "layout": layout}


def _validate_pipeline_payload(
    pipeline: dict[str, Any],
    *,
    extra_known_agents: set[str] | None = None,
) -> dict[str, Any]:
    model = pipeline_from_dict({"id": "pipe_assist_draft", **pipeline})
    return validate_pipeline(model, extra_known_agents=extra_known_agents)


def normalize_pipeline_proposal(
    proposal: dict[str, Any],
    *,
    intent: dict[str, Any],
    baseline: dict[str, Any] | None = None,
    root: Any | None = None,
    extra_warnings: list[str] | None = None,
    extra_known_agents: set[str] | None = None,
    skip_baseline_fallback: bool = False,
) -> dict[str, Any]:
    from ratatoskr.paths import project_root

    repo = root or project_root()
    known = set(list_agent_names(root=repo))
    if extra_known_agents:
        known |= extra_known_agents

    pipeline_body = proposal.get("pipeline") if isinstance(proposal.get("pipeline"), dict) else proposal
    if not isinstance(pipeline_body, dict) or not pipeline_body.get("nodes"):
        pipeline_body = baseline or build_baseline_pipeline(intent, root=repo)

    merged = copy.deepcopy(baseline or pipeline_body)
    for key in ("name", "nodes", "edges", "layout"):
        if pipeline_body.get(key):
            merged[key] = pipeline_body[key]
    if intent.get("pipeline_name"):
        merged["name"] = intent["pipeline_name"]

    merged, injected_window = ensure_kafka_dynamic_window(merged, intent=intent)
    pipeline = _sanitize_pipeline(merged, known=known)
    validation = _validate_pipeline_payload(pipeline, extra_known_agents=extra_known_agents)

    warnings = [str(w) for w in (proposal.get("warnings") or []) if str(w).strip()]
    if injected_window:
        warnings.append("Added a dynamic session window after the Kafka source.")
    if extra_warnings:
        warnings.extend(str(w) for w in extra_warnings if str(w).strip())
    if intent["react_policy"] == "high_severity_only":
        warnings.append("ReAct enrichment is intended for high-severity records only at runtime.")
    if not validation["valid"] and baseline and not skip_baseline_fallback:
        pipeline = _sanitize_pipeline(baseline, known=known)
        validation = _validate_pipeline_payload(pipeline, extra_known_agents=extra_known_agents)
        warnings.append("LLM draft failed validation; applied rule-based baseline instead.")

    rationale = str(proposal.get("rationale") or "").strip()
    if not rationale:
        rationale = f"Built a linear pipeline for: {intent['goal']}"

    return {
        "pipeline": pipeline,
        "rationale": rationale,
        "warnings": warnings,
        "validation": validation,
    }


def _system_prompt() -> str:
    return (
        "You are an expert Ratatoskr Studio pipeline assistant. "
        "Return a single JSON object with keys: pipeline, rationale, warnings. "
        "pipeline must include: name, nodes, edges, layout. "
        "Each node: id, kind (source|window|agent|sink), config, and agent when kind=agent. "
        "Each edge: id, source, target, optional mapping (JSONPath like $.field). "
        "Rules: exactly one source and one sink, linear chain only, at most one window, "
        "at most 6 nodes, use only registered agents from context. "
        "When source_type is kafka, always include a dynamic_session window node directly after the source. "
        "Prefer existing templates when they fit. "
        "Respond with JSON only, no markdown fences."
    )


def _user_prompt(intent: dict[str, Any], context: dict[str, Any], baseline: dict[str, Any]) -> str:
    return (
        f"Build or refine a Ratatoskr Studio pipeline.\n\n"
        f"User goal:\n{intent['goal']}\n\n"
        f"Structured intent:\n{json.dumps(intent, indent=2)}\n\n"
        f"Rule-based baseline draft:\n{json.dumps(baseline, indent=2)}\n\n"
        f"Platform context:\n{json.dumps(context, indent=2)}"
    )


def generate_pipeline_assist(body: dict[str, Any], *, root: Any | None = None) -> dict[str, Any]:
    from ratatoskr.designer.llm_client import LlmNotConfiguredError, chat_completion_json
    from ratatoskr.paths import project_root
    from ratatoskr.pipelines.agent_factory import (
        reused_agents_from_pipeline,
        suggest_missing_agents,
    )

    repo = root or project_root()
    intent = normalize_intent(body)
    baseline = build_baseline_pipeline(intent, root=repo)
    context = pipeline_assist_context(root=repo)

    if not intent.get("use_llm"):
        result = normalize_pipeline_proposal(
            {"pipeline": baseline, "rationale": f"Rule-based draft for: {intent['goal']}", "warnings": []},
            intent=intent,
            baseline=baseline,
            root=repo,
        )
    else:
        try:
            raw = chat_completion_json(
                system=_system_prompt(),
                user=_user_prompt(intent, context, baseline),
            )
            result = normalize_pipeline_proposal(raw, intent=intent, baseline=baseline, root=repo)
        except LlmNotConfiguredError:
            result = normalize_pipeline_proposal(
                {
                    "pipeline": baseline,
                    "rationale": f"Rule-based draft for: {intent['goal']}",
                    "warnings": ["Designer LLM is not configured; returned a deterministic baseline draft."],
                },
                intent=intent,
                baseline=baseline,
                root=repo,
            )

    return _attach_agent_suggestions(result, intent=intent, baseline=baseline, context=context, root=repo)


def build_pipeline_assist(body: dict[str, Any], *, root: Any | None = None) -> dict[str, Any]:
    from ratatoskr.paths import project_root
    from ratatoskr.pipelines.agent_factory import (
        apply_agent_overrides,
        publish_approved_suggestions,
        reused_agents_from_pipeline,
        suggest_missing_agents,
    )

    repo = root or project_root()
    intent = normalize_intent(body)
    approved = list(body.get("approved_suggestions") or [])
    baseline = build_baseline_pipeline(intent, root=repo)
    context = pipeline_assist_context(root=repo)

    created_agents: list[dict[str, str]] = []
    pipeline_source = baseline
    warnings: list[str] = []
    extra_known_agents: set[str] = set()

    if approved:
        manifest_by_suggestion = publish_approved_suggestions(approved, root=repo)
        pipeline_source = apply_agent_overrides(baseline, approved, manifest_by_suggestion)
        extra_known_agents = set(manifest_by_suggestion.values())
        for suggestion in approved:
            suggestion_id = str(suggestion.get("suggestion_id") or "")
            manifest = manifest_by_suggestion.get(suggestion_id)
            if manifest:
                created_agents.append(
                    {
                        "suggestion_id": suggestion_id,
                        "manifest": manifest,
                        "display_name": str(suggestion.get("display_name") or manifest),
                    }
                )
        warnings.append(f"Created and published {len(created_agents)} new agent(s) for this pipeline.")

    result = normalize_pipeline_proposal(
        {
            "pipeline": pipeline_source,
            "rationale": f"Built pipeline for: {intent['goal']}",
            "warnings": warnings,
        },
        intent=intent,
        baseline=baseline,
        root=repo,
        extra_known_agents=extra_known_agents,
        skip_baseline_fallback=bool(approved),
    )
    result["created_agents"] = created_agents
    result["suggested_agents"] = suggest_missing_agents(intent, result["pipeline"], context, root=repo)
    result["reused_agents"] = reused_agents_from_pipeline(result["pipeline"])
    result["agent_creation_mode"] = intent["agent_creation_mode"]
    return result


def _attach_agent_suggestions(
    result: dict[str, Any],
    *,
    intent: dict[str, Any],
    baseline: dict[str, Any],
    context: dict[str, Any],
    root: Any,
) -> dict[str, Any]:
    from ratatoskr.pipelines.agent_factory import reused_agents_from_pipeline, suggest_missing_agents

    pipeline = result.get("pipeline") or baseline
    suggestions = suggest_missing_agents(intent, pipeline, context, root=root)
    result["suggested_agents"] = suggestions
    result["reused_agents"] = reused_agents_from_pipeline(pipeline)
    result["agent_creation_mode"] = intent["agent_creation_mode"]
    if suggestions and intent["agent_creation_mode"] == "suggest":
        result["warnings"] = list(result.get("warnings") or []) + [
            f"{len(suggestions)} new agent(s) suggested — review and approve before creating."
        ]
    return result


def assist_result_to_dict(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "pipeline": result.get("pipeline") or {},
        "rationale": result.get("rationale") or "",
        "warnings": result.get("warnings") or [],
        "validation": result.get("validation") or {"valid": False, "errors": [], "warnings": []},
        "suggested_agents": result.get("suggested_agents") or [],
        "reused_agents": result.get("reused_agents") or [],
        "created_agents": result.get("created_agents") or [],
        "agent_creation_mode": result.get("agent_creation_mode") or "suggest",
    }


__all__ = [
    "assist_result_to_dict",
    "build_baseline_pipeline",
    "build_pipeline_assist",
    "ensure_kafka_dynamic_window",
    "generate_pipeline_assist",
    "normalize_intent",
    "normalize_pipeline_proposal",
    "pipeline_assist_context",
]
