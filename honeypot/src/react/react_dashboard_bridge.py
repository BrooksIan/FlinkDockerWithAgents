"""Bridge ReAct analysis into dashboard alerts — bytecode core + reasoning enrich."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any, Dict, Optional


def _boot() -> None:
    shim_paths = (
        Path("/opt/flink/_pyc_shim.py"),
        Path(__file__).resolve().parents[1] / "_pyc_shim.py",
    )
    for shim_path in shim_paths:
        if not shim_path.is_file():
            continue
        spec = importlib.util.spec_from_file_location("_pyc_shim", shim_path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.load_sibling_pyc(globals())
        return
    raise ImportError("Cannot load _pyc_shim.py for bytecode-backed module")


_boot()


def _synthesize_reasoning(log_entry: Optional[Dict[str, Any]], analysis: Any) -> str:
    """Build readable reasoning when the LLM omits the field."""
    data: Dict[str, Any] = {}
    if hasattr(analysis, "model_dump"):
        try:
            data = analysis.model_dump()  # type: ignore[assignment]
        except Exception:
            data = {}
    elif isinstance(analysis, dict):
        data = analysis

    existing = str(data.get("reasoning") or "").strip()
    if existing and not existing.startswith("No reasoning provided"):
        return existing

    log_entry = log_entry or {}
    eventid = str(log_entry.get("eventid") or "")
    src = str(log_entry.get("src_ip") or data.get("source_ip") or "unknown")
    cmd = str(log_entry.get("input") or log_entry.get("command") or "")
    severity = str(data.get("severity") or "UNKNOWN")
    threat = str(data.get("threat_type") or "UNKNOWN")
    bits = [
        f"Policy classified `{eventid or 'event'}` from `{src}` as {severity} {threat}.",
    ]
    if cmd:
        bits.append(f"Command: `{cmd[:160]}{'…' if len(cmd) > 160 else ''}`.")
    desc = str(data.get("description") or "").strip()
    if desc and desc not in bits[0]:
        bits.append(desc[:200])
    return " ".join(bits)


_orig_coerce = coerce_threat_analysis_payload  # type: ignore[name-defined]
_orig_build = build_react_dashboard_alert  # type: ignore[name-defined]


def coerce_threat_analysis_payload(data):  # type: ignore[no-redef]
    out = _orig_coerce(data)
    if isinstance(out, dict):
        reasoning = str(out.get("reasoning") or "").strip()
        if not reasoning or reasoning.startswith("No reasoning provided"):
            # Leave placeholder; build_react_dashboard_alert enriches with log context.
            out["reasoning"] = reasoning or "No reasoning provided"
    return out


def _unwrap_analysis(analysis: Any) -> Any:
    """LocalRunner ``to_list()`` yields ``{key: ThreatAnalysisResult}`` dicts."""
    if analysis is None:
        return analysis
    if hasattr(analysis, "model_dump") or (
        isinstance(analysis, dict) and ("reasoning" in analysis or "threat_type" in analysis)
    ):
        return analysis
    if isinstance(analysis, dict) and len(analysis) == 1:
        return next(iter(analysis.values()))
    return analysis


def _analysis_reasoning(analysis: Any) -> str:
    analysis = _unwrap_analysis(analysis)
    if isinstance(analysis, dict):
        return str(analysis.get("reasoning") or "").strip()
    if hasattr(analysis, "model_dump"):
        try:
            dumped = analysis.model_dump()
            if isinstance(dumped, dict) and dumped.get("reasoning"):
                return str(dumped.get("reasoning") or "").strip()
        except Exception:
            pass
    return str(getattr(analysis, "reasoning", "") or "").strip()


def build_react_dashboard_alert(log_entry, analysis, force_emit=False, **kwargs):  # type: ignore[no-redef]
    analysis = _unwrap_analysis(analysis)
    # Keep real LLM reasoning; only synthesize when the model omitted it.
    llm_reasoning = _analysis_reasoning(analysis)
    synthesized = _synthesize_reasoning(log_entry if isinstance(log_entry, dict) else None, analysis)
    fill = llm_reasoning if llm_reasoning and not llm_reasoning.startswith("No reasoning provided") else synthesized

    if isinstance(analysis, dict):
        raw = str(analysis.get("reasoning") or "").strip()
        if not raw or raw.startswith("No reasoning provided"):
            analysis = dict(analysis)
            analysis["reasoning"] = fill
    elif hasattr(analysis, "reasoning"):
        raw = str(getattr(analysis, "reasoning", "") or "").strip()
        if not raw or raw.startswith("No reasoning provided"):
            try:
                analysis.reasoning = fill
            except Exception:
                pass

    alert = _orig_build(log_entry, analysis, force_emit=force_emit, **kwargs)
    if not isinstance(alert, dict):
        return alert

    ad = alert.get("attack_details")
    if not isinstance(ad, dict):
        ad = {}
        alert["attack_details"] = ad

    val = str(ad.get("react_reasoning") or alert.get("reasoning") or "").strip()
    if not val or val.startswith("No reasoning provided"):
        suffix = ""
        if "[guardrail:" in val:
            suffix = " " + val[val.index("[guardrail:") :]
        ad["react_reasoning"] = fill + suffix
        alert["reasoning"] = ad["react_reasoning"]
    elif llm_reasoning and val.startswith("Policy classified"):
        # Prefer real LLM text over our synthetic fallback if guardrail annotated it away.
        suffix = ""
        if "[guardrail:" in val:
            suffix = " " + val[val.index("[guardrail:") :]
        ad["react_reasoning"] = llm_reasoning + suffix
        alert["reasoning"] = ad["react_reasoning"]
    return alert
