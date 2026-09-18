"""Cowrie log processor — bytecode core with ReAct prompt-input fix."""

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


def _react_prompt_input_row(log_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the Agents ``from_list`` row so Prompt ``{log_entry}`` is substituted.

    LocalRunner only forwards ``data["v"]`` / ``data["value"]`` into InputEvent.
    That value must be a **dict** (not ``json.dumps``): ReActAgent.start_action
    then calls ``prompt.format_messages(**row)``. A JSON string takes the primitive
    path (``input=<string>``), leaves ``{log_entry}`` literal, and Cloudera never
    sees the Cowrie event — hence guardrail-only "No reasoning provided".

    ``_build_react_input_payload`` already includes ``log_entry`` + ``honeypot_context``.
    """
    payload = _build_react_input_payload(log_entry)  # type: ignore[name-defined]
    if not isinstance(payload, dict):
        payload = {"log_entry": json.dumps(log_entry)}
    elif "log_entry" not in payload:
        payload = dict(payload)
        payload["log_entry"] = json.dumps(log_entry)
    return {"v": payload}


def _process_cloudera_react_fixed(
    self,
    log_entry: Dict[str, Any],
    stream_env: Any = None,
) -> Optional[Dict[str, Any]]:
    """Same as bytecode ``_process_cloudera_react`` with corrected Prompt input keys."""
    from react_dashboard_bridge import build_react_dashboard_alert

    mod = getattr(self, "_demo_cloudera_mod", None)
    if mod is None:
        return self._process_workflow_with_env(log_entry, stream_env)

    try:
        # Always use a fresh env: LocalExecutionEnvironment allows from_list only once,
        # and reusing self.agents_env can keep a prior stringified ``v`` payload so
        # ``{log_entry}`` never substitutes.
        from flink_agents.api.execution_environment import AgentsExecutionEnvironment

        agents_env = AgentsExecutionEnvironment.get_execution_environment(env=stream_env)

        mod.register_all_cloudera_react_tools(agents_env)
        react_agent = mod.create_cloudera_react_agent(agents_env)

        input_row = _react_prompt_input_row(log_entry)
        builder = agents_env.from_list(input=[input_row]).apply(react_agent)
        results = builder.to_list()
        agents_env.execute()
    except Exception as exc:
        print(f"⚠️  Cloudera ReAct setup/execution failed ({exc}); using workflow for this event.")
        return self._process_workflow_with_env(log_entry, stream_env)

    if not results:
        return None

    tr = getattr(mod, "ThreatAnalysisResult", None)

    def _unwrap_output(result_event: Any) -> Any:
        """LocalRunner ``to_list()`` yields ``{key: output}`` dicts, not OutputEvent."""
        if hasattr(result_event, "output"):
            return result_event.output
        if isinstance(result_event, dict):
            # Prefer ThreatAnalysisResult / analysis-shaped values over the key wrapper.
            if tr is not None:
                for val in result_event.values():
                    if isinstance(val, tr):
                        return val
            if "reasoning" in result_event or "threat_type" in result_event:
                return result_event
            if len(result_event) == 1:
                return next(iter(result_event.values()))
        return result_event

    for result_event in results:
        output = _unwrap_output(result_event)
        if tr is not None and isinstance(output, tr):
            alert = build_react_dashboard_alert(log_entry, output)
            if alert:
                return _canonical_alert_envelope(alert=alert, log_entry=log_entry)  # type: ignore[name-defined]
            return None
        if isinstance(output, dict):
            alert = build_react_dashboard_alert(log_entry, output)
            if alert:
                return _canonical_alert_envelope(alert=alert, log_entry=log_entry)  # type: ignore[name-defined]
            return None
    return None


# Install fixed method on the recovered class.
if "CowrieLogProcessor" in globals():
    CowrieLogProcessor._process_cloudera_react = _process_cloudera_react_fixed  # type: ignore[misc,name-defined]
