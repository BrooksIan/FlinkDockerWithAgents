"""Deterministic workflow agent — polls REST APIs and emits normalized events."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

_INPUT_EVENT = InputEvent.EVENT_TYPE
_EVENT_TYPE = "api.fetch.result"
_WRAPPER_KEYS = ("items", "results", "data", "records", "rows", "hits", "entries")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _payload_from_input(event: Event) -> dict[str, Any]:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        # Pipeline records arrive wrapped as {"key", "value": {...}}; unwrap them.
        if isinstance(payload.get("value"), dict):
            return payload["value"]
        if isinstance(payload.get("v"), dict):
            return payload["v"]
        return payload
    if payload is None:
        return {}
    return {"value": payload}


def _record_payload(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return item
    return {"value": item}


def _extract_items(data: Any) -> tuple[list[Any], str | None]:
    """Return list items and optional wrapper key from varied API JSON."""
    if data is None:
        return [], None
    if isinstance(data, list):
        return data, None
    if isinstance(data, dict):
        for key in _WRAPPER_KEYS:
            nested = data.get(key)
            if isinstance(nested, list):
                return nested, key
        return [data], None
    return [{"value": data}], None


class ApiFetchAgent(Agent):
    """Poll REST APIs (one fetch per input event), normalize JSON, emit structured events."""

    @tool
    @staticmethod
    def normalize_api_response(
        data: Any,
        *,
        url: str,
        fetched_at: str | None = None,
    ) -> list[dict[str, Any]]:
        """Turn API JSON into uniform record objects for downstream pipelines."""
        timestamp = fetched_at or _utc_now()
        items, wrapper = _extract_items(data)
        records: list[dict[str, Any]] = []
        for index, item in enumerate(items):
            record: dict[str, Any] = {
                "index": index,
                "payload": _record_payload(item),
                "source_url": url,
                "fetched_at": timestamp,
            }
            if wrapper:
                record["wrapper"] = wrapper
            records.append(record)
        return records

    @tool
    @staticmethod
    def fetch_api(input_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Call the configured HTTP endpoint and return parsed JSON."""
        from ratatoskr.designer.api_fetch_settings import fetch_with_settings, resolve_api_fetch_settings_from_body

        settings = resolve_api_fetch_settings_from_body(input_payload)
        result = fetch_with_settings(settings, input_payload=input_payload or {})
        return {
            "url": result.get("url"),
            "status_code": result.get("status_code"),
            "ok": result.get("ok"),
            "data": result.get("data"),
            "error": result.get("error"),
            "http_method": settings.normalized_method(),
        }

    @staticmethod
    def _build_output(
        *,
        payload: dict[str, Any],
        fetched: dict[str, Any],
        fetched_at: str,
    ) -> dict[str, Any]:
        url = str(fetched.get("url") or "")
        records = ApiFetchAgent.normalize_api_response(
            fetched.get("data"),
            url=url,
            fetched_at=fetched_at,
        )
        return {
            "agent": "workflow_api_fetch",
            "event_type": _EVENT_TYPE,
            "input": payload,
            "url": url,
            "http_method": fetched.get("http_method"),
            "status_code": fetched.get("status_code"),
            "ok": fetched.get("ok"),
            "fetched_at": fetched_at,
            "record_count": len(records),
            "records": records,
            "error": fetched.get("error"),
        }

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        payload = _payload_from_input(event)
        fetched_at = _utc_now()
        expand_records = bool(payload.get("expand_records"))
        try:
            fetched = ApiFetchAgent.fetch_api(payload)
            output = ApiFetchAgent._build_output(
                payload=payload,
                fetched=fetched,
                fetched_at=fetched_at,
            )
        except ValueError as exc:
            output = {
                "agent": "workflow_api_fetch",
                "event_type": _EVENT_TYPE,
                "input": payload,
                "ok": False,
                "fetched_at": fetched_at,
                "record_count": 0,
                "records": [],
                "error": str(exc),
            }

        if expand_records and output.get("records"):
            for record in output["records"]:
                ctx.send_event(
                    OutputEvent(
                        output={
                            "agent": output["agent"],
                            "event_type": output["event_type"],
                            "ok": output.get("ok"),
                            "url": output.get("url"),
                            "http_method": output.get("http_method"),
                            "status_code": output.get("status_code"),
                            "fetched_at": output.get("fetched_at"),
                            "record": record,
                            "error": output.get("error"),
                        }
                    )
                )
            return

        ctx.send_event(OutputEvent(output=output))
