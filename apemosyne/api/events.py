"""Server-sent events for dashboard live updates."""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

from apemosyne.api import flink_client, services
from apemosyne.api.config import ApiSettings


async def event_stream(
    settings: ApiSettings,
    *,
    interval_sec: float = 5.0,
) -> AsyncIterator[str]:
    """Yield SSE payloads with health and job snapshots."""
    while True:
        payload: dict = {
            "type": "snapshot",
            "health": await asyncio.to_thread(services.pipeline_health, settings),
        }
        try:
            payload["jobs"] = await asyncio.to_thread(flink_client.list_jobs)
        except flink_client.FlinkUnavailableError as exc:
            payload["jobs_error"] = str(exc)
            payload["jobs"] = []
        yield f"data: {json.dumps(payload)}\n\n"
        await asyncio.sleep(interval_sec)
