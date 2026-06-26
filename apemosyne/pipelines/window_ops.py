"""PyFlink window operators — domain-neutral defaults."""

from __future__ import annotations

from typing import Any, Iterable

from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import SessionWindowTimeGapExtractor

from apemosyne.pipelines.window_policies import default_summarize, resolve_gap_policy


class FixedGapExtractor(SessionWindowTimeGapExtractor):
    """Constant inactivity gap for every event (``default`` policy)."""

    def __init__(self, gap_ms: int = 1_000) -> None:
        self._gap_ms = max(1, int(gap_ms))

    def extract(self, element: dict[str, Any]) -> int:
        return self._gap_ms


class PolicyGapExtractor(SessionWindowTimeGapExtractor):
    """Dispatch per-event gap to a named policy (e.g. optional ``session_detect``)."""

    def __init__(self, policy: str, *, gap_ms: int = 1_000) -> None:
        self._gap_fn = resolve_gap_policy(policy, gap_ms=gap_ms)

    def extract(self, element: dict[str, Any]) -> int:
        return self._gap_fn(element)


class GenericSessionSummaryFunction(ProcessWindowFunction):
    """Emit a generic session summary when a dynamic window closes."""

    def __init__(self, *, key_field: str = "key", gap_policy: str = "default") -> None:
        self._key_field = key_field
        self._gap_policy = gap_policy

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        events = list(elements)
        if not events:
            return
        if self._gap_policy == "session_detect":
            from examples.agents.session_window_policy import summarize_session

            yield summarize_session(str(key), events)
            return
        yield default_summarize(str(key), events, key_field=self._key_field)
