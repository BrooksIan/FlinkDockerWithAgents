"""PyFlink window operators for session aggregation (cluster runner)."""

from __future__ import annotations

from typing import Any, Iterable

from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import SessionWindowTimeGapExtractor

from examples.agents.session_window_policy import session_gap_ms, summarize_session


class CowrieActivityGapExtractor(SessionWindowTimeGapExtractor):
    """Dynamic processing-time session gap — later codegen target for workflow tools."""

    def extract(self, element: dict[str, Any]) -> int:
        return session_gap_ms(element)


class SessionSummaryFunction(ProcessWindowFunction):
    """Emit one session summary dict when a dynamic session window closes."""

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        events = list(elements)
        if not events:
            return
        yield summarize_session(str(key), events)
