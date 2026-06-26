"""PyFlink codegen helpers for Studio window nodes."""

from __future__ import annotations

from ratatoskr.pipelines.window_config import (
    EXECUTION_AGENT_BRIDGE,
    GAP_POLICY_SESSION_DETECT,
    WindowNodeConfig,
    default_bridge_topic,
    parse_window_config,
)
from ratatoskr.pipelines.window_policies import GAP_POLICY_DEFAULT


def window_uses_logic_agent(pipeline, window_config: WindowNodeConfig) -> bool:
    if window_config.execution_mode == EXECUTION_AGENT_BRIDGE:
        return False
    return True


def cluster_source_block(
    *,
    source_type: str,
    records_literal: str,
    kafka_topic: str | None,
    key_field: str = "key",
) -> tuple[list[str], str]:
    """Return (imports, stream assignment)."""
    imports: list[str] = []
    if source_type == "kafka":
        imports.extend(
            [
                "from pyflink.common.serialization import SimpleStringSchema",
                "from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer",
                "from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers",
                "import json",
            ]
        )
        topic = kafka_topic or "session.window.input"
        block = f"""
_props = {{
    "bootstrap.servers": cluster_kafka_bootstrap_servers(),
    "group.id": "ratatoskr-pipeline-{topic.replace('.', '-')}",
    "auto.offset.reset": "earliest",
}}
_consumer = FlinkKafkaConsumer(
    topics="{topic}",
    deserialization_schema=SimpleStringSchema(),
    properties=_props,
)
def _parse_line(raw: str):
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {{"key": str(payload)}}
    except Exception:
        return {{"key": "unknown", "timestamp": 0}}
stream = (
    env.add_source(_consumer)
    .map(_parse_line)
    .filter(lambda e: str(e.get("{key_field}") or e.get("key") or "") not in ("", "unknown"))
)
""".strip("\n")
        return imports, block

    return [], "stream = env.from_collection(RECORDS)"


def cluster_window_block(window_config: WindowNodeConfig) -> tuple[list[str], str]:
    key_field = window_config.key_field
    imports = [
        "from pyflink.datastream.window import DynamicProcessingTimeSessionWindows",
        "from ratatoskr.pipelines.window_ops import GenericSessionSummaryFunction",
    ]

    if window_config.gap_policy == GAP_POLICY_SESSION_DETECT:
        imports.append("from ratatoskr.pipelines.window_ops import PolicyGapExtractor")
        gap_expr = f'PolicyGapExtractor("{GAP_POLICY_SESSION_DETECT}", gap_ms={window_config.gap_ms})'
    else:
        imports.append("from ratatoskr.pipelines.window_ops import FixedGapExtractor")
        gap_expr = f"FixedGapExtractor({window_config.gap_ms})"

    summary_expr = (
        f'GenericSessionSummaryFunction(key_field="{key_field}", '
        f'gap_policy="{window_config.gap_policy}")'
    )

    block = f"""
stream = (
    stream.key_by(lambda e: str(e.get("{key_field}") or e.get("key") or "unknown"))
    .window(DynamicProcessingTimeSessionWindows.with_dynamic_gap({gap_expr}))
    .process({summary_expr})
)
""".strip("\n")
    return imports, block


def cluster_agent_logic_block(agent: str, window_config: WindowNodeConfig) -> tuple[list[str], str]:
    key_field = window_config.key_field
    if agent == "session_detect" and window_config.gap_policy == GAP_POLICY_SESSION_DETECT:
        return (
            ["from examples.agents.session_detect_logic import process_session_summary"],
            "stream = stream.map(process_session_summary)",
        )
    return (
        ["from ratatoskr.pipelines.window_policies import prepare_agent_input"],
        (
            f"stream = stream.map(lambda row: prepare_agent_input("
            f"row, agent={agent!r}, key_field={key_field!r}))"
        ),
    )


def cluster_kafka_sink_block(topic: str) -> tuple[list[str], str]:
    imports = [
        "from pyflink.common.serialization import SimpleStringSchema",
        "from pyflink.datastream.connectors.kafka import FlinkKafkaProducer",
        "import json",
    ]
    block = f"""
def _to_json(row):
    return json.dumps(row, default=str)
_producer = FlinkKafkaProducer(
    topic="{topic}",
    serialization_schema=SimpleStringSchema(),
    producer_config={{"bootstrap.servers": cluster_kafka_bootstrap_servers()}},
)
stream.map(_to_json).add_sink(_producer)
""".strip("\n")
    imports.append("from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers")
    return imports, block


def bridge_topic_for_pipeline(pipeline_id: str, window_config: WindowNodeConfig) -> str:
    return window_config.bridge_topic or default_bridge_topic(pipeline_id)
