"""Prompt templates for react_double_value (Flink Agents Prompt API).

See: https://nightlies.apache.org/flink/flink-agents-docs-main/docs/development/prompts/
"""

from __future__ import annotations

# JSON examples use single braces — Flink Agents passes other { } through verbatim.
# Placeholders {message} and {value} are filled from prompt_args / input fields.

DOUBLE_VALUE_SYSTEM = """
Extract the numeric input value from the user message and compute doubled = input * 2.

Example input format:
{
    "message": "Please double input value 7",
    "value": 7
}

Ensure your response can be parsed by Python json, using this format as an example:
{
    "input": 7,
    "doubled": 14,
    "reasoning": "Identified 7 as the input and doubled it."
}

If multiple numbers appear, use the primary numeric value referenced as the input.
Respond with JSON only.
""".strip()

DOUBLE_VALUE_USER = """
"message": {message},
"value": {value}
""".strip()


def double_value_prompt():
    """Build a Flink Agents Prompt (lazy import — requires flink_agents)."""
    from flink_agents.api.chat_message import ChatMessage, MessageRole
    from flink_agents.api.prompts.prompt import Prompt

    return Prompt.from_messages(
        messages=[
            ChatMessage(role=MessageRole.SYSTEM, content=DOUBLE_VALUE_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=DOUBLE_VALUE_USER),
        ],
    )
