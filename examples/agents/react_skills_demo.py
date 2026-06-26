"""ReAct skills demo — native Flink chat model + math-calculator skill."""

from __future__ import annotations

from flink_agents.api.agents.agent import Agent
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.decorators import (
    action,
    chat_model_connection,
    chat_model_setup,
    prompt,
    skills,
)
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.prompts.prompt import Prompt
from flink_agents.api.resource import ResourceDescriptor
from flink_agents.api.runner_context import RunnerContext
from flink_agents.api.skills import Skills

from examples.agents.react_skills_paths import examples_skills_dir


class ReactSkillsDemoAgent(Agent):
    """Answer arithmetic questions using the math-calculator Flink Agents skill."""

    @skills
    @staticmethod
    def platform_skills() -> Skills:
        return Skills.from_local_dir(str(examples_skills_dir()))

    @prompt
    @staticmethod
    def system_prompt() -> Prompt:
        return Prompt.from_messages(
            messages=[
                ChatMessage(
                    role=MessageRole.SYSTEM,
                    content=(
                        "You are a helpful math assistant. Use the math-calculator skill "
                        "when asked to evaluate an expression. You must load the skill first "
                        "and strictly follow its instructions. Reply with only the final "
                        "numeric result."
                    ),
                )
            ],
        )

    @chat_model_connection
    @staticmethod
    def designer_llm_connection() -> ResourceDescriptor:
        from ratatoskr.designer.flink_llm import react_llm_connection_descriptor

        return react_llm_connection_descriptor()

    @chat_model_setup
    @staticmethod
    def skills_model() -> ResourceDescriptor:
        from ratatoskr.designer.flink_llm import react_skills_chat_model_descriptor

        return react_skills_chat_model_descriptor(
            connection="designer_llm_connection",
            prompt="system_prompt",
            skills=["math-calculator"],
            allowed_commands=["echo", "bc"],
        )

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def process_input(event: Event, ctx: RunnerContext) -> None:
        payload = InputEvent.from_event(event).input
        if isinstance(payload, dict):
            question = str(payload.get("message") or payload.get("question") or payload)
        else:
            question = str(payload)
        ctx.send_event(
            ChatRequestEvent(
                model="skills_model",
                messages=[ChatMessage(role=MessageRole.USER, content=question)],
            )
        )

    @action(ChatResponseEvent.EVENT_TYPE)
    @staticmethod
    def process_chat_response(event: Event, ctx: RunnerContext) -> None:
        chat_response = ChatResponseEvent.from_event(event)
        ctx.send_event(
            OutputEvent(
                output={
                    "answer": chat_response.response.content,
                    "agent": "react_skills_demo",
                }
            )
        )
