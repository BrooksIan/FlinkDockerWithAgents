import type { AgentDefinitionCreate, LlmCallConfig } from "../api/types";
import { defaultPromptConfig } from "./promptDefaults";

export type NewAgentType = "workflow" | "react" | "react_skills";

export function defaultAgentName(type: NewAgentType): string {
  if (type === "react_skills") return "New ReAct skills agent";
  return type === "react" ? "New ReAct agent" : "New workflow agent";
}

const SKILLS_SYSTEM_PROMPT = [
  "You are a helpful math assistant. Use the math-calculator skill when asked to evaluate an expression.",
  "You must load the skill first with load_skill and strictly follow its instructions.",
  "Reply with only the final numeric result.",
].join(" ");

const SKILLS_LLM_CONFIG: LlmCallConfig = {
  use_platform_llm: true,
  mode: "flink_skills",
  skills: ["math-calculator"],
  allowed_commands: ["echo", "bc"],
};

export function newAgentPayload(type: NewAgentType, name?: string): AgentDefinitionCreate {
  const agentName = name?.trim() || defaultAgentName(type);

  if (type === "react_skills") {
    return {
      name: agentName,
      type: "react",
      description: "ReAct agent with native Flink skills — configure LLM in Settings.",
      catalog_category_id: "react",
      catalog_subcategory_id: "numeric",
      catalog_tags: ["custom", "skills"],
      input_schema: {
        type: "object",
        required: ["message"],
        properties: {
          message: { type: "string", description: "Question or task for the agent" },
        },
      },
      output_schema: {
        type: "object",
        properties: {
          answer: { type: "string" },
          result: { type: "string" },
          agent: { type: "string" },
        },
      },
      nodes: [
        {
          id: "in1",
          kind: "input_event",
          name: "InputEvent",
          config: { event_type: "_input_event" },
        },
        {
          id: "act1",
          kind: "action",
          name: "process",
          config: { listens_to: ["_input_event"] },
        },
        {
          id: "prompt1",
          kind: "prompt",
          name: "prompt",
          config: {
            template: "skills_math",
            system: SKILLS_SYSTEM_PROMPT,
            user: "{message}",
          },
        },
        {
          id: "llm1",
          kind: "llm_call",
          name: "llm",
          config: { ...SKILLS_LLM_CONFIG },
        },
        {
          id: "out1",
          kind: "output_event",
          name: "OutputEvent",
          config: { event_type: "_output_event" },
        },
      ],
      edges: [
        { id: "e1", source: "in1", target: "act1", kind: "listens_to" },
        { id: "e2", source: "act1", target: "prompt1", kind: "calls" },
        { id: "e3", source: "act1", target: "llm1", kind: "calls" },
        { id: "e4", source: "act1", target: "out1", kind: "emits" },
      ],
      layout: {
        in1: { x: 80, y: 200 },
        act1: { x: 320, y: 200 },
        prompt1: { x: 560, y: 120 },
        llm1: { x: 560, y: 200 },
        out1: { x: 560, y: 280 },
      },
    };
  }

  if (type === "react") {
    return {
      name: agentName,
      type: "react",
      description: "ReAct agent with LLM prompt — configure LLM in Settings.",
      catalog_category_id: "react",
      catalog_subcategory_id: "numeric",
      catalog_tags: ["custom"],
      input_schema: {
        type: "object",
        required: ["message"],
        properties: {
          message: { type: "string", description: "User or upstream message" },
        },
      },
      output_schema: {
        type: "object",
        properties: {
          message: { type: "string" },
          result: { type: "string" },
          agent: { type: "string" },
        },
      },
      nodes: [
        {
          id: "in1",
          kind: "input_event",
          name: "InputEvent",
          config: { event_type: "_input_event" },
        },
        {
          id: "act1",
          kind: "action",
          name: "process",
          config: { listens_to: ["_input_event"] },
        },
        {
          id: "prompt1",
          kind: "prompt",
          name: "prompt",
          config: defaultPromptConfig(),
        },
        {
          id: "llm1",
          kind: "llm_call",
          name: "llm",
          config: { use_platform_llm: true, mode: "simple" },
        },
        {
          id: "out1",
          kind: "output_event",
          name: "OutputEvent",
          config: { event_type: "_output_event" },
        },
      ],
      edges: [
        { id: "e1", source: "in1", target: "act1", kind: "listens_to" },
        { id: "e2", source: "act1", target: "prompt1", kind: "calls" },
        { id: "e3", source: "act1", target: "llm1", kind: "calls" },
        { id: "e4", source: "act1", target: "out1", kind: "emits" },
      ],
      layout: {
        in1: { x: 80, y: 200 },
        act1: { x: 320, y: 200 },
        prompt1: { x: 560, y: 120 },
        llm1: { x: 560, y: 200 },
        out1: { x: 560, y: 280 },
      },
    };
  }

  return {
    name: agentName,
    type: "workflow",
    description: "Deterministic workflow agent — add tools and wire the graph.",
    catalog_category_id: "workflow",
    catalog_subcategory_id: "transform",
    catalog_tags: ["custom"],
    input_schema: {
      type: "object",
      required: ["value"],
      properties: {
        value: { type: "integer", description: "Numeric input" },
      },
    },
    output_schema: {
      type: "object",
      properties: {
        input: { type: "integer" },
        doubled: { type: "integer" },
        agent: { type: "string" },
      },
    },
    nodes: [
      {
        id: "in1",
        kind: "input_event",
        name: "InputEvent",
        config: { event_type: "_input_event" },
      },
      {
        id: "act1",
        kind: "action",
        name: "process",
        config: { listens_to: ["_input_event"] },
      },
      {
        id: "tool1",
        kind: "tool",
        name: "double",
        config: { tool_ref: "double", expression: "value * 2" },
      },
      {
        id: "out1",
        kind: "output_event",
        name: "OutputEvent",
        config: { event_type: "_output_event" },
      },
    ],
    edges: [
      { id: "e1", source: "in1", target: "act1", kind: "listens_to" },
      { id: "e2", source: "act1", target: "tool1", kind: "calls" },
      { id: "e3", source: "act1", target: "out1", kind: "emits" },
    ],
    layout: {
      in1: { x: 80, y: 200 },
      act1: { x: 320, y: 200 },
      tool1: { x: 560, y: 120 },
      out1: { x: 560, y: 280 },
    },
  };
}