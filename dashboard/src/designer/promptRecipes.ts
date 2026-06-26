/** Starter prompts and preview helpers for ReAct agent authoring. */

import { DEFAULT_PROMPT_SYSTEM, DEFAULT_PROMPT_USER } from "./promptDefaults";

export interface PromptRecipe {
  id: string;
  label: string;
  description: string;
  system: string;
  user: string;
}

export const PROMPT_CONSTRAINT_NOTE =
  "Describe your task in the system prompt. The LLM must return JSON with at least " +
  "input, result, and reasoning — put your main answer in result. " +
  "Compile appends JSON formatting rules automatically.";

/** Sample pipeline record used for live user-prompt preview. */
export const PROMPT_SAMPLE = {
  message: "3",
  value: "3",
};

export const PROMPT_RECIPES: PromptRecipe[] = [
  {
    id: "double",
    label: "Double a number",
    description: "Extract a numeric input and compute doubled = input × 2.",
    system: DEFAULT_PROMPT_SYSTEM,
    user: DEFAULT_PROMPT_USER,
  },
  {
    id: "greet",
    label: "Multi-language greeting",
    description: "Greet the user in N languages, where N is the numeric input.",
    system: [
      "You receive a numeric input N.",
      "Greet the user in exactly N different languages.",
      "Number each greeting (1., 2., …).",
      'Put the full greeting block in "result".',
      'Set "input" to N.',
      'Explain your language choices in "reasoning".',
    ].join(" "),
    user: DEFAULT_PROMPT_USER,
  },
  {
    id: "classify",
    label: "Classify severity",
    description: "Label the message as low, medium, or high severity.",
    system: [
      "Read the user message and classify its severity as low, medium, or high.",
      'Put the severity label in "result".',
      'Set "input" to the primary numeric value when present, otherwise 0.',
      'Explain your classification in "reasoning".',
    ].join(" "),
    user: DEFAULT_PROMPT_USER,
  },
  {
    id: "skills_math",
    label: "Math calculator (skills)",
    description: "Use the math-calculator Flink skill via load_skill and bc.",
    system: [
      "You are a helpful math assistant. Use the math-calculator skill when asked to evaluate an expression.",
      "You must load the skill first with load_skill and strictly follow its instructions.",
      "Reply with only the final numeric result.",
    ].join(" "),
    user: "{message}",
  },
];

/** Resolve user template placeholders the same way compile/runtime does. */
export function applyUserPromptTemplate(
  template: string,
  sample: { message: string; value: string } = PROMPT_SAMPLE,
): string {
  const resolved = template || DEFAULT_PROMPT_USER;
  const messageJson = JSON.stringify(sample.message);
  return resolved.replace(/\{message\}/g, messageJson).replace(/\{value\}/g, sample.value);
}

/** Studio pipeline template — source → window → agent → capture (domain-neutral). */
export const SESSION_WINDOW_PIPELINE_RECIPE = {
  id: "session_window",
  name: "Session window",
  description: "Dynamic session window on a key field, then workflow_counter.",
  pipeline: defaultSessionWindowPipelineFromRecipe(),
};

function defaultSessionWindowPipelineFromRecipe() {
  return {
    name: "Session window",
    nodes: [
      {
        id: "src1",
        kind: "source" as const,
        config: {
          source_type: "records",
          records: [
            { key: "user-a", value: 1, timestamp: 100 },
            { key: "user-a", value: 2, timestamp: 101 },
            { key: "user-a", value: 3, timestamp: 102 },
            { key: "user-b", value: 10, timestamp: 200 },
            { key: "user-b", value: 11, timestamp: 201 },
          ],
        },
      },
      {
        id: "win1",
        kind: "window" as const,
        config: {
          window_type: "dynamic_session",
          key_field: "key",
          gap_policy: "default",
          gap_ms: 1000,
          time_mode: "processing",
          execution_mode: "logic",
        },
      },
      { id: "agent_wc", kind: "agent" as const, agent: "workflow_counter" },
      { id: "sink1", kind: "sink" as const, config: { sink_type: "capture" } },
    ],
    edges: [
      { id: "e1", source: "src1", target: "win1" },
      { id: "e2", source: "win1", target: "agent_wc" },
      { id: "e3", source: "agent_wc", target: "sink1" },
    ],
    layout: {
      src1: { x: 80, y: 200 },
      win1: { x: 280, y: 200 },
      agent_wc: { x: 480, y: 200 },
      sink1: { x: 680, y: 200 },
    },
  };
}

/** Optional cyber example — source → window → session_detect → capture. */
export const SESSION_DETECT_PIPELINE_RECIPE = {
  id: "session_detect",
  name: "Session detect (Cowrie)",
  description: "Cowrie-themed session_detect policy on src_ip.",
  pipeline: defaultSessionDetectPipelineFromRecipe(),
};

function defaultSessionDetectPipelineFromRecipe() {
  return {
    name: "Session detect (Cowrie)",
    nodes: [
      {
        id: "src1",
        kind: "source" as const,
        config: {
          source_type: "records",
          records: [
            { eventid: "cowrie.login.failed", src_ip: "10.0.0.42", timestamp: 1719412800 },
            { eventid: "cowrie.login.failed", src_ip: "10.0.0.42", timestamp: 1719412801 },
            { eventid: "cowrie.login.failed", src_ip: "10.0.0.42", timestamp: 1719412802 },
            { eventid: "cowrie.login.failed", src_ip: "10.0.0.42", timestamp: 1719412803 },
            { eventid: "cowrie.login.failed", src_ip: "10.0.0.42", timestamp: 1719412804 },
            { eventid: "cowrie.command.input", src_ip: "10.0.0.99", timestamp: 1719412810, input: "uname -a" },
          ],
        },
      },
      {
        id: "win1",
        kind: "window" as const,
        config: {
          window_type: "dynamic_session",
          key_field: "src_ip",
          gap_policy: "session_detect",
          time_mode: "processing",
          execution_mode: "logic",
        },
      },
      { id: "agent_sd", kind: "agent" as const, agent: "session_detect" },
      { id: "sink1", kind: "sink" as const, config: { sink_type: "capture" } },
    ],
    edges: [
      { id: "e1", source: "src1", target: "win1" },
      { id: "e2", source: "win1", target: "agent_sd" },
      { id: "e3", source: "agent_sd", target: "sink1" },
    ],
    layout: {
      src1: { x: 80, y: 200 },
      win1: { x: 280, y: 200 },
      agent_sd: { x: 480, y: 200 },
      sink1: { x: 680, y: 200 },
    },
  };
}
