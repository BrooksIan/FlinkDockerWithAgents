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
