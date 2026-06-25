/** Default ReAct prompt text — used when creating or seeding prompt nodes. */

export const DEFAULT_PROMPT_SYSTEM = `Extract the numeric input value from the user message and compute doubled = input * 2.

Ensure your response can be parsed by Python json, using this format as an example:
{"input": 7, "doubled": 14, "reasoning": "Identified 7 as the input and doubled it."}

If multiple numbers appear, use the primary numeric value referenced as the input.
Respond with JSON only.`;

export const DEFAULT_PROMPT_USER = '"message": {message},\n"value": {value}';

export function defaultPromptConfig(): Record<string, unknown> {
  return {
    template: "system",
    system: DEFAULT_PROMPT_SYSTEM,
    user: DEFAULT_PROMPT_USER,
  };
}
