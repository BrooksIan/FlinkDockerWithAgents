/** Built-in workflow tools mirrored from ratatoskr.tools.builtins. */

export interface BuiltinToolSpec {
  name: string;
  description: string;
  defaultExpression: string;
}

export const BUILTIN_TOOLS: BuiltinToolSpec[] = [
  {
    name: "double",
    description: "Return twice the input value.",
    defaultExpression: "value * 2",
  },
  {
    name: "scale",
    description: "Multiply input by a configured factor.",
    defaultExpression: "value * factor",
  },
  {
    name: "identity",
    description: "Return the input unchanged.",
    defaultExpression: "value",
  },
];

export function builtinToolByName(name: string): BuiltinToolSpec | undefined {
  return BUILTIN_TOOLS.find((tool) => tool.name === name);
}
