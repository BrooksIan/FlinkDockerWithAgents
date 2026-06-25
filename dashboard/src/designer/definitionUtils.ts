import { addEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import type {
  AgentDefinition,
  AgentDefinitionEdge,
  AgentDefinitionNode,
  AgentEdgeKind,
  AgentNodeKind,
} from "../api/types";

let _id = 0;
export function nextId(prefix: string) {
  _id += 1;
  return `${prefix}_${Date.now()}_${_id}`;
}

const KIND_LABELS: Record<AgentNodeKind, string> = {
  input_event: "Input",
  action: "Action",
  tool: "Tool",
  output_event: "Output",
  prompt: "Prompt",
  llm_call: "LLM",
};

export function kindLabel(kind: AgentNodeKind): string {
  return KIND_LABELS[kind] || kind;
}

export function definitionToFlow(definition: AgentDefinition): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = definition.nodes.map((n) => {
    const pos = definition.layout[n.id] || { x: 0, y: 0 };
    return {
      id: n.id,
      type: n.kind,
      position: pos,
      data: {
        label: n.name || kindLabel(n.kind),
        kind: n.kind,
        name: n.name,
        config: n.config || {},
      },
    };
  });

  const edges: Edge[] = definition.edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    label: e.kind,
    data: { kind: e.kind },
    type: "smoothstep",
  }));

  return { nodes, edges };
}

export function flowToDefinition(
  base: AgentDefinition,
  nodes: Node[],
  edges: Edge[],
): AgentDefinition {
  const layout: Record<string, { x: number; y: number }> = {};
  const defNodes: AgentDefinitionNode[] = nodes.map((n) => {
    layout[n.id] = { x: n.position.x, y: n.position.y };
    const data = n.data as {
      kind?: AgentNodeKind;
      name?: string;
      config?: Record<string, unknown>;
    };
    return {
      id: n.id,
      kind: (n.type as AgentNodeKind) || data.kind || "action",
      name: String(data.name || data.kind || n.type || ""),
      config: (data.config as Record<string, unknown>) || {},
    };
  });

  const defEdges: AgentDefinitionEdge[] = edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    kind: ((e.data as { kind?: AgentEdgeKind })?.kind ||
      inferEdgeKind(
        nodes.find((n) => n.id === e.source),
        nodes.find((n) => n.id === e.target),
      )) as AgentEdgeKind,
  }));

  return {
    ...base,
    nodes: defNodes,
    edges: defEdges,
    layout,
  };
}

export function inferEdgeKind(source?: Node, target?: Node): AgentEdgeKind {
  const sk = source?.type;
  const tk = target?.type;
  if (sk === "input_event" && tk === "action") return "listens_to";
  if (sk === "action" && tk === "tool") return "calls";
  if (sk === "action" && tk === "output_event") return "emits";
  if (sk === "action" && tk === "action") return "listens_to";
  return "listens_to";
}

export function connectDesignerEdge(
  edges: Edge[],
  params: Connection,
  nodes: Node[],
): Edge[] {
  if (!params.source || !params.target) return edges;
  const exists = edges.some((e) => e.source === params.source && e.target === params.target);
  if (exists) return edges;
  const source = nodes.find((n) => n.id === params.source);
  const target = nodes.find((n) => n.id === params.target);
  const kind = inferEdgeKind(source, target);
  return addEdge(
    {
      ...params,
      id: nextId("e"),
      type: "smoothstep",
      label: kind,
      data: { kind },
    },
    edges,
  );
}

/** Wire input → action → tools → output by x position. */
export function autoWireAgentGraph(nodes: Node[], edges: Edge[]): Edge[] {
  if (nodes.length < 2) return edges;

  const input = nodes.find((n) => n.type === "input_event");
  const action = nodes.find((n) => n.type === "action");
  const tools = nodes.filter((n) => n.type === "tool").sort((a, b) => a.position.x - b.position.x);
  const output = nodes.find((n) => n.type === "output_event");

  const chain: Node[] = [];
  if (input) chain.push(input);
  if (action) chain.push(action);
  chain.push(...tools);
  if (output) chain.push(output);

  if (chain.length < 2) {
    const ordered = [...nodes].sort((a, b) => a.position.x - b.position.x);
    return _wireChain(ordered, edges);
  }
  return _wireChain(chain, edges);
}

function _wireChain(chain: Node[], edges: Edge[]): Edge[] {
  const existing = new Set(edges.map((e) => `${e.source}->${e.target}`));
  let next = [...edges];
  for (let i = 0; i < chain.length - 1; i += 1) {
    const src = chain[i];
    const tgt = chain[i + 1];
    const key = `${src.id}->${tgt.id}`;
    if (existing.has(key)) continue;
    const kind = inferEdgeKind(src, tgt);
    next = addEdge(
      {
        id: nextId("e"),
        source: src.id,
        target: tgt.id,
        label: kind,
        data: { kind },
        type: "smoothstep",
      },
      next,
    );
    existing.add(key);
  }
  return next;
}

export type DesignerDroppedSpec = {
  kind: AgentNodeKind;
  name?: string;
  config?: Record<string, unknown>;
};

export function defaultConfigForKind(kind: AgentNodeKind): Record<string, unknown> {
  switch (kind) {
    case "input_event":
      return { event_type: "_input_event" };
    case "action":
      return { listens_to: ["_input_event"] };
    case "tool":
      return { tool_ref: "double", expression: "value * 2" };
    case "output_event":
      return { event_type: "_output_event" };
    case "prompt":
      return { template: "system" };
    case "llm_call":
      return { use_platform_llm: true };
    default:
      return {};
  }
}

export function defaultNameForKind(kind: AgentNodeKind): string {
  switch (kind) {
    case "input_event":
      return "InputEvent";
    case "action":
      return "process";
    case "tool":
      return "double";
    case "output_event":
      return "OutputEvent";
    case "prompt":
      return "prompt";
    case "llm_call":
      return "llm";
    default:
      return kind;
  }
}
