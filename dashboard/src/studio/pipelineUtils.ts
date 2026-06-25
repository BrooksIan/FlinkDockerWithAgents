import { addEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import type { AgentSummary, PipelineEdgeDef, PipelineNodeDef, PipelineSummary } from "../api/types";

let _id = 0;
export function nextId(prefix: string) {
  _id += 1;
  return `${prefix}_${Date.now()}_${_id}`;
}

export function pipelineToFlow(
  pipeline: PipelineSummary,
  agents: AgentSummary[],
): { nodes: Node[]; edges: Edge[] } {
  const agentMap = Object.fromEntries(agents.map((a) => [a.name, a]));
  const nodes: Node[] = pipeline.nodes.map((n) => {
    const pos = pipeline.layout[n.id] || { x: 0, y: 0 };
    if (n.kind === "source") {
      const records = (n.config?.records as unknown[]) || [];
      return {
        id: n.id,
        type: "source",
        position: pos,
        data: { label: "Source", recordCount: records.length, config: n.config },
      };
    }
    if (n.kind === "sink") {
      return { id: n.id, type: "sink", position: pos, data: { label: "Sink" } };
    }
    const meta = n.agent ? agentMap[n.agent] : undefined;
    return {
      id: n.id,
      type: "agent",
      position: pos,
      data: {
        label: n.agent,
        agent: n.agent,
        agentType: meta?.type || "workflow",
        description: meta?.description,
      },
    };
  });

  const edges: Edge[] = pipeline.edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    data: { mapping: e.mapping || {} },
  }));

  return { nodes, edges };
}

export function flowToPipeline(
  pipelineId: string,
  name: string,
  nodes: Node[],
  edges: Edge[],
  meta: { created_at: string; updated_at: string },
): PipelineSummary {
  const pipelineNodes: PipelineNodeDef[] = nodes.map((n) => {
    if (n.type === "source") {
      const config = (n.data as { config?: Record<string, unknown> }).config || {
        records: [{ key: "1", value: 3 }],
      };
      return { id: n.id, kind: "source", config };
    }
    if (n.type === "sink") {
      return { id: n.id, kind: "sink" };
    }
    return {
      id: n.id,
      kind: "agent",
      agent: (n.data as { agent?: string }).agent,
    };
  });

  const pipelineEdges: PipelineEdgeDef[] = edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    mapping: ((e.data as { mapping?: Record<string, string> })?.mapping || {}) as Record<string, string>,
  }));

  const layout: Record<string, { x: number; y: number }> = {};
  for (const n of nodes) {
    layout[n.id] = { x: n.position.x, y: n.position.y };
  }

  return {
    id: pipelineId,
    name,
    nodes: pipelineNodes,
    edges: pipelineEdges,
    layout,
    created_at: meta.created_at,
    updated_at: meta.updated_at,
  };
}

export function connectEdge(edges: Edge[], params: Connection): Edge[] {
  if (!params.source || !params.target) return edges;
  const exists = edges.some((e) => e.source === params.source && e.target === params.target);
  if (exists) return edges;
  return addEdge(
    {
      ...params,
      id: nextId("e"),
      type: "smoothstep",
      data: { mapping: {} },
    },
    edges,
  );
}

/** Default field mappings for known agent pairs. */
const DEFAULT_EDGE_MAPPINGS: Record<string, Record<string, string>> = {
  "workflow_counter->react_echo": { message: "$.doubled" },
};

function mappingForEdge(source: Node, target: Node): Record<string, string> {
  if (source.type !== "agent" || target.type !== "agent") return {};
  const from = (source.data as { agent?: string }).agent || "";
  const to = (target.data as { agent?: string }).agent || "";
  return DEFAULT_EDGE_MAPPINGS[`${from}->${to}`] || {};
}

/** Wire nodes left-to-right (by x) when edges are missing. */
export function autoWireLinear(nodes: Node[], edges: Edge[]): Edge[] {
  if (nodes.length < 2) return edges;

  const ordered = [...nodes].sort((a, b) => a.position.x - b.position.x || a.position.y - b.position.y);
  const source = ordered.find((n) => n.type === "source");
  const sink = ordered.find((n) => n.type === "sink");
  const agents = ordered.filter((n) => n.type === "agent");
  const chain = [source, ...agents, sink].filter(Boolean) as Node[];
  if (chain.length < 2) return edges;

  const existing = new Set(edges.map((e) => `${e.source}->${e.target}`));
  let next = [...edges];
  for (let i = 0; i < chain.length - 1; i += 1) {
    const src = chain[i];
    const tgt = chain[i + 1];
    const key = `${src.id}->${tgt.id}`;
    if (existing.has(key)) continue;
    next = addEdge(
      {
        id: nextId("e"),
        source: src.id,
        target: tgt.id,
        data: { mapping: mappingForEdge(src, tgt) },
      },
      next,
    );
    existing.add(key);
  }
  return next;
}

export function defaultDemoPipeline(): Partial<PipelineSummary> {
  return {
    name: "Counter then Echo",
    nodes: [
      {
        id: "src1",
        kind: "source",
        config: {
          records: [
            { key: "1", value: 3 },
            { key: "2", value: 10 },
          ],
        },
      },
      { id: "agent_wc", kind: "agent", agent: "workflow_counter" },
      { id: "agent_re", kind: "agent", agent: "react_echo" },
      { id: "sink1", kind: "sink" },
    ],
    edges: [
      { id: "e1", source: "src1", target: "agent_wc" },
      { id: "e2", source: "agent_wc", target: "agent_re", mapping: { message: "$.doubled" } },
      { id: "e3", source: "agent_re", target: "sink1" },
    ],
    layout: {
      src1: { x: 80, y: 200 },
      agent_wc: { x: 320, y: 200 },
      agent_re: { x: 560, y: 200 },
      sink1: { x: 800, y: 200 },
    },
  };
}
