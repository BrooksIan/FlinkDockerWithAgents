import { addEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import type { AgentSummary, PipelineEdgeDef, PipelineNodeDef, PipelineSummary } from "../api/types";

export const DEFAULT_KAFKA_OUTPUT_TOPIC = "workflow.test.output";

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
      const config = n.config || {};
      const sourceType = config.source_type === "kafka" ? "kafka" : "records";
      const records = (config.records as unknown[]) || [];
      return {
        id: n.id,
        type: "source",
        position: pos,
        data: {
          label: sourceType === "kafka" ? "Kafka source" : "Source",
          sourceType,
          kafkaTopic: sourceType === "kafka" ? (config.topic as string) : undefined,
          recordCount: sourceType === "records" ? records.length : undefined,
          config,
        },
      };
    }
    if (n.kind === "window") {
      const config = n.config || {};
      return {
        id: n.id,
        type: "window",
        position: pos,
        data: {
          label: "Session window",
          keyField: (config.key_field as string) || "key",
          gapPolicy: (config.gap_policy as string) || "default",
          gapMs: typeof config.gap_ms === "number" ? config.gap_ms : 1000,
          executionMode: (config.execution_mode as string) || "logic",
          config,
        },
      };
    }
    if (n.kind === "sink") {
      const config = n.config || {};
      const sinkType = config.sink_type === "kafka" ? "kafka" : "capture";
      return {
        id: n.id,
        type: "sink",
        position: pos,
        data: {
          label: sinkType === "kafka" ? "Kafka sink" : "Sink",
          sinkType,
          kafkaTopic: sinkType === "kafka"
            ? ((config.topic as string) || DEFAULT_KAFKA_OUTPUT_TOPIC)
            : undefined,
          config,
        },
      };
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

  const edges: Edge[] = pruneOrphanEdges(
    nodes,
    pipeline.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      data: { mapping: e.mapping || {} },
    })),
  );

  return { nodes, edges };
}

/** Drop edges whose source or target no longer exists on the canvas. */
export function pruneOrphanEdges(nodes: Node[], edges: Edge[]): Edge[] {
  const nodeIds = new Set(nodes.map((n) => n.id));
  return edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
}

function linearChainNodes(nodes: Node[]): Node[] {
  const ordered = [...nodes].sort((a, b) => a.position.x - b.position.x || a.position.y - b.position.y);
  const source = ordered.find((n) => n.type === "source");
  const sink = ordered.find((n) => n.type === "sink");
  const window = ordered.find((n) => n.type === "window");
  const agents = ordered.filter((n) => n.type === "agent");
  return [source, window, ...agents, sink].filter(Boolean) as Node[];
}

export function ensureLinearChainEdges(nodes: Node[], edges: Edge[]): Edge[] {
  const pruned = pruneOrphanEdges(nodes, edges);
  if (nodes.length >= 2 && pruned.length < nodes.length - 1) {
    return buildLinearChainEdges(nodes);
  }
  return pruned;
}

/** Build source → window? → agents → sink edges (replaces stale wiring). */
export function buildLinearChainEdges(nodes: Node[]): Edge[] {
  const chain = linearChainNodes(nodes);
  if (chain.length < 2) return [];

  const edges: Edge[] = [];
  for (let i = 0; i < chain.length - 1; i += 1) {
    const src = chain[i];
    const tgt = chain[i + 1];
    edges.push({
      id: nextId("e"),
      source: src.id,
      target: tgt.id,
      type: "smoothstep",
      data: { mapping: mappingForEdge(src, tgt) },
    });
  }
  return edges;
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
      const existing = (n.data as { config?: Record<string, unknown> }).config;
      const config =
        existing ||
        ({
          source_type: "records",
          records: [{ key: "1", value: 3 }],
        } as Record<string, unknown>);
      return { id: n.id, kind: "source", config };
    }
    if (n.type === "window") {
      const existing = (n.data as { config?: Record<string, unknown> }).config;
      const config =
        existing ||
        ({
          window_type: "dynamic_session",
          key_field: "key",
          gap_policy: "default",
          gap_ms: 1000,
          time_mode: "processing",
          execution_mode: "logic",
        } as Record<string, unknown>);
      return { id: n.id, kind: "window", config };
    }
    if (n.type === "sink") {
      const existing = (n.data as { config?: Record<string, unknown> }).config;
      const sinkType = (existing?.sink_type as string) || "capture";
      const config =
        existing ||
        ({
          sink_type: "capture",
        } as Record<string, unknown>);
      if (sinkType === "kafka" && !String(config.topic || "").trim()) {
        config.topic = DEFAULT_KAFKA_OUTPUT_TOPIC;
        config.sink_type = "kafka";
      }
      return { id: n.id, kind: "sink", config };
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
  const valid = pruneOrphanEdges(nodes, edges);
  if (nodes.length < 2) return valid;

  const chain = linearChainNodes(nodes);
  if (chain.length < 2) return valid;

  const existing = new Set(valid.map((e) => `${e.source}->${e.target}`));
  let next = [...valid];
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

export function emptyPipeline(): Partial<PipelineSummary> {
  return {
    name: "",
    nodes: [],
    edges: [],
    layout: {},
  };
}

export function defaultSessionWindowPipeline(): Partial<PipelineSummary> {
  return {
    name: "Session window",
    nodes: [
      {
        id: "src1",
        kind: "source",
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
        kind: "window",
        config: {
          window_type: "dynamic_session",
          key_field: "key",
          gap_policy: "default",
          gap_ms: 1000,
          time_mode: "processing",
          execution_mode: "logic",
        },
      },
      { id: "agent_wc", kind: "agent", agent: "workflow_counter" },
      { id: "sink1", kind: "sink", config: { sink_type: "capture" } },
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

export function defaultSessionDetectPipeline(): Partial<PipelineSummary> {
  return {
    name: "Session detect (Cowrie)",
    nodes: [
      {
        id: "src1",
        kind: "source",
        config: {
          source_type: "records",
          records: [
            {
              eventid: "cowrie.login.failed",
              src_ip: "10.0.0.42",
              timestamp: 1719412800,
              session: "sess-brute",
            },
            {
              eventid: "cowrie.login.failed",
              src_ip: "10.0.0.42",
              timestamp: 1719412801,
              session: "sess-brute",
            },
            {
              eventid: "cowrie.login.failed",
              src_ip: "10.0.0.42",
              timestamp: 1719412802,
              session: "sess-brute",
            },
            {
              eventid: "cowrie.login.failed",
              src_ip: "10.0.0.42",
              timestamp: 1719412803,
              session: "sess-brute",
            },
            {
              eventid: "cowrie.login.failed",
              src_ip: "10.0.0.42",
              timestamp: 1719412804,
              session: "sess-brute",
            },
            {
              eventid: "cowrie.command.input",
              src_ip: "10.0.0.99",
              timestamp: 1719412810,
              session: "sess-probe",
              input: "uname -a",
            },
          ],
        },
      },
      {
        id: "win1",
        kind: "window",
        config: {
          window_type: "dynamic_session",
          key_field: "src_ip",
          gap_policy: "session_detect",
          time_mode: "processing",
          execution_mode: "logic",
        },
      },
      { id: "agent_sd", kind: "agent", agent: "session_detect" },
      { id: "sink1", kind: "sink", config: { sink_type: "capture" } },
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
