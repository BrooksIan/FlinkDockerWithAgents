import {
  Background,
  Controls,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { AgentGraph } from "../api/types";

interface Props {
  agentName: string;
  onClose: () => void;
}

function graphToFlow(graph: AgentGraph): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = graph.nodes.map((n, i) => ({
    id: n.id,
    type: "default",
    position: { x: 40 + (i % 3) * 180, y: 40 + Math.floor(i / 3) * 100 },
    data: { label: `${n.kind}: ${n.name}` },
    draggable: false,
  }));
  const edges: Edge[] = graph.edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    animated: false,
  }));
  return { nodes, edges };
}

export function AgentGraphPanel({ agentName, onClose }: Props) {
  const [graph, setGraph] = useState<AgentGraph | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [flow, setFlow] = useState<{ nodes: Node[]; edges: Edge[] }>({ nodes: [], edges: [] });

  useEffect(() => {
    api
      .agentGraph(agentName)
      .then((g) => {
        setGraph(g);
        setFlow(graphToFlow(g));
      })
      .catch((e) => setError(String(e)));
  }, [agentName]);

  return (
    <div className="studio-drawer">
      <div className="studio-drawer-header">
        <h3 style={{ margin: 0 }}>{agentName} — internal graph</h3>
        <button type="button" className="secondary" onClick={onClose}>
          Close
        </button>
      </div>
      {error && <p className="error">{error}</p>}
      {graph?.note && <p className="muted">{graph.note}</p>}
      {graph && graph.nodes.length > 0 ? (
        <div className="studio-agent-graph card">
          <ReactFlow nodes={flow.nodes} edges={flow.edges} fitView nodesDraggable={false} nodesConnectable={false} elementsSelectable={false}>
            <Background gap={16} />
            <Controls showInteractive={false} />
          </ReactFlow>
        </div>
      ) : (
        !error && <p className="muted">Loading graph…</p>
      )}
      {graph?.source && (
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          Source: {graph.source}
        </p>
      )}
    </div>
  );
}
