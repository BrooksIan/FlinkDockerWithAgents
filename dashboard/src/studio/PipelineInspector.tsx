import type { Edge, Node } from "@xyflow/react";

interface Props {
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  onUpdateNode: (nodeId: string, patch: { config?: Record<string, unknown> }) => void;
  onUpdateEdge: (edgeId: string, mapping: Record<string, string>) => void;
}

export function PipelineInspector({ selectedNode, selectedEdge, onUpdateNode, onUpdateEdge }: Props) {
  if (!selectedNode && !selectedEdge) {
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Inspector</h3>
        <p className="muted">Select a node or edge to configure it.</p>
      </div>
    );
  }

  if (selectedEdge) {
    const mappingJson = JSON.stringify(
      (selectedEdge.data as { mapping?: Record<string, string> })?.mapping || {},
      null,
      2,
    );
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Edge mapping</h3>
        <p className="muted">
          Map output fields to the next agent&apos;s input. Use JSONPath like <code>$.doubled</code>.
        </p>
        <label className="studio-label">Mapping (JSON)</label>
        <textarea
          className="studio-textarea"
          rows={6}
          defaultValue={mappingJson}
          key={selectedEdge.id}
          onBlur={(e) => {
            try {
              const parsed = JSON.parse(e.target.value || "{}") as Record<string, string>;
              onUpdateEdge(selectedEdge.id, parsed);
            } catch {
              /* keep previous mapping on invalid JSON */
            }
          }}
        />
        <p className="muted" style={{ fontSize: "0.8rem" }}>
          Example: <code>{`{"message": "$.doubled"}`}</code> for workflow_counter → react_echo
        </p>
      </div>
    );
  }

  if (!selectedNode) return null;
  const kind = selectedNode.type;

  if (kind === "source") {
    const config = (selectedNode.data as { config?: { records?: unknown[] } }).config || {};
    const recordsJson = JSON.stringify(config.records || [{ key: "1", value: 3 }], null, 2);
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Source input</h3>
        <p className="muted">Records fed into the first agent when running locally.</p>
        <label className="studio-label">Records (JSON array)</label>
        <textarea
          className="studio-textarea"
          rows={10}
          defaultValue={recordsJson}
          key={selectedNode.id}
          onBlur={(e) => {
            try {
              const records = JSON.parse(e.target.value) as unknown[];
              onUpdateNode(selectedNode.id, { config: { records } });
            } catch {
              /* ignore invalid JSON */
            }
          }}
        />
      </div>
    );
  }

  if (kind === "agent") {
    const d = selectedNode.data as { agent?: string; agentType?: string; description?: string };
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Agent: {d.agent}</h3>
        <p className="muted">Type: {d.agentType}</p>
        <p className="muted">Double-click the node to view its internal action/tool graph.</p>
      </div>
    );
  }

  return (
    <div className="studio-inspector card">
      <h3 style={{ marginTop: 0 }}>Sink</h3>
      <p className="muted">Final pipeline output is captured here after the last agent runs.</p>
    </div>
  );
}
