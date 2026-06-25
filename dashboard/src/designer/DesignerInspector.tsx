import type { Edge, Node } from "@xyflow/react";
import type { AgentEdgeKind, AgentNodeKind } from "../api/types";
import { PromptInstructionFields } from "./DesignerPromptPanel";
import { kindLabel } from "./definitionUtils";

interface Props {
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  onUpdateNode: (nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) => void;
  onUpdateEdge: (edgeId: string, kind: AgentEdgeKind) => void;
  onDeleteNode: (nodeId: string) => void;
  onDeleteEdge: (edgeId: string) => void;
}

const EDGE_KINDS: AgentEdgeKind[] = ["listens_to", "calls", "emits"];

export function DesignerInspector({
  selectedNode,
  selectedEdge,
  onUpdateNode,
  onUpdateEdge,
  onDeleteNode,
  onDeleteEdge,
}: Props) {
  if (!selectedNode && !selectedEdge) {
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Inspector</h3>
        <p className="muted">Select a node or edge to configure agent logic.</p>
      </div>
    );
  }

  if (selectedEdge) {
    const kind = ((selectedEdge.data as { kind?: AgentEdgeKind })?.kind || "listens_to") as AgentEdgeKind;
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Edge</h3>
        <p className="muted">
          {selectedEdge.source} → {selectedEdge.target}
        </p>
        <label className="studio-label">Edge kind</label>
        <select
          className="studio-select"
          value={kind}
          onChange={(e) => onUpdateEdge(selectedEdge.id, e.target.value as AgentEdgeKind)}
        >
          {EDGE_KINDS.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>
        <div className="actions" style={{ marginTop: "1rem" }}>
          <button type="button" className="secondary" onClick={() => onDeleteEdge(selectedEdge.id)}>
            Delete edge
          </button>
        </div>
      </div>
    );
  }

  if (!selectedNode) return null;

  const kind = (selectedNode.type as AgentNodeKind) || "action";
  const data = selectedNode.data as {
    name?: string;
    config?: Record<string, unknown>;
  };
  const config = data.config || {};
  const name = data.name || "";

  return (
    <div className="studio-inspector card">
      <h3 style={{ marginTop: 0 }}>{kindLabel(kind)}</h3>
      <p className="muted">
        Node <code>{selectedNode.id}</code>
      </p>

      <label className="studio-label">Name</label>
      <input
        className="studio-input"
        type="text"
        value={name}
        onChange={(e) => onUpdateNode(selectedNode.id, { name: e.target.value })}
      />

      {kind === "input_event" || kind === "output_event" ? (
        <>
          <label className="studio-label">Event type</label>
          <input
            className="studio-input"
            type="text"
            value={String(config.event_type || "")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, event_type: e.target.value },
              })
            }
          />
        </>
      ) : null}

      {kind === "action" ? (
        <>
          <label className="studio-label">Listens to (JSON array)</label>
          <textarea
            className="studio-textarea"
            rows={3}
            defaultValue={JSON.stringify(config.listens_to || ["_input_event"])}
            key={`${selectedNode.id}-listens`}
            onBlur={(e) => {
              try {
                const listens_to = JSON.parse(e.target.value) as string[];
                onUpdateNode(selectedNode.id, { config: { ...config, listens_to } });
              } catch {
                /* keep previous */
              }
            }}
          />
        </>
      ) : null}

      {kind === "tool" ? (
        <>
          <label className="studio-label">Tool ref</label>
          <select
            className="studio-select"
            value={String(config.tool_ref || "double")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, tool_ref: e.target.value },
              })
            }
          >
            <option value="double">double</option>
            <option value="scale">scale</option>
            <option value="identity">identity</option>
          </select>
          <label className="studio-label">Expression</label>
          <input
            className="studio-input"
            type="text"
            value={String(config.expression || "")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, expression: e.target.value },
              })
            }
          />
          {config.tool_ref === "scale" || String(config.expression || "").includes("factor") ? (
            <>
              <label className="studio-label">Scale factor</label>
              <input
                className="studio-input"
                type="number"
                min={1}
                value={Number(config.factor || 2)}
                onChange={(e) =>
                  onUpdateNode(selectedNode.id, {
                    config: { ...config, factor: parseInt(e.target.value, 10) || 2 },
                  })
                }
              />
            </>
          ) : null}
        </>
      ) : null}

      {kind === "prompt" ? (
        <PromptInstructionFields
          nodeId={selectedNode.id}
          config={config}
          onUpdate={(nodeId, patch) => onUpdateNode(nodeId, patch)}
        />
      ) : null}

      {kind === "llm_call" ? (
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          Uses platform LLM settings from <strong>Settings</strong> when{" "}
          <code>use_platform_llm</code> is enabled.
        </p>
      ) : null}

      <div className="actions" style={{ marginTop: "1rem" }}>
        <button type="button" className="secondary" onClick={() => onDeleteNode(selectedNode.id)}>
          Delete node
        </button>
      </div>
    </div>
  );
}
