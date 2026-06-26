import { Link } from "react-router-dom";
import type { Edge, Node } from "@xyflow/react";
import type {
  AgentDefinition,
  AgentEdgeKind,
  AgentNodeKind,
  McpCatalog,
  McpInstance,
} from "../api/types";
import { DesignerLlmCallFields } from "./DesignerLlmCallFields";
import { PromptInstructionFields } from "./DesignerPromptPanel";
import { kindLabel } from "./definitionUtils";
import {
  attachedMcpOptions,
  catalogToolsForInstance,
  instanceById,
} from "./mcpUtils";

interface Props {
  definition: AgentDefinition | null;
  mcpInstances: McpInstance[];
  mcpCatalog: McpCatalog | null;
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  onUpdateDefinition: (patch: Partial<AgentDefinition>) => void;
  onUpdateNode: (nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) => void;
  onUpdateEdge: (edgeId: string, kind: AgentEdgeKind) => void;
  onDeleteNode: (nodeId: string) => void;
  onDeleteEdge: (edgeId: string) => void;
}

const EDGE_KINDS: AgentEdgeKind[] = ["listens_to", "calls", "emits"];

export function DesignerInspector({
  definition,
  mcpInstances,
  mcpCatalog,
  selectedNode,
  selectedEdge,
  onUpdateDefinition,
  onUpdateNode,
  onUpdateEdge,
  onDeleteNode,
  onDeleteEdge,
}: Props) {
  if (!selectedNode && !selectedEdge) {
    const attached = definition?.mcp_servers || [];
    const options = attachedMcpOptions(mcpInstances, attached);
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Agent</h3>
        <p className="muted">
          Attach platform MCP instances this agent may call. Configure servers in{" "}
          <Link to="/settings">Settings</Link>.
        </p>
        {options.length === 0 ? (
          <p className="muted">No enabled MCP instances. Enable one in Settings first.</p>
        ) : (
          <div className="designer-mcp-attach-list">
            {options.map((inst) => {
              const checked = attached.includes(inst.instance_id);
              return (
                <label key={inst.instance_id} className="designer-field designer-checkbox-field">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const next = e.target.checked
                        ? [...attached, inst.instance_id]
                        : attached.filter((id) => id !== inst.instance_id);
                      onUpdateDefinition({ mcp_servers: next });
                    }}
                  />
                  <span>
                    {inst.display_name} <code>{inst.instance_id}</code>
                  </span>
                </label>
              );
            })}
          </div>
        )}
        {definition?.type === "react" && attached.length > 0 && (
          <p className="muted" style={{ fontSize: "0.85rem", marginTop: "1rem" }}>
            ReAct agents expose attached MCP tools to the LLM planner loop.
          </p>
        )}
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
  const attached = definition?.mcp_servers || [];
  const mcpServerOptions = attachedMcpOptions(mcpInstances, attached);

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

      {kind === "mcp_tool" ? (
        <>
          <label className="studio-label">MCP server</label>
          <select
            className="studio-select"
            value={String(config.server_ref || "")}
            onChange={(e) => {
              const serverRef = e.target.value;
              const instance = instanceById(mcpInstances, serverRef);
              const toolOptions = catalogToolsForInstance(mcpCatalog, instance);
              onUpdateNode(selectedNode.id, {
                config: {
                  ...config,
                  server_ref: serverRef,
                  tool_name: toolOptions[0] || String(config.tool_name || ""),
                },
              });
            }}
          >
            <option value="">Select server…</option>
            {mcpServerOptions.map((inst) => (
              <option key={inst.instance_id} value={inst.instance_id}>
                {inst.display_name}
              </option>
            ))}
          </select>
          <label className="studio-label">Tool name</label>
          {(() => {
            const instance = instanceById(mcpInstances, String(config.server_ref || ""));
            const toolOptions = catalogToolsForInstance(mcpCatalog, instance);
            if (toolOptions.length > 0) {
              return (
                <select
                  className="studio-select"
                  value={String(config.tool_name || toolOptions[0])}
                  onChange={(e) =>
                    onUpdateNode(selectedNode.id, {
                      config: { ...config, tool_name: e.target.value },
                    })
                  }
                >
                  {toolOptions.map((tool) => (
                    <option key={tool} value={tool}>
                      {tool}
                    </option>
                  ))}
                </select>
              );
            }
            return (
              <input
                className="studio-input"
                type="text"
                value={String(config.tool_name || "")}
                onChange={(e) =>
                  onUpdateNode(selectedNode.id, {
                    config: { ...config, tool_name: e.target.value },
                  })
                }
              />
            );
          })()}
          <label className="studio-label">Argument name</label>
          <input
            className="studio-input"
            type="text"
            value={String(config.arg_name || "ip")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, arg_name: e.target.value },
              })
            }
          />
          <p className="muted" style={{ fontSize: "0.85rem" }}>
            Workflow agents call this tool with a fixed mapping from the input event field.
          </p>
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
        <DesignerLlmCallFields
          config={config}
          onChange={(next) => onUpdateNode(selectedNode.id, { config: next })}
        />
      ) : null}

      <div className="actions" style={{ marginTop: "1rem" }}>
        <button type="button" className="secondary" onClick={() => onDeleteNode(selectedNode.id)}>
          Delete node
        </button>
      </div>
    </div>
  );
}
