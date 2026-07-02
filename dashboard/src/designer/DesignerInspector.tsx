import { Link } from "react-router-dom";
import type { Edge, Node } from "@xyflow/react";
import { useMemo, useState } from "react";
import type {
  AgentDefinition,
  AgentEdgeKind,
  AgentNodeKind,
  McpCatalog,
  McpInstance,
} from "../api/types";
import { DesignerLlmCallFields } from "./DesignerLlmCallFields";
import { PromptInstructionFields } from "./DesignerPromptPanel";
import { BUILTIN_TOOLS, builtinToolByName } from "./builtinTools";
import { kindLabel } from "./definitionUtils";
import {
  attachedMcpOptions,
  catalogToolsForInstance,
  instanceById,
} from "./mcpUtils";

interface Props {
  definition: AgentDefinition | null;
  nodes: Node[];
  mcpInstances: McpInstance[];
  mcpCatalog: McpCatalog | null;
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  onOpenSettings?: () => void;
  onUpdateNode: (nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) => void;
  onUpdateEdge: (edgeId: string, kind: AgentEdgeKind) => void;
  onDeleteNode: (nodeId: string) => void;
  onDeleteEdge: (edgeId: string) => void;
}

const EDGE_KINDS: AgentEdgeKind[] = ["listens_to", "calls", "emits"];

function eventTypeOptions(nodes: Node[]): string[] {
  const values = new Set<string>(["_input_event", "_output_event"]);
  for (const node of nodes) {
    if (node.type === "input_event" || node.type === "output_event") {
      const config = (node.data as { config?: Record<string, unknown> })?.config || {};
      const eventType = String(config.event_type || "").trim();
      if (eventType) values.add(eventType);
    }
  }
  return Array.from(values);
}

export function DesignerInspector({
  definition,
  nodes,
  mcpInstances,
  mcpCatalog,
  selectedNode,
  selectedEdge,
  onOpenSettings,
  onUpdateNode,
  onUpdateEdge,
  onDeleteNode,
  onDeleteEdge,
}: Props) {
  const [listensDraft, setListensDraft] = useState<string | null>(null);
  const [listensError, setListensError] = useState<string | null>(null);

  const eventOptions = useMemo(() => eventTypeOptions(nodes), [nodes]);

  if (!selectedNode && !selectedEdge) {
    const attachedCount = definition?.mcp_servers?.length || 0;
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Inspector</h3>
        <p className="muted">
          Select a node or edge on the canvas to edit it. Agent-wide settings — MCP servers,
          schemas, and catalog metadata — live in{" "}
          {onOpenSettings ? (
            <button type="button" className="designer-inline-link" onClick={onOpenSettings}>
              Agent settings
            </button>
          ) : (
            "Agent settings"
          )}
          .
        </p>
        <dl className="designer-inspector-summary">
          <div>
            <dt>Type</dt>
            <dd>{definition?.type || "—"}</dd>
          </div>
          <div>
            <dt>Status</dt>
            <dd>{definition?.status || "—"}</dd>
          </div>
          <div>
            <dt>MCP attached</dt>
            <dd>{attachedCount}</dd>
          </div>
          <div>
            <dt>Schemas</dt>
            <dd>
              {Object.keys(definition?.input_schema?.properties || {}).length || 0} in /{" "}
              {Object.keys(definition?.output_schema?.properties || {}).length || 0} out
            </dd>
          </div>
        </dl>
        {onOpenSettings && (
          <button type="button" className="secondary" onClick={onOpenSettings}>
            Open agent settings
          </button>
        )}
        <p className="muted" style={{ fontSize: "0.85rem", marginTop: "1rem" }}>
          Configure MCP servers in <Link to="/settings">Settings</Link> before attaching them here.
        </p>
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
  const listensTo = Array.isArray(config.listens_to)
    ? (config.listens_to as string[])
    : ["_input_event"];

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
          <label className="studio-label">Listens to</label>
          <div className="designer-chip-list">
            {eventOptions.map((eventType) => {
              const checked = listensTo.includes(eventType);
              return (
                <label key={eventType} className="designer-chip">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const next = e.target.checked
                        ? [...listensTo, eventType]
                        : listensTo.filter((item) => item !== eventType);
                      onUpdateNode(selectedNode.id, {
                        config: { ...config, listens_to: next.length ? next : ["_input_event"] },
                      });
                    }}
                  />
                  <span>{eventType}</span>
                </label>
              );
            })}
          </div>
          <details className="designer-env-hint">
            <summary>Advanced JSON</summary>
            <textarea
              className="studio-textarea"
              rows={3}
              value={listensDraft ?? JSON.stringify(listensTo, null, 2)}
              onChange={(e) => {
                setListensDraft(e.target.value);
                setListensError(null);
              }}
              onBlur={(e) => {
                try {
                  const parsed = JSON.parse(e.target.value) as unknown;
                  if (!Array.isArray(parsed) || !parsed.every((item) => typeof item === "string")) {
                    setListensError("Must be a JSON array of strings.");
                    return;
                  }
                  onUpdateNode(selectedNode.id, {
                    config: { ...config, listens_to: parsed },
                  });
                  setListensDraft(null);
                  setListensError(null);
                } catch {
                  setListensError("Invalid JSON.");
                }
              }}
            />
            {listensError && <p className="error">{listensError}</p>}
          </details>
        </>
      ) : null}

      {kind === "tool" ? (
        <>
          <label className="studio-label">Tool ref</label>
          <select
            className="studio-select"
            value={String(config.tool_ref || "double")}
            onChange={(e) => {
              const tool = builtinToolByName(e.target.value);
              onUpdateNode(selectedNode.id, {
                config: {
                  ...config,
                  tool_ref: e.target.value,
                  expression: tool?.defaultExpression || config.expression,
                },
              });
            }}
          >
            {BUILTIN_TOOLS.map((tool) => (
              <option key={tool.name} value={tool.name}>
                {tool.name}
              </option>
            ))}
          </select>
          <p className="muted" style={{ fontSize: "0.85rem" }}>
            {builtinToolByName(String(config.tool_ref || "double"))?.description}
          </p>
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
