import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { AgentNodeKind, McpCatalog, McpInstance } from "../api/types";
import type { DesignerDroppedSpec } from "./definitionUtils";
import { defaultPromptConfig } from "./promptDefaults";
import { kindLabel } from "./definitionUtils";
import { attachedMcpOptions } from "./mcpUtils";

interface Props {
  agentType: string;
  mcpInstances: McpInstance[];
  mcpAttached: string[];
  onAdd: (spec: DesignerDroppedSpec) => void;
}

function dragPayload(spec: DesignerDroppedSpec) {
  return JSON.stringify(spec);
}

function PaletteItem({
  label,
  sub,
  spec,
  onClick,
}: {
  label: string;
  sub?: string;
  spec: DesignerDroppedSpec;
  onClick: () => void;
}) {
  return (
    <div
      className="studio-palette-item"
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData("application/reactflow", dragPayload(spec));
        e.dataTransfer.effectAllowed = "move";
      }}
      onClick={onClick}
      role="button"
      tabIndex={0}
      aria-label={sub ? `${label} — ${sub}` : label}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick();
        }
      }}
    >
      <span className="studio-palette-item-label">{label}</span>
      {sub && <span className="studio-palette-item-sub muted">{sub}</span>}
    </div>
  );
}

const WORKFLOW_BLOCKS: DesignerDroppedSpec[] = [
  { kind: "input_event", name: "InputEvent", config: { event_type: "_input_event" } },
  { kind: "action", name: "process", config: { listens_to: ["_input_event"] } },
  { kind: "tool", name: "double", config: { tool_ref: "double", expression: "value * 2" } },
  {
    kind: "tool",
    name: "scale",
    config: { tool_ref: "scale", expression: "value * 3", factor: 3 },
  },
  { kind: "output_event", name: "OutputEvent", config: { event_type: "_output_event" } },
];

function blockSpec(kind: AgentNodeKind, name: string, config: Record<string, unknown>): DesignerDroppedSpec {
  return { kind, name, config };
}

export function DesignerPalette({ agentType, mcpInstances, mcpAttached, onAdd }: Props) {
  const [mcpCatalog, setMcpCatalog] = useState<McpCatalog | null>(null);

  useEffect(() => {
    api.mcpCatalog().then(setMcpCatalog).catch(() => setMcpCatalog(null));
  }, []);

  const blocks =
    agentType === "react"
      ? [
          ...WORKFLOW_BLOCKS.slice(0, 2),
          blockSpec("prompt", "prompt", defaultPromptConfig()),
          blockSpec("llm_call", "llm", { use_platform_llm: true, mode: "simple" }),
          WORKFLOW_BLOCKS[4],
        ]
      : WORKFLOW_BLOCKS;

  const mcpOptions = attachedMcpOptions(mcpInstances, mcpAttached);

  function mcpToolSpecs(): DesignerDroppedSpec[] {
    const specs: DesignerDroppedSpec[] = [];
    for (const inst of mcpOptions) {
      const tools =
        mcpCatalog?.categories
          .flatMap((category) => category.servers)
          .find((server) => server.id === inst.catalog_id)?.tools ?? [];
      if (tools.length === 0) {
        specs.push({
          kind: "mcp_tool",
          name: "mcp_tool",
          config: { server_ref: inst.instance_id, tool_name: "", arg_name: "ip" },
        });
        continue;
      }
      for (const tool of tools) {
        specs.push({
          kind: "mcp_tool",
          name: tool.name,
          config: {
            server_ref: inst.instance_id,
            tool_name: tool.name,
            arg_name: tool.name.includes("ip") ? "ip" : "input",
          },
        });
      }
    }
    return specs;
  }

  const mcpSpecs = mcpToolSpecs();

  return (
    <div className="studio-palette card">
      <h3 style={{ marginTop: 0 }}>Palette</h3>
      <p className="muted">
        Drag blocks onto the canvas. Connect input → action → tools → output.
      </p>

      <h4>Events</h4>
      <div className="studio-palette-actions">
        {blocks
          .filter((b) => b.kind === "input_event" || b.kind === "output_event")
          .map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.name}`}
              label={`+ ${kindLabel(spec.kind)}`}
              sub={spec.name}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))}
      </div>

      <h4>Logic</h4>
      <div className="studio-palette-actions">
        {blocks
          .filter((b) => b.kind === "action" || b.kind === "prompt" || b.kind === "llm_call")
          .map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.name}`}
              label={`+ ${kindLabel(spec.kind)}`}
              sub={spec.name}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))}
      </div>

      <h4>MCP tools</h4>
      <div className="studio-palette-actions">
        {mcpSpecs.length === 0 ? (
          <p className="muted" style={{ fontSize: "0.85rem", margin: 0 }}>
            Attach MCP servers on the canvas background to add tools here.
          </p>
        ) : (
          mcpSpecs.map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.config?.server_ref}-${spec.name}`}
              label={`+ ${spec.name}`}
              sub={String(spec.config?.server_ref || "")}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))
        )}
      </div>

      <h4>Built-in tools</h4>
      <div className="studio-palette-actions">
        {blocks
          .filter((b) => b.kind === "tool")
          .map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.name}`}
              label={`+ ${spec.name}`}
              sub={(spec.config?.expression as string) || ""}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))}
      </div>
    </div>
  );
}
