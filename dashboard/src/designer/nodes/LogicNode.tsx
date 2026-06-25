import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { AgentNodeKind } from "../../api/types";
import { kindLabel } from "../definitionUtils";

export type LogicNodeData = {
  label: string;
  kind: AgentNodeKind;
  name: string;
  config: Record<string, unknown>;
};

export function LogicNode({ data, selected, type }: NodeProps) {
  const d = data as LogicNodeData;
  const kind = (type as AgentNodeKind) || d.kind;
  const showTarget = kind !== "input_event";
  const showSource = kind !== "output_event" && kind !== "tool";

  return (
    <div className={`studio-node designer-node ${kind} ${selected ? "selected" : ""}`}>
      {showTarget && (
        <Handle
          id="in"
          type="target"
          position={Position.Left}
          className="studio-handle studio-handle-in"
          isConnectable
        />
      )}
      <div className="studio-node-body">
        <div className="designer-node-kind muted">{kindLabel(kind)}</div>
        <div className="studio-node-title">{d.name || d.label}</div>
        {kind === "tool" && (
          <div className="studio-node-sub muted">
            {(d.config?.expression as string) || (d.config?.tool_ref as string) || "tool"}
          </div>
        )}
        {kind === "prompt" && (
          <div className="studio-node-sub muted designer-node-prompt-preview">
            {String(d.config?.system || "No system prompt yet").slice(0, 48)}
            {String(d.config?.system || "").length > 48 ? "…" : ""}
          </div>
        )}
      </div>
      {showSource && (
        <Handle
          id="out"
          type="source"
          position={Position.Right}
          className="studio-handle studio-handle-out"
          isConnectable
        />
      )}
      {kind === "action" && (
        <Handle
          id="tool"
          type="source"
          position={Position.Bottom}
          className="studio-handle designer-handle-tool"
          isConnectable
        />
      )}
      {kind === "tool" && (
        <Handle
          id="in-call"
          type="target"
          position={Position.Top}
          className="studio-handle designer-handle-tool"
          isConnectable
        />
      )}
    </div>
  );
}
