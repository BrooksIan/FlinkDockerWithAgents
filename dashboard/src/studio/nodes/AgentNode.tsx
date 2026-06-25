import { Handle, Position, type NodeProps } from "@xyflow/react";
import { TypeBadge } from "../../components/StatusBadge";

export type AgentNodeData = {
  label: string;
  agent: string;
  agentType?: string;
  runStatus?: string;
};

export function AgentNode({ data, selected }: NodeProps) {
  const d = data as AgentNodeData;
  return (
    <div className={`studio-node agent ${selected ? "selected" : ""} ${d.runStatus || ""}`}>
      <Handle
        id="in"
        type="target"
        position={Position.Left}
        className="studio-handle studio-handle-in"
        isConnectable
      />
      <div className="studio-node-body">
        <div className="studio-node-title">{d.agent}</div>
        {d.agentType && <TypeBadge type={d.agentType} />}
        <div className="studio-node-sub muted">Double-click to inspect</div>
      </div>
      <Handle
        id="out"
        type="source"
        position={Position.Right}
        className="studio-handle studio-handle-out"
        isConnectable
      />
    </div>
  );
}
