import { Handle, Position, type NodeProps } from "@xyflow/react";

export type SourceNodeData = {
  label: string;
  recordCount?: number;
  runStatus?: string;
};

export function SourceNode({ data, selected }: NodeProps) {
  const d = data as SourceNodeData;
  return (
    <div className={`studio-node source ${selected ? "selected" : ""} ${d.runStatus || ""}`}>
      <div className="studio-node-body">
        <div className="studio-node-title">Source</div>
        <div className="studio-node-sub muted">
          {d.recordCount != null ? `${d.recordCount} record(s)` : "Sample input"}
        </div>
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
