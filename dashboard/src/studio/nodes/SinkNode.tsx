import { Handle, Position, type NodeProps } from "@xyflow/react";

export type SinkNodeData = {
  label: string;
  runStatus?: string;
};

export function SinkNode({ data, selected }: NodeProps) {
  const d = data as SinkNodeData;
  return (
    <div className={`studio-node sink ${selected ? "selected" : ""} ${d.runStatus || ""}`}>
      <Handle
        id="in"
        type="target"
        position={Position.Left}
        className="studio-handle studio-handle-in"
        isConnectable
      />
      <div className="studio-node-body">
        <div className="studio-node-title">Sink</div>
        <div className="studio-node-sub muted">Pipeline output</div>
      </div>
    </div>
  );
}
