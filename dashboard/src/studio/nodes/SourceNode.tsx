import { Handle, Position, type NodeProps } from "@xyflow/react";

export type SourceNodeData = {
  label: string;
  sourceType?: "records" | "kafka";
  kafkaTopic?: string;
  recordCount?: number;
  runStatus?: string;
};

export function SourceNode({ data, selected }: NodeProps) {
  const d = data as SourceNodeData;
  const isKafka = d.sourceType === "kafka" || Boolean(d.kafkaTopic);
  return (
    <div className={`studio-node source ${selected ? "selected" : ""} ${d.runStatus || ""}`}>
      <div className="studio-node-body">
        <div className="studio-node-title">{isKafka ? "Kafka source" : "Source"}</div>
        <div className="studio-node-sub muted">
          {isKafka
            ? d.kafkaTopic || "Select topic"
            : d.recordCount != null
              ? `${d.recordCount} record(s)`
              : "Sample input"}
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
