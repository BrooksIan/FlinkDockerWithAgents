import { Handle, Position, type NodeProps } from "@xyflow/react";

export type WindowNodeData = {
  label: string;
  keyField?: string;
  gapPolicy?: string;
  gapMs?: number;
  executionMode?: string;
  config?: Record<string, unknown>;
};

export function WindowNode({ data, selected }: NodeProps) {
  const d = data as WindowNodeData;
  const config = d.config || {};
  const keyField = d.keyField || (config.key_field as string) || "key";
  const gapPolicy = d.gapPolicy || (config.gap_policy as string) || "default";
  const gapMs = d.gapMs ?? (config.gap_ms as number) ?? 1000;
  const gapLabel = gapPolicy === "default" ? `${gapMs}ms` : gapPolicy;
  return (
    <div className={`studio-node window ${selected ? "selected" : ""}`}>
      <Handle
        id="in"
        type="target"
        position={Position.Left}
        className="studio-handle studio-handle-in"
        isConnectable
      />
      <div className="studio-node-body">
        <div className="studio-node-title">Session window</div>
        <div className="studio-node-sub muted">
          key: {keyField} · {gapLabel}
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
