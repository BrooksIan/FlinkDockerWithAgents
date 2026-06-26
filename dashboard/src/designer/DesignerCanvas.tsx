import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type Edge,
  type Node,
  type OnConnect,
  type OnEdgesChange,
  type OnNodesChange,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useCallback, useEffect, useMemo, type DragEvent } from "react";
import type { DesignerDroppedSpec } from "./definitionUtils";
import { LogicNode } from "./nodes/LogicNode";

const nodeTypes = {
  input_event: LogicNode,
  action: LogicNode,
  tool: LogicNode,
  mcp_tool: LogicNode,
  output_event: LogicNode,
  prompt: LogicNode,
  llm_call: LogicNode,
};

interface InnerProps {
  nodes: Node[];
  edges: Edge[];
  onNodesChange: OnNodesChange;
  onEdgesChange: OnEdgesChange;
  onConnect: OnConnect;
  onNodeDragStop?: (nodes: Node[]) => void;
  onSelectionChange: (selection: { nodes: Node[]; edges: Edge[] }) => void;
  onDropNode: (spec: DesignerDroppedSpec, position: { x: number; y: number }) => void;
}

function DesignerCanvasInner({
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onNodeDragStop,
  onSelectionChange,
  onDropNode,
}: InnerProps) {
  const { screenToFlowPosition, fitView } = useReactFlow();
  const proOptions = useMemo(() => ({ hideAttribution: true }), []);

  useEffect(() => {
    if (nodes.length > 0) {
      fitView({ padding: 0.2, duration: 200 });
    }
  }, [nodes.length, fitView]);

  const isValidConnection = useCallback((connection: Connection | Edge) => {
    const source = "source" in connection ? connection.source : null;
    const target = "target" in connection ? connection.target : null;
    if (!source || !target) return false;
    return source !== target;
  }, []);

  const onDragOver = useCallback((event: DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
  }, []);

  const onDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault();
      const raw = event.dataTransfer.getData("application/reactflow");
      if (!raw) return;
      try {
        const spec = JSON.parse(raw) as DesignerDroppedSpec;
        const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
        onDropNode(spec, position);
      } catch {
        /* ignore malformed drag payload */
      }
    },
    [onDropNode, screenToFlowPosition],
  );

  return (
    <div className="studio-canvas card designer-canvas" onDragOver={onDragOver} onDrop={onDrop}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        isValidConnection={isValidConnection}
        onNodeDragStop={(_, __, draggedNodes) => onNodeDragStop?.(draggedNodes)}
        onSelectionChange={({ nodes: ns, edges: es }) => onSelectionChange({ nodes: ns, edges: es })}
        nodeTypes={nodeTypes}
        proOptions={proOptions}
        snapToGrid
        snapGrid={[16, 16]}
        connectionRadius={28}
        nodesDraggable
        nodesConnectable
        elementsSelectable
        elevateEdgesOnSelect
        defaultEdgeOptions={{ type: "smoothstep", animated: false }}
      >
        <Background gap={16} />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable />
      </ReactFlow>
    </div>
  );
}

interface Props extends Omit<InnerProps, "onDropNode"> {
  onDropNode: (spec: DesignerDroppedSpec, position: { x: number; y: number }) => void;
}

export function DesignerCanvas(props: Props) {
  return (
    <ReactFlowProvider>
      <DesignerCanvasInner {...props} />
    </ReactFlowProvider>
  );
}
