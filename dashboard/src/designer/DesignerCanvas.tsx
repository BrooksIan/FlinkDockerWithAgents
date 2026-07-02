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
import { useCallback, useEffect, useMemo, useRef, type DragEvent } from "react";
import type { DesignerDroppedSpec } from "./definitionUtils";
import { connectionInvalidReason, isValidDesignerConnection } from "./definitionUtils";
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
  highlightedNodeIds?: string[];
  highlightedEdgeIds?: string[];
  onNodesChange: OnNodesChange;
  onEdgesChange: OnEdgesChange;
  onConnect: OnConnect;
  onNodeDragStop?: (nodes: Node[]) => void;
  onSelectionChange: (selection: { nodes: Node[]; edges: Edge[] }) => void;
  onDropNode: (spec: DesignerDroppedSpec, position: { x: number; y: number }) => void;
  onInvalidConnection?: (message: string) => void;
}

function DesignerCanvasInner({
  nodes,
  edges,
  highlightedNodeIds = [],
  highlightedEdgeIds = [],
  onNodesChange,
  onEdgesChange,
  onConnect,
  onNodeDragStop,
  onSelectionChange,
  onDropNode,
  onInvalidConnection,
}: InnerProps) {
  const { screenToFlowPosition, fitView } = useReactFlow();
  const proOptions = useMemo(() => ({ hideAttribution: true }), []);
  const initialFitDone = useRef(false);

  useEffect(() => {
    if (nodes.length > 0 && !initialFitDone.current) {
      fitView({ padding: 0.2, duration: 200 });
      initialFitDone.current = true;
    }
  }, [nodes.length, fitView]);

  const decoratedNodes = useMemo(
    () =>
      nodes.map((node) => ({
        ...node,
        className: highlightedNodeIds.includes(node.id) ? "designer-node-highlight" : undefined,
      })),
    [nodes, highlightedNodeIds],
  );

  const decoratedEdges = useMemo(
    () =>
      edges.map((edge) => ({
        ...edge,
        animated: highlightedEdgeIds.includes(edge.id),
        style: highlightedEdgeIds.includes(edge.id)
          ? { stroke: "var(--warn)", strokeWidth: 2 }
          : undefined,
      })),
    [edges, highlightedEdgeIds],
  );

  const isValidConnection = useCallback(
    (connection: Connection | Edge) => {
      const source = nodes.find((node) => node.id === connection.source);
      const target = nodes.find((node) => node.id === connection.target);
      return isValidDesignerConnection(source, target);
    },
    [nodes],
  );

  const handleConnect = useCallback(
    (connection: Connection) => {
      const source = nodes.find((node) => node.id === connection.source);
      const target = nodes.find((node) => node.id === connection.target);
      const reason = connectionInvalidReason(source, target);
      if (reason) {
        onInvalidConnection?.(reason);
        return;
      }
      onConnect(connection);
    },
    [nodes, onConnect, onInvalidConnection],
  );

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
      <div className="designer-canvas-toolbar">
        <button
          type="button"
          className="secondary designer-canvas-fit"
          onClick={() => fitView({ padding: 0.2, duration: 200 })}
        >
          Fit view
        </button>
      </div>
      <ReactFlow
        nodes={decoratedNodes}
        edges={decoratedEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={handleConnect}
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
