import { useCallback, useRef, useState } from "react";
import type { Edge, Node } from "@xyflow/react";

type Snapshot = {
  nodes: Node[];
  edges: Edge[];
};

const MAX_HISTORY = 50;

function cloneSnapshot(nodes: Node[], edges: Edge[]): Snapshot {
  return {
    nodes: structuredClone(nodes),
    edges: structuredClone(edges),
  };
}

export function useDesignerHistory(_initialNodes: Node[], _initialEdges: Edge[]) {
  const [past, setPast] = useState<Snapshot[]>([]);
  const [future, setFuture] = useState<Snapshot[]>([]);
  const skipNextPush = useRef(false);

  const pushSnapshot = useCallback((nodes: Node[], edges: Edge[]) => {
    if (skipNextPush.current) {
      skipNextPush.current = false;
      return;
    }
    setPast((current) => [...current.slice(-(MAX_HISTORY - 1)), cloneSnapshot(nodes, edges)]);
    setFuture([]);
  }, []);

  const undo = useCallback(
    (currentNodes: Node[], currentEdges: Edge[], apply: (nodes: Node[], edges: Edge[]) => void) => {
      setPast((currentPast) => {
        if (currentPast.length === 0) return currentPast;
        const previous = currentPast[currentPast.length - 1];
        setFuture((currentFuture) => [
          cloneSnapshot(currentNodes, currentEdges),
          ...currentFuture,
        ]);
        skipNextPush.current = true;
        apply(previous.nodes, previous.edges);
        return currentPast.slice(0, -1);
      });
    },
    [],
  );

  const redo = useCallback(
    (currentNodes: Node[], currentEdges: Edge[], apply: (nodes: Node[], edges: Edge[]) => void) => {
      setFuture((currentFuture) => {
        if (currentFuture.length === 0) return currentFuture;
        const [next, ...rest] = currentFuture;
        setPast((currentPast) => [...currentPast, cloneSnapshot(currentNodes, currentEdges)]);
        skipNextPush.current = true;
        apply(next.nodes, next.edges);
        return rest;
      });
    },
    [],
  );

  const resetHistory = useCallback(() => {
    setPast([]);
    setFuture([]);
    skipNextPush.current = false;
  }, []);

  return {
    canUndo: past.length > 0,
    canRedo: future.length > 0,
    pushSnapshot,
    undo,
    redo,
    resetHistory,
  };
}
