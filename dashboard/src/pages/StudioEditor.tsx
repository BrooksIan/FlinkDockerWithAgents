import {
  useEdgesState,
  useNodesState,
  applyEdgeChanges,
  type Edge,
  type EdgeChange,
  type Node,
} from "@xyflow/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import type { AgentSummary, KafkaTopicSummary, PipelineSummary, PipelineValidation } from "../api/types";
import { AgentGraphPanel } from "../studio/AgentGraphPanel";
import { NodePalette } from "../studio/NodePalette";
import { PipelineInspector } from "../studio/PipelineInspector";
import { RunPipelineBar } from "../studio/RunPipelineBar";
import { StudioCanvas, type DroppedNodeSpec } from "../studio/StudioCanvas";
import { connectEdge, autoWireLinear, flowToPipeline, nextId, pipelineToFlow } from "../studio/pipelineUtils";

export function StudioEditorPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [pipeline, setPipeline] = useState<PipelineSummary | null>(null);
  const [agents, setAgents] = useState<AgentSummary[]>([]);
  const [kafkaTopics, setKafkaTopics] = useState<KafkaTopicSummary[]>([]);
  const [kafkaReachable, setKafkaReachable] = useState<boolean | undefined>(undefined);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges] = useEdgesState<Edge>([]);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [validation, setValidation] = useState<PipelineValidation | null>(null);
  const [running, setRunning] = useState(false);
  const [lastRunId, setLastRunId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [drillAgent, setDrillAgent] = useState<string | null>(null);
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved">("idle");
  const saveTimer = useRef<number | null>(null);

  const nodesRef = useRef(nodes);
  const edgesRef = useRef(edges);
  nodesRef.current = nodes;
  edgesRef.current = edges;

  useEffect(() => {
    if (!id) return;
    Promise.all([api.pipeline(id), api.agents(), api.kafkaTopics()])
      .then(([p, a, kafka]) => {
        setPipeline(p);
        setAgents(a);
        setKafkaTopics(kafka.topics);
        setKafkaReachable(kafka.reachable);
        const flow = pipelineToFlow(p, a);
        setNodes(flow.nodes);
        setEdges(flow.edges);
      })
      .catch((e) => setError(String(e)));
  }, [id, setNodes, setEdges]);

  const persist = useCallback(
    (nextNodes: Node[], nextEdges: Edge[], name?: string): Promise<void> => {
      if (!pipeline || !id) return Promise.resolve();
      const body = flowToPipeline(id, name || pipeline.name, nextNodes, nextEdges, {
        created_at: pipeline.created_at,
        updated_at: pipeline.updated_at,
      });
      setSaveState("saving");
      return api
        .updatePipeline(id, { name: body.name, nodes: body.nodes, edges: body.edges, layout: body.layout })
        .then((updated) => {
          setPipeline(updated);
          setSaveState("saved");
        })
        .catch((e) => {
          setError(String(e));
          setSaveState("idle");
          throw e;
        });
    },
    [pipeline, id],
  );

  const scheduleSave = useCallback(
    (nextNodes: Node[], nextEdges: Edge[]) => {
      if (saveTimer.current) window.clearTimeout(saveTimer.current);
      saveTimer.current = window.setTimeout(() => persist(nextNodes, nextEdges), 800);
    },
    [persist],
  );

  const onConnect = useCallback(
    (params: Parameters<typeof connectEdge>[1]) => {
      setEdges((eds) => {
        const next = connectEdge(eds, params);
        edgesRef.current = next;
        scheduleSave(nodesRef.current, next);
        return next;
      });
    },
    [scheduleSave, setEdges],
  );

  const handleEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      setEdges((eds) => {
        const next = applyEdgeChanges(changes, eds);
        edgesRef.current = next;
        scheduleSave(nodesRef.current, next);
        return next;
      });
    },
    [scheduleSave, setEdges],
  );

  function handleNodeDragStop(draggedNodes: Node[]) {
    scheduleSave(draggedNodes, edgesRef.current);
  }

  function createNode(spec: DroppedNodeSpec, position: { x: number; y: number }): Node {
    if (spec.kind === "source") {
      if (spec.kafkaTopic) {
        return {
          id: nextId("src"),
          type: "source",
          position,
          data: {
            label: "Kafka source",
            sourceType: "kafka",
            kafkaTopic: spec.kafkaTopic,
            config: {
              source_type: "kafka",
              topic: spec.kafkaTopic,
              max_records: 10,
            },
          },
        };
      }
      return {
        id: nextId("src"),
        type: "source",
        position,
        data: {
          label: "Source",
          sourceType: "records",
          recordCount: 2,
          config: {
            source_type: "records",
            records: [{ key: "1", value: 3 }],
          },
        },
      };
    }
    if (spec.kind === "sink") {
      return { id: nextId("sink"), type: "sink", position, data: { label: "Sink" } };
    }
    return {
      id: nextId("agent"),
      type: "agent",
      position,
      data: {
        label: spec.agent,
        agent: spec.agent,
        agentType: spec.agentType || "workflow",
        description: spec.description,
      },
    };
  }

  function addNodeAt(spec: DroppedNodeSpec, position: { x: number; y: number }) {
    const node = createNode(spec, position);
    const next = [...nodes, node];
    setNodes(next);
    scheduleSave(next, edges);
  }

  function handleDropNode(spec: DroppedNodeSpec, position: { x: number; y: number }) {
    addNodeAt(spec, position);
  }

  function addSource() {
    addNodeAt({ kind: "source" }, { x: 80, y: 120 + nodes.length * 40 });
  }

  function addKafkaSource(topic: KafkaTopicSummary) {
    addNodeAt(
      { kind: "source", kafkaTopic: topic.name, kafkaDescription: topic.description },
      { x: 80, y: 120 + nodes.length * 40 },
    );
  }

  function addSink() {
    addNodeAt({ kind: "sink" }, { x: 600, y: 120 + nodes.length * 40 });
  }

  function addAgent(agent: AgentSummary) {
    addNodeAt(
      { kind: "agent", agent: agent.name, agentType: agent.type, description: agent.description },
      { x: 300, y: 120 + nodes.length * 40 },
    );
  }

  function handleUpdateNode(nodeId: string, patch: { config?: Record<string, unknown> }) {
    const next = nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const prev = n.data as {
        config?: Record<string, unknown>;
        sourceType?: string;
        kafkaTopic?: string;
        recordCount?: number;
      };
      const config = patch.config ?? prev.config;
      const data: typeof prev & Record<string, unknown> = { ...n.data, config };
      if (config?.source_type === "kafka") {
        data.sourceType = "kafka";
        data.kafkaTopic = config.topic;
        data.recordCount = undefined;
      } else if (config?.records) {
        data.sourceType = "records";
        data.kafkaTopic = undefined;
        data.recordCount = (config.records as unknown[]).length;
      }
      return { ...n, data };
    });
    setNodes(next);
    scheduleSave(next, edges);
  }

  function handleUpdateEdge(edgeId: string, mapping: Record<string, string>) {
    const next = edges.map((e) => (e.id === edgeId ? { ...e, data: { mapping } } : e));
    setEdges(next);
    scheduleSave(nodes, next);
  }

  async function handleValidate() {
    if (!id) return;
    try {
      await persist(nodes, edges);
      const result = await api.validatePipeline(id);
      setValidation(result);
      if (!result.valid) {
        setError(result.errors.join(" · "));
      } else {
        setError(null);
      }
    } catch (e) {
      setError(String(e));
    }
  }

  function handleConnectChain() {
    const nextEdges = autoWireLinear(nodes, edges);
    setEdges(nextEdges);
    scheduleSave(nodes, nextEdges);
    setError(null);
  }

  async function handleRun() {
    if (!id) return;
    setRunning(true);
    setError(null);
    try {
      let nextEdges = edges;
      if (nextEdges.length < nodes.length - 1 && nodes.length >= 2) {
        nextEdges = autoWireLinear(nodes, nextEdges);
        setEdges(nextEdges);
      }
      await persist(nodes, nextEdges);
      const check = await api.validatePipeline(id);
      setValidation(check);
      if (!check.valid) {
        setError(check.errors.join(" · "));
        return;
      }
      const result = await api.runPipeline(id);
      setLastRunId(result.run_id);
      if (result.validation) setValidation(result.validation);
      navigate(`/runs/${result.run_id}`);
    } catch (e) {
      setError(String(e));
    } finally {
      setRunning(false);
    }
  }

  function handleNodeDoubleClick(node: Node) {
    if (node.type === "agent") {
      const agent = (node.data as { agent?: string }).agent;
      if (agent) setDrillAgent(agent);
    }
  }

  if (!id) return null;

  return (
    <div className="studio-page">
      <p>
        <Link to="/studio">← Studio</Link>
      </p>
      <div className="studio-header">
        <h2 style={{ margin: 0 }}>
          {pipeline?.name || "Loading…"}
          <span className="muted" style={{ fontSize: "0.85rem", marginLeft: "0.75rem" }}>
            {saveState === "saving" ? "Saving…" : saveState === "saved" ? "Saved" : ""}
          </span>
        </h2>
      </div>
      {error && <p className="error">{error}</p>}

      <RunPipelineBar
        validation={validation}
        running={running}
        lastRunId={lastRunId}
        onValidate={handleValidate}
        onConnectChain={handleConnectChain}
        onRun={handleRun}
      />

      <div className="studio-layout">
        <NodePalette
          agents={agents}
          kafkaTopics={kafkaTopics}
          kafkaReachable={kafkaReachable}
          onAddSource={addSource}
          onAddKafkaSource={addKafkaSource}
          onAddSink={addSink}
          onAddAgent={addAgent}
        />
        <StudioCanvas
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={handleEdgesChange}
          onConnect={onConnect}
          onNodeDoubleClick={handleNodeDoubleClick}
          onNodeDragStop={handleNodeDragStop}
          onDropNode={handleDropNode}
          onSelectionChange={({ nodes: ns, edges: es }) => {
            setSelectedNode(ns[0] || null);
            setSelectedEdge(es[0] || null);
          }}
        />
        <PipelineInspector
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          kafkaTopics={kafkaTopics}
          onUpdateNode={handleUpdateNode}
          onUpdateEdge={handleUpdateEdge}
        />
      </div>

      {drillAgent && <AgentGraphPanel agentName={drillAgent} onClose={() => setDrillAgent(null)} />}
    </div>
  );
}
