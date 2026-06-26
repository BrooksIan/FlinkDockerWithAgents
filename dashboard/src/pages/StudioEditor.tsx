import {
  useEdgesState,
  useNodesState,
  applyEdgeChanges,
  applyNodeChanges,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
} from "@xyflow/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import type { AgentCatalog, AgentSummary, KafkaTopicSummary, PipelineSummary, PipelineValidation } from "../api/types";
import { AgentGraphPanel } from "../studio/AgentGraphPanel";
import { NodePalette } from "../studio/NodePalette";
import { PipelineInspector } from "../studio/PipelineInspector";
import { RunPipelineBar } from "../studio/RunPipelineBar";
import { StudioCanvas, type DroppedNodeSpec } from "../studio/StudioCanvas";
import { connectEdge, buildLinearChainEdges, ensureLinearChainEdges, DEFAULT_KAFKA_OUTPUT_TOPIC, flowToPipeline, nextId, pipelineToFlow, pruneOrphanEdges } from "../studio/pipelineUtils";

export function StudioEditorPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [pipeline, setPipeline] = useState<PipelineSummary | null>(null);
  const [agents, setAgents] = useState<AgentSummary[]>([]);
  const [catalog, setCatalog] = useState<AgentCatalog | null>(null);
  const [kafkaTopics, setKafkaTopics] = useState<KafkaTopicSummary[]>([]);
  const [kafkaReachable, setKafkaReachable] = useState<boolean | undefined>(undefined);
  const [nodes, setNodes] = useNodesState<Node>([]);
  const [edges, setEdges] = useEdgesState<Edge>([]);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [validation, setValidation] = useState<PipelineValidation | null>(null);
  const [running, setRunning] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [lastRunId, setLastRunId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [drillAgent, setDrillAgent] = useState<string | null>(null);
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved">("idle");
  const saveTimer = useRef<number | null>(null);
  const nameSaveTimer = useRef<number | null>(null);
  const [pipelineName, setPipelineName] = useState("");

  const nodesRef = useRef(nodes);
  const edgesRef = useRef(edges);
  nodesRef.current = nodes;
  edgesRef.current = edges;

  useEffect(() => {
    if (!id) return;
    Promise.all([api.pipeline(id), api.agents(), api.agentCatalog(), api.kafkaTopics()])
      .then(([p, a, cat, kafka]) => {
        setPipeline(p);
        setPipelineName(p.name);
        setAgents(a);
        setCatalog(cat);
        setKafkaTopics(kafka.topics.filter((t) => !t.name.startsWith("__")));
        setKafkaReachable(kafka.reachable);
        const flow = pipelineToFlow(p, a);
        setNodes(flow.nodes);
        setEdges(flow.edges);
        nodesRef.current = flow.nodes;
        edgesRef.current = flow.edges;
        if (flow.edges.length !== p.edges.length) {
          const body = flowToPipeline(id, p.name, flow.nodes, flow.edges, {
            created_at: p.created_at,
            updated_at: p.updated_at,
          });
          api
            .updatePipeline(id, { nodes: body.nodes, edges: body.edges, layout: body.layout })
            .then((updated) => setPipeline(updated))
            .catch((e) => setError(String(e)));
        }
        api.validatePipeline(id).then(setValidation).catch(() => {});
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
          if (id) {
            api.validatePipeline(id).then(setValidation).catch(() => {});
          }
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

  const saveName = useCallback(
    (name: string) => {
      if (!id || !pipeline) return;
      setSaveState("saving");
      api
        .updatePipeline(id, { name: name.trim() })
        .then((updated) => {
          setPipeline(updated);
          setPipelineName(updated.name);
          setSaveState("saved");
        })
        .catch((e) => {
          setError(String(e));
          setSaveState("idle");
        });
    },
    [id, pipeline],
  );

  const handleNameChange = useCallback(
    (value: string) => {
      setPipelineName(value);
      if (nameSaveTimer.current) window.clearTimeout(nameSaveTimer.current);
      nameSaveTimer.current = window.setTimeout(() => saveName(value), 600);
    },
    [saveName],
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

  const handleNodesChange = useCallback(
    (changes: NodeChange[]) => {
      const removed = changes.some((change) => change.type === "remove");
      setNodes((nds) => {
        const nextNodes = applyNodeChanges(changes, nds);
        nodesRef.current = nextNodes;
        if (removed) {
          setEdges((eds) => {
            const nextEdges = pruneOrphanEdges(nextNodes, eds);
            edgesRef.current = nextEdges;
            scheduleSave(nextNodes, nextEdges);
            return nextEdges;
          });
        } else {
          scheduleSave(nextNodes, edgesRef.current);
        }
        return nextNodes;
      });
    },
    [scheduleSave, setNodes, setEdges],
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
      if (spec.kafkaSource) {
        const defaultTopic = kafkaTopics.find((t) => t.name === "cowrie.normalized")?.name
          || kafkaTopics[0]?.name
          || "";
        return {
          id: nextId("src"),
          type: "source",
          position,
          data: {
            label: "Kafka source",
            sourceType: "kafka",
            kafkaTopic: defaultTopic || undefined,
            config: {
              source_type: "kafka",
              topic: defaultTopic,
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
      if (spec.kafkaSink) {
        const defaultTopic =
          kafkaTopics.find((t) => t.name === DEFAULT_KAFKA_OUTPUT_TOPIC)?.name ||
          DEFAULT_KAFKA_OUTPUT_TOPIC;
        return {
          id: nextId("sink"),
          type: "sink",
          position,
          data: {
            label: "Kafka sink",
            sinkType: "kafka",
            kafkaTopic: defaultTopic || undefined,
            config: {
              sink_type: "kafka",
              topic: defaultTopic,
            },
          },
        };
      }
      return {
        id: nextId("sink"),
        type: "sink",
        position,
        data: {
          label: "Sink",
          sinkType: "capture",
          config: { sink_type: "capture" },
        },
      };
    }
    if (spec.kind === "window") {
      return {
        id: nextId("win"),
        type: "window",
        position,
        data: {
          label: "Session window",
          keyField: "key",
          gapPolicy: "default",
          gapMs: 1000,
          executionMode: "logic",
          config: {
            window_type: "dynamic_session",
            key_field: "key",
            gap_policy: "default",
            gap_ms: 1000,
            time_mode: "processing",
            execution_mode: "logic",
          },
        },
      };
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

  function addKafkaSource() {
    addNodeAt({ kind: "source", kafkaSource: true }, { x: 80, y: 120 + nodes.length * 40 });
  }

  function addWindow() {
    addNodeAt({ kind: "window" }, { x: 240, y: 120 + nodes.length * 40 });
  }

  function addSink() {
    addNodeAt({ kind: "sink" }, { x: 600, y: 120 + nodes.length * 40 });
  }

  function addKafkaSink() {
    addNodeAt({ kind: "sink", kafkaSink: true }, { x: 600, y: 120 + nodes.length * 40 });
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
        sinkType?: string;
        kafkaTopic?: string;
        recordCount?: number;
        keyField?: string;
        gapPolicy?: string;
        executionMode?: string;
      };
      const config = patch.config ?? prev.config;
      const data: typeof prev & Record<string, unknown> = { ...n.data, config };
      if (config?.source_type === "kafka") {
        data.sourceType = "kafka";
        data.kafkaTopic = config.topic as string | undefined;
        data.recordCount = undefined;
      } else if (config?.records) {
        data.sourceType = "records";
        data.kafkaTopic = undefined;
        data.recordCount = (config.records as unknown[]).length;
      }
      if (config?.sink_type === "kafka") {
        data.sinkType = "kafka";
        data.kafkaTopic = config.topic as string | undefined;
      } else if (config?.sink_type === "capture") {
        data.sinkType = "capture";
        data.kafkaTopic = undefined;
      }
      if (n.type === "window" && config) {
        data.keyField = (config.key_field as string) || "key";
        data.gapPolicy = (config.gap_policy as string) || "default";
        data.gapMs = typeof config.gap_ms === "number" ? config.gap_ms : 1000;
        data.executionMode = (config.execution_mode as string) || "logic";
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

  function handleDeleteNode(nodeId: string) {
    handleNodesChange([{ type: "remove", id: nodeId }]);
    setSelectedNode(null);
  }

  async function handleValidate() {
    if (!id) return;
    try {
      const nextEdges = ensureLinearChainEdges(nodes, edges);
      if (nextEdges.length !== edges.length) {
        setEdges(nextEdges);
        edgesRef.current = nextEdges;
      }
      await persist(nodes, nextEdges);
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
    const nextEdges = buildLinearChainEdges(nodes);
    setEdges(nextEdges);
    edgesRef.current = nextEdges;
    scheduleSave(nodes, nextEdges);
    setError(null);
    setValidation(null);
  }

  async function handleRun() {
    if (!id) return;
    setRunning(true);
    setError(null);
    try {
      let nextEdges = ensureLinearChainEdges(nodes, edges);
      if (nextEdges.length !== edges.length) {
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

  async function handleClusterSubmit() {
    if (!id) return;
    setSubmitting(true);
    setError(null);
    try {
      let nextEdges = ensureLinearChainEdges(nodes, edges);
      if (nextEdges.length !== edges.length) {
        setEdges(nextEdges);
      }
      await persist(nodes, nextEdges);
      const check = await api.validatePipeline(id);
      setValidation(check);
      const clusterErrors = check.cluster?.errors ?? [];
      if (!check.valid || (check.cluster && !check.cluster.valid)) {
        setError([...check.errors, ...clusterErrors].join(" · "));
        return;
      }
      const result = await api.submitPipeline(id);
      setLastRunId(result.run_id);
      if (result.validation) {
        setValidation({
          valid: result.validation.valid,
          errors: result.validation.errors,
          warnings: result.validation.warnings,
          cluster: check.cluster,
        });
      }
      navigate(`/runs/${result.run_id}`);
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }

  function clusterBatchBlockedReason(): string | null {
    const source = nodes.find((n) => n.type === "source");
    const hasWindow = nodes.some((n) => n.type === "window");
    const sourceData = source?.data as {
      sourceType?: string;
      config?: { source_type?: string };
    };
    const sourceType = sourceData?.sourceType || sourceData?.config?.source_type || "records";
    if (sourceType === "kafka" && !hasWindow) {
      return "Kafka streaming source requires a window node before cluster submit.";
    }
    return null;
  }

  const windowPreview = nodes.find((n) => n.type === "window");
  const executionPlan = nodes.length
    ? ["source", "window", "agent", "sink"]
        .filter((kind) => nodes.some((n) => n.type === kind))
        .join(" → ")
    : null;

  if (!id) return null;

  return (
    <div className="studio-page">
      <p>
        <Link to="/studio">← Studio</Link>
      </p>
      <div className="studio-header designer-editor-header">
        <div className="designer-editor-title">
          <label className="designer-agent-name-field">
            <span className="muted">Pipeline name</span>
            <input
              className="designer-agent-name-input"
              type="text"
              value={pipelineName}
              placeholder={pipeline ? "Untitled pipeline" : "Loading…"}
              disabled={!pipeline}
              maxLength={120}
              onChange={(e) => handleNameChange(e.target.value)}
              onBlur={() => {
                if (nameSaveTimer.current) {
                  window.clearTimeout(nameSaveTimer.current);
                  nameSaveTimer.current = null;
                }
                if (pipeline && pipelineName.trim() !== pipeline.name) {
                  saveName(pipelineName);
                }
              }}
            />
          </label>
          <span className="muted designer-editor-meta">
            {saveState === "saving" ? "Saving…" : saveState === "saved" ? "Saved" : ""}
          </span>
        </div>
      </div>
      {error && <p className="error">{error}</p>}

      <RunPipelineBar
        validation={validation}
        running={running}
        submitting={submitting}
        lastRunId={lastRunId}
        clusterBlockedReason={clusterBatchBlockedReason()}
        executionPlan={executionPlan}
        windowPreview={
          windowPreview
            ? {
                keyField: (windowPreview.data as { keyField?: string }).keyField || "key",
                gapPolicy: (windowPreview.data as { gapPolicy?: string }).gapPolicy || "default",
              }
            : null
        }
        onValidate={handleValidate}
        onConnectChain={handleConnectChain}
        onRun={handleRun}
        onClusterSubmit={handleClusterSubmit}
      />

      <div className="studio-layout">
        <NodePalette
          agents={agents}
          catalog={catalog}
          kafkaTopics={kafkaTopics}
          kafkaReachable={kafkaReachable}
          onAddSource={addSource}
          onAddKafkaSource={addKafkaSource}
          onAddWindow={addWindow}
          onAddSink={addSink}
          onAddKafkaSink={addKafkaSink}
          onAddAgent={addAgent}
        />
        <StudioCanvas
          nodes={nodes}
          edges={edges}
          onNodesChange={handleNodesChange}
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
          onDeleteNode={handleDeleteNode}
        />
      </div>

      {drillAgent && <AgentGraphPanel agentName={drillAgent} onClose={() => setDrillAgent(null)} />}
    </div>
  );
}
