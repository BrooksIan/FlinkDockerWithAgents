import {
  applyEdgeChanges,
  applyNodeChanges,
  useEdgesState,
  useNodesState,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
} from "@xyflow/react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import type {
  AgentDefinition,
  AgentDefinitionCompileResult,
  AgentDefinitionValidation,
} from "../api/types";
import { CompilePreviewPanel } from "../components/CompilePreviewPanel";
import { DesignerCanvas } from "../designer/DesignerCanvas";
import { DesignerInspector } from "../designer/DesignerInspector";
import { DesignerPalette } from "../designer/DesignerPalette";
import { DesignerPromptPanel } from "../designer/DesignerPromptPanel";
import {
  autoWireAgentGraph,
  connectDesignerEdge,
  defaultConfigForKind,
  defaultNameForKind,
  definitionToFlow,
  flowToDefinition,
  nextId,
  type DesignerDroppedSpec,
} from "../designer/definitionUtils";
import { defaultPromptConfig } from "../designer/promptDefaults";

export function DesignerEditorPage() {
  const { id } = useParams<{ id: string }>();
  const [definition, setDefinition] = useState<AgentDefinition | null>(null);
  const [nodes, setNodes] = useNodesState<Node>([]);
  const [edges, setEdges] = useEdgesState<Edge>([]);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [validation, setValidation] = useState<AgentDefinitionValidation | null>(null);
  const [compileResult, setCompileResult] = useState<AgentDefinitionCompileResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [agentName, setAgentName] = useState("");
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved">("idle");
  const [busy, setBusy] = useState<"validate" | "compile" | "publish" | null>(null);
  const [publishMessage, setPublishMessage] = useState<string | null>(null);
  const saveTimer = useRef<number | null>(null);
  const nameSaveTimer = useRef<number | null>(null);

  const nodesRef = useRef(nodes);
  const edgesRef = useRef(edges);
  const definitionRef = useRef(definition);
  nodesRef.current = nodes;
  edgesRef.current = edges;
  definitionRef.current = definition;

  useEffect(() => {
    if (!id) return;
    api
      .getDesignerDefinition(id)
      .then((def) => {
        setDefinition(def);
        setAgentName(def.name);
        const flow = definitionToFlow(def);
        setNodes(flow.nodes);
        setEdges(flow.edges);
      })
      .catch((e) => setError(String(e)));
  }, [id, setNodes, setEdges]);

  const persist = useCallback(
    (nextNodes: Node[], nextEdges: Edge[]): Promise<void> => {
      if (!definition || !id) return Promise.resolve();
      const body = flowToDefinition(definition, nextNodes, nextEdges);
      setSaveState("saving");
      return api
        .updateDesignerDefinition(id, {
          nodes: body.nodes,
          edges: body.edges,
          layout: body.layout,
          status: "draft",
        })
        .then((updated) => {
          setDefinition(updated);
          setSaveState("saved");
        })
        .catch((e) => {
          setError(String(e));
          setSaveState("idle");
          throw e;
        });
    },
    [definition, id],
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
      if (!id) return;
      const trimmed = name.trim();
      if (!trimmed) {
        setError("Agent name is required");
        return;
      }
      setSaveState("saving");
      api
        .updateDesignerDefinition(id, { name: trimmed })
        .then((updated) => {
          setDefinition(updated);
          setAgentName(updated.name);
          setSaveState("saved");
          setError(null);
        })
        .catch((e) => {
          setError(String(e));
          setSaveState("idle");
        });
    },
    [id],
  );

  const handleNameChange = useCallback(
    (value: string) => {
      setAgentName(value);
      if (nameSaveTimer.current) window.clearTimeout(nameSaveTimer.current);
      nameSaveTimer.current = window.setTimeout(() => saveName(value), 600);
    },
    [saveName],
  );

  const handleNodesChange = useCallback(
    (changes: NodeChange[]) => {
      setNodes((nds) => {
        const next = applyNodeChanges(changes, nds);
        nodesRef.current = next;
        scheduleSave(next, edgesRef.current);
        return next;
      });
    },
    [scheduleSave, setNodes],
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

  const onConnect = useCallback(
    (params: Parameters<typeof connectDesignerEdge>[1]) => {
      setEdges((eds) => {
        const next = connectDesignerEdge(eds, params, nodesRef.current);
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

  function createNode(spec: DesignerDroppedSpec, position: { x: number; y: number }): Node {
    const kind = spec.kind;
    const name = spec.name || defaultNameForKind(kind);
    const config = spec.config || defaultConfigForKind(kind);
    return {
      id: nextId(kind.slice(0, 3)),
      type: kind,
      position,
      data: { label: name, kind, name, config },
    };
  }

  function handleDropNode(spec: DesignerDroppedSpec, position: { x: number; y: number }) {
    const node = createNode(spec, position);
    const nextNodes = [...nodes, node];
    setNodes(nextNodes);
    scheduleSave(nextNodes, edges);
  }

  function handleAddNode(spec: DesignerDroppedSpec) {
    const x = 120 + nodes.length * 40;
    const y = 160 + (nodes.length % 3) * 80;
    handleDropNode(spec, { x, y });
  }

  function handleAddPromptNode() {
    handleAddNode({ kind: "prompt", name: "prompt", config: defaultPromptConfig() });
  }

  function handleUpdateNode(nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) {
    const next = nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const data = n.data as Record<string, unknown>;
      return {
        ...n,
        data: {
          ...data,
          name: patch.name ?? data.name,
          label: patch.name ?? data.label,
          config: patch.config ?? data.config,
        },
      };
    });
    setNodes(next);
    scheduleSave(next, edges);
  }

  function handleUpdateEdge(edgeId: string, kind: AgentDefinition["edges"][0]["kind"]) {
    const next = edges.map((e) =>
      e.id === edgeId ? { ...e, label: kind, data: { kind } } : e,
    );
    setEdges(next);
    scheduleSave(nodes, next);
  }

  function handleDeleteNode(nodeId: string) {
    const nextNodes = nodes.filter((n) => n.id !== nodeId);
    const nextEdges = edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
    setNodes(nextNodes);
    setEdges(nextEdges);
    setSelectedNode(null);
    scheduleSave(nextNodes, nextEdges);
  }

  function handleDeleteEdge(edgeId: string) {
    const nextEdges = edges.filter((e) => e.id !== edgeId);
    setEdges(nextEdges);
    setSelectedEdge(null);
    scheduleSave(nodes, nextEdges);
  }

  function handleAutoWire() {
    const nextEdges = autoWireAgentGraph(nodes, edges);
    setEdges(nextEdges);
    scheduleSave(nodes, nextEdges);
    setError(null);
  }

  async function handleValidate() {
    if (!id) return;
    setBusy("validate");
    setError(null);
    try {
      await persist(nodes, edges);
      const result = await api.validateAgentDefinition(id);
      setValidation(result);
      if (!result.valid) {
        setError(result.errors.join(" · "));
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(null);
    }
  }

  async function handleCompile() {
    if (!id) return;
    setBusy("compile");
    setError(null);
    setPublishMessage(null);
    try {
      await persist(nodes, edges);
      const result = await api.compileAgentDefinition(id);
      setCompileResult(result);
      if (result.definition) setDefinition(result.definition);
      setValidation(result.validation);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(null);
    }
  }

  async function handlePublish() {
    if (!id) return;
    setBusy("publish");
    setError(null);
    setPublishMessage(null);
    try {
      await persist(nodes, edges);
      const result = await api.publishAgentDefinition(id);
      if (result.definition) setDefinition(result.definition);
      setPublishMessage(
        `Added to catalog as ${result.manifest_name} — visible in Agents and Studio palette.`,
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(null);
    }
  }

  if (!id) return null;

  return (
    <div className="studio-page designer-editor-page">
      <p>
        <Link to="/designer">← Designer</Link>
      </p>
      <div className="studio-header designer-editor-header">
        <div className="designer-editor-title">
          <label className="designer-agent-name-field">
            <span className="muted">Agent name</span>
            <input
              className="designer-agent-name-input"
              type="text"
              value={agentName}
              placeholder={definition ? "Untitled agent" : "Loading…"}
              disabled={!definition}
              maxLength={120}
              onChange={(e) => handleNameChange(e.target.value)}
              onBlur={() => {
                if (nameSaveTimer.current) {
                  window.clearTimeout(nameSaveTimer.current);
                  nameSaveTimer.current = null;
                }
                if (!agentName.trim() && definition) {
                  setAgentName(definition.name);
                  setError(null);
                  return;
                }
                if (definition && agentName.trim() !== definition.name) {
                  saveName(agentName);
                }
              }}
            />
          </label>
          <span className="muted designer-editor-meta">
            {definition?.type} · {definition?.status}
            {saveState === "saving" ? " · Saving…" : saveState === "saved" ? " · Saved" : ""}
          </span>
        </div>
      </div>
      {error && <p className="error">{error}</p>}
      {publishMessage && <p className="badge ok">{publishMessage}</p>}

      <div className="designer-toolbar card">
        <button type="button" className="secondary" onClick={handleAutoWire}>
          Auto-wire
        </button>
        <button type="button" className="secondary" disabled={busy !== null} onClick={handleValidate}>
          {busy === "validate" ? "Validating…" : "Validate"}
        </button>
        <button type="button" disabled={busy !== null} onClick={handleCompile}>
          {busy === "compile" ? "Compiling…" : "Compile"}
        </button>
        <button type="button" className="secondary" disabled={busy !== null} onClick={handlePublish}>
          {busy === "publish" ? "Publishing…" : "Add to catalog"}
        </button>
        {definition?.manifest_name && (
          <Link to={`/agents/${definition.manifest_name}`} className="secondary-link">
            View in catalog
          </Link>
        )}
        {validation && (
          <span className={`badge ${validation.valid ? "ok" : "warn"}`} style={{ marginLeft: "0.5rem" }}>
            {validation.valid ? "Valid graph" : "Invalid graph"}
          </span>
        )}
      </div>

      {definition?.type === "react" && (
        <DesignerPromptPanel
          nodes={nodes}
          onUpdateNode={handleUpdateNode}
          onAddPrompt={handleAddPromptNode}
        />
      )}

      <div className="studio-layout">
        <DesignerPalette agentType={definition?.type || "workflow"} onAdd={handleAddNode} />
        <DesignerCanvas
          nodes={nodes}
          edges={edges}
          onNodesChange={handleNodesChange}
          onEdgesChange={handleEdgesChange}
          onConnect={onConnect}
          onNodeDragStop={handleNodeDragStop}
          onDropNode={handleDropNode}
          onSelectionChange={({ nodes: ns, edges: es }) => {
            setSelectedNode(ns[0] || null);
            setSelectedEdge(es[0] || null);
          }}
        />
        <DesignerInspector
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          onUpdateNode={handleUpdateNode}
          onUpdateEdge={handleUpdateEdge}
          onDeleteNode={handleDeleteNode}
          onDeleteEdge={handleDeleteEdge}
        />
      </div>

      <div style={{ marginTop: "1rem" }}>
        <CompilePreviewPanel result={compileResult} />
      </div>
    </div>
  );
}
