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
  McpCatalog,
  ReactLlmSettings,
} from "../api/types";
import { CompilePreviewPanel } from "../components/CompilePreviewPanel";
import { ToastStack, useToastStack } from "../components/ToastStack";
import { DesignerCanvas } from "../designer/DesignerCanvas";
import { DesignerInspector } from "../designer/DesignerInspector";
import { DesignerPalette } from "../designer/DesignerPalette";
import { DesignerPromptPanel } from "../designer/DesignerPromptPanel";
import { DesignerPublishSteps } from "../designer/DesignerPublishSteps";
import { DesignerReadinessBanner } from "../designer/DesignerReadinessBanner";
import { DesignerValidationBar } from "../designer/DesignerValidationBar";
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

const SKILLS_MATH_LLM_CONFIG = {
  use_platform_llm: true,
  mode: "flink_skills",
  skills: ["math-calculator"],
  allowed_commands: ["echo", "bc"],
};

export function DesignerEditorPage() {
  const { id } = useParams<{ id: string }>();
  const [definition, setDefinition] = useState<AgentDefinition | null>(null);
  const [loading, setLoading] = useState(true);
  const [nodes, setNodes] = useNodesState<Node>([]);
  const [edges, setEdges] = useEdgesState<Edge>([]);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [validation, setValidation] = useState<AgentDefinitionValidation | null>(null);
  const [compileResult, setCompileResult] = useState<AgentDefinitionCompileResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [agentName, setAgentName] = useState("");
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved">("idle");
  const [busy, setBusy] = useState<"validate" | "compile" | "publish" | "run" | null>(null);
  const [mcpInstances, setMcpInstances] = useState<import("../api/types").McpInstance[]>([]);
  const [mcpCatalog, setMcpCatalog] = useState<McpCatalog | null>(null);
  const [llmSettings, setLlmSettings] = useState<ReactLlmSettings | null>(null);
  const { toasts, push, dismiss } = useToastStack();
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
    setLoading(true);
    api
      .getDesignerDefinition(id)
      .then((def) => {
        setDefinition(def);
        setAgentName(def.name);
        const flow = definitionToFlow(def);
        setNodes(flow.nodes);
        setEdges(flow.edges);
        setError(null);
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
    api
      .mcpInstances()
      .then((response) => setMcpInstances(response.instances))
      .catch(() => setMcpInstances([]));
    api.mcpCatalog().then(setMcpCatalog).catch(() => setMcpCatalog(null));
    api.reactLlmSettings().then(setLlmSettings).catch(() => setLlmSettings(null));
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

  function handleApplySkillsRecipe() {
    const llmNode = nodes.find((n) => n.type === "llm_call");
    if (llmNode) {
      const data = llmNode.data as Record<string, unknown>;
      const config = (data.config as Record<string, unknown>) || {};
      handleUpdateNode(llmNode.id, { config: { ...config, ...SKILLS_MATH_LLM_CONFIG } });
    } else {
      handleAddNode({
        kind: "llm_call",
        name: "llm",
        config: SKILLS_MATH_LLM_CONFIG,
      });
    }
    push("Skills recipe applied — LLM node set to Flink skills mode", "ok");
  }

  function handleUpdateDefinition(patch: Partial<AgentDefinition>) {
    if (!id || !definition) return;
    setSaveState("saving");
    api
      .updateDesignerDefinition(id, patch)
      .then((updated) => {
        setDefinition(updated);
        setSaveState("saved");
        setError(null);
      })
      .catch((e) => {
        setError(String(e));
        setSaveState("idle");
      });
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
    push("Auto-wired graph edges", "info");
  }

  async function handleValidate() {
    if (!id) return;
    setBusy("validate");
    setError(null);
    try {
      await persist(nodes, edges);
      const result = await api.validateAgentDefinition(id);
      setValidation(result);
      if (result.valid) {
        push("Graph validation passed", "ok");
      } else {
        push(`Validation failed: ${result.errors[0] || "see panel"}`, "error");
      }
    } catch (e) {
      setError(String(e));
      push(String(e), "error");
    } finally {
      setBusy(null);
    }
  }

  async function handleCompile() {
    if (!id) return;
    if (validation && !validation.valid) {
      push("Fix validation errors before compiling", "error");
      return;
    }
    setBusy("compile");
    setError(null);
    try {
      await persist(nodes, edges);
      const result = await api.compileAgentDefinition(id);
      setCompileResult(result);
      if (result.definition) setDefinition(result.definition);
      setValidation(result.validation);
      if (result.validation.valid) {
        push(`Compiled to ${result.output_dir}`, "ok");
      } else {
        push("Compile finished with validation warnings", "error");
      }
    } catch (e) {
      setError(String(e));
      push(String(e), "error");
    } finally {
      setBusy(null);
    }
  }

  async function handlePublish() {
    if (!id) return;
    if (validation && !validation.valid) {
      push("Validate the graph before publishing", "error");
      return;
    }
    setBusy("publish");
    setError(null);
    try {
      await persist(nodes, edges);
      const result = await api.publishAgentDefinition(id);
      if (result.definition) setDefinition(result.definition);
      push(
        `Added to catalog as ${result.manifest_name} — visible in Agents and Studio palette`,
        "ok",
      );
    } catch (e) {
      setError(String(e));
      push(String(e), "error");
    } finally {
      setBusy(null);
    }
  }

  async function handleRunLocal() {
    if (!id) return;
    if (definition?.type === "react" && llmSettings && !llmSettings.configured) {
      push("Configure LLM settings before test run", "error");
      return;
    }
    setBusy("run");
    setError(null);
    try {
      await persist(nodes, edges);
      const result = await api.runAgentDefinitionLocal(id);
      if (result.return_code === 0) {
        push(`Local test run finished (run ${result.run_id})`, "ok");
      } else {
        push(`Local test run failed with exit code ${result.return_code}`, "error");
      }
    } catch (e) {
      setError(String(e));
      push(String(e), "error");
    } finally {
      setBusy(null);
    }
  }

  if (!id) return null;

  const compileBlocked = validation !== null && !validation.valid;
  const busyAny = busy !== null;

  return (
    <div className="studio-page designer-editor-page">
      <ToastStack toasts={toasts} onDismiss={dismiss} />
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
              placeholder={loading ? "Loading…" : "Untitled agent"}
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
      {loading && <p className="muted">Loading agent definition…</p>}

      <DesignerReadinessBanner
        definitionType={definition?.type}
        llmSettings={llmSettings}
        nodes={nodes}
        mcpAttachedCount={definition?.mcp_servers?.length ?? 0}
      />

      <DesignerPublishSteps
        status={definition?.status}
        validationValid={validation?.valid ?? null}
        hasCompile={compileResult !== null}
        manifestName={definition?.manifest_name}
      />

      <DesignerValidationBar
        validation={validation}
        busy={busy === "validate"}
        compileBlocked={compileBlocked}
        onValidate={handleValidate}
      />

      <div className="designer-toolbar card">
        <button type="button" className="secondary" onClick={handleAutoWire} disabled={busyAny}>
          Auto-wire
        </button>
        <button type="button" disabled={busyAny || compileBlocked} onClick={handleCompile}>
          {busy === "compile" ? "Compiling…" : "Compile"}
        </button>
        <button
          type="button"
          className="secondary"
          disabled={busyAny || compileBlocked}
          onClick={handlePublish}
        >
          {busy === "publish" ? "Publishing…" : "Add to catalog"}
        </button>
        <button
          type="button"
          className="secondary"
          disabled={busyAny}
          onClick={handleRunLocal}
          title="Compile if needed, then run the generated local runner"
        >
          {busy === "run" ? "Running…" : "Test run locally"}
        </button>
        {definition?.manifest_name && (
          <Link to={`/agents/${definition.manifest_name}`} className="secondary-link">
            View in catalog
          </Link>
        )}
      </div>

      {definition?.type === "react" && (
        <DesignerPromptPanel
          nodes={nodes}
          onUpdateNode={handleUpdateNode}
          onAddPrompt={handleAddPromptNode}
          onApplySkillsRecipe={handleApplySkillsRecipe}
        />
      )}

      <div className="studio-layout">
        <DesignerPalette
          agentType={definition?.type || "workflow"}
          mcpInstances={mcpInstances}
          mcpAttached={definition?.mcp_servers || []}
          onAdd={handleAddNode}
        />
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
          definition={definition}
          mcpInstances={mcpInstances}
          mcpCatalog={mcpCatalog}
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          onUpdateDefinition={handleUpdateDefinition}
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
