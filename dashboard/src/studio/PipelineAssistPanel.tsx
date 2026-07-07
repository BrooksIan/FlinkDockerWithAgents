import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type {
  AgentSummary,
  KafkaTopicSummary,
  PipelineAssistGenerateRequest,
  PipelineAssistResult,
  PipelineSummary,
  ReactLlmSettings,
  SuggestedAgent,
} from "../api/types";

interface Props {
  pipelineName: string;
  agents: AgentSummary[];
  kafkaTopics: KafkaTopicSummary[];
  llmSettings?: ReactLlmSettings | null;
  busy?: boolean;
  onApply: (pipeline: Partial<PipelineSummary>, result: PipelineAssistResult) => void | Promise<void>;
  onError?: (message: string) => void;
}

export function PipelineAssistPanel({
  pipelineName,
  agents,
  kafkaTopics,
  llmSettings,
  busy: externalBusy,
  onApply,
  onError,
}: Props) {
  const [goal, setGoal] = useState("");
  const [name, setName] = useState(pipelineName);
  const [domain, setDomain] = useState<PipelineAssistGenerateRequest["domain"]>("auto");
  const [sourceType, setSourceType] = useState<"records" | "kafka">("records");
  const [sourceTopic, setSourceTopic] = useState("");
  const [useWindowing, setUseWindowing] = useState(false);
  const [windowKeyField, setWindowKeyField] = useState("key");
  const [windowGapPolicy, setWindowGapPolicy] = useState("default");
  const [workflowAgent, setWorkflowAgent] = useState("auto");
  const [useReactEnrichment, setUseReactEnrichment] = useState(false);
  const [reactAgent, setReactAgent] = useState("auto");
  const [reactPolicy, setReactPolicy] = useState<PipelineAssistGenerateRequest["react_policy"]>("none");
  const [sinkType, setSinkType] = useState<"capture" | "kafka">("capture");
  const [sinkTopic, setSinkTopic] = useState("");
  const [preference, setPreference] = useState<PipelineAssistGenerateRequest["preference"]>("balanced");
  const [agentCreationMode, setAgentCreationMode] =
    useState<NonNullable<PipelineAssistGenerateRequest["agent_creation_mode"]>>("suggest");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<PipelineAssistResult | null>(null);
  const [selectedSuggestionIds, setSelectedSuggestionIds] = useState<string[]>([]);

  const llmConfigured = llmSettings?.configured ?? true;
  const isBusy = busy || Boolean(externalBusy);

  const workflowAgents = useMemo(
    () => agents.filter((agent) => agent.type === "workflow"),
    [agents],
  );
  const reactAgents = useMemo(
    () => agents.filter((agent) => agent.type === "react"),
    [agents],
  );

  const suggestions = result?.suggested_agents ?? [];
  const selectedSuggestions = useMemo(
    () => suggestions.filter((suggestion) => selectedSuggestionIds.includes(suggestion.suggestion_id)),
    [suggestions, selectedSuggestionIds],
  );
  const needsAgentApproval =
    agentCreationMode === "suggest" && suggestions.length > 0 && selectedSuggestions.length > 0;

  useEffect(() => {
    if (!result?.suggested_agents?.length) {
      setSelectedSuggestionIds([]);
      return;
    }
    const defaults = result.suggested_agents
      .filter((suggestion) => suggestion.selected_by_default !== false)
      .map((suggestion) => suggestion.suggestion_id);
    setSelectedSuggestionIds(
      agentCreationMode === "auto_create"
        ? result.suggested_agents.map((suggestion) => suggestion.suggestion_id)
        : defaults,
    );
  }, [result, agentCreationMode]);

  useEffect(() => {
    if (sourceType === "kafka") {
      setUseWindowing(true);
    }
  }, [sourceType]);

  function buildRequest(): PipelineAssistGenerateRequest {
    return {
      goal: goal.trim(),
      pipeline_name: name.trim() || undefined,
      domain,
      source_type: sourceType,
      source_topic: sourceType === "kafka" ? sourceTopic.trim() || undefined : undefined,
      use_windowing: useWindowing || sourceType === "kafka",
      window_key_field: windowKeyField.trim() || "key",
      window_gap_policy: windowGapPolicy.trim() || "default",
      workflow_agent: workflowAgent,
      use_react_enrichment: useReactEnrichment,
      react_agent: reactAgent,
      react_policy: useReactEnrichment ? reactPolicy : "none",
      sink_type: sinkType,
      sink_topic: sinkType === "kafka" ? sinkTopic.trim() || undefined : undefined,
      preference,
      use_llm: true,
      agent_creation_mode: agentCreationMode,
    };
  }

  function toggleSuggestion(suggestionId: string, checked: boolean) {
    setSelectedSuggestionIds((current) =>
      checked ? [...new Set([...current, suggestionId])] : current.filter((id) => id !== suggestionId),
    );
  }

  async function handleGenerate() {
    if (!goal.trim()) {
      onError?.("Describe what the pipeline should do.");
      return;
    }
    setBusy(true);
    onError?.("");
    try {
      const response = await api.assistGeneratePipeline(buildRequest());
      setResult(response);
    } catch (err) {
      onError?.(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function handleApply() {
    if (!result) return;
    setBusy(true);
    onError?.("");
    try {
      let finalResult = result;
      const shouldBuild =
        selectedSuggestions.length > 0 &&
        (agentCreationMode === "auto_create" || agentCreationMode === "suggest");

      if (shouldBuild) {
        finalResult = await api.assistBuildPipeline({
          ...buildRequest(),
          approved_suggestions: selectedSuggestions,
        });
        setResult(finalResult);
      }

      await onApply(finalResult.pipeline, finalResult);
    } catch (err) {
      onError?.(String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="card designer-assist-panel studio-assist-panel">
      <div className="designer-assist-header">
        <div>
          <h3 style={{ margin: 0 }}>Build pipeline with assistant</h3>
          <p className="muted designer-assist-hint">
            Fill in the pipeline requirements below. Ratatoskr builds a draft using your form inputs
            and optionally refines it with the Designer LLM. Nothing is applied until you accept.
          </p>
        </div>
        {!llmConfigured && (
          <p className="error designer-assist-llm-warning">
            Designer LLM is not configured. You will still get a rule-based draft. Open{" "}
            <Link to="/settings">Settings</Link> to connect an endpoint, model, and API key.
          </p>
        )}
      </div>

      <div className="designer-assist-form studio-assist-form">
        <label className="studio-label">Pipeline goal</label>
        <textarea
          className="studio-textarea designer-assist-goal"
          rows={4}
          value={goal}
          disabled={isBusy}
          placeholder="Example: Detect suspicious Cowrie sessions, enrich high severity alerts, and write to Kafka."
          onChange={(e) => setGoal(e.target.value)}
        />

        <div className="studio-assist-grid">
          <label className="create-agent-field">
            <span className="muted">Pipeline name</span>
            <input
              className="studio-input"
              type="text"
              value={name}
              disabled={isBusy}
              placeholder={pipelineName || "Untitled pipeline"}
              onChange={(e) => setName(e.target.value)}
            />
          </label>

          <label className="create-agent-field">
            <span className="muted">Domain</span>
            <select
              className="studio-select"
              value={domain}
              disabled={isBusy}
              onChange={(e) => setDomain(e.target.value as PipelineAssistGenerateRequest["domain"])}
            >
              <option value="auto">Auto-detect</option>
              <option value="cowrie_security">Cowrie / security</option>
              <option value="numeric_transform">Numeric transform</option>
              <option value="generic">Generic</option>
            </select>
          </label>

          <label className="create-agent-field">
            <span className="muted">Source</span>
            <select
              className="studio-select"
              value={sourceType}
              disabled={isBusy}
              onChange={(e) => setSourceType(e.target.value as "records" | "kafka")}
            >
              <option value="records">Static sample records</option>
              <option value="kafka">Kafka topic</option>
            </select>
          </label>

          {sourceType === "kafka" && (
            <label className="create-agent-field">
              <span className="muted">Input topic</span>
              <select
                className="studio-select"
                value={sourceTopic}
                disabled={isBusy}
                onChange={(e) => setSourceTopic(e.target.value)}
              >
                <option value="">Select topic…</option>
                {kafkaTopics.map((topic) => (
                  <option key={topic.name} value={topic.name}>
                    {topic.name}
                  </option>
                ))}
              </select>
            </label>
          )}

          <label className="create-agent-field studio-assist-checkbox">
            <input
              type="checkbox"
              checked={useWindowing || sourceType === "kafka"}
              disabled={isBusy || sourceType === "kafka"}
              onChange={(e) => setUseWindowing(e.target.checked)}
            />
            <span>
              {sourceType === "kafka"
                ? "Dynamic session window (required for Kafka)"
                : "Use session window"}
            </span>
          </label>

          {useWindowing && (
            <>
              <label className="create-agent-field">
                <span className="muted">Window key field</span>
                <input
                  className="studio-input"
                  type="text"
                  value={windowKeyField}
                  disabled={isBusy}
                  onChange={(e) => setWindowKeyField(e.target.value)}
                />
              </label>
              <label className="create-agent-field">
                <span className="muted">Gap policy</span>
                <select
                  className="studio-select"
                  value={windowGapPolicy}
                  disabled={isBusy}
                  onChange={(e) => setWindowGapPolicy(e.target.value)}
                >
                  <option value="default">Default</option>
                  <option value="session_detect">Session detect</option>
                </select>
              </label>
            </>
          )}

          <label className="create-agent-field">
            <span className="muted">Workflow agent</span>
            <select
              className="studio-select"
              value={workflowAgent}
              disabled={isBusy}
              onChange={(e) => setWorkflowAgent(e.target.value)}
            >
              <option value="auto">Let assistant choose</option>
              {workflowAgents.map((agent) => (
                <option key={agent.name} value={agent.name}>
                  {agent.display_name || agent.name}
                </option>
              ))}
            </select>
          </label>

          <label className="create-agent-field studio-assist-checkbox">
            <input
              type="checkbox"
              checked={useReactEnrichment}
              disabled={isBusy}
              onChange={(e) => {
                setUseReactEnrichment(e.target.checked);
                if (e.target.checked && reactPolicy === "none") {
                  setReactPolicy("high_severity_only");
                }
              }}
            />
            <span>Add ReAct enrichment</span>
          </label>

          {useReactEnrichment && (
            <>
              <label className="create-agent-field">
                <span className="muted">ReAct agent</span>
                <select
                  className="studio-select"
                  value={reactAgent}
                  disabled={isBusy}
                  onChange={(e) => setReactAgent(e.target.value)}
                >
                  <option value="auto">Let assistant choose</option>
                  {reactAgents.map((agent) => (
                    <option key={agent.name} value={agent.name}>
                      {agent.display_name || agent.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="create-agent-field">
                <span className="muted">ReAct policy</span>
                <select
                  className="studio-select"
                  value={reactPolicy || "none"}
                  disabled={isBusy}
                  onChange={(e) =>
                    setReactPolicy(e.target.value as PipelineAssistGenerateRequest["react_policy"])
                  }
                >
                  <option value="high_severity_only">High severity only</option>
                  <option value="all">All records</option>
                </select>
              </label>
            </>
          )}

          <label className="create-agent-field">
            <span className="muted">Output sink</span>
            <select
              className="studio-select"
              value={sinkType}
              disabled={isBusy}
              onChange={(e) => setSinkType(e.target.value as "capture" | "kafka")}
            >
              <option value="capture">Capture in Studio</option>
              <option value="kafka">Kafka topic</option>
            </select>
          </label>

          {sinkType === "kafka" && (
            <label className="create-agent-field">
              <span className="muted">Output topic</span>
              <select
                className="studio-select"
                value={sinkTopic}
                disabled={isBusy}
                onChange={(e) => setSinkTopic(e.target.value)}
              >
                <option value="">Select topic…</option>
                {kafkaTopics.map((topic) => (
                  <option key={topic.name} value={topic.name}>
                    {topic.name}
                  </option>
                ))}
              </select>
            </label>
          )}

          <label className="create-agent-field">
            <span className="muted">Preference</span>
            <select
              className="studio-select"
              value={preference}
              disabled={isBusy}
              onChange={(e) =>
                setPreference(e.target.value as PipelineAssistGenerateRequest["preference"])
              }
            >
              <option value="fast">Fast / deterministic</option>
              <option value="balanced">Balanced</option>
              <option value="deep">Deeper analysis</option>
            </select>
          </label>

          <label className="create-agent-field">
            <span className="muted">Missing agents</span>
            <select
              className="studio-select"
              value={agentCreationMode}
              disabled={isBusy}
              onChange={(e) =>
                setAgentCreationMode(
                  e.target.value as NonNullable<PipelineAssistGenerateRequest["agent_creation_mode"]>,
                )
              }
            >
              <option value="suggest">Suggest before creating (default)</option>
              <option value="existing_only">Use catalog agents only</option>
              <option value="auto_create">Create missing agents automatically</option>
            </select>
          </label>
        </div>

        <div className="designer-assist-controls">
          <button type="button" disabled={isBusy} onClick={handleGenerate}>
            {isBusy ? "Building draft…" : "Generate pipeline draft"}
          </button>
        </div>
      </div>

      {result && (
        <div className="designer-assist-preview">
          <div className="designer-assist-preview-header">
            <h4 style={{ margin: 0 }}>Draft preview</h4>
            <span
              className={`badge ${result.validation.valid ? "ok" : "warn"}`}
              title={result.validation.errors.join("; ")}
            >
              {result.validation.valid ? "Valid draft" : "Needs fixes"}
            </span>
          </div>

          {result.rationale && (
            <div className="designer-assist-block">
              <strong>Rationale</strong>
              <p className="muted" style={{ margin: "0.35rem 0 0" }}>
                {result.rationale}
              </p>
            </div>
          )}

          {result.warnings.length > 0 && (
            <div className="designer-assist-block">
              <strong>Assumptions</strong>
              <ul className="designer-assist-warnings">
                {result.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </div>
          )}

          <div className="designer-assist-block">
            <strong>Proposed pipeline</strong>
            <p className="muted" style={{ margin: "0.35rem 0 0" }}>
              {result.pipeline.name || "Untitled pipeline"} · {result.pipeline.nodes?.length || 0} nodes ·{" "}
              {result.pipeline.edges?.length || 0} edges
            </p>
            {(result.reused_agents?.length ?? 0) > 0 && (
              <div className="studio-assist-reused">
                {result.reused_agents!.map((manifest) => (
                  <span key={manifest} className="badge ok">
                    {manifest}
                  </span>
                ))}
              </div>
            )}
          </div>

          {suggestions.length > 0 && agentCreationMode !== "existing_only" && (
            <div className="designer-assist-block">
              <strong>Suggested new agents</strong>
              <p className="muted" style={{ margin: "0.35rem 0 0" }}>
                Review proposed agents before they are created and wired into the pipeline.
              </p>
              <ul className="studio-assist-suggestions">
                {suggestions.map((suggestion: SuggestedAgent) => (
                  <li key={suggestion.suggestion_id} className="studio-assist-suggestion">
                    <label>
                      <input
                        type="checkbox"
                        checked={selectedSuggestionIds.includes(suggestion.suggestion_id)}
                        disabled={isBusy || agentCreationMode === "auto_create"}
                        onChange={(e) => toggleSuggestion(suggestion.suggestion_id, e.target.checked)}
                      />
                      <span>
                        <strong>{suggestion.display_name}</strong>
                        <span className="studio-assist-suggestion-meta muted">
                          Replaces {suggestion.replaces_manifest} · proposes {suggestion.proposed_manifest}
                        </span>
                        <span className="studio-assist-suggestion-meta muted">{suggestion.reason}</span>
                        {!suggestion.validation.valid && (
                          <span className="studio-assist-suggestion-meta error">
                            Draft validation failed — uncheck to skip this agent.
                          </span>
                        )}
                      </span>
                    </label>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {(result.created_agents?.length ?? 0) > 0 && (
            <div className="designer-assist-block">
              <strong>Created agents</strong>
              <ul className="designer-assist-warnings">
                {result.created_agents!.map((agent) => (
                  <li key={agent.suggestion_id}>
                    {agent.display_name} ({agent.manifest})
                  </li>
                ))}
              </ul>
            </div>
          )}

          {!result.validation.valid && (
            <div className="designer-assist-block">
              <strong>Validation issues</strong>
              <ul className="designer-assist-warnings">
                {result.validation.errors.map((issue) => (
                  <li key={issue}>{issue}</li>
                ))}
              </ul>
            </div>
          )}

          <div className="designer-assist-actions">
            <button type="button" disabled={isBusy || !result.validation.valid} onClick={handleApply}>
              {needsAgentApproval
                ? `Create ${selectedSuggestions.length} agent(s) & apply`
                : "Apply to canvas"}
            </button>
            {suggestions.length > 0 && agentCreationMode === "suggest" && (
              <button
                type="button"
                className="secondary"
                disabled={isBusy || !result.validation.valid}
                onClick={async () => {
                  setSelectedSuggestionIds([]);
                  setBusy(true);
                  onError?.("");
                  try {
                    await onApply(result.pipeline, result);
                  } catch (err) {
                    onError?.(String(err));
                  } finally {
                    setBusy(false);
                  }
                }}
              >
                Apply without creating agents
              </button>
            )}
            <button type="button" className="secondary" disabled={isBusy} onClick={() => setResult(null)}>
              Discard preview
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
