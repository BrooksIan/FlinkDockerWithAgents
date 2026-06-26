import type {
  AgentCatalog,
  AgentDefinition,
  AgentDefinitionCompileResult,
  AgentDefinitionCreate,
  AgentDefinitionPublishResult,
  AgentDefinitionValidation,
  AgentDetail,
  AgentGraph,
  AgentSummary,
  JobSummary,
  ClusterReadiness,
  KafkaTopicsResponse,
  PipelineHealth,
  PipelineRunResult,
  PipelineSubmitResult,
  PipelineSummary,
  PipelineValidation,
  ReactLlmSettings,
  ReactLlmSettingsTestRequest,
  ReactLlmSettingsTestResult,
  ReactLlmSettingsUpdate,
  RunDetail,
  RunSummary,
} from "./types";

const API_BASE =
  (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/$/, "") || "";

function apiKey(): string | undefined {
  return (import.meta.env.VITE_API_KEY as string | undefined) || undefined;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  const key = apiKey();
  if (key) headers.set("X-API-Key", key);

  const res = await fetch(`${API_BASE}${path}`, { ...init, headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${path}: ${text}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export const api = {
  health: () => request<PipelineHealth>("/v1/health"),

  clusterStatus: () => request<ClusterReadiness>("/v1/cluster/status"),

  validateCluster: () =>
    request<ClusterReadiness>("/v1/cluster/validate", { method: "POST" }),

  agents: () => request<AgentSummary[]>("/v1/agents"),

  agentCatalog: () => request<AgentCatalog>("/v1/agents/catalog"),

  reactLlmSettings: () => request<ReactLlmSettings>("/v1/designer/llm-settings"),

  updateReactLlmSettings: (body: ReactLlmSettingsUpdate) =>
    request<ReactLlmSettings>("/v1/designer/llm-settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  testReactLlmSettings: (body?: ReactLlmSettingsTestRequest) =>
    request<ReactLlmSettingsTestResult>("/v1/designer/llm-settings/test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body ?? {}),
    }),

  agentDefinitions: () => request<AgentDefinition[]>("/v1/agent-definitions"),

  getDesignerDefinition: (id: string) =>
    request<AgentDefinition>(`/v1/agent-definitions/${encodeURIComponent(id)}`),

  updateDesignerDefinition: (id: string, body: Partial<AgentDefinition>) =>
    request<AgentDefinition>(`/v1/agent-definitions/${encodeURIComponent(id)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  createAgentDefinition: (body: AgentDefinitionCreate) =>
    request<AgentDefinition>("/v1/agent-definitions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  validateAgentDefinition: (id: string) =>
    request<AgentDefinitionValidation>(
      `/v1/agent-definitions/${encodeURIComponent(id)}/validate`,
      { method: "POST" },
    ),

  compileAgentDefinition: (id: string) =>
    request<AgentDefinitionCompileResult>(
      `/v1/agent-definitions/${encodeURIComponent(id)}/compile`,
      { method: "POST" },
    ),

  publishAgentDefinition: (id: string) =>
    request<AgentDefinitionPublishResult>(
      `/v1/agent-definitions/${encodeURIComponent(id)}/publish`,
      { method: "POST" },
    ),

  agent: (name: string) => request<AgentDetail>(`/v1/agents/${encodeURIComponent(name)}`),

  agentRuntimeDefinition: (name: string) =>
    request<AgentDetail>(`/v1/agents/${encodeURIComponent(name)}/definition`),

  jobs: () => request<JobSummary[]>("/v1/jobs"),

  job: (id: string) => request<Record<string, unknown>>(`/v1/jobs/${encodeURIComponent(id)}`),

  submitAgent: (name: string) =>
    request<{ agent: string; status: string; run_id?: string; flink_job_id?: string; jobs: JobSummary[] }>(
      `/v1/agents/${encodeURIComponent(name)}/submit`,
      { method: "POST" },
    ),

  runs: (agent?: string) =>
    request<RunSummary[]>(agent ? `/v1/runs?agent=${encodeURIComponent(agent)}` : "/v1/runs"),

  run: (id: string) => request<RunDetail>(`/v1/runs/${encodeURIComponent(id)}`),

  agentRuns: (name: string) =>
    request<RunSummary[]>(`/v1/agents/${encodeURIComponent(name)}/runs`),

  cancelJob: (id: string) =>
    request<{ id: string; status: string }>(`/v1/jobs/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),

  pipelines: () => request<PipelineSummary[]>("/v1/pipelines"),

  pipeline: (id: string) => request<PipelineSummary>(`/v1/pipelines/${encodeURIComponent(id)}`),

  createPipeline: (body: Partial<PipelineSummary>) =>
    request<PipelineSummary>("/v1/pipelines", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  updatePipeline: (id: string, body: Partial<PipelineSummary>) =>
    request<PipelineSummary>(`/v1/pipelines/${encodeURIComponent(id)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  deletePipeline: (id: string) =>
    request<{ id: string; status: string }>(`/v1/pipelines/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),

  validatePipeline: (id: string) =>
    request<PipelineValidation>(`/v1/pipelines/${encodeURIComponent(id)}/validate`, {
      method: "POST",
    }),

  runPipeline: (id: string, records?: Record<string, unknown>[]) =>
    request<PipelineRunResult>(`/v1/pipelines/${encodeURIComponent(id)}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(records ? { records } : {}),
    }),

  submitPipeline: (id: string) =>
    request<PipelineSubmitResult>(`/v1/pipelines/${encodeURIComponent(id)}/submit`, {
      method: "POST",
    }),

  agentGraph: (name: string) =>
    request<AgentGraph>(`/v1/agents/${encodeURIComponent(name)}/graph`),

  kafkaTopics: (bootstrap?: string) =>
    request<KafkaTopicsResponse>(
      bootstrap
        ? `/v1/kafka/topics?bootstrap=${encodeURIComponent(bootstrap)}`
        : "/v1/kafka/topics",
    ),

  eventsUrl: () => `${API_BASE}/v1/events`,
};
