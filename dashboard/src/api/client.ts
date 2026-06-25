import type {
  AgentDetail,
  AgentGraph,
  AgentSummary,
  JobSummary,
  PipelineHealth,
  PipelineRunResult,
  PipelineSummary,
  PipelineValidation,
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

  agents: () => request<AgentSummary[]>("/v1/agents"),

  agent: (name: string) => request<AgentDetail>(`/v1/agents/${encodeURIComponent(name)}`),

  agentDefinition: (name: string) =>
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

  agentGraph: (name: string) =>
    request<AgentGraph>(`/v1/agents/${encodeURIComponent(name)}/graph`),

  eventsUrl: () => `${API_BASE}/v1/events`,
};
