/** Hand-maintained types aligned with Control API /v1 (regenerate via npm run generate-api). */

export type HealthStatus = "ok" | "degraded" | "unavailable";

export interface PipelineHealth {
  status: HealthStatus;
  flink: {
    reachable: boolean;
    url: string;
    flink_version?: string;
    taskmanagers?: number;
    slots_total?: number;
    slots_free?: number;
    jobs_running?: number;
    jobs_finished?: number;
    error?: string;
  };
  agents: { ok: boolean; registered: number };
  api_version: string;
}

export interface AgentSummary {
  name: string;
  type: string;
  description: string;
  entry: string;
  runner: string;
  cluster_script: string;
  flink_yaml?: string | null;
}

export interface AgentDetail extends AgentSummary {
  class?: string;
  members?: string[];
  import_note?: string;
  flink_yaml_path?: string | null;
  flink_yaml?: string | null;
}

export interface JobSummary {
  id: string;
  name: string;
  state: string;
  start_time?: number;
  end_time?: number;
}

export interface RunSummary {
  id: string;
  agent: string;
  kind: "local" | "cluster";
  status: string;
  started_at: string;
  finished_at?: string | null;
  flink_job_id?: string | null;
  error?: string | null;
  record_count: number;
  spans: SpanSummary[];
}

export interface SpanSummary {
  id: string;
  run_id: string;
  kind: string;
  name: string;
  status: string;
  started_at: string;
  parent_id?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  input?: unknown;
  output?: unknown;
}

export interface PlanStep {
  kind: string;
  name: string;
  description?: string;
  parent?: string;
}

export interface RunDetail extends RunSummary {
  plan: PlanStep[];
}

export interface KafkaTopicSummary {
  name: string;
  description: string;
  present?: boolean | null;
}

export interface KafkaTopicsResponse {
  bootstrap: string;
  reachable: boolean;
  topics: KafkaTopicSummary[];
}

export interface PipelineNodeDef {
  id: string;
  kind: "source" | "agent" | "sink";
  agent?: string | null;
  config?: Record<string, unknown>;
}

export interface PipelineEdgeDef {
  id: string;
  source: string;
  target: string;
  mapping?: Record<string, string>;
}

export interface PipelineSummary {
  id: string;
  name: string;
  nodes: PipelineNodeDef[];
  edges: PipelineEdgeDef[];
  layout: Record<string, { x: number; y: number }>;
  created_at: string;
  updated_at: string;
}

export interface PipelineValidation {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface PipelineRunResult {
  run_id: string;
  status: string;
  output: unknown[];
  validation?: PipelineValidation;
}

export interface AgentGraphNode {
  id: string;
  kind: string;
  name: string;
  description?: string;
}

export interface AgentGraphEdge {
  id: string;
  source: string;
  target: string;
}

export interface AgentGraph {
  agent: string;
  nodes: AgentGraphNode[];
  edges: AgentGraphEdge[];
  source?: string;
  note?: string;
}

export interface EventSnapshot {
  type: "snapshot";
  health: PipelineHealth;
  jobs: JobSummary[];
  jobs_error?: string;
}
