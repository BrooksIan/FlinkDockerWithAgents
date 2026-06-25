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
  catalog_id?: string;
  display_name?: string;
  tags?: string[];
}

export interface CatalogAgentEntry {
  id: string;
  manifest: string;
  display_name: string;
  description: string;
  type: string;
  entry: string;
  runner: string;
  cluster_script: string;
  flink_yaml?: string | null;
  tags: string[];
  input_schema?: Record<string, unknown>;
  output_schema?: Record<string, unknown>;
  category_id: string;
  category_label: string;
  subcategory_id: string;
  subcategory_label: string;
}

export interface CatalogSubcategory {
  id: string;
  label: string;
  description: string;
  agents: CatalogAgentEntry[];
}

export interface CatalogCategory {
  id: string;
  label: string;
  description: string;
  llm_required?: boolean;
  subcategories: CatalogSubcategory[];
}

export interface AgentCatalog {
  categories: CatalogCategory[];
  react_llm_defaults?: ReactLlmSettings;
}

export interface ReactLlmSettings {
  scope: string;
  endpoint_url: string;
  model_id: string;
  api_key_set: boolean;
  api_key_hint?: string | null;
  configured: boolean;
  source: "designer" | "environment" | "unset" | string;
  env_fallback?: {
    endpoint_url?: string | null;
    model_id?: string | null;
    api_key_set?: boolean;
  };
}

export interface ReactLlmSettingsUpdate {
  endpoint_url: string;
  model_id: string;
  api_key?: string | null;
}

export interface ReactLlmSettingsTestRequest {
  endpoint_url?: string | null;
  model_id?: string | null;
  api_key?: string | null;
}

export interface ReactLlmSettingsTestResult {
  ok: boolean;
  duration_ms: number;
  model_id: string;
  endpoint_url: string;
  message: string;
  result: {
    input: number;
    doubled: number;
    reasoning: string;
  };
}

export type AgentNodeKind =
  | "input_event"
  | "action"
  | "tool"
  | "output_event"
  | "prompt"
  | "llm_call";

export type AgentEdgeKind = "listens_to" | "calls" | "emits";

export interface AgentDefinitionNode {
  id: string;
  kind: AgentNodeKind;
  name: string;
  config: Record<string, unknown>;
}

export interface AgentDefinitionEdge {
  id: string;
  source: string;
  target: string;
  kind: AgentEdgeKind;
}

export interface AgentDefinitionSummary {
  id: string;
  name: string;
  type: "workflow" | "react" | string;
  version: number;
  description: string;
  status: "draft" | "compiled" | "published" | string;
  manifest_name?: string | null;
  catalog_category_id?: string | null;
  catalog_subcategory_id?: string | null;
  catalog_tags: string[];
  created_at: string;
  updated_at: string;
}

export interface AgentDefinition extends AgentDefinitionSummary {
  nodes: AgentDefinitionNode[];
  edges: AgentDefinitionEdge[];
  layout: Record<string, { x: number; y: number }>;
  input_schema: Record<string, unknown>;
  output_schema: Record<string, unknown>;
}

export interface AgentDefinitionValidation {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface AgentDefinitionCompileFile {
  path: string;
  content: string;
}

export interface AgentDefinitionCompileResult {
  definition_id: string;
  agent_slug: string;
  class_name: string;
  output_dir: string;
  status: string;
  validation: AgentDefinitionValidation;
  files: AgentDefinitionCompileFile[];
  definition?: AgentDefinition;
}

export interface AgentDefinitionPublishResult {
  definition_id: string;
  manifest_name: string;
  catalog_id: string;
  manifest_path: string;
  catalog_path: string;
  shim_path: string;
  status: string;
  definition?: AgentDefinition;
}

export interface AgentDefinitionCreate {
  name?: string;
  type?: string;
  description?: string;
  nodes?: AgentDefinitionNode[];
  edges?: AgentDefinitionEdge[];
  layout?: Record<string, { x: number; y: number }>;
  input_schema?: Record<string, unknown>;
  output_schema?: Record<string, unknown>;
  manifest_name?: string | null;
  catalog_category_id?: string | null;
  catalog_subcategory_id?: string | null;
  catalog_tags?: string[];
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
  output?: unknown;
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
