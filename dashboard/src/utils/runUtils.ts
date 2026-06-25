export function isPipelineRun(agent: string): boolean {
  return agent.startsWith("pipeline:");
}

export function pipelineRunName(agent: string): string {
  return isPipelineRun(agent) ? agent.slice("pipeline:".length) : agent;
}
