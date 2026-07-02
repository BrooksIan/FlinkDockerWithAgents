import type { AgentDefinitionValidation, AgentDefinitionValidationIssue } from "../api/types";
import type { Edge, Node } from "@xyflow/react";

export function validationIssues(validation: AgentDefinitionValidation | null): AgentDefinitionValidationIssue[] {
  if (!validation) return [];
  if (validation.issues && validation.issues.length > 0) {
    return validation.issues;
  }
  return [
    ...validation.errors.map((message) => ({ message, level: "error" as const })),
    ...validation.warnings.map((message) => ({ message, level: "warning" as const })),
  ];
}

export function nodeLabel(nodes: Node[], nodeId: string | null | undefined): string | null {
  if (!nodeId) return null;
  const node = nodes.find((item) => item.id === nodeId);
  if (!node) return nodeId;
  const data = node.data as { name?: string; label?: string };
  return String(data.name || data.label || nodeId);
}

export function edgeLabel(edges: Edge[], edgeId: string | null | undefined): string | null {
  if (!edgeId) return null;
  const edge = edges.find((item) => item.id === edgeId);
  if (!edge) return edgeId;
  return `${edge.source} → ${edge.target}`;
}

export function issueTargetLabel(
  issue: AgentDefinitionValidationIssue,
  nodes: Node[],
  edges: Edge[],
): string | null {
  if (issue.node_id) return nodeLabel(nodes, issue.node_id);
  if (issue.edge_id) return edgeLabel(edges, issue.edge_id);
  return null;
}
