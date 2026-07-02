import type {
  AgentDefinition,
  AgentDefinitionAssistDiff,
  AgentDefinitionCreate,
} from "../api/types";

function nodeLabel(nodes: { id: string; name?: string }[], id: string): string {
  const node = nodes.find((item) => item.id === id);
  return node?.name ? `${node.name} (${id})` : id;
}

export function computeAssistDiff(
  current: AgentDefinition | null | undefined,
  proposal: AgentDefinitionCreate,
): AgentDefinitionAssistDiff {
  const currentNodes = current?.nodes || [];
  const proposalNodes = proposal.nodes || [];
  const currentNodeIds = new Set(currentNodes.map((node) => node.id));
  const proposalNodeIds = new Set(proposalNodes.map((node) => node.id));

  const nodes_added = proposalNodes
    .filter((node) => !currentNodeIds.has(node.id))
    .map((node) => nodeLabel(proposalNodes, node.id));
  const nodes_removed = currentNodes
    .filter((node) => !proposalNodeIds.has(node.id))
    .map((node) => nodeLabel(currentNodes, node.id));

  const currentEdges = current?.edges || [];
  const proposalEdges = proposal.edges || [];
  const edgeKey = (edge: { source: string; target: string; kind: string }) =>
    `${edge.source}->${edge.target}:${edge.kind}`;
  const currentEdgeKeys = new Set(currentEdges.map(edgeKey));
  const proposalEdgeKeys = new Set(proposalEdges.map(edgeKey));

  const edges_added = proposalEdges
    .filter((edge) => !currentEdgeKeys.has(edgeKey(edge)))
    .map((edge) => `${edge.kind}: ${edge.source} → ${edge.target}`);
  const edges_removed = currentEdges
    .filter((edge) => !proposalEdgeKeys.has(edgeKey(edge)))
    .map((edge) => `${edge.kind}: ${edge.source} → ${edge.target}`);

  const fields_changed: string[] = [];
  if (current) {
    if ((current.name || "") !== (proposal.name || "")) fields_changed.push("name");
    if ((current.type || "") !== (proposal.type || "")) fields_changed.push("type");
    if ((current.description || "") !== (proposal.description || "")) {
      fields_changed.push("description");
    }
    if (JSON.stringify(current.input_schema || {}) !== JSON.stringify(proposal.input_schema || {})) {
      fields_changed.push("input_schema");
    }
    if (JSON.stringify(current.output_schema || {}) !== JSON.stringify(proposal.output_schema || {})) {
      fields_changed.push("output_schema");
    }
    if (JSON.stringify(current.mcp_servers || []) !== JSON.stringify(proposal.mcp_servers || [])) {
      fields_changed.push("mcp_servers");
    }
    if (JSON.stringify(current.catalog_tags || []) !== JSON.stringify(proposal.catalog_tags || [])) {
      fields_changed.push("catalog_tags");
    }
  }

  return {
    nodes_added,
    nodes_removed,
    edges_added,
    edges_removed,
    fields_changed,
  };
}
