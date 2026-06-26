import type { McpCatalog, McpInstance } from "../api/types";

export function attachedMcpOptions(instances: McpInstance[], attached: string[]) {
  const enabled = instances.filter((inst) => inst.enabled && inst.configured);
  const selectedDisabled = instances.filter(
    (inst) => attached.includes(inst.instance_id) && !enabled.some((e) => e.instance_id === inst.instance_id),
  );
  return [...enabled, ...selectedDisabled];
}

export function catalogToolsForInstance(
  catalog: McpCatalog | null,
  instance: McpInstance | undefined,
): string[] {
  if (!instance || !catalog) return [];
  for (const category of catalog.categories) {
    const server = category.servers.find((item) => item.id === instance.catalog_id);
    if (server?.tools?.length) {
      return server.tools.map((tool) => tool.name);
    }
  }
  return [];
}

export function instanceById(instances: McpInstance[], instanceId: string): McpInstance | undefined {
  return instances.find((inst) => inst.instance_id === instanceId);
}
