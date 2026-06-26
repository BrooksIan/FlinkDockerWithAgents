import type { DesignerSkill, LlmCallConfig } from "../api/types";

export function llmConfigFromNode(config: Record<string, unknown>): LlmCallConfig {
  const mode = config.mode === "flink_skills" ? "flink_skills" : "simple";
  const skills = Array.isArray(config.skills)
    ? config.skills.map((item) => String(item)).filter(Boolean)
    : [];
  const allowed_commands = Array.isArray(config.allowed_commands)
    ? config.allowed_commands.map((item) => String(item)).filter(Boolean)
    : [];
  return {
    use_platform_llm: config.use_platform_llm !== false,
    mode,
    skills,
    allowed_commands,
  };
}

export function mergeAllowedCommands(
  selectedSkills: string[],
  catalog: DesignerSkill[],
  current: string[],
): string[] {
  const merged = [...current];
  for (const skillId of selectedSkills) {
    const entry = catalog.find((item) => item.id === skillId);
    if (!entry) continue;
    for (const command of entry.default_allowed_commands) {
      if (!merged.includes(command)) merged.push(command);
    }
  }
  return merged;
}

export function llmCallSubtitle(config: Record<string, unknown>): string {
  const llm = llmConfigFromNode(config);
  if (llm.mode === "flink_skills") {
    const count = llm.skills?.length ?? 0;
    return count ? `skills · ${count} enabled` : "skills · none selected";
  }
  return llm.use_platform_llm === false ? "HTTP LLM off" : "simple · platform LLM";
}
