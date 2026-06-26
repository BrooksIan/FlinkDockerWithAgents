import { Link } from "react-router-dom";
import type { ReactLlmSettings } from "../api/types";

interface Props {
  definitionType: string | undefined;
  llmSettings: ReactLlmSettings | null;
  nodes: Array<{ type?: string; data?: Record<string, unknown> }>;
  mcpAttachedCount: number;
}

function usesFlinkSkills(nodes: Props["nodes"]): boolean {
  return nodes.some((node) => {
    if (node.type !== "llm_call") return false;
    const config = (node.data?.config as Record<string, unknown>) || {};
    return config.mode === "flink_skills";
  });
}

export function DesignerReadinessBanner({
  definitionType,
  llmSettings,
  nodes,
  mcpAttachedCount,
}: Props) {
  if (definitionType !== "react") return null;

  const needsLlm = llmSettings ? !llmSettings.configured : true;
  const skillsMode = usesFlinkSkills(nodes);
  const llmNode = nodes.find((node) => node.type === "llm_call");
  const llmConfig = (llmNode?.data?.config as Record<string, unknown>) || {};
  const skills = Array.isArray(llmConfig.skills) ? llmConfig.skills : [];
  const needsSkills = skillsMode && skills.length === 0;
  const hasMcpTools = nodes.some((node) => node.type === "mcp_tool");
  const needsMcp = hasMcpTools && mcpAttachedCount === 0;

  if (!needsLlm && !needsSkills && !needsMcp) {
    return (
      <div className="designer-readiness card ok" role="status">
        <span className="badge ok">Ready</span>
        <p className="muted" style={{ margin: "0.35rem 0 0" }}>
          ReAct configuration looks good. Validate, compile, then test locally or add to catalog.
          {skillsMode ? " Skills mode uses the native Flink chat model." : ""}
        </p>
      </div>
    );
  }

  return (
    <div className="designer-readiness card warn" role="status">
      <span className="badge warn">Setup needed</span>
      <ul className="designer-readiness-list">
        {needsLlm && (
          <li>
            Configure LLM endpoint, model, and API key in{" "}
            <Link to="/settings">Settings</Link> before compile or test run.
          </li>
        )}
        {needsSkills && (
          <li>
            Flink skills mode is enabled but no skills are selected on the LLM node.
          </li>
        )}
        {needsMcp && (
          <li>
            This graph has MCP tool nodes but no MCP servers attached. Select the canvas background
            in the inspector to attach servers.
          </li>
        )}
      </ul>
    </div>
  );
}
