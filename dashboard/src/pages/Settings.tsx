import { Link } from "react-router-dom";
import { FlinkClusterPanel } from "../components/FlinkClusterPanel";
import { LlmSettingsToolLoader } from "../components/LlmSettingsTool";
import { McpSettingsToolLoader } from "../components/McpSettingsTool";

export function SettingsPage() {
  return (
    <>
      <h2>Settings</h2>
      <p className="muted">
        Platform-wide configuration for the Flink cluster, ReAct agents, MCP servers, and other
        designer tools.
      </p>

      <section className="designer-section" style={{ maxWidth: 720 }}>
        <FlinkClusterPanel />
      </section>

      <section className="designer-section" style={{ maxWidth: 640, marginTop: "1.5rem" }}>
        <LlmSettingsToolLoader />
      </section>

      <section className="designer-section" style={{ maxWidth: 640, marginTop: "1.5rem" }}>
        <McpSettingsToolLoader />
      </section>

      <p className="muted" style={{ marginTop: "1.5rem" }}>
        ReAct agents in the <Link to="/designer">Designer</Link> and{" "}
        <Link to="/studio">Studio</Link> use LLM defaults unless overridden per agent. Attach MCP
        servers per agent in the Designer inspector.
      </p>
    </>
  );
}
