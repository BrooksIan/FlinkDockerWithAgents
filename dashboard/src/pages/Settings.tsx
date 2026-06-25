import { Link } from "react-router-dom";
import { LlmSettingsToolLoader } from "../components/LlmSettingsTool";

export function SettingsPage() {
  return (
    <>
      <h2>Settings</h2>
      <p className="muted">
        Platform-wide configuration for ReAct agents and other designer tools.
      </p>

      <section className="designer-section" style={{ maxWidth: 640 }}>
        <LlmSettingsToolLoader />
      </section>

      <p className="muted" style={{ marginTop: "1.5rem" }}>
        ReAct agents in the <Link to="/designer">Designer</Link> and{" "}
        <Link to="/studio">Studio</Link> use these LLM defaults unless overridden per agent.
      </p>
    </>
  );
}
