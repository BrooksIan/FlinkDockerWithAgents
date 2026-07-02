import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import type { AgentCatalog, AgentDefinition, McpInstance } from "../api/types";
import { attachedMcpOptions } from "./mcpUtils";

interface Props {
  open: boolean;
  definition: AgentDefinition | null;
  catalog: AgentCatalog | null;
  mcpInstances: McpInstance[];
  onClose: () => void;
  onUpdate: (patch: Partial<AgentDefinition>) => void;
}

function schemaText(schema: Record<string, unknown> | undefined): string {
  return JSON.stringify(schema || {}, null, 2);
}

function parseSchema(text: string): { value?: Record<string, unknown>; error?: string } {
  const trimmed = text.trim();
  if (!trimmed) return { value: {} };
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { error: "Schema must be a JSON object." };
    }
    return { value: parsed as Record<string, unknown> };
  } catch {
    return { error: "Invalid JSON." };
  }
}

export function DesignerSettingsDrawer({
  open,
  definition,
  catalog,
  mcpInstances,
  onClose,
  onUpdate,
}: Props) {
  const [description, setDescription] = useState("");
  const [inputSchemaText, setInputSchemaText] = useState("{}");
  const [outputSchemaText, setOutputSchemaText] = useState("{}");
  const [catalogCategoryId, setCatalogCategoryId] = useState("");
  const [catalogSubcategoryId, setCatalogSubcategoryId] = useState("");
  const [catalogTags, setCatalogTags] = useState("");
  const [inputSchemaError, setInputSchemaError] = useState<string | null>(null);
  const [outputSchemaError, setOutputSchemaError] = useState<string | null>(null);

  useEffect(() => {
    if (!definition) return;
    setDescription(definition.description || "");
    setInputSchemaText(schemaText(definition.input_schema));
    setOutputSchemaText(schemaText(definition.output_schema));
    setCatalogCategoryId(definition.catalog_category_id || "");
    setCatalogSubcategoryId(definition.catalog_subcategory_id || "");
    setCatalogTags((definition.catalog_tags || []).join(", "));
    setInputSchemaError(null);
    setOutputSchemaError(null);
  }, [definition, open]);

  if (!open || !definition) return null;

  const attached = definition.mcp_servers || [];
  const mcpOptions = attachedMcpOptions(mcpInstances, attached);
  const categories = catalog?.categories || [];
  const subcategories =
    categories.find((category) => category.id === catalogCategoryId)?.subcategories || [];

  function saveSchemas() {
    const input = parseSchema(inputSchemaText);
    const output = parseSchema(outputSchemaText);
    setInputSchemaError(input.error || null);
    setOutputSchemaError(output.error || null);
    if (input.error || output.error) return;
    onUpdate({
      input_schema: input.value,
      output_schema: output.value,
    });
  }

  function saveCatalog() {
    onUpdate({
      description: description.trim(),
      catalog_category_id: catalogCategoryId || null,
      catalog_subcategory_id: catalogSubcategoryId || null,
      catalog_tags: catalogTags
        .split(",")
        .map((tag) => tag.trim())
        .filter(Boolean),
    });
  }

  return (
    <div className="studio-drawer designer-settings-drawer" role="dialog" aria-label="Agent settings">
      <div className="studio-drawer-header">
        <h3 style={{ margin: 0 }}>Agent settings</h3>
        <button type="button" className="secondary" onClick={onClose}>
          Close
        </button>
      </div>

      <section className="designer-settings-section">
        <h4>Description</h4>
        <textarea
          className="studio-textarea"
          rows={3}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          onBlur={saveCatalog}
        />
      </section>

      <section className="designer-settings-section">
        <h4>MCP servers</h4>
        <p className="muted">
          Attach platform MCP instances this agent may call. Configure servers in{" "}
          <Link to="/settings">Settings</Link>.
        </p>
        {mcpOptions.length === 0 ? (
          <p className="muted">No enabled MCP instances.</p>
        ) : (
          <div className="designer-mcp-attach-list">
            {mcpOptions.map((inst) => {
              const checked = attached.includes(inst.instance_id);
              return (
                <label key={inst.instance_id} className="designer-field designer-checkbox-field">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const next = e.target.checked
                        ? [...attached, inst.instance_id]
                        : attached.filter((id) => id !== inst.instance_id);
                      onUpdate({ mcp_servers: next });
                    }}
                  />
                  <span>
                    {inst.display_name} <code>{inst.instance_id}</code>
                  </span>
                </label>
              );
            })}
          </div>
        )}
      </section>

      <section className="designer-settings-section">
        <h4>Catalog metadata</h4>
        <label className="studio-label">Category</label>
        <select
          className="studio-select"
          value={catalogCategoryId}
          onChange={(e) => {
            setCatalogCategoryId(e.target.value);
            setCatalogSubcategoryId("");
          }}
          onBlur={saveCatalog}
        >
          <option value="">Select category…</option>
          {categories.map((category) => (
            <option key={category.id} value={category.id}>
              {category.label}
            </option>
          ))}
        </select>
        <label className="studio-label">Subcategory</label>
        <select
          className="studio-select"
          value={catalogSubcategoryId}
          onChange={(e) => setCatalogSubcategoryId(e.target.value)}
          onBlur={saveCatalog}
        >
          <option value="">Select subcategory…</option>
          {subcategories.map((subcategory) => (
            <option key={subcategory.id} value={subcategory.id}>
              {subcategory.label}
            </option>
          ))}
        </select>
        <label className="studio-label">Tags</label>
        <input
          className="studio-input"
          type="text"
          value={catalogTags}
          placeholder="demo, transform"
          onChange={(e) => setCatalogTags(e.target.value)}
          onBlur={saveCatalog}
        />
      </section>

      <section className="designer-settings-section">
        <h4>Input schema</h4>
        <textarea
          className="studio-textarea designer-schema-textarea"
          rows={8}
          value={inputSchemaText}
          onChange={(e) => {
            setInputSchemaText(e.target.value);
            setInputSchemaError(null);
          }}
          onBlur={saveSchemas}
        />
        {inputSchemaError && <p className="error">{inputSchemaError}</p>}
      </section>

      <section className="designer-settings-section">
        <h4>Output schema</h4>
        <textarea
          className="studio-textarea designer-schema-textarea"
          rows={8}
          value={outputSchemaText}
          onChange={(e) => {
            setOutputSchemaText(e.target.value);
            setOutputSchemaError(null);
          }}
          onBlur={saveSchemas}
        />
        {outputSchemaError && <p className="error">{outputSchemaError}</p>}
      </section>

      <details className="designer-env-hint">
        <summary>Advanced: raw definition JSON</summary>
        <pre className="designer-json-preview">{JSON.stringify(definition, null, 2)}</pre>
      </details>
    </div>
  );
}
