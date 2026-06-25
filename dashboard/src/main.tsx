import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { AgentDetailPage } from "./pages/AgentDetail";
import { AgentsPage } from "./pages/Agents";
import { DesignerPage } from "./pages/Designer";
import { DesignerEditorPage } from "./pages/DesignerEditor";
import { JobDetailPage } from "./pages/JobDetail";
import { JobsPage } from "./pages/Jobs";
import { OverviewPage } from "./pages/Overview";
import { RunsPage } from "./pages/Runs";
import { RunDetailPage } from "./pages/RunDetail";
import { StudioEditorPage } from "./pages/StudioEditor";
import { StudioListPage } from "./pages/StudioList";
import { SettingsPage } from "./pages/Settings";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<OverviewPage />} />
          <Route path="agents" element={<AgentsPage />} />
          <Route path="agents/:name" element={<AgentDetailPage />} />
          <Route path="designer" element={<DesignerPage />} />
          <Route path="designer/:id" element={<DesignerEditorPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="runs" element={<RunsPage />} />
          <Route path="runs/:id" element={<RunDetailPage />} />
          <Route path="studio" element={<StudioListPage />} />
          <Route path="studio/:id" element={<StudioEditorPage />} />
          <Route path="jobs" element={<JobsPage />} />
          <Route path="jobs/:id" element={<JobDetailPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
);
