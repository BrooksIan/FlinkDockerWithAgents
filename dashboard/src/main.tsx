import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { AgentDetailPage } from "./pages/AgentDetail";
import { AgentsPage } from "./pages/Agents";
import { JobDetailPage } from "./pages/JobDetail";
import { JobsPage } from "./pages/Jobs";
import { OverviewPage } from "./pages/Overview";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<OverviewPage />} />
          <Route path="agents" element={<AgentsPage />} />
          <Route path="agents/:name" element={<AgentDetailPage />} />
          <Route path="jobs" element={<JobsPage />} />
          <Route path="jobs/:id" element={<JobDetailPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
);
