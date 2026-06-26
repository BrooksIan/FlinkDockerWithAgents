import { NavLink, Outlet } from "react-router-dom";
import { useFlinkUrl } from "../hooks/useFlinkUrl";

export function Layout() {
  const flinkUrl = useFlinkUrl();
  return (
    <div className="layout">
      <nav className="nav">
        <div className="nav-brand">
          <img className="nav-logo" src="/ratatoskr-icon.svg" alt="" width={40} height={40} />
          <div>
            <h1>Ratatoskr</h1>
            <p className="sub">Flink Agents</p>
          </div>
        </div>
        <NavLink to="/" end className={({ isActive }) => (isActive ? "active" : "")}>
          Overview
        </NavLink>
        <NavLink to="/agents" className={({ isActive }) => (isActive ? "active" : "")}>
          Agents
        </NavLink>
        <NavLink to="/designer" className={({ isActive }) => (isActive ? "active" : "")}>
          Designer
        </NavLink>
        <NavLink to="/runs" className={({ isActive }) => (isActive ? "active" : "")}>
          Runs
        </NavLink>
        <NavLink to="/studio" className={({ isActive }) => (isActive ? "active" : "")}>
          Studio
        </NavLink>
        <NavLink to="/settings" className={({ isActive }) => (isActive ? "active" : "")}>
          Settings
        </NavLink>
        <NavLink to="/jobs" className={({ isActive }) => (isActive ? "active" : "")}>
          Jobs
        </NavLink>
        <p className="muted" style={{ marginTop: "2rem" }}>
          API:{" "}
          <a href="/v1/health" target="_blank" rel="noreferrer">
            /v1/health
          </a>
          <br />
          <a href={flinkUrl} target="_blank" rel="noreferrer">
            Flink UI
          </a>
        </p>
      </nav>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
