import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./App.css";
import {
  type AppearancePreference,
  applyAppearancePreference,
  readAppearancePreference,
} from "./lib/appearance";
import { setAppAppearanceTheme } from "./lib/tauri";

function readBootAppearancePreference(): AppearancePreference {
  try {
    return readAppearancePreference(window.localStorage);
  } catch {
    return readAppearancePreference();
  }
}

const appearancePreference = readBootAppearancePreference();

applyAppearancePreference(appearancePreference);
void setAppAppearanceTheme(appearancePreference);

const shouldRenderDockviewPrototype =
  import.meta.env.VITE_MILENA_PROTOTYPE === "dockview" ||
  new URLSearchParams(window.location.search).get("prototype") === "dockview";

const DockviewWorkspacePrototype = React.lazy(
  () => import("./prototype/dockview-workspace/DockviewWorkspacePrototype"),
);

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  shouldRenderDockviewPrototype ? (
    <React.Suspense fallback={<div>Loading Dockview prototype...</div>}>
      <DockviewWorkspacePrototype />
    </React.Suspense>
  ) : (
    <React.StrictMode>
      <App />
    </React.StrictMode>
  ),
);
