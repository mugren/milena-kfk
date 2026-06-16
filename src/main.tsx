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

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
