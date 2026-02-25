/**
 * ADMapper Main Entry Point
 *
 * Initializes the application UI: menu bar, sidebars, and graph visualization.
 */

import { initMenuBar, handleMenubarOutsideClick } from "./components/menubar";
import { initSidebars, handleSidebarClicks } from "./components/sidebars";
import { initGraph, handleGraphClicks } from "./components/graph-view";
import { initKeyboardShortcuts } from "./components/keyboard";
import { initImport } from "./components/import";
import { initSearch, handleSearchClicks } from "./components/search";
import { initQueries, handleQueryTreeClicks } from "./components/queries";
import { initQueryHistory } from "./components/query-history";
import { initDbManager } from "./components/db-manager";
import { initDbConnect } from "./components/db-connect";
import { initRunQuery } from "./components/run-query";
import { initManageQueries } from "./components/manage-queries";
import { initQueryActivity } from "./components/query-activity";
import { initSettings, applyInitialSettings } from "./components/settings";
import { initListView } from "./components/list-view";
import { isRunningInTauri } from "./api/client";

/** Application state */
export interface AppState {
  navSidebarCollapsed: boolean;
  detailSidebarCollapsed: boolean;
  selectedNodeId: string | null;
  selectedEdgeId: string | null;
  databaseConnected: boolean;
  databaseType: string | null;
}

export const appState: AppState = {
  navSidebarCollapsed: false,
  detailSidebarCollapsed: false,
  selectedNodeId: null,
  selectedEdgeId: null,
  databaseConnected: false,
  databaseType: null,
};

/**
 * Centralized document click handler.
 * Routes clicks to component handlers, reducing the number of document-level listeners.
 */
function handleDocumentClick(e: MouseEvent): void {
  // Each handler can return true to stop further processing
  // Order matters: more specific handlers (sidebar, query tree, search) before generic graph handler
  if (handleSidebarClicks(e)) return;
  if (handleQueryTreeClicks(e)) return;
  if (handleSearchClicks(e)) return;
  if (handleGraphClicks(e)) return;
  handleMenubarOutsideClick(e);
}

/** Fetch and display app version */
async function initVersion(): Promise<void> {
  const versionEl = document.getElementById("app-version");
  if (!versionEl) return;

  try {
    let version: string | undefined;

    if (isRunningInTauri()) {
      version = await window.__TAURI__!.core.invoke<string>("app_version");
    } else {
      const response = await fetch("/api/health");
      if (response.ok) {
        const data = await response.json();
        version = data.version;
      }
    }

    if (version) {
      versionEl.textContent = `v${version}`;
    }
  } catch {
    // Ignore errors, version display is optional
  }
}

/** Initialize the application */
async function init(): Promise<void> {
  // Apply settings (especially theme) before UI renders
  await applyInitialSettings();

  // Initialize components (they no longer add document click listeners)
  initMenuBar();
  initSidebars();
  initGraph();
  initKeyboardShortcuts();
  initImport();
  initSearch();
  initQueries();
  initQueryHistory();
  initDbManager();
  initDbConnect();
  initRunQuery();
  initManageQueries();
  initQueryActivity();
  initSettings();
  initListView();
  initVersion();

  // Single consolidated document click handler
  document.addEventListener("click", handleDocumentClick);

  console.log("ADMapper initialized");
}

// Initialize when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
