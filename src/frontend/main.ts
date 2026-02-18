/**
 * ADMapper Main Entry Point
 *
 * Initializes the application UI: menu bar, sidebars, and graph visualization.
 */

import { initMenuBar } from "./components/menubar";
import { initSidebars } from "./components/sidebars";
import { initGraph } from "./components/graph-view";
import { initKeyboardShortcuts } from "./components/keyboard";
import { initImport } from "./components/import";
import { initSearch } from "./components/search";
import { initQueries } from "./components/queries";
import { initQueryHistory } from "./components/query-history";
import { initDbManager } from "./components/db-manager";
import { initDbConnect } from "./components/db-connect";
import { initRunQuery } from "./components/run-query";
import { initManageQueries } from "./components/manage-queries";

/** Application state */
export interface AppState {
  navSidebarCollapsed: boolean;
  detailSidebarCollapsed: boolean;
  selectedNodeId: string | null;
  databaseConnected: boolean;
  databaseType: string | null;
}

export const appState: AppState = {
  navSidebarCollapsed: false,
  detailSidebarCollapsed: false,
  selectedNodeId: null,
  databaseConnected: false,
  databaseType: null,
};

/** Initialize the application */
function init(): void {
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

  console.log("ADMapper initialized");
}

// Initialize when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}

// Global modal functions for inline onclick handlers
function showPlaceholderModal(): void {
  const modal = document.getElementById("placeholder-modal");
  if (modal) modal.hidden = false;
}

function hidePlaceholderModal(): void {
  const modal = document.getElementById("placeholder-modal");
  if (modal) modal.hidden = true;
}

// Expose modal functions to window for inline onclick handlers
declare global {
  interface Window {
    showPlaceholderModal: () => void;
    hidePlaceholderModal: () => void;
  }
}
window.showPlaceholderModal = showPlaceholderModal;
window.hidePlaceholderModal = hidePlaceholderModal;
