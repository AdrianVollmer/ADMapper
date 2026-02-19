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

/**
 * Centralized document click handler.
 * Routes clicks to component handlers, reducing the number of document-level listeners.
 */
function handleDocumentClick(e: MouseEvent): void {
  // Each handler can return true to stop further processing
  if (handleSidebarClicks(e)) return;
  if (handleGraphClicks(e)) return;
  if (handleSearchClicks(e)) return;
  if (handleQueryTreeClicks(e)) return;
  handleMenubarOutsideClick(e);
}

/** Initialize the application */
function init(): void {
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
