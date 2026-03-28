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
import { initQueryHistory, handleEscapeKey as queryHistoryEscape } from "./components/query-history";
import { initDbConnect } from "./components/db-connect";
import { initRunQuery, closeRunQuery } from "./components/run-query";
import { initManageQueries, handleEscapeKey as manageQueriesEscape } from "./components/manage-queries";
import { initQueryActivity } from "./components/query-activity";
import { applyInitialSettings } from "./components/settings";
import { initListView, closeListView } from "./components/list-view";
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

/**
 * Centralized Escape key handler for modals.
 * Finds the topmost visible modal overlay and closes it, replacing
 * individual per-modal document keydown listeners that were never removed.
 *
 * Modals with complex Escape behavior (sub-state navigation) have their
 * own exported handleEscapeKey functions that are called here.
 */
const modalEscapeHandlers: Record<string, () => void> = {
  "query-history-modal": queryHistoryEscape,
  "manage-queries-modal": manageQueriesEscape,
  "run-query-modal": closeRunQuery,
  "list-view-modal": closeListView,
};

function handleGlobalEscape(e: KeyboardEvent): void {
  if (e.key !== "Escape") return;

  // If an input/textarea/select is focused, let it handle Escape (blur) first.
  // The next Escape press (when nothing is focused) will close the modal.
  const active = document.activeElement;
  if (active) {
    const tag = active.tagName.toLowerCase();
    if (
      tag === "input" ||
      tag === "textarea" ||
      tag === "select" ||
      active.getAttribute("contenteditable") === "true"
    ) {
      return;
    }
  }

  // Find all visible modal overlays, take the last one (topmost in DOM order)
  const visibleModals = document.querySelectorAll<HTMLElement>(".modal-overlay:not([hidden])");
  const topModal = visibleModals[visibleModals.length - 1];
  if (!topModal) return;

  // Use a specific handler if the modal has complex Escape behavior
  const handler = modalEscapeHandlers[topModal.id];
  if (handler) {
    handler();
  } else {
    // Default: simply hide the modal
    topModal.hidden = true;
  }

  e.preventDefault();
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
  initDbConnect();
  initRunQuery();
  initManageQueries();
  initQueryActivity();
  initListView();
  initVersion();

  // Single consolidated document click handler
  document.addEventListener("click", handleDocumentClick);

  // Single global Escape key handler for all modals
  document.addEventListener("keydown", handleGlobalEscape);

  console.log("ADMapper initialized");
}

// Initialize when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
