/**
 * Action Dispatcher
 *
 * Central handler for all application actions triggered by menu items,
 * keyboard shortcuts, or UI buttons.
 */

import { toggleNavSidebar, toggleDetailSidebar, toggleSidebars } from "./sidebars";
import { getRenderer, setLayout, relayoutGraph, toggleLabelVisibility, cycleLayout } from "./graph-view";
import { triggerBloodHoundImport } from "./import";
import { openQueryHistory, goBackInHistory } from "./query-history";
import { showKeyboardShortcuts } from "./keyboard";
import { openDbManager, clearDatabase, clearDisabledObjects } from "./db-manager";
import { exportPNG, exportSVG, exportJSON } from "./export";
import { openInsights } from "./insights";
import { openAddNode, openAddEdge } from "./add-node-edge";
import { openDbConnect, disconnectDb, connectToUrl } from "./db-connect";
import { openRunQuery } from "./run-query";
import { openManageQueries } from "./manage-queries";
import { getRecentConnections, clearConnectionHistory } from "./connection-history";
import { escapeHtml } from "../utils/html";
import { openSettings, toggleTheme } from "./settings";
import { openListView } from "./list-view";

/** Action name constants for type-safe dispatch */
export const Actions = {
  // File menu
  CONNECT_DB: "connect-db",
  DISCONNECT_DB: "disconnect-db",
  CLEAR_RECENT_CONNECTIONS: "clear-recent-connections",
  EXPORT_PNG: "export-png",
  EXPORT_SVG: "export-svg",
  EXPORT_JSON: "export-json",
  SETTINGS: "settings",
  QUIT: "quit",
  // Edit menu
  ADD_NODE: "add-node",
  ADD_EDGE: "add-edge",
  CLEAR_DISABLED: "clear-disabled",
  CLEAR_DB: "clear-db",
  // View menu
  TOGGLE_THEME: "toggle-theme",
  TOGGLE_SIDEBARS: "toggle-sidebars",
  TOGGLE_NAV_SIDEBAR: "toggle-nav-sidebar",
  TOGGLE_DETAIL_SIDEBAR: "toggle-detail-sidebar",
  ZOOM_IN: "zoom-in",
  ZOOM_OUT: "zoom-out",
  ZOOM_RESET: "zoom-reset",
  FIT_GRAPH: "fit-graph",
  FULLSCREEN: "fullscreen",
  TOGGLE_LABEL_VISIBILITY: "toggle-label-visibility",
  // Tools menu
  IMPORT_BLOODHOUND: "import-bloodhound",
  RUN_QUERY: "run-query",
  MANAGE_QUERIES: "manage-queries",
  QUERY_HISTORY: "query-history",
  HISTORY_BACK: "history-back",
  MANAGE_DB: "manage-db",
  INSIGHTS: "insights",
  LAYOUT_GRAPH: "layout-graph",
  LAYOUT_FORCE: "layout-force",
  LAYOUT_HIERARCHICAL: "layout-hierarchical",
  LAYOUT_GRID: "layout-grid",
  LAYOUT_CIRCULAR: "layout-circular",
  CYCLE_LAYOUT: "cycle-layout",
  LIST_VIEW: "list-view",
  // Help menu
  DOCUMENTATION: "documentation",
  KEYBOARD_SHORTCUTS: "keyboard-shortcuts",
  CHECK_UPDATES: "check-updates",
  ABOUT: "about",
  // Modals
  SHOW_PLACEHOLDER_MODAL: "show-placeholder-modal",
  HIDE_PLACEHOLDER_MODAL: "hide-placeholder-modal",
} as const;

/** Static action type derived from the Actions const */
export type StaticAction = (typeof Actions)[keyof typeof Actions];

/** Dynamic action for recent connections (e.g., "recent-connection-0") */
export type RecentConnectionAction = `recent-connection-${number}`;

/** All valid action types */
export type Action = StaticAction | RecentConnectionAction;

/** Action handlers lookup table */
const actionHandlers: Record<StaticAction, () => void> = {
  // File menu
  "connect-db": () => openDbConnect(),
  "disconnect-db": () => disconnectDb(),
  "clear-recent-connections": () => clearRecentConnectionsMenu(),
  "export-png": () => exportPNG(),
  "export-svg": () => exportSVG(),
  "export-json": () => exportJSON(),
  settings: () => openSettings(),
  quit: () => {
    if ("__TAURI__" in window) {
      // @ts-expect-error Tauri global
      window.__TAURI__.process.exit(0);
    }
  },
  // Edit menu
  "add-node": () => openAddNode(),
  "add-edge": () => openAddEdge(),
  "clear-disabled": () => clearDisabledObjects(),
  "clear-db": () => clearDatabase(),
  // View menu
  "toggle-theme": () => toggleTheme(),
  "toggle-sidebars": () => toggleSidebars(),
  "toggle-nav-sidebar": () => toggleNavSidebar(),
  "toggle-detail-sidebar": () => toggleDetailSidebar(),
  "zoom-in": () => getRenderer()?.sigma.getCamera().animatedZoom({ duration: 200 }),
  "zoom-out": () => getRenderer()?.sigma.getCamera().animatedUnzoom({ duration: 200 }),
  "zoom-reset": () => getRenderer()?.resetCamera(),
  "fit-graph": () => getRenderer()?.resetCamera(),
  fullscreen: () => {
    if (document.fullscreenElement) {
      document.exitFullscreen();
    } else {
      document.documentElement.requestFullscreen();
    }
  },
  "toggle-label-visibility": () => {
    const modeName = toggleLabelVisibility();
    // Import dynamically to avoid circular dependency
    import("../utils/notifications").then(({ showInfo }) => {
      showInfo(modeName);
    });
  },
  // Tools menu
  "import-bloodhound": () => triggerBloodHoundImport(),
  "run-query": () => openRunQuery(),
  "manage-queries": () => openManageQueries(),
  "query-history": () => openQueryHistory(),
  "history-back": () => goBackInHistory(),
  "manage-db": () => openDbManager(),
  insights: () => openInsights(),
  "layout-graph": () => relayoutGraph(),
  "layout-force": () => setLayout("force"),
  "layout-hierarchical": () => setLayout("hierarchical"),
  "layout-grid": () => setLayout("grid"),
  "layout-circular": () => setLayout("circular"),
  "cycle-layout": () => {
    const layoutName = cycleLayout();
    import("../utils/notifications").then(({ showInfo }) => {
      showInfo(`Layout: ${layoutName}`);
    });
  },
  "list-view": () => openListView(),
  // Help menu
  documentation: () => window.open("https://github.com/admapper/admapper", "_blank"),
  "keyboard-shortcuts": () => showKeyboardShortcuts(),
  "check-updates": () => {
    // TODO: Update checker
  },
  about: () => {
    // TODO: About dialog
  },
  // Modals
  "show-placeholder-modal": () => {
    const modal = document.getElementById("placeholder-modal");
    if (modal) modal.hidden = false;
  },
  "hide-placeholder-modal": () => {
    const modal = document.getElementById("placeholder-modal");
    if (modal) modal.hidden = true;
  },
};

/** Dispatch an action by name */
export function dispatchAction(action: Action): void {
  // Handle dynamic recent connection actions
  if (action.startsWith("recent-connection-")) {
    handleRecentConnection(action);
    return;
  }

  // After filtering out RecentConnectionAction, action is StaticAction
  const handler = actionHandlers[action as StaticAction];
  if (handler) {
    handler();
  } else {
    console.log(`Unknown action: ${action}`);
  }
}

/** Handle connecting to a recent connection */
async function handleRecentConnection(action: string): Promise<void> {
  const index = parseInt(action.replace("recent-connection-", ""), 10);
  const connections = await getRecentConnections();
  const connection = connections[index];
  if (connection) {
    await connectToUrl(connection.url);
  }
}

/** Clear recent connections menu */
async function clearRecentConnectionsMenu(): Promise<void> {
  await clearConnectionHistory();
  await updateRecentConnectionsMenu();
}

/** Update the recent connections submenu */
export async function updateRecentConnectionsMenu(): Promise<void> {
  const submenu = document.getElementById("recent-connections-submenu");
  if (!submenu) return;

  const connections = await getRecentConnections();

  if (connections.length === 0) {
    submenu.innerHTML = `
      <div class="menu-empty">No recent connections</div>
    `;
    return;
  }

  let html = "";
  for (const [index, conn] of connections.entries()) {
    const escapedName = escapeHtml(conn.displayName);
    html += `
      <button class="menu-option" data-action="recent-connection-${index}" role="menuitem">
        <span>${escapedName}</span>
      </button>
    `;
  }

  html += `
    <div class="menu-separator" role="separator"></div>
    <button class="menu-option" data-action="clear-recent-connections" role="menuitem">
      <span>Clear Recent</span>
    </button>
  `;

  submenu.innerHTML = html;
}
