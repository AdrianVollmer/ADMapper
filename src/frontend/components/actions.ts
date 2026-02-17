/**
 * Action Dispatcher
 *
 * Central handler for all application actions triggered by menu items,
 * keyboard shortcuts, or UI buttons.
 */

import { toggleNavSidebar, toggleDetailSidebar, toggleSidebars } from "./sidebars";
import { getRenderer } from "./graph-view";
import { triggerBloodHoundImport } from "./import";
import { openQueryHistory, goBackInHistory } from "./query-history";
import { showKeyboardShortcuts } from "./keyboard";
import { openDbManager } from "./db-manager";
import { exportPNG, exportSVG, exportJSON } from "./export";
import { openInsights } from "./insights";
import { openAddNode, openAddEdge } from "./add-node-edge";
import { openDbConnect, disconnectDb, connectToUrl } from "./db-connect";
import { openRunQuery } from "./run-query";
import { getRecentConnections, clearConnectionHistory } from "./connection-history";

/** Dispatch an action by name */
export function dispatchAction(action: string): void {
  switch (action) {
    // File menu
    case "connect-db":
      openDbConnect();
      break;

    case "disconnect-db":
      disconnectDb();
      break;

    case "clear-recent-connections":
      clearRecentConnectionsMenu();
      break;

    case "export-png":
      exportPNG();
      break;

    case "export-svg":
      exportSVG();
      break;

    case "export-json":
      exportJSON();
      break;

    case "settings":
      console.log("Action: settings");
      // TODO: Settings dialog
      break;

    case "quit":
      console.log("Action: quit");
      // In Tauri, we'd call tauri.exit()
      if ("__TAURI__" in window) {
        // @ts-expect-error Tauri global
        window.__TAURI__.process.exit(0);
      }
      break;

    // Edit menu
    case "select-all":
      console.log("Action: select-all");
      // TODO: Select all nodes
      break;

    case "find":
      console.log("Action: find");
      // Focus the search input
      document.getElementById("node-search")?.focus();
      break;

    case "add-node":
      openAddNode();
      break;

    case "add-edge":
      openAddEdge();
      break;

    // View menu
    case "toggle-sidebars":
      toggleSidebars();
      break;

    case "toggle-nav-sidebar":
      toggleNavSidebar();
      break;

    case "toggle-detail-sidebar":
      toggleDetailSidebar();
      break;

    case "zoom-in": {
      const renderer = getRenderer();
      renderer?.sigma.getCamera().animatedZoom({ duration: 200 });
      break;
    }

    case "zoom-out": {
      const renderer = getRenderer();
      renderer?.sigma.getCamera().animatedUnzoom({ duration: 200 });
      break;
    }

    case "zoom-reset":
    case "fit-graph": {
      const renderer = getRenderer();
      renderer?.resetCamera();
      break;
    }

    case "fullscreen":
      if (document.fullscreenElement) {
        document.exitFullscreen();
      } else {
        document.documentElement.requestFullscreen();
      }
      break;

    // Tools menu
    case "import-bloodhound":
      triggerBloodHoundImport();
      break;

    case "run-query":
      openRunQuery();
      break;

    case "saved-queries":
      console.log("Action: saved-queries");
      // TODO: Show saved queries
      break;

    case "query-history":
      openQueryHistory();
      break;

    case "history-back":
      goBackInHistory();
      break;

    case "manage-db":
      openDbManager();
      break;

    case "insights":
      openInsights();
      break;

    case "layout-graph": {
      console.log("Action: layout-graph");
      // Re-layout is handled in graph-view.ts via the button click
      break;
    }

    // Help menu
    case "documentation":
      console.log("Action: documentation");
      window.open("https://github.com/admapper/admapper", "_blank");
      break;

    case "keyboard-shortcuts":
      showKeyboardShortcuts();
      break;

    case "check-updates":
      console.log("Action: check-updates");
      // TODO: Update checker
      break;

    case "about":
      console.log("Action: about");
      // TODO: About dialog
      break;

    default:
      // Handle dynamic recent connection actions
      if (action.startsWith("recent-connection-")) {
        handleRecentConnection(action);
      } else {
        console.log(`Unknown action: ${action}`);
      }
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
    const escapedName = conn.displayName.replace(/"/g, "&quot;").replace(/</g, "&lt;");
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
