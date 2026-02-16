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

/** Dispatch an action by name */
export function dispatchAction(action: string): void {
  switch (action) {
    // File menu
    case "new-project":
      console.log("Action: new-project");
      // TODO: Implement
      break;

    case "open-file":
      console.log("Action: open-file");
      // TODO: Open file dialog
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
      console.log("Action: run-query");
      // TODO: Query runner
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
      console.log(`Unknown action: ${action}`);
  }
}
