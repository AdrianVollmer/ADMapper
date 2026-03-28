/**
 * Graph View Component
 *
 * Integrates the Sigma.js graph renderer with the application UI.
 */

import { loadGraph, createRenderer, applyLayoutAsync as applyLayoutFromModule, getGraphStats } from "../graph";
import type { ADGraphRenderer, LayoutType } from "../graph";
import type { RawADGraph } from "../graph/types";
import { updateDetailPanel, updateDetailPanelForEdge } from "./sidebars";
import { clearCollapseState } from "../graph/collapse";
import { destroyMagnifier } from "../graph/magnifier";
import { dispatchAction, Actions, type Action } from "./actions";
import { api } from "../api/client";
import { cycleLabelVisibility, getLabelVisibilityName } from "../graph/label-visibility";
import { getDefaultLayout } from "./settings";
import { getCurrentTheme } from "../utils/theme";
import { showConfirm } from "../utils/notifications";
import { escapeHtml } from "../utils/html";

let renderer: ADGraphRenderer | null = null;
let currentLayout: LayoutType = "force";

/** Show the layout spinner (hides stats) */
function showLayoutSpinner(): void {
  const spinner = document.getElementById("layout-spinner");
  const stats = document.getElementById("graph-stats");
  if (spinner) spinner.hidden = false;
  if (stats) stats.hidden = true;
}

/** Hide the layout spinner (shows stats) */
function hideLayoutSpinner(): void {
  const spinner = document.getElementById("layout-spinner");
  const stats = document.getElementById("graph-stats");
  if (spinner) spinner.hidden = true;
  if (stats) stats.hidden = false;
}

/** Apply layout asynchronously with spinner */
async function applyLayoutAsync(graph: ReturnType<typeof loadGraph>, layout: LayoutType): Promise<void> {
  const nodeCount = graph.order;
  // Show spinner for larger graphs or heavy layouts
  const showSpinner = nodeCount > 50 || layout === "hierarchical";

  if (showSpinner) {
    showLayoutSpinner();
    // Yield to allow spinner to render before starting
    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  try {
    // Use the async layout that yields to UI during computation
    await applyLayoutFromModule(graph, { type: layout });
  } finally {
    if (showSpinner) {
      hideLayoutSpinner();
    }
  }
}

/** Initialize the graph view */
export function initGraph(): void {
  // Set initial layout from user's default setting (only once at startup)
  currentLayout = getDefaultLayout();

  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Only show demo data in development mode
  // In production, the placeholder will be shown after connection status is checked
  if (import.meta.env.DEV) {
    const demoGraph = generateDemoGraph(200);
    loadGraphData(demoGraph);
  }
  // Note: In production, the placeholder is shown via updateGraphForConnectionState()
  // which is called after the connection status is fetched from the server
}

/**
 * Handle clicks for graph-related actions.
 * Called from the central document click handler in main.ts.
 * Returns true if the click was handled.
 */
export function handleGraphClicks(e: MouseEvent): boolean {
  const target = e.target as HTMLElement;
  const button = target.closest("[data-action]") as HTMLElement;
  if (!button) return false;

  const action = button.getAttribute("data-action");
  if (action) {
    dispatchAction(action as Action);
    return true;
  }
  return false;
}

/** Update the graph view based on connection state.
 *  When `isNewConnection` is true (user just connected to a different DB),
 *  any existing graph is cleared and the "run a query" placeholder is shown. */
export function updateGraphForConnectionState(connected: boolean, error?: string, isNewConnection = false): void {
  // Don't interfere with dev mode demo data
  if (import.meta.env.DEV) return;

  // On a fresh connection, always clear stale data from the previous DB
  if (isNewConnection) {
    if (!connected) {
      showNoConnectionPlaceholder(error);
    } else {
      showConnectedPlaceholder();
    }
    return;
  }

  // Status refresh on page load: preserve existing graph if any
  if (renderer) return;

  if (!connected) {
    showNoConnectionPlaceholder(error);
  } else {
    showConnectedPlaceholder();
  }
}

/** Show placeholder when connected but no data loaded */
export async function showConnectedPlaceholder(): Promise<void> {
  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Clean up existing renderer and magnifier
  destroyMagnifier();
  if (renderer) {
    renderer.destroy();
    renderer = null;
  }

  // Check if the database has any nodes
  let hasNodes = false;
  try {
    const stats = await api.get<{ total_nodes: number; total_edges: number }>("/api/graph/detailed-stats");
    hasNodes = stats.total_nodes > 0;
  } catch {
    // If stats fail, fall back to showing Run Query
  }

  // Create placeholder
  const buttonHtml = hasNodes
    ? `<button
        class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded transition-colors"
        data-action="${Actions.RUN_QUERY}"
      >
        Run Query
      </button>`
    : `<button
        class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded transition-colors"
        data-action="${Actions.IMPORT_BLOODHOUND}"
      >
        Import Data
      </button>`;

  container.innerHTML = `
    <div class="flex flex-col items-center justify-center h-full text-gray-400">
      <svg class="w-16 h-16 mb-4 text-green-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <p class="text-lg mb-2 text-green-400">Database connected</p>
      <p class="text-sm text-gray-500 mb-4">${
        hasNodes
          ? "Run a query or select one from the sidebar to visualize data"
          : "Import BloodHound data to get started"
      }</p>
      ${buttonHtml}
    </div>
  `;

  // Update stats
  const statsEl = document.getElementById("graph-stats");
  if (statsEl) {
    statsEl.textContent = "No graph loaded";
  }
}

/** Show placeholder when no database is connected */
export function showNoConnectionPlaceholder(error?: string): void {
  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Clean up existing renderer and magnifier
  destroyMagnifier();
  if (renderer) {
    renderer.destroy();
    renderer = null;
  }

  // Create placeholder
  container.innerHTML = `
    <div class="flex flex-col items-center justify-center h-full text-gray-400">
      <svg class="w-16 h-16 mb-4 text-gray-600" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <path d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
      </svg>
      ${error ? `<p class="text-red-400 mb-2 text-center max-w-md">${escapeHtml(error)}</p>` : ""}
      <p class="text-lg mb-2">No database connected</p>
      <p class="text-sm text-gray-500 mb-4">Connect to a database to visualize Active Directory permissions</p>
      <button
        class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded transition-colors"
        data-action="connect-db"
      >
        Connect to Database
      </button>
    </div>
  `;

  // Update stats
  const statsEl = document.getElementById("graph-stats");
  if (statsEl) {
    statsEl.textContent = "No graph loaded";
  }
}

/** Thresholds for large graph warning */
const LARGE_GRAPH_NODE_THRESHOLD = 1000;
const LARGE_GRAPH_EDGE_THRESHOLD = 500;

/** Load graph data and render */
export async function loadGraphData(data: RawADGraph): Promise<void> {
  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Check for large graphs and ask for confirmation
  const nodeCount = data.nodes.length;
  const edgeCount = data.relationships.length;

  if (nodeCount > LARGE_GRAPH_NODE_THRESHOLD || edgeCount > LARGE_GRAPH_EDGE_THRESHOLD) {
    const confirmed = await showConfirm(
      `This graph is very large (${nodeCount.toLocaleString()} nodes, ${edgeCount.toLocaleString()} relationships) ` +
        `and may cause performance issues or browser slowdown. Do you want to render it anyway?`,
      { title: "Large Graph Warning", confirmText: "Render Anyway" }
    );
    if (!confirmed) {
      // Update stats to show what was skipped
      const statsEl = document.getElementById("graph-stats");
      if (statsEl) {
        statsEl.textContent = `Skipped: ${nodeCount.toLocaleString()} nodes, ${edgeCount.toLocaleString()} relationships`;
      }
      return;
    }
  }

  // Clean up existing renderer and magnifier
  destroyMagnifier();
  if (renderer) {
    renderer.destroy();
    renderer = null;
  }

  // Clear any placeholder content
  container.innerHTML = "";

  // Clear previous collapse state
  clearCollapseState();

  // Load and layout the graph
  // Use lattice layout for graphs with no relationships (e.g., stale objects) to avoid label collision
  const graph = loadGraph(data);
  const layoutToUse = edgeCount === 0 ? "lattice" : currentLayout;
  await applyLayoutAsync(graph, layoutToUse);

  // Create renderer
  renderer = createRenderer({
    container,
    graph,
    theme: getCurrentTheme(),
    onNodeClick: (nodeId, attrs) => {
      updateDetailPanel(nodeId, attrs);
      renderer?.selectNode(nodeId);
    },
    onEdgeClick: (edgeId, attrs, source, target) => {
      const sourceLabel = graph.getNodeAttribute(source, "label") ?? source;
      const targetLabel = graph.getNodeAttribute(target, "label") ?? target;
      updateDetailPanelForEdge(edgeId, attrs, source, target, sourceLabel, targetLabel);
      renderer?.clearSelection();
    },
    onBackgroundClick: () => {
      updateDetailPanel(null, null);
      renderer?.clearSelection();
    },
    onNodeHover: () => {
      // Could update a tooltip here
    },
  });

  // Force a refresh to ensure sigma computes bounds from current positions,
  // then fit camera to show all nodes with padding (no animation on initial load)
  renderer.refresh();
  renderer.resetCamera(false);

  // Update stats display
  const stats = getGraphStats(graph);
  const statsEl = document.getElementById("graph-stats");
  if (statsEl) {
    statsEl.textContent = `${stats.nodeCount} nodes, ${stats.edgeCount} relationships`;
  }
}

/** Set the layout type and re-layout */
export async function setLayout(layout: LayoutType): Promise<void> {
  currentLayout = layout;
  await relayoutGraph();
  updateLayoutIndicator();
}

/** Cycle through available layouts and return the new layout name */
export async function cycleLayout(): Promise<string> {
  const layouts: LayoutType[] = ["force", "hierarchical", "grid", "circular", "lattice"];
  const currentIndex = layouts.indexOf(currentLayout);
  const nextIndex = (currentIndex + 1) % layouts.length;
  currentLayout = layouts[nextIndex]!;
  await relayoutGraph();
  updateLayoutIndicator();
  return currentLayout.charAt(0).toUpperCase() + currentLayout.slice(1);
}

/** Update UI to show current layout */
export function updateLayoutIndicator(): void {
  // Update radio-style menu checkmarks
  for (const el of document.querySelectorAll("[data-action^='layout-']")) {
    const action = el.getAttribute("data-action");
    const isActive =
      (action === "layout-force" && currentLayout === "force") ||
      (action === "layout-hierarchical" && currentLayout === "hierarchical") ||
      (action === "layout-grid" && currentLayout === "grid") ||
      (action === "layout-circular" && currentLayout === "circular") ||
      (action === "layout-lattice" && currentLayout === "lattice");
    el.setAttribute("data-checked", isActive ? "true" : "false");
  }
}

/** Re-run layout algorithm with current layout type */
export async function relayoutGraph(): Promise<void> {
  if (!renderer) return;

  const graph = renderer.sigma.getGraph();
  await applyLayoutAsync(graph, currentLayout);
  renderer.refresh();
  renderer.resetCamera();
}

/** Get the current renderer */
export function getRenderer(): ADGraphRenderer | null {
  return renderer;
}

/** Toggle label visibility mode and refresh the graph */
export function toggleLabelVisibility(): string {
  cycleLabelVisibility();
  const modeName = getLabelVisibilityName();

  // Refresh the renderer to apply the new label mode
  if (renderer) {
    renderer.refresh();
  }

  return modeName;
}

/** Generate a demo graph for testing - curated to show multi-relationship support */
function generateDemoGraph(_nodeCount: number): RawADGraph {
  const nodes: RawADGraph["nodes"] = [];
  const relationships: RawADGraph["relationships"] = [];

  // Domain
  nodes.push({
    id: "domain",
    name: "CORP.LOCAL",
    type: "Domain",
    properties: {
      objectid: "S-1-5-21-1234567890-1234567890-1234567890",
      distinguishedname: "DC=corp,DC=local",
    },
  });

  // OUs
  nodes.push({ id: "ou-it", name: "IT Department", type: "OU" });
  nodes.push({ id: "ou-hr", name: "HR Department", type: "OU" });

  // Groups
  nodes.push({ id: "grp-admins", name: "Domain Admins", type: "Group" });
  nodes.push({ id: "grp-it", name: "IT Staff", type: "Group" });
  nodes.push({ id: "grp-hr", name: "HR Staff", type: "Group" });
  nodes.push({ id: "grp-rdp", name: "RDP Users", type: "Group" });

  // Users
  nodes.push({ id: "user-alice", name: "alice", type: "User" });
  nodes.push({ id: "user-bob", name: "bob", type: "User" });
  nodes.push({ id: "user-carol", name: "carol", type: "User" });
  nodes.push({ id: "user-dave", name: "dave", type: "User" });
  nodes.push({ id: "user-eve", name: "eve", type: "User" });

  // Computers
  nodes.push({ id: "comp-dc01", name: "DC01", type: "Computer" });
  nodes.push({ id: "comp-srv01", name: "SRV01", type: "Computer" });
  nodes.push({ id: "comp-ws01", name: "WS01", type: "Computer" });
  nodes.push({ id: "comp-ws02", name: "WS02", type: "Computer" });

  // Structure relationships
  relationships.push({ source: "ou-it", target: "domain", type: "Contains" });
  relationships.push({ source: "ou-hr", target: "domain", type: "Contains" });

  // Group memberships
  relationships.push({ source: "user-alice", target: "grp-admins", type: "MemberOf" });
  relationships.push({ source: "user-alice", target: "grp-it", type: "MemberOf" });
  relationships.push({ source: "user-bob", target: "grp-it", type: "MemberOf" });
  relationships.push({ source: "user-bob", target: "grp-rdp", type: "MemberOf" });
  relationships.push({ source: "user-carol", target: "grp-hr", type: "MemberOf" });
  relationships.push({ source: "user-dave", target: "grp-hr", type: "MemberOf" });
  relationships.push({ source: "user-eve", target: "grp-rdp", type: "MemberOf" });
  relationships.push({ source: "grp-it", target: "grp-rdp", type: "MemberOf" });

  // Sessions
  relationships.push({ source: "user-alice", target: "comp-dc01", type: "HasSession" });
  relationships.push({ source: "user-bob", target: "comp-srv01", type: "HasSession" });
  relationships.push({ source: "user-carol", target: "comp-ws01", type: "HasSession" });
  relationships.push({ source: "user-dave", target: "comp-ws02", type: "HasSession" });

  // MULTI-EDGES: Multiple different relationships between same nodes
  // alice has multiple permissions on DC01
  relationships.push({ source: "user-alice", target: "comp-dc01", type: "AdminTo" });
  relationships.push({ source: "user-alice", target: "comp-dc01", type: "CanRDP" });

  // bob has multiple permissions on SRV01
  relationships.push({ source: "user-bob", target: "comp-srv01", type: "AdminTo" });
  relationships.push({ source: "user-bob", target: "comp-srv01", type: "CanRDP" });
  relationships.push({ source: "user-bob", target: "comp-srv01", type: "GenericAll" });

  // grp-admins has multiple permissions on domain
  relationships.push({ source: "grp-admins", target: "domain", type: "GenericAll" });
  relationships.push({ source: "grp-admins", target: "domain", type: "WriteDacl" });
  relationships.push({ source: "grp-admins", target: "domain", type: "DCSync" });

  // RDP access
  relationships.push({ source: "grp-rdp", target: "comp-ws01", type: "CanRDP" });
  relationships.push({ source: "grp-rdp", target: "comp-ws02", type: "CanRDP" });
  relationships.push({ source: "grp-rdp", target: "comp-srv01", type: "CanRDP" });

  return { nodes, relationships };
}
