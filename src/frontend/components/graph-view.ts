/**
 * Graph View Component
 *
 * Integrates the Sigma.js graph renderer with the application UI.
 */

import { loadGraph, createRenderer, applyLayout, getGraphStats } from "../graph";
import type { ADGraphRenderer, LayoutType } from "../graph";
import type { RawADGraph } from "../graph/types";
import { updateDetailPanel } from "./sidebars";
import { autoCollapseGraph, clearCollapseState } from "../graph/collapse";
import { dispatchAction } from "./actions";
import { cycleLabelVisibility, getLabelVisibilityName } from "../graph/label-visibility";

let renderer: ADGraphRenderer | null = null;
let currentLayout: LayoutType = "force";

/** Initialize the graph view */
export function initGraph(): void {
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

  // Handle toolbar actions - delegate to central action dispatcher
  document.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    const button = target.closest("[data-action]") as HTMLElement;
    if (!button) return;

    const action = button.getAttribute("data-action");
    if (action) {
      dispatchAction(action);
    }
  });
}

/** Update the graph view based on connection state */
export function updateGraphForConnectionState(connected: boolean, error?: string): void {
  // Don't interfere with dev mode demo data
  if (import.meta.env.DEV) return;

  // If we have a renderer, the user has already loaded data - don't replace it
  if (renderer) return;

  if (!connected) {
    showNoConnectionPlaceholder(error);
  } else {
    // Connected but no data loaded yet - show a different message
    showConnectedPlaceholder();
  }
}

/** Show placeholder when connected but no data loaded */
export function showConnectedPlaceholder(): void {
  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Clean up existing renderer
  if (renderer) {
    renderer.destroy();
    renderer = null;
  }

  // Create placeholder
  container.innerHTML = `
    <div class="flex flex-col items-center justify-center h-full text-gray-400">
      <svg class="w-16 h-16 mb-4 text-green-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <p class="text-lg mb-2 text-green-400">Database connected</p>
      <p class="text-sm text-gray-500 mb-4">Run a query or select one from the sidebar to visualize data</p>
      <button
        class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded transition-colors"
        data-action="run-query"
      >
        Run Query
      </button>
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

  // Clean up existing renderer
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

/** Escape HTML to prevent XSS */
function escapeHtml(text: string): string {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

/** Load graph data and render */
export function loadGraphData(data: RawADGraph): void {
  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Clean up existing renderer
  if (renderer) {
    renderer.destroy();
    renderer = null;
  }

  // Clear any placeholder content
  container.innerHTML = "";

  // Clear previous collapse state
  clearCollapseState();

  // Load and layout the graph
  const graph = loadGraph(data);
  applyLayout(graph);

  // Auto-collapse nodes with many children for large graphs
  autoCollapseGraph(graph);

  // Create renderer
  renderer = createRenderer({
    container,
    graph,
    theme: "dark",
    onNodeClick: (nodeId, attrs) => {
      updateDetailPanel(nodeId, attrs);
      renderer?.selectNode(nodeId);
    },
    onBackgroundClick: () => {
      updateDetailPanel(null, null);
      renderer?.clearSelection();
    },
    onNodeHover: () => {
      // Could update a tooltip here
    },
  });

  // Update stats display
  const stats = getGraphStats(graph);
  const statsEl = document.getElementById("graph-stats");
  if (statsEl) {
    statsEl.textContent = `${stats.nodeCount} nodes, ${stats.edgeCount} edges`;
  }
}

/** Set the layout type and re-layout */
export function setLayout(layout: LayoutType): void {
  currentLayout = layout;
  relayoutGraph();
  updateLayoutIndicator();
}

/** Update UI to show current layout */
function updateLayoutIndicator(): void {
  // Update radio-style menu checkmarks
  for (const el of document.querySelectorAll("[data-action^='layout-']")) {
    const action = el.getAttribute("data-action");
    const isActive =
      (action === "layout-force" && currentLayout === "force") ||
      (action === "layout-hierarchical" && currentLayout === "hierarchical");
    el.setAttribute("data-checked", isActive ? "true" : "false");
  }
}

/** Re-run layout algorithm with current layout type */
export function relayoutGraph(): void {
  if (!renderer) return;

  const graph = renderer.sigma.getGraph();
  applyLayout(graph, { type: currentLayout });
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

/** Generate a demo graph for testing - curated to show multi-edge support */
function generateDemoGraph(_nodeCount: number): RawADGraph {
  const nodes: RawADGraph["nodes"] = [];
  const edges: RawADGraph["edges"] = [];

  // Domain
  nodes.push({
    id: "domain",
    label: "CORP.LOCAL",
    type: "Domain",
    properties: {
      objectid: "S-1-5-21-1234567890-1234567890-1234567890",
      distinguishedname: "DC=corp,DC=local",
    },
  });

  // OUs
  nodes.push({ id: "ou-it", label: "IT Department", type: "OU" });
  nodes.push({ id: "ou-hr", label: "HR Department", type: "OU" });

  // Groups
  nodes.push({ id: "grp-admins", label: "Domain Admins", type: "Group" });
  nodes.push({ id: "grp-it", label: "IT Staff", type: "Group" });
  nodes.push({ id: "grp-hr", label: "HR Staff", type: "Group" });
  nodes.push({ id: "grp-rdp", label: "RDP Users", type: "Group" });

  // Users
  nodes.push({ id: "user-alice", label: "alice", type: "User" });
  nodes.push({ id: "user-bob", label: "bob", type: "User" });
  nodes.push({ id: "user-carol", label: "carol", type: "User" });
  nodes.push({ id: "user-dave", label: "dave", type: "User" });
  nodes.push({ id: "user-eve", label: "eve", type: "User" });

  // Computers
  nodes.push({ id: "comp-dc01", label: "DC01", type: "Computer" });
  nodes.push({ id: "comp-srv01", label: "SRV01", type: "Computer" });
  nodes.push({ id: "comp-ws01", label: "WS01", type: "Computer" });
  nodes.push({ id: "comp-ws02", label: "WS02", type: "Computer" });

  // Structure edges
  edges.push({ source: "ou-it", target: "domain", type: "Contains" });
  edges.push({ source: "ou-hr", target: "domain", type: "Contains" });

  // Group memberships
  edges.push({ source: "user-alice", target: "grp-admins", type: "MemberOf" });
  edges.push({ source: "user-alice", target: "grp-it", type: "MemberOf" });
  edges.push({ source: "user-bob", target: "grp-it", type: "MemberOf" });
  edges.push({ source: "user-bob", target: "grp-rdp", type: "MemberOf" });
  edges.push({ source: "user-carol", target: "grp-hr", type: "MemberOf" });
  edges.push({ source: "user-dave", target: "grp-hr", type: "MemberOf" });
  edges.push({ source: "user-eve", target: "grp-rdp", type: "MemberOf" });
  edges.push({ source: "grp-it", target: "grp-rdp", type: "MemberOf" });

  // Sessions
  edges.push({ source: "user-alice", target: "comp-dc01", type: "HasSession" });
  edges.push({ source: "user-bob", target: "comp-srv01", type: "HasSession" });
  edges.push({ source: "user-carol", target: "comp-ws01", type: "HasSession" });
  edges.push({ source: "user-dave", target: "comp-ws02", type: "HasSession" });

  // MULTI-EDGES: Multiple different relationships between same nodes
  // alice has multiple permissions on DC01
  edges.push({ source: "user-alice", target: "comp-dc01", type: "AdminTo" });
  edges.push({ source: "user-alice", target: "comp-dc01", type: "CanRDP" });

  // bob has multiple permissions on SRV01
  edges.push({ source: "user-bob", target: "comp-srv01", type: "AdminTo" });
  edges.push({ source: "user-bob", target: "comp-srv01", type: "CanRDP" });
  edges.push({ source: "user-bob", target: "comp-srv01", type: "GenericAll" });

  // grp-admins has multiple permissions on domain
  edges.push({ source: "grp-admins", target: "domain", type: "GenericAll" });
  edges.push({ source: "grp-admins", target: "domain", type: "WriteDacl" });
  edges.push({ source: "grp-admins", target: "domain", type: "DCSync" });

  // RDP access
  edges.push({ source: "grp-rdp", target: "comp-ws01", type: "CanRDP" });
  edges.push({ source: "grp-rdp", target: "comp-ws02", type: "CanRDP" });
  edges.push({ source: "grp-rdp", target: "comp-srv01", type: "CanRDP" });

  return { nodes, edges };
}
