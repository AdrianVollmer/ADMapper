/**
 * Graph View Component
 *
 * Integrates the Sigma.js graph renderer with the application UI.
 */

import { loadGraph, createRenderer, applyLayout, getGraphStats } from "../graph";
import type { ADGraphRenderer } from "../graph";
import type { RawADGraph } from "../graph/types";
import { updateDetailPanel } from "./sidebars";

let renderer: ADGraphRenderer | null = null;

/** Initialize the graph view */
export function initGraph(): void {
  const container = document.getElementById("graph-canvas");
  if (!container) return;

  // Load demo graph for now
  const demoGraph = generateDemoGraph(200);
  loadGraphData(demoGraph);

  // Handle toolbar actions
  document.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    const button = target.closest("[data-action]") as HTMLElement;
    if (!button) return;

    const action = button.getAttribute("data-action");
    handleGraphAction(action);
  });
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

  // Load and layout the graph
  const graph = loadGraph(data);
  applyLayout(graph);

  // Create renderer
  renderer = createRenderer({
    container,
    graph,
    theme: "dark",
    onNodeClick: (nodeId, attrs) => {
      updateDetailPanel(nodeId, attrs);
      renderer?.selectNode(nodeId, true);
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

/** Handle graph-related actions */
function handleGraphAction(action: string | null): void {
  if (!renderer || !action) return;

  switch (action) {
    case "zoom-in":
      renderer.sigma.getCamera().animatedZoom({ duration: 200 });
      break;

    case "zoom-out":
      renderer.sigma.getCamera().animatedUnzoom({ duration: 200 });
      break;

    case "zoom-reset":
    case "fit-graph":
      renderer.resetCamera();
      break;

    case "layout-graph":
      relayoutGraph();
      break;
  }
}

/** Re-run layout algorithm */
function relayoutGraph(): void {
  if (!renderer) return;

  const graph = renderer.sigma.getGraph();
  applyLayout(graph);
  renderer.refresh();
}

/** Get the current renderer */
export function getRenderer(): ADGraphRenderer | null {
  return renderer;
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
