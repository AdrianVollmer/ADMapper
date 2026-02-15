/**
 * Demo/test script for AD graph visualization.
 *
 * Run with: npm run dev
 * Then open the browser to see the graph.
 */

import { loadGraph, createRenderer, applyLayout, getGraphStats } from "./index";
import type { RawADGraph } from "./types";

/** Generate a mock AD graph for testing */
function generateMockGraph(_nodeCount: number): RawADGraph {
  const nodes: RawADGraph["nodes"] = [];
  const edges: RawADGraph["edges"] = [];

  // Create a small, curated graph to demonstrate features including multi-edges

  // Domain
  nodes.push({ id: "domain", label: "CORP.LOCAL", type: "Domain" });

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

/** Initialize the demo */
export function initDemo(containerId: string, nodeCount = 100): void {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`Container #${containerId} not found`);
    return;
  }

  console.log(`Generating mock graph with ${nodeCount} nodes...`);
  const startGen = performance.now();
  const mockData = generateMockGraph(nodeCount);
  console.log(`Generated in ${(performance.now() - startGen).toFixed(1)}ms`);

  console.log("Loading graph...");
  const startLoad = performance.now();
  const graph = loadGraph(mockData);
  console.log(`Loaded in ${(performance.now() - startLoad).toFixed(1)}ms`);

  const stats = getGraphStats(graph);
  console.log("Graph stats:", stats);

  console.log("Applying layout...");
  const startLayout = performance.now();
  applyLayout(graph);
  console.log(`Layout complete in ${(performance.now() - startLayout).toFixed(1)}ms`);

  console.log("Creating renderer...");
  const startRender = performance.now();
  const renderer = createRenderer({
    container,
    graph,
    theme: "dark",
    onNodeClick: (nodeId, attrs) => {
      console.log("Node clicked:", nodeId, attrs);
      renderer.selectNode(nodeId, true);
      renderer.focusNode(nodeId);
    },
    onBackgroundClick: () => {
      console.log("Background clicked");
      renderer.clearSelection();
    },
    onNodeHover: (nodeId, attrs) => {
      if (nodeId) {
        console.log("Node hover:", nodeId, attrs?.nodeType);
      }
    },
  });
  console.log(`Renderer created in ${(performance.now() - startRender).toFixed(1)}ms`);

  // Expose for debugging
  (window as unknown as Record<string, unknown>).adGraph = { graph, renderer, stats };
  console.log("Debug: window.adGraph available");
}

// Auto-init if running in browser with demo container
if (typeof document !== "undefined") {
  document.addEventListener("DOMContentLoaded", () => {
    const container = document.getElementById("graph-demo");
    if (container) {
      initDemo("graph-demo");
    }
  });
}
