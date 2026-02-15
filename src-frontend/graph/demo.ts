/**
 * Demo/test script for AD graph visualization.
 *
 * Run with: npm run dev
 * Then open the browser to see the graph.
 */

import { loadGraph, createRenderer, applyLayout, getGraphStats } from "./index";
import type { RawADGraph, ADNodeType, ADEdgeType } from "./types";

/** Generate a mock AD graph for testing */
function generateMockGraph(nodeCount: number): RawADGraph {
  const nodes: RawADGraph["nodes"] = [];
  const edges: RawADGraph["edges"] = [];

  const nodeTypes: ADNodeType[] = [
    "User", "User", "User", "User", // Users are most common
    "Group", "Group",
    "Computer",
    "Domain",
    "OU",
  ];

  const edgeTypes: ADEdgeType[] = [
    "MemberOf", "MemberOf", "MemberOf", // Most common
    "HasSession",
    "AdminTo",
    "CanRDP",
    "GenericAll",
  ];

  // Create domain node
  nodes.push({
    id: "domain-0",
    label: "CORP.LOCAL",
    type: "Domain",
  });

  // Create other nodes
  for (let i = 1; i < nodeCount; i++) {
    const type = nodeTypes[i % nodeTypes.length] ?? "User";
    const prefix = type.toLowerCase();
    nodes.push({
      id: `${prefix}-${i}`,
      label: `${type} ${i}`,
      type,
    });
  }

  // Create edges (roughly 2x nodes for realistic density)
  const edgeCount = Math.floor(nodeCount * 2);
  const usedEdges = new Set<string>();

  for (let i = 0; i < edgeCount; i++) {
    const sourceIdx = Math.floor(Math.random() * nodeCount);
    const targetIdx = Math.floor(Math.random() * nodeCount);

    if (sourceIdx === targetIdx) continue;

    const source = nodes[sourceIdx]?.id;
    const target = nodes[targetIdx]?.id;
    if (!source || !target) continue;

    const type = edgeTypes[i % edgeTypes.length] ?? "MemberOf";
    const edgeKey = `${source}-${type}-${target}`;

    if (usedEdges.has(edgeKey)) continue;
    usedEdges.add(edgeKey);

    edges.push({ source, target, type });
  }

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
      // Get node count from URL param or default to 500
      const params = new URLSearchParams(window.location.search);
      const nodeCount = parseInt(params.get("nodes") ?? "500", 10);
      initDemo("graph-demo", nodeCount);
    }
  });
}
