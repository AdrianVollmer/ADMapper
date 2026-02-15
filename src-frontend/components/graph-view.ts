/**
 * Graph View Component
 *
 * Integrates the Sigma.js graph renderer with the application UI.
 */

import { loadGraph, createRenderer, applyLayout, getGraphStats } from "../graph";
import type { ADGraphRenderer } from "../graph";
import type { RawADGraph, ADNodeType, ADEdgeType } from "../graph/types";
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

/** Generate a demo graph for testing */
function generateDemoGraph(nodeCount: number): RawADGraph {
  const nodes: RawADGraph["nodes"] = [];
  const edges: RawADGraph["edges"] = [];

  const nodeTypes: ADNodeType[] = [
    "User", "User", "User", "User",
    "Group", "Group",
    "Computer",
    "Domain",
    "OU",
  ];

  const edgeTypes: ADEdgeType[] = [
    "MemberOf", "MemberOf", "MemberOf",
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
    properties: {
      objectid: "S-1-5-21-1234567890-1234567890-1234567890",
      distinguishedname: "DC=corp,DC=local",
    },
  });

  // Create other nodes
  for (let i = 1; i < nodeCount; i++) {
    const type = nodeTypes[i % nodeTypes.length] ?? "User";
    const prefix = type.toLowerCase();
    nodes.push({
      id: `${prefix}-${i}`,
      label: `${type} ${i}`,
      type,
      properties: {
        objectid: `S-1-5-21-1234567890-1234567890-${i}`,
      },
    });
  }

  // Create edges
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
