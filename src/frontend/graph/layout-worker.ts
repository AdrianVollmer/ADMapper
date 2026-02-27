/**
 * Web Worker for heavy layout computations.
 *
 * Runs dagre hierarchical layout off the main thread to prevent UI lockup.
 */

import dagre from "dagre";

// TypeScript: declare worker global scope
declare const self: Worker;

export interface LayoutWorkerInput {
  nodes: Array<{ id: string }>;
  edges: Array<{ source: string; target: string }>;
  settings: {
    layerSpacing: number;
    nodeSpacing: number;
    direction: "left-to-right" | "top-to-bottom";
  };
}

export interface LayoutWorkerOutput {
  positions: Array<{ nodeId: string; x: number; y: number }>;
}

self.onmessage = (event: MessageEvent<LayoutWorkerInput>) => {
  const { nodes, edges, settings } = event.data;

  // Create a dagre graph
  const g = new dagre.graphlib.Graph();

  // Configure the graph layout
  g.setGraph({
    rankdir: settings.direction === "left-to-right" ? "LR" : "TB",
    nodesep: settings.nodeSpacing,
    ranksep: settings.layerSpacing,
    marginx: 0,
    marginy: 0,
  });

  // Default edge label (required by dagre)
  g.setDefaultEdgeLabel(() => ({}));

  // Add nodes to dagre graph
  for (const node of nodes) {
    g.setNode(node.id, { width: 40, height: 40 });
  }

  // Add edges to dagre graph
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  // Run dagre layout
  dagre.layout(g);

  // Collect positions from dagre layout
  const positions: Array<{ nodeId: string; x: number; y: number }> = [];

  for (const nodeId of g.nodes()) {
    const node = g.node(nodeId);
    if (node) {
      positions.push({ nodeId, x: node.x, y: node.y });
    }
  }

  // Send result back to main thread
  const output: LayoutWorkerOutput = { positions };
  self.postMessage(output);
};
