/**
 * Layout algorithms for AD graph positioning.
 *
 * Supports:
 * - ForceAtlas2: Force-directed layout, good for exploring relationships
 * - Hierarchical: Left-to-right layered layout, edges flow from sources to targets
 */

import forceAtlas2 from "graphology-layout-forceatlas2";
import type { ADGraphType } from "./ADGraph";

/** Available layout algorithms */
export type LayoutType = "force" | "hierarchical";

export interface LayoutOptions {
  /** Layout algorithm to use */
  type?: LayoutType;
  /** Number of iterations to run (for force layout) */
  iterations?: number;
  /** Settings for ForceAtlas2 */
  settings?: ForceAtlas2Settings;
  /** Settings for hierarchical layout */
  hierarchical?: HierarchicalSettings;
}

export interface HierarchicalSettings {
  /** Horizontal spacing between layers */
  layerSpacing?: number;
  /** Vertical spacing between nodes in the same layer */
  nodeSpacing?: number;
  /** Direction of the layout */
  direction?: "left-to-right" | "top-to-bottom";
}

export interface ForceAtlas2Settings {
  /** Attraction strength */
  gravity?: number;
  /** Scaling factor */
  scalingRatio?: number;
  /** Slow down factor */
  slowDown?: number;
  /** Prevent node overlap */
  barnesHutOptimize?: boolean;
  /** Theta for Barnes-Hut optimization */
  barnesHutTheta?: number;
  /** Adjust speed based on graph size */
  adjustSizes?: boolean;
  /** Edge weight influence */
  edgeWeightInfluence?: number;
  /** LinLog mode (better for clusters) */
  linLogMode?: boolean;
  /** Strong gravity mode */
  strongGravityMode?: boolean;
}

/** Default layout settings optimized for AD graphs */
const DEFAULT_FORCE_SETTINGS: ForceAtlas2Settings = {
  gravity: 1,
  scalingRatio: 2,
  slowDown: 1,
  barnesHutOptimize: true,
  barnesHutTheta: 0.5,
  adjustSizes: false,
  edgeWeightInfluence: 1,
  linLogMode: false,
  strongGravityMode: false,
};

const DEFAULT_HIERARCHICAL_SETTINGS: HierarchicalSettings = {
  layerSpacing: 200,
  nodeSpacing: 80,
  direction: "left-to-right",
};

/** Default iterations based on graph size */
function getDefaultIterations(nodeCount: number): number {
  if (nodeCount < 100) return 100;
  if (nodeCount < 1000) return 50;
  if (nodeCount < 5000) return 30;
  return 20; // Large graphs: fewer iterations, rely on Barnes-Hut
}

/**
 * Apply layout to the graph.
 *
 * This modifies node positions in place.
 */
export function applyLayout(graph: ADGraphType, options: LayoutOptions = {}): void {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const layoutType = options.type ?? "force";

  if (layoutType === "hierarchical") {
    applyHierarchicalLayout(graph, options.hierarchical);
  } else {
    applyForceLayout(graph, options);
  }
}

/** Apply ForceAtlas2 force-directed layout */
function applyForceLayout(graph: ADGraphType, options: LayoutOptions): void {
  const nodeCount = graph.order;
  const iterations = options.iterations ?? getDefaultIterations(nodeCount);
  const settings = { ...DEFAULT_FORCE_SETTINGS, ...options.settings };

  forceAtlas2.assign(graph, {
    iterations,
    settings,
  });
}

/**
 * Apply hierarchical layout with edges flowing left-to-right.
 *
 * Computes layers based on longest path from source nodes (nodes with no incoming edges).
 * Handles cycles by using the first-visit layer assignment.
 */
function applyHierarchicalLayout(graph: ADGraphType, options: HierarchicalSettings = {}): void {
  const settings = { ...DEFAULT_HIERARCHICAL_SETTINGS, ...options };
  const { layerSpacing, nodeSpacing, direction } = settings;

  // Step 1: Compute layer for each node (longest path from roots)
  const layers = computeNodeLayers(graph);

  // Step 2: Group nodes by layer
  const layerGroups = new Map<number, string[]>();
  for (const [nodeId, layer] of layers.entries()) {
    const group = layerGroups.get(layer) ?? [];
    group.push(nodeId);
    layerGroups.set(layer, group);
  }

  // Step 3: Assign positions
  for (const [layer, nodes] of layerGroups.entries()) {
    // Sort nodes in each layer for consistent ordering (by out-degree, then by id)
    nodes.sort((a, b) => {
      const degDiff = graph.outDegree(b) - graph.outDegree(a);
      return degDiff !== 0 ? degDiff : a.localeCompare(b);
    });

    const layerHeight = nodes.length * nodeSpacing!;
    const startY = -layerHeight / 2;

    for (let i = 0; i < nodes.length; i++) {
      const nodeId = nodes[i];
      if (direction === "left-to-right") {
        graph.setNodeAttribute(nodeId, "x", layer * layerSpacing!);
        graph.setNodeAttribute(nodeId, "y", startY + i * nodeSpacing!);
      } else {
        // top-to-bottom
        graph.setNodeAttribute(nodeId, "x", startY + i * nodeSpacing!);
        graph.setNodeAttribute(nodeId, "y", layer * layerSpacing!);
      }
    }
  }
}

/**
 * Compute the layer (depth) for each node based on longest path from source nodes.
 * Source nodes are those with no incoming edges, or all nodes if the graph is cyclic.
 */
function computeNodeLayers(graph: ADGraphType): Map<string, number> {
  const layers = new Map<string, number>();

  // Find source nodes (no incoming edges)
  const sources: string[] = [];
  graph.forEachNode((nodeId) => {
    if (graph.inDegree(nodeId) === 0) {
      sources.push(nodeId);
    }
  });

  // If no sources (fully cyclic), use nodes with minimum in-degree
  if (sources.length === 0) {
    let minInDegree = Infinity;
    graph.forEachNode((nodeId) => {
      const inDeg = graph.inDegree(nodeId);
      if (inDeg < minInDegree) {
        minInDegree = inDeg;
        sources.length = 0;
        sources.push(nodeId);
      } else if (inDeg === minInDegree) {
        sources.push(nodeId);
      }
    });
  }

  // BFS to compute longest path (layer = max layer of predecessors + 1)
  // Use iterative approach to handle cycles
  const incomingCount = new Map<string, number>();
  const maxPredLayer = new Map<string, number>();

  // Initialize
  graph.forEachNode((nodeId) => {
    incomingCount.set(nodeId, graph.inDegree(nodeId));
    maxPredLayer.set(nodeId, -1);
  });

  // Start with sources at layer 0
  const queue: string[] = [];
  for (const source of sources) {
    layers.set(source, 0);
    queue.push(source);
  }

  // Process nodes in topological-ish order
  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layers.get(current) ?? 0;

    graph.forEachOutNeighbor(current, (neighbor) => {
      const prevMax = maxPredLayer.get(neighbor) ?? -1;
      maxPredLayer.set(neighbor, Math.max(prevMax, currentLayer));

      // Decrement incoming count
      const count = (incomingCount.get(neighbor) ?? 1) - 1;
      incomingCount.set(neighbor, count);

      // If all predecessors processed (or first visit for cycles)
      if (count <= 0 || !layers.has(neighbor)) {
        const newLayer = (maxPredLayer.get(neighbor) ?? -1) + 1;
        if (!layers.has(neighbor) || newLayer > layers.get(neighbor)!) {
          layers.set(neighbor, newLayer);
          queue.push(neighbor);
        }
      }
    });
  }

  // Handle any remaining unvisited nodes (isolated or in unprocessed cycles)
  graph.forEachNode((nodeId) => {
    if (!layers.has(nodeId)) {
      layers.set(nodeId, 0);
    }
  });

  return layers;
}

/**
 * Apply layout with progress callback.
 *
 * Runs layout in chunks to allow UI updates.
 * Only applicable for force layout; hierarchical is fast enough to run synchronously.
 */
export async function applyLayoutAsync(
  graph: ADGraphType,
  options: LayoutOptions = {},
  onProgress?: (progress: number) => void
): Promise<void> {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const layoutType = options.type ?? "force";

  // Hierarchical layout is fast, run synchronously
  if (layoutType === "hierarchical") {
    applyHierarchicalLayout(graph, options.hierarchical);
    if (onProgress) onProgress(1);
    return;
  }

  // Force layout: run in chunks
  const totalIterations = options.iterations ?? getDefaultIterations(nodeCount);
  const settings = { ...DEFAULT_FORCE_SETTINGS, ...options.settings };

  const chunkSize = 10;
  let completed = 0;

  while (completed < totalIterations) {
    const remaining = totalIterations - completed;
    const iterations = Math.min(chunkSize, remaining);

    forceAtlas2.assign(graph, {
      iterations,
      settings,
    });

    completed += iterations;

    if (onProgress) {
      onProgress(completed / totalIterations);
    }

    // Yield to allow UI updates
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

/**
 * Scale and center the graph positions.
 *
 * Useful after layout to fit the graph in a specific viewport.
 */
export function normalizePositions(graph: ADGraphType, width = 1000, height = 1000, padding = 50): void {
  if (graph.order === 0) return;

  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;

  // Find bounds
  graph.forEachNode((_, attrs) => {
    minX = Math.min(minX, attrs.x);
    maxX = Math.max(maxX, attrs.x);
    minY = Math.min(minY, attrs.y);
    maxY = Math.max(maxY, attrs.y);
  });

  const currentWidth = maxX - minX || 1;
  const currentHeight = maxY - minY || 1;

  const targetWidth = width - padding * 2;
  const targetHeight = height - padding * 2;

  const scale = Math.min(targetWidth / currentWidth, targetHeight / currentHeight);

  const offsetX = padding + (targetWidth - currentWidth * scale) / 2 - minX * scale;
  const offsetY = padding + (targetHeight - currentHeight * scale) / 2 - minY * scale;

  // Apply transformation
  graph.forEachNode((nodeId, attrs) => {
    graph.setNodeAttribute(nodeId, "x", attrs.x * scale + offsetX);
    graph.setNodeAttribute(nodeId, "y", attrs.y * scale + offsetY);
  });
}
