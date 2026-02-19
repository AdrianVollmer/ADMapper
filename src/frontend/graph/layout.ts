/**
 * Layout algorithms for AD graph positioning.
 *
 * Supports:
 * - ForceAtlas2: Force-directed layout, good for exploring relationships
 * - Hierarchical: Left-to-right layered layout, edges flow from sources to targets
 * - Grid: Simple grid arrangement
 * - Circular: Concentric circles based on node depth, sinks at center
 */

import forceAtlas2 from "graphology-layout-forceatlas2";
import type { ADGraphType } from "./ADGraph";

/** Available layout algorithms */
export type LayoutType = "force" | "hierarchical" | "grid" | "circular";

export interface LayoutOptions {
  /** Layout algorithm to use */
  type?: LayoutType;
  /** Number of iterations to run (for force layout) */
  iterations?: number;
  /** Settings for ForceAtlas2 */
  settings?: ForceAtlas2Settings;
  /** Settings for hierarchical layout */
  hierarchical?: HierarchicalSettings;
  /** Settings for grid layout */
  grid?: GridSettings;
  /** Settings for circular layout */
  circular?: CircularSettings;
}

export interface HierarchicalSettings {
  /** Horizontal spacing between layers */
  layerSpacing?: number;
  /** Vertical spacing between nodes in the same layer */
  nodeSpacing?: number;
  /** Direction of the layout */
  direction?: "left-to-right" | "top-to-bottom";
}

export interface GridSettings {
  /** Horizontal spacing between nodes */
  columnSpacing?: number;
  /** Vertical spacing between nodes */
  rowSpacing?: number;
  /** Number of columns (if not set, computed from node count) */
  columns?: number;
}

export interface CircularSettings {
  /** Spacing between concentric rings */
  ringSpacing?: number;
  /** Minimum radius for first ring (when center is empty) */
  minRadius?: number;
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

const DEFAULT_GRID_SETTINGS: GridSettings = {
  columnSpacing: 150,
  rowSpacing: 150,
};

const DEFAULT_CIRCULAR_SETTINGS: CircularSettings = {
  ringSpacing: 150,
  minRadius: 100,
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

  switch (layoutType) {
    case "hierarchical":
      applyHierarchicalLayout(graph, options.hierarchical);
      break;
    case "grid":
      applyGridLayout(graph, options.grid);
      break;
    case "circular":
      applyCircularLayout(graph, options.circular);
      break;
    default:
      applyForceLayout(graph, options);
      break;
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
 * Apply grid layout - arranges nodes in a simple grid pattern.
 */
function applyGridLayout(graph: ADGraphType, options: GridSettings = {}): void {
  const settings = { ...DEFAULT_GRID_SETTINGS, ...options };
  const { columnSpacing, rowSpacing } = settings;

  // Get all nodes and sort for consistent ordering
  const nodes: string[] = [];
  graph.forEachNode((nodeId) => nodes.push(nodeId));
  nodes.sort((a, b) => {
    // Sort by type first, then by label
    const typeA = graph.getNodeAttribute(a, "nodeType") || "";
    const typeB = graph.getNodeAttribute(b, "nodeType") || "";
    if (typeA !== typeB) return typeA.localeCompare(typeB);
    const labelA = graph.getNodeAttribute(a, "label") || a;
    const labelB = graph.getNodeAttribute(b, "label") || b;
    return labelA.localeCompare(labelB);
  });

  // Compute grid dimensions
  const columns = settings.columns ?? Math.ceil(Math.sqrt(nodes.length));
  const rows = Math.ceil(nodes.length / columns);

  // Center the grid
  const gridWidth = (columns - 1) * columnSpacing!;
  const gridHeight = (rows - 1) * rowSpacing!;
  const startX = -gridWidth / 2;
  const startY = -gridHeight / 2;

  // Assign positions
  for (let i = 0; i < nodes.length; i++) {
    const col = i % columns;
    const row = Math.floor(i / columns);
    graph.setNodeAttribute(nodes[i], "x", startX + col * columnSpacing!);
    graph.setNodeAttribute(nodes[i], "y", startY + row * rowSpacing!);
  }
}

/**
 * Apply circular layout - arranges nodes in concentric circles based on depth.
 *
 * Sinks (nodes with no outgoing edges) are at the center or innermost ring.
 * If there's exactly one sink, it's placed at the center.
 * Otherwise, sinks form the first ring around an empty center.
 * Higher-depth nodes (those that feed into sinks) form outer rings.
 */
function applyCircularLayout(graph: ADGraphType, options: CircularSettings = {}): void {
  const settings = { ...DEFAULT_CIRCULAR_SETTINGS, ...options };
  const { ringSpacing, minRadius } = settings;

  // Compute layers from sinks (reverse of hierarchical)
  const layers = computeSinkBasedLayers(graph);

  // Group nodes by layer
  const layerGroups = new Map<number, string[]>();
  let maxLayer = 0;
  for (const [nodeId, layer] of layers.entries()) {
    const group = layerGroups.get(layer) ?? [];
    group.push(nodeId);
    layerGroups.set(layer, group);
    maxLayer = Math.max(maxLayer, layer);
  }

  // Sort nodes within each layer for consistent ordering
  for (const nodes of layerGroups.values()) {
    nodes.sort((a, b) => {
      const degDiff = graph.degree(b) - graph.degree(a);
      return degDiff !== 0 ? degDiff : a.localeCompare(b);
    });
  }

  // Check if there's exactly one sink (layer 0)
  const sinks = layerGroups.get(0) ?? [];
  const singleCenterNode = sinks.length === 1;

  // Place nodes on concentric circles
  if (singleCenterNode) {
    // Place the single sink at center
    graph.setNodeAttribute(sinks[0], "x", 0);
    graph.setNodeAttribute(sinks[0], "y", 0);

    // Place other layers in rings starting from minRadius
    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      if (nodes.length === 0) continue;

      const radius = minRadius! + (layer - 1) * ringSpacing!;
      placeNodesOnCircle(graph, nodes, radius);
    }
  } else {
    // No center node - sinks form the first ring
    for (let layer = 0; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      if (nodes.length === 0) continue;

      const radius = minRadius! + layer * ringSpacing!;
      placeNodesOnCircle(graph, nodes, radius);
    }
  }
}

/**
 * Place nodes evenly distributed on a circle.
 */
function placeNodesOnCircle(graph: ADGraphType, nodes: string[], radius: number): void {
  const angleStep = (2 * Math.PI) / nodes.length;
  // Start from top (-PI/2) and go clockwise
  const startAngle = -Math.PI / 2;

  for (let i = 0; i < nodes.length; i++) {
    const angle = startAngle + i * angleStep;
    const x = radius * Math.cos(angle);
    const y = radius * Math.sin(angle);
    graph.setNodeAttribute(nodes[i], "x", x);
    graph.setNodeAttribute(nodes[i], "y", y);
  }
}

/**
 * Compute layers based on distance from sink nodes (nodes with no outgoing edges).
 * Sinks are at layer 0, nodes that connect directly to sinks are at layer 1, etc.
 */
function computeSinkBasedLayers(graph: ADGraphType): Map<string, number> {
  const layers = new Map<string, number>();

  // Find sink nodes (no outgoing edges)
  const sinks: string[] = [];
  graph.forEachNode((nodeId) => {
    if (graph.outDegree(nodeId) === 0) {
      sinks.push(nodeId);
    }
  });

  // If no sinks (fully cyclic or all nodes have outgoing edges), use nodes with minimum out-degree
  if (sinks.length === 0) {
    let minOutDegree = Infinity;
    graph.forEachNode((nodeId) => {
      const outDeg = graph.outDegree(nodeId);
      if (outDeg < minOutDegree) {
        minOutDegree = outDeg;
        sinks.length = 0;
        sinks.push(nodeId);
      } else if (outDeg === minOutDegree) {
        sinks.push(nodeId);
      }
    });
  }

  // BFS from sinks, traversing edges in reverse
  const queue: string[] = [];
  for (const sink of sinks) {
    layers.set(sink, 0);
    queue.push(sink);
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layers.get(current)!;

    // Traverse incoming edges (reverse direction)
    graph.forEachInNeighbor(current, (neighbor) => {
      if (!layers.has(neighbor)) {
        layers.set(neighbor, currentLayer + 1);
        queue.push(neighbor);
      }
    });
  }

  // Handle any remaining unvisited nodes (isolated)
  graph.forEachNode((nodeId) => {
    if (!layers.has(nodeId)) {
      layers.set(nodeId, 0);
    }
  });

  return layers;
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
 * Only applicable for force layout; other layouts are fast enough to run synchronously.
 */
export async function applyLayoutAsync(
  graph: ADGraphType,
  options: LayoutOptions = {},
  onProgress?: (progress: number) => void
): Promise<void> {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const layoutType = options.type ?? "force";

  // Non-force layouts are fast, run synchronously
  if (layoutType === "hierarchical") {
    applyHierarchicalLayout(graph, options.hierarchical);
    if (onProgress) onProgress(1);
    return;
  }
  if (layoutType === "grid") {
    applyGridLayout(graph, options.grid);
    if (onProgress) onProgress(1);
    return;
  }
  if (layoutType === "circular") {
    applyCircularLayout(graph, options.circular);
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
