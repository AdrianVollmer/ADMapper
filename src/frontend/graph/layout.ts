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
 * Uses barycenter heuristic to position parents at the vertical center of their children.
 */
function applyHierarchicalLayout(graph: ADGraphType, options: HierarchicalSettings = {}): void {
  const settings = { ...DEFAULT_HIERARCHICAL_SETTINGS, ...options };
  const { layerSpacing, nodeSpacing, direction } = settings;

  // Step 1: Compute layer for each node (longest path from roots)
  const nodeLayers = computeNodeLayers(graph);

  // Step 2: Group nodes by layer
  const layerGroups = new Map<number, string[]>();
  let maxLayer = 0;
  for (const [nodeId, layer] of nodeLayers.entries()) {
    const group = layerGroups.get(layer) ?? [];
    group.push(nodeId);
    layerGroups.set(layer, group);
    maxLayer = Math.max(maxLayer, layer);
  }

  // Step 3: Initial ordering - sort by out-degree for first pass
  for (const nodes of layerGroups.values()) {
    nodes.sort((a, b) => {
      const degDiff = graph.outDegree(b) - graph.outDegree(a);
      return degDiff !== 0 ? degDiff : a.localeCompare(b);
    });
  }

  // Step 4: Barycenter iterations to minimize edge crossings
  // Position nodes at the barycenter (average position) of their neighbors
  const nodePositions = new Map<string, number>();

  // Initialize positions based on order in layer
  for (const [, nodes] of layerGroups.entries()) {
    for (let i = 0; i < nodes.length; i++) {
      nodePositions.set(nodes[i], i);
    }
  }

  // Run barycenter iterations
  const iterations = 4;
  for (let iter = 0; iter < iterations; iter++) {
    // Forward pass: position based on predecessors (incoming neighbors)
    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      reorderByBarycenter(graph, nodes, nodePositions, "in");
      // Update positions after reordering
      for (let i = 0; i < nodes.length; i++) {
        nodePositions.set(nodes[i], i);
      }
    }

    // Backward pass: position based on successors (outgoing neighbors)
    for (let layer = maxLayer - 1; layer >= 0; layer--) {
      const nodes = layerGroups.get(layer) ?? [];
      reorderByBarycenter(graph, nodes, nodePositions, "out");
      // Update positions after reordering
      for (let i = 0; i < nodes.length; i++) {
        nodePositions.set(nodes[i], i);
      }
    }
  }

  // Step 5: Assign initial coordinates based on order
  for (const [layer, nodes] of layerGroups.entries()) {
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

  // Step 6: Coordinate adjustment - position parents at center of gravity of children
  // Only forward pass: center each layer's nodes on their incoming neighbors (children)
  // We don't do backward pass because it would pull children toward parents,
  // disrupting spacing when multiple children share one parent.
  const coordAttr = direction === "left-to-right" ? "y" : "x";
  const coordIterations = 4;

  for (let iter = 0; iter < coordIterations; iter++) {
    // Forward pass only: adjust nodes to center on their predecessors
    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      adjustToNeighborCentroid(graph, nodes, coordAttr, "in", nodeSpacing!);
    }
  }
}

/**
 * Reorder nodes in a layer based on barycenter of their neighbors.
 * This positions nodes at the average position of their connected neighbors.
 */
function reorderByBarycenter(
  graph: ADGraphType,
  nodes: string[],
  positions: Map<string, number>,
  direction: "in" | "out"
): void {
  // Compute barycenter for each node
  const barycenters: { node: string; barycenter: number; hasNeighbors: boolean }[] = [];

  for (const node of nodes) {
    let sum = 0;
    let count = 0;

    const neighbors =
      direction === "in" ? Array.from(graph.inNeighborEntries(node)) : Array.from(graph.outNeighborEntries(node));

    for (const { neighbor } of neighbors) {
      const pos = positions.get(neighbor);
      if (pos !== undefined) {
        sum += pos;
        count++;
      }
    }

    if (count > 0) {
      barycenters.push({ node, barycenter: sum / count, hasNeighbors: true });
    } else {
      // Keep original position for nodes with no neighbors in that direction
      barycenters.push({ node, barycenter: positions.get(node) ?? 0, hasNeighbors: false });
    }
  }

  // Sort by barycenter, keeping nodes without neighbors in their relative positions
  barycenters.sort((a, b) => {
    // Nodes with neighbors should be positioned by their barycenter
    // Nodes without neighbors maintain relative order
    if (a.hasNeighbors && b.hasNeighbors) {
      return a.barycenter - b.barycenter;
    }
    if (a.hasNeighbors) return -1;
    if (b.hasNeighbors) return 1;
    return a.barycenter - b.barycenter;
  });

  // Update the nodes array in place
  nodes.length = 0;
  for (const { node } of barycenters) {
    nodes.push(node);
  }
}

/**
 * Adjust node coordinates to center them on their neighbors' centroid.
 * Places each node at its ideal position, then resolves overlaps symmetrically.
 */
function adjustToNeighborCentroid(
  graph: ADGraphType,
  nodes: string[],
  coordAttr: "x" | "y",
  direction: "in" | "out",
  minSpacing: number
): void {
  if (nodes.length === 0) return;

  // Compute ideal coordinate for each node (centroid of neighbors)
  const nodeData: { node: string; ideal: number }[] = [];

  for (const node of nodes) {
    const current = graph.getNodeAttribute(node, coordAttr) as number;
    const neighbors =
      direction === "in" ? Array.from(graph.inNeighborEntries(node)) : Array.from(graph.outNeighborEntries(node));

    if (neighbors.length > 0) {
      let sum = 0;
      for (const { neighbor } of neighbors) {
        sum += graph.getNodeAttribute(neighbor, coordAttr) as number;
      }
      const ideal = sum / neighbors.length;
      nodeData.push({ node, ideal });
    } else {
      nodeData.push({ node, ideal: current });
    }
  }

  // Sort by ideal position
  nodeData.sort((a, b) => a.ideal - b.ideal);

  // Place nodes at ideal positions, then resolve overlaps
  const positions = nodeData.map((d) => d.ideal);

  // Resolve overlaps: push nodes apart symmetrically when they're too close
  for (let iter = 0; iter < 10; iter++) {
    let changed = false;
    for (let i = 0; i < positions.length - 1; i++) {
      const gap = positions[i + 1] - positions[i];
      if (gap < minSpacing) {
        // Push apart symmetrically
        const overlap = minSpacing - gap;
        positions[i] -= overlap / 2;
        positions[i + 1] += overlap / 2;
        changed = true;
      }
    }
    if (!changed) break;
  }

  // Apply positions
  for (let i = 0; i < nodeData.length; i++) {
    graph.setNodeAttribute(nodeData[i].node, coordAttr, positions[i]);
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
 * Higher-depth nodes form outer rings, positioned at angular centroid of their children.
 */
function applyCircularLayout(graph: ADGraphType, options: CircularSettings = {}): void {
  const settings = { ...DEFAULT_CIRCULAR_SETTINGS, ...options };
  const { ringSpacing, minRadius } = settings;

  // Compute layers from sinks (reverse of hierarchical)
  const nodeLayers = computeSinkBasedLayers(graph);

  // Group nodes by layer
  const layerGroups = new Map<number, string[]>();
  let maxLayer = 0;
  for (const [nodeId, layer] of nodeLayers.entries()) {
    const group = layerGroups.get(layer) ?? [];
    group.push(nodeId);
    layerGroups.set(layer, group);
    maxLayer = Math.max(maxLayer, layer);
  }

  // Initial ordering - sort by degree for first pass
  for (const nodes of layerGroups.values()) {
    nodes.sort((a, b) => {
      const degDiff = graph.degree(b) - graph.degree(a);
      return degDiff !== 0 ? degDiff : a.localeCompare(b);
    });
  }

  // Check if there's exactly one sink (layer 0)
  const sinks = layerGroups.get(0) ?? [];
  const singleCenterNode = sinks.length === 1;

  // Assign initial angles to all nodes
  const nodeAngles = new Map<string, number>();

  if (singleCenterNode) {
    // Center node has no angle (it's at origin)
    nodeAngles.set(sinks[0], 0);

    // Assign initial angles to other layers
    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      assignAngles(nodes, nodeAngles);
    }
  } else {
    // Assign initial angles to all layers
    for (let layer = 0; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      assignAngles(nodes, nodeAngles);
    }
  }

  // Barycenter iterations: position outer nodes at angular centroid of their children
  const iterations = 4;
  for (let iter = 0; iter < iterations; iter++) {
    // Work from outer layers inward - position parents based on children
    for (let layer = maxLayer; layer >= (singleCenterNode ? 1 : 0); layer--) {
      const nodes = layerGroups.get(layer) ?? [];
      reorderByAngularBarycenter(graph, nodes, nodeAngles, nodeLayers);
      // Reassign angles after reordering
      assignAngles(nodes, nodeAngles);
    }
  }

  // Place nodes at final positions
  if (singleCenterNode) {
    graph.setNodeAttribute(sinks[0], "x", 0);
    graph.setNodeAttribute(sinks[0], "y", 0);

    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      if (nodes.length === 0) continue;
      const radius = minRadius! + (layer - 1) * ringSpacing!;
      placeNodesOnCircleWithAngles(graph, nodes, radius, nodeAngles);
    }
  } else {
    for (let layer = 0; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      if (nodes.length === 0) continue;
      const radius = minRadius! + layer * ringSpacing!;
      placeNodesOnCircleWithAngles(graph, nodes, radius, nodeAngles);
    }
  }
}

/**
 * Assign evenly distributed angles to nodes.
 */
function assignAngles(nodes: string[], nodeAngles: Map<string, number>): void {
  const angleStep = (2 * Math.PI) / nodes.length;
  const startAngle = -Math.PI / 2;

  for (let i = 0; i < nodes.length; i++) {
    nodeAngles.set(nodes[i], startAngle + i * angleStep);
  }
}

/**
 * Reorder nodes based on angular barycenter of their children (outgoing neighbors in inner layers).
 */
function reorderByAngularBarycenter(
  graph: ADGraphType,
  nodes: string[],
  angles: Map<string, number>,
  nodeLayers: Map<string, number>
): void {
  const currentLayer = nodeLayers.get(nodes[0]) ?? 0;

  // Compute angular barycenter for each node based on children (nodes in inner layers)
  const barycenters: { node: string; angle: number; hasChildren: boolean }[] = [];

  for (const node of nodes) {
    const childAngles: number[] = [];

    // Get outgoing neighbors that are in inner layers (closer to center)
    graph.forEachOutNeighbor(node, (neighbor) => {
      const neighborLayer = nodeLayers.get(neighbor) ?? 0;
      if (neighborLayer < currentLayer) {
        const angle = angles.get(neighbor);
        if (angle !== undefined) {
          childAngles.push(angle);
        }
      }
    });

    if (childAngles.length > 0) {
      // Compute circular mean angle
      const meanAngle = circularMean(childAngles);
      barycenters.push({ node, angle: meanAngle, hasChildren: true });
    } else {
      // Keep original angle for nodes without children
      barycenters.push({ node, angle: angles.get(node) ?? 0, hasChildren: false });
    }
  }

  // Sort by angle
  barycenters.sort((a, b) => {
    // Normalize angles to [0, 2π) for sorting
    const angleA = ((a.angle % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
    const angleB = ((b.angle % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
    return angleA - angleB;
  });

  // Update the nodes array in place
  nodes.length = 0;
  for (const { node } of barycenters) {
    nodes.push(node);
  }
}

/**
 * Compute circular mean of angles.
 */
function circularMean(angles: number[]): number {
  let sinSum = 0;
  let cosSum = 0;

  for (const angle of angles) {
    sinSum += Math.sin(angle);
    cosSum += Math.cos(angle);
  }

  return Math.atan2(sinSum / angles.length, cosSum / angles.length);
}

/**
 * Place nodes on a circle using precomputed angles.
 */
function placeNodesOnCircleWithAngles(
  graph: ADGraphType,
  nodes: string[],
  radius: number,
  angles: Map<string, number>
): void {
  for (const node of nodes) {
    const angle = angles.get(node) ?? 0;
    const x = radius * Math.cos(angle);
    const y = radius * Math.sin(angle);
    graph.setNodeAttribute(node, "x", x);
    graph.setNodeAttribute(node, "y", y);
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
