/**
 * Circular layout algorithm for AD graph positioning.
 *
 * Arranges nodes in concentric circles based on depth from sink nodes.
 * Sinks (nodes with no outgoing relationships) are at the center or innermost ring.
 * Higher-depth nodes form outer rings, positioned at the angular centroid of their children.
 */

import type { ADGraphType } from "./ADGraph";

export interface CircularSettings {
  /** Spacing between concentric rings */
  ringSpacing?: number;
  /** Minimum radius for first ring (when center is empty) */
  minRadius?: number;
}

const DEFAULT_CIRCULAR_SETTINGS: CircularSettings = {
  ringSpacing: 150,
  minRadius: 100,
};

/**
 * Apply circular layout - arranges nodes in concentric circles based on depth.
 *
 * Sinks (nodes with no outgoing relationships) are at the center or innermost ring.
 * If there's exactly one sink, it's placed at the center.
 * Otherwise, sinks form the first ring around an empty center.
 * Higher-depth nodes form outer rings, positioned at angular centroid of their children.
 */
export function applyCircularLayout(graph: ADGraphType, options: CircularSettings = {}): void {
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
    nodeAngles.set(sinks[0]!, 0);

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
    nodeAngles.set(nodes[i]!, startAngle + i * angleStep);
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
  if (nodes.length === 0) return;
  const currentLayer = nodeLayers.get(nodes[0]!) ?? 0;

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
 * Compute layers based on distance from sink nodes (nodes with no outgoing relationships).
 * Sinks are at layer 0, nodes that connect directly to sinks are at layer 1, etc.
 */
function computeSinkBasedLayers(graph: ADGraphType): Map<string, number> {
  const layers = new Map<string, number>();

  // Find sink nodes (no outgoing relationships)
  const sinks: string[] = [];
  graph.forEachNode((nodeId) => {
    if (graph.outDegree(nodeId) === 0) {
      sinks.push(nodeId);
    }
  });

  // If no sinks (fully cyclic or all nodes have outgoing relationships), use nodes with minimum out-degree
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

  // BFS from sinks, traversing relationships in reverse
  const queue: string[] = [];
  for (const sink of sinks) {
    layers.set(sink, 0);
    queue.push(sink);
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layers.get(current)!;

    // Traverse incoming relationships (reverse direction)
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
