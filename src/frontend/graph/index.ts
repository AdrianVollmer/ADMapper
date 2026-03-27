/**
 * AD Graph Visualization Module
 *
 * Provides Sigma.js-based graph rendering for BloodHound-style
 * Active Directory permission visualization.
 *
 * @example
 * ```ts
 * import { loadGraph, createRenderer, applyLayout } from "./graph";
 *
 * // Load graph from server data
 * const graph = loadGraph(serverData);
 *
 * // Apply force-directed layout
 * applyLayout(graph);
 *
 * // Create renderer
 * const renderer = createRenderer({
 *   container: "#graph-container",
 *   graph,
 *   onNodeClick: (id, attrs) => console.log("Clicked:", id, attrs),
 * });
 * ```
 */

// Types
export type {
  ADNodeType,
  ADEdgeType,
  ADNodeAttributes,
  ADEdgeAttributes,
  RawADNode,
  RawADEdge,
  RawADGraph,
} from "./types";

// Graph data structure
export {
  createGraph,
  loadGraph,
  addNode,
  addEdge,
  getNodesByType,
  getNeighbors,
  getReachableNodes,
  getGraphStats,
  clearGraph,
  exportGraph,
} from "./ADGraph";
export type { ADGraphType } from "./ADGraph";

// Renderer
export { createRenderer } from "./ADGraphRenderer";
export type { RendererOptions, ADGraphRenderer } from "./ADGraphRenderer";

// Layout
export {
  applyLayout,
  applyLayoutAsync,
  normalizeGraphPositions,
  mergeForceSettings,
  validateAndFixPositions,
  setUserForceSettings,
  getUserForceSettings,
} from "./layout";
export type {
  LayoutType,
  LayoutOptions,
  ForceAtlas2Settings,
  HierarchicalSettings,
  GridSettings,
  CircularSettings,
  UserForceSettings,
} from "./layout";

// Theme
export {
  NODE_COLORS,
  EDGE_COLORS,
  HIGHLIGHT_COLORS,
  DIM_COLORS,
  BACKGROUND_COLOR,
  getNodeColor,
  getEdgeColor,
} from "./theme";

// Collapse functionality
export {
  isNodeCollapsed,
  getHiddenChildCount,
  collapseNode,
  expandNode,
  toggleNodeCollapse,
  clearCollapseState,
  getNodeCollapseInfo,
} from "./collapse";

// Label visibility
export {
  getLabelVisibilityMode,
  setLabelVisibilityMode,
  cycleLabelVisibility,
  getLabelVisibilityName,
  getLabelParts,
} from "./label-visibility";
export type { LabelVisibilityMode, LabelParts } from "./label-visibility";
