/**
 * ADGraphRenderer: Sigma.js renderer for AD graphs.
 *
 * Handles WebGL rendering, user interaction, and visual state management.
 */

import Sigma from "sigma";
import { EdgeTriangleProgram } from "sigma/rendering";
import { createNodeImageProgram } from "@sigma/node-image";
import { createEdgeCurveProgram } from "@sigma/edge-curve";
import type { ADGraphType } from "./ADGraph";
import type { ADNodeAttributes, ADEdgeAttributes } from "./types";
import {
  HIGHLIGHT_COLORS,
  HIGHLIGHT_SIZE_MULTIPLIER,
  BACKGROUND_COLOR,
  LABEL_COLOR,
  DEFAULT_EDGE_COLOR,
} from "./theme";

export interface RendererOptions {
  /** Container element or selector */
  container: HTMLElement | string;
  /** Initial graph to render */
  graph: ADGraphType;
  /** Theme: light or dark */
  theme?: "light" | "dark";
  /** Enable hover highlighting */
  enableHover?: boolean;
  /** Callback when a node is clicked */
  onNodeClick?: (nodeId: string, attrs: ADNodeAttributes) => void;
  /** Callback when the background is clicked */
  onBackgroundClick?: () => void;
  /** Callback when a node is hovered */
  onNodeHover?: (nodeId: string | null, attrs: ADNodeAttributes | null) => void;
}

export interface ADGraphRenderer {
  /** The underlying Sigma instance */
  sigma: Sigma<ADNodeAttributes, ADEdgeAttributes>;
  /** Currently hovered node */
  hoveredNode: string | null;
  /** Currently selected nodes */
  selectedNodes: Set<string>;
  /** Destroy the renderer and clean up resources */
  destroy: () => void;
  /** Refresh the rendering */
  refresh: () => void;
  /** Focus camera on a specific node */
  focusNode: (nodeId: string, animate?: boolean) => void;
  /** Reset camera to show all nodes */
  resetCamera: (animate?: boolean) => void;
  /** Select a node */
  selectNode: (nodeId: string) => void;
  /** Clear selection */
  clearSelection: () => void;
  /** Set theme */
  setTheme: (theme: "light" | "dark") => void;
  /** Highlight a path (list of node IDs) */
  highlightPath: (path: string[]) => void;
}

/** Create an AD graph renderer */
export function createRenderer(options: RendererOptions): ADGraphRenderer {
  const { container, graph, theme = "dark", enableHover = true, onNodeClick, onBackgroundClick, onNodeHover } = options;

  // Resolve container element
  const containerEl = typeof container === "string" ? document.querySelector<HTMLElement>(container) : container;

  if (!containerEl) {
    throw new Error(`Container not found: ${container}`);
  }

  // State
  let hoveredNode: string | null = null;
  let hoveredReachableEdges: Set<string> = new Set();
  const selectedNodes = new Set<string>();
  const highlightedPath = new Set<string>();
  const highlightedPathEdges = new Set<string>();
  let currentTheme = theme;
  let draggedNode: string | null = null;

  /** Compute all edges reachable via outgoing edges (transitive) */
  function computeReachableEdges(startNode: string): Set<string> {
    const visitedNodes = new Set<string>();
    const reachableEdges = new Set<string>();
    const queue = [startNode];

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (visitedNodes.has(current)) continue;
      visitedNodes.add(current);

      // Follow outgoing edges
      graph.forEachOutEdge(current, (edge, _attrs, _source, target) => {
        reachableEdges.add(edge);
        if (!visitedNodes.has(target)) {
          queue.push(target);
        }
      });
    }

    return reachableEdges;
  }

  // Custom label renderer: draws label below node, centered
  function drawLabel(
    context: CanvasRenderingContext2D,
    data: { label: string | null; x: number; y: number; size: number; color: string },
    settings: { labelSize: number; labelWeight: string; labelColor: { color?: string } }
  ): void {
    const label = data.label;
    if (!label) return;

    const size = settings.labelSize;
    const font = `${settings.labelWeight} ${size}px sans-serif`;
    const color = settings.labelColor.color ?? LABEL_COLOR[currentTheme];

    context.font = font;
    context.fillStyle = color;
    context.textAlign = "center";
    context.textBaseline = "top";

    // Position below the node with a small gap
    const yOffset = data.size + 4;
    context.fillText(label, data.x, data.y + yOffset);
  }

  // Custom hover renderer: draws a glow effect behind the hovered node
  // Selected nodes get a red, tighter, more intense glow
  function drawNodeHover(
    context: CanvasRenderingContext2D,
    data: { x: number; y: number; size: number; color: string },
    _settings: unknown,
    nodeId?: string
  ): void {
    const isSelected = nodeId ? selectedNodes.has(nodeId) : false;

    if (isSelected) {
      // Selected: red, tight, intense glow
      const glowRadius = data.size * 1.8;
      const gradient = context.createRadialGradient(data.x, data.y, data.size * 0.8, data.x, data.y, glowRadius);
      gradient.addColorStop(0, "rgba(255, 50, 50, 1)");
      gradient.addColorStop(0.6, "rgba(255, 50, 50, 0.6)");
      gradient.addColorStop(1, "rgba(255, 50, 50, 0)");

      context.beginPath();
      context.arc(data.x, data.y, glowRadius, 0, Math.PI * 2);
      context.fillStyle = gradient;
      context.fill();
    } else {
      // Hovered: yellow, softer glow
      const glowRadius = data.size * 2;
      const gradient = context.createRadialGradient(data.x, data.y, data.size * 0.5, data.x, data.y, glowRadius);
      gradient.addColorStop(0, "rgba(255, 247, 0, 0.6)");
      gradient.addColorStop(0.5, "rgba(255, 247, 0, 0.2)");
      gradient.addColorStop(1, "rgba(255, 247, 0, 0)");

      context.beginPath();
      context.arc(data.x, data.y, glowRadius, 0, Math.PI * 2);
      context.fillStyle = gradient;
      context.fill();
    }
  }

  // Create node image program for rendering icons with high-quality settings
  // "background" mode draws the node color as a circle behind the icon
  const NodeImageProgram = createNodeImageProgram({
    size: { mode: "force", value: 512 }, // Higher resolution for crisp icons
    drawingMode: "background",
    colorAttribute: "color",
    imageAttribute: "image",
    padding: 0.12, // Slightly less padding for better icon visibility
    keepWithinCircle: true,
  });

  // Create curved edge program with smooth arrows
  const CurvedArrowProgram = createEdgeCurveProgram({
    arrowHead: {
      extremity: "target",
      lengthToThicknessRatio: 3.5,
      widenessToThicknessRatio: 3,
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as any;

  // Create Sigma instance with high-quality rendering settings
  const sigma = new Sigma(graph, containerEl, {
    allowInvalidContainer: false,
    renderLabels: true,
    renderEdgeLabels: true,
    labelDensity: 0.08,
    labelGridCellSize: 80,
    labelRenderedSizeThreshold: 5,
    zIndex: true,
    defaultNodeColor: "#adb5bd",
    defaultEdgeColor: "#6c757d",
    labelColor: { color: LABEL_COLOR[theme] },
    defaultDrawNodeLabel: drawLabel,
    defaultDrawNodeHover: drawNodeHover,
    defaultNodeType: "image",
    nodeProgramClasses: {
      image: NodeImageProgram,
    },
    // Use curved arrows for better visual appearance
    defaultEdgeType: "curvedArrow",
    edgeProgramClasses: {
      triangle: EdgeTriangleProgram,
      curvedArrow: CurvedArrowProgram,
    },
    // Better performance settings
    minCameraRatio: 0.01,
    maxCameraRatio: 10,
    // Improved edge rendering
    enableEdgeEvents: false, // Disable for performance, we don't need edge clicks
    // Smooth camera transitions
    stagePadding: 30,

    // Node reducer: bring hovered/selected/path nodes to front
    nodeReducer: (nodeId, data) => {
      const res: Record<string, unknown> = { ...data };

      // Path nodes get highlighted
      if (highlightedPath.has(nodeId)) {
        res.zIndex = 2;
        res.highlighted = true;
      }

      // Selected nodes get higher z-index and will show stronger glow via drawNodeHover
      if (selectedNodes.has(nodeId)) {
        res.zIndex = 2;
        res.highlighted = true;
      }

      // Bring hovered node to front
      if (nodeId === hoveredNode) {
        res.zIndex = 3;
      }

      return res;
    },

    // Edge reducer: highlight path edges, transitive edges on hover
    edgeReducer: (edge, data) => {
      const res: Record<string, unknown> = { ...data };

      // Path edges get special highlight
      if (highlightedPathEdges.has(edge)) {
        res.color = "#22c55e"; // Green for path
        res.size = ((data.size as number | undefined) ?? 3) * 1.5;
        res.zIndex = 2;
      } else if (hoveredNode && hoveredReachableEdges.has(edge)) {
        // On hover: highlight edges reachable from hovered node
        res.color = HIGHLIGHT_COLORS.edge;
        res.size = ((data.size as number | undefined) ?? 3) * HIGHLIGHT_SIZE_MULTIPLIER;
        res.zIndex = 1;
      } else {
        // Default: uniform color (no dimming)
        res.color = DEFAULT_EDGE_COLOR;
      }

      return res;
    },
  });

  // Set initial background and label color
  updateThemeStyles(currentTheme);

  function updateThemeStyles(t: "light" | "dark") {
    containerEl!.style.backgroundColor = BACKGROUND_COLOR[t];
    sigma.setSetting("labelColor", { color: LABEL_COLOR[t] });
  }

  // Event handlers
  if (enableHover) {
    sigma.on("enterNode", (event) => {
      hoveredNode = event.node;
      // Compute transitive outgoing edges
      hoveredReachableEdges = computeReachableEdges(event.node);
      sigma.refresh();
      if (onNodeHover) {
        const attrs = graph.getNodeAttributes(event.node) as ADNodeAttributes;
        onNodeHover(event.node, attrs);
      }
    });

    sigma.on("leaveNode", () => {
      hoveredNode = null;
      hoveredReachableEdges = new Set();
      sigma.refresh();
      if (onNodeHover) {
        onNodeHover(null, null);
      }
    });
  }

  sigma.on("clickNode", (event) => {
    if (onNodeClick) {
      const attrs = graph.getNodeAttributes(event.node) as ADNodeAttributes;
      onNodeClick(event.node, attrs);
    }
  });

  sigma.on("clickStage", () => {
    if (onBackgroundClick) {
      onBackgroundClick();
    }
  });

  // Node dragging: start drag on mousedown over a node
  sigma.on("downNode", (event) => {
    draggedNode = event.node;
    // Disable camera drag while dragging a node
    sigma.getCamera().disable();
  });

  // Track mouse movement for dragging
  sigma.getMouseCaptor().on("mousemovebody", (event) => {
    if (!draggedNode) return;

    // Convert viewport coordinates to graph coordinates
    const pos = sigma.viewportToGraph(event);

    // Update node position in the graph
    graph.setNodeAttribute(draggedNode, "x", pos.x);
    graph.setNodeAttribute(draggedNode, "y", pos.y);
  });

  // End drag on mouseup
  sigma.getMouseCaptor().on("mouseup", () => {
    if (draggedNode) {
      draggedNode = null;
      // Re-enable camera drag
      sigma.getCamera().enable();
    }
  });

  // Also handle mouse leaving the container
  sigma.getMouseCaptor().on("mouseleave", () => {
    if (draggedNode) {
      draggedNode = null;
      sigma.getCamera().enable();
    }
  });

  // Public API
  const renderer: ADGraphRenderer = {
    sigma: sigma as unknown as Sigma<ADNodeAttributes, ADEdgeAttributes>,

    get hoveredNode() {
      return hoveredNode;
    },

    get selectedNodes() {
      return selectedNodes;
    },

    destroy() {
      sigma.kill();
    },

    refresh() {
      sigma.refresh();
    },

    focusNode(nodeId: string, animate = true) {
      if (!graph.hasNode(nodeId)) return;

      const nodePosition = sigma.getNodeDisplayData(nodeId);
      if (!nodePosition) return;

      if (animate) {
        sigma.getCamera().animate({ x: nodePosition.x, y: nodePosition.y, ratio: 0.5 }, { duration: 300 });
      } else {
        sigma.getCamera().setState({ x: nodePosition.x, y: nodePosition.y, ratio: 0.5 });
      }
    },

    resetCamera(animate = true) {
      if (animate) {
        sigma.getCamera().animatedReset({ duration: 300 });
      } else {
        sigma.getCamera().setState({ x: 0.5, y: 0.5, ratio: 1, angle: 0 });
      }
    },

    selectNode(nodeId: string) {
      selectedNodes.clear();
      selectedNodes.add(nodeId);
      sigma.refresh();
    },

    clearSelection() {
      selectedNodes.clear();
      highlightedPath.clear();
      highlightedPathEdges.clear();
      hoveredNode = null;
      hoveredReachableEdges = new Set();
      sigma.refresh();
    },

    setTheme(t: "light" | "dark") {
      currentTheme = t;
      updateThemeStyles(t);
      sigma.refresh();
    },

    highlightPath(path: string[]) {
      // Clear ALL highlight state first
      selectedNodes.clear();
      highlightedPath.clear();
      highlightedPathEdges.clear();
      hoveredNode = null;
      hoveredReachableEdges = new Set();

      if (path.length === 0) {
        sigma.refresh();
        return;
      }

      // Add path nodes to highlight set
      for (const nodeId of path) {
        highlightedPath.add(nodeId);
      }

      // Find and highlight edges between consecutive path nodes
      // Check both directions since the path might traverse edges in reverse
      for (let i = 0; i < path.length - 1; i++) {
        const source = path[i];
        const target = path[i + 1];
        // Try source -> target
        graph.forEachEdge(source, target, (edge) => {
          highlightedPathEdges.add(edge);
        });
        // Also try target -> source (in case edge is reversed)
        graph.forEachEdge(target, source, (edge) => {
          highlightedPathEdges.add(edge);
        });
      }

      // Refresh to apply highlight styles
      sigma.refresh();

      // Reset camera to show all nodes (the path graph should only contain path nodes)
      sigma.getCamera().animatedReset({ duration: 300 });
    },
  };

  return renderer;
}
