/**
 * ADGraphRenderer: Sigma.js renderer for AD graphs.
 *
 * Handles WebGL rendering, user interaction, and visual state management.
 */

import Sigma from "sigma";
import { createNodeImageProgram } from "@sigma/node-image";
import type { ADGraphType } from "./ADGraph";
import type { ADNodeAttributes, ADEdgeAttributes } from "./types";
import {
  HIGHLIGHT_COLORS,
  DIM_COLORS,
  HIGHLIGHT_SIZE_MULTIPLIER,
  BACKGROUND_COLOR,
  LABEL_COLOR,
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
  /** Select a node (and optionally its neighbors) */
  selectNode: (nodeId: string, includeNeighbors?: boolean) => void;
  /** Clear selection */
  clearSelection: () => void;
  /** Set theme */
  setTheme: (theme: "light" | "dark") => void;
}

/** Create an AD graph renderer */
export function createRenderer(options: RendererOptions): ADGraphRenderer {
  const {
    container,
    graph,
    theme = "dark",
    enableHover = true,
    onNodeClick,
    onBackgroundClick,
    onNodeHover,
  } = options;

  // Resolve container element
  const containerEl =
    typeof container === "string"
      ? document.querySelector<HTMLElement>(container)
      : container;

  if (!containerEl) {
    throw new Error(`Container not found: ${container}`);
  }

  // State
  let hoveredNode: string | null = null;
  const selectedNodes = new Set<string>();
  let currentTheme = theme;

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
  function drawNodeHover(
    context: CanvasRenderingContext2D,
    data: { x: number; y: number; size: number; color: string }
  ): void {
    const glowRadius = data.size * 2;
    const gradient = context.createRadialGradient(
      data.x, data.y, data.size * 0.5,
      data.x, data.y, glowRadius
    );
    gradient.addColorStop(0, "rgba(255, 247, 0, 0.6)");
    gradient.addColorStop(0.5, "rgba(255, 247, 0, 0.2)");
    gradient.addColorStop(1, "rgba(255, 247, 0, 0)");

    context.beginPath();
    context.arc(data.x, data.y, glowRadius, 0, Math.PI * 2);
    context.fillStyle = gradient;
    context.fill();
  }

  // Create node image program for rendering icons
  // "background" mode draws the node color as a circle behind the icon
  const NodeImageProgram = createNodeImageProgram({
    size: { mode: "force", value: 256 },
    drawingMode: "background",
    colorAttribute: "color",
    imageAttribute: "image",
    padding: 0.15,
    keepWithinCircle: true,
  });

  // Create Sigma instance with custom reducers for dynamic styling
  const sigma = new Sigma(graph, containerEl, {
    allowInvalidContainer: false,
    renderLabels: true,
    renderEdgeLabels: false,
    labelDensity: 0.07,
    labelGridCellSize: 60,
    labelRenderedSizeThreshold: 6,
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

    // Node reducer: apply highlighting/dimming only for selection (not hover)
    nodeReducer: (nodeId, data) => {
      const res: Record<string, unknown> = { ...data };

      // Only dim nodes when there's a selection (not on hover)
      if (selectedNodes.size > 0) {
        const isSelected = selectedNodes.has(nodeId);
        if (isSelected) {
          res.zIndex = 1;
        } else {
          res.color = DIM_COLORS.node;
          res.zIndex = 0;
        }
      }

      // Bring hovered node to front
      if (nodeId === hoveredNode) {
        res.zIndex = 2;
      }

      return res;
    },

    // Edge reducer: apply highlighting/dimming only for selection (not hover)
    edgeReducer: (edge, data) => {
      const res: Record<string, unknown> = { ...data };
      const source = graph.source(edge);
      const target = graph.target(edge);

      // Only dim edges when there's a selection (not on hover)
      if (selectedNodes.size > 0) {
        const isHighlighted = selectedNodes.has(source) || selectedNodes.has(target);
        if (isHighlighted) {
          res.color = HIGHLIGHT_COLORS.edge;
          res.size = ((data.size as number | undefined) ?? 1) * HIGHLIGHT_SIZE_MULTIPLIER;
          res.zIndex = 1;
        } else {
          res.color = DIM_COLORS.edge;
          res.zIndex = 0;
        }
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
      sigma.refresh();
      if (onNodeHover) {
        const attrs = graph.getNodeAttributes(event.node) as ADNodeAttributes;
        onNodeHover(event.node, attrs);
      }
    });

    sigma.on("leaveNode", () => {
      hoveredNode = null;
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
        sigma.getCamera().animate(
          { x: nodePosition.x, y: nodePosition.y, ratio: 0.5 },
          { duration: 300 }
        );
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

    selectNode(nodeId: string, includeNeighbors = false) {
      selectedNodes.clear();
      selectedNodes.add(nodeId);

      if (includeNeighbors && graph.hasNode(nodeId)) {
        for (const neighbor of graph.neighbors(nodeId)) {
          selectedNodes.add(neighbor);
        }
      }

      sigma.refresh();
    },

    clearSelection() {
      selectedNodes.clear();
      hoveredNode = null;
      sigma.refresh();
    },

    setTheme(t: "light" | "dark") {
      currentTheme = t;
      updateThemeStyles(t);
      sigma.refresh();
    },
  };

  return renderer;
}
