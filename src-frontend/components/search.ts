/**
 * Search and Path Finding
 *
 * Handles node search and path finding functionality.
 */

import { getRenderer } from "./graph-view";
import { updateDetailPanel } from "./sidebars";
import { NODE_COLORS } from "../graph/theme";
import type { ADNodeType, ADNodeAttributes, ADEdgeAttributes } from "../graph/types";
import type { ADGraphType } from "../graph/ADGraph";

let nodeSearchInput: HTMLInputElement | null = null;
let nodeSearchResults: HTMLElement | null = null;
let pathStartInput: HTMLInputElement | null = null;
let pathEndInput: HTMLInputElement | null = null;
let pathResultsEl: HTMLElement | null = null;
let findPathBtn: HTMLElement | null = null;

/** Initialize search functionality */
export function initSearch(): void {
  nodeSearchInput = document.getElementById("node-search") as HTMLInputElement;
  nodeSearchResults = document.getElementById("node-search-results");
  pathStartInput = document.getElementById("path-start") as HTMLInputElement;
  pathEndInput = document.getElementById("path-end") as HTMLInputElement;
  pathResultsEl = document.getElementById("path-results");
  findPathBtn = document.getElementById("find-path-btn");

  if (nodeSearchInput) {
    nodeSearchInput.addEventListener("input", handleNodeSearch);
    nodeSearchInput.addEventListener("keydown", handleNodeSearchKeydown);
  }

  if (pathStartInput) {
    pathStartInput.addEventListener("keydown", handlePathKeydown);
  }

  if (pathEndInput) {
    pathEndInput.addEventListener("keydown", handlePathKeydown);
  }

  if (findPathBtn) {
    findPathBtn.addEventListener("click", findPath);
  }

  // Click handler for search results
  document.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    const resultItem = target.closest(".search-result-item") as HTMLElement;
    if (resultItem) {
      const nodeId = resultItem.getAttribute("data-node-id");
      if (nodeId) {
        selectNode(nodeId);
        clearNodeSearch();
      }
    }
  });
}

/** Handle node search input */
function handleNodeSearch(): void {
  if (!nodeSearchInput || !nodeSearchResults) return;

  const query = nodeSearchInput.value.trim().toLowerCase();
  if (query.length < 2) {
    nodeSearchResults.hidden = true;
    return;
  }

  const renderer = getRenderer();
  if (!renderer) {
    nodeSearchResults.hidden = true;
    return;
  }

  const graph = renderer.sigma.getGraph();
  const results: Array<{ id: string; label: string; type: string }> = [];

  graph.forEachNode((nodeId, attrs) => {
    const label = attrs.label || nodeId;
    if (label.toLowerCase().includes(query) || nodeId.toLowerCase().includes(query)) {
      results.push({
        id: nodeId,
        label,
        type: attrs.nodeType,
      });
    }
  });

  // Limit results
  const limited = results.slice(0, 10);

  if (limited.length === 0) {
    nodeSearchResults.innerHTML = '<div class="search-no-results">No nodes found</div>';
  } else {
    nodeSearchResults.innerHTML = limited
      .map((r) => {
        const color = NODE_COLORS[r.type as ADNodeType] || "#6c757d";
        return `
        <div class="search-result-item" data-node-id="${escapeHtml(r.id)}">
          <span class="node-badge" style="background-color: ${color}">${escapeHtml(r.type)}</span>
          <span class="node-name">${escapeHtml(r.label)}</span>
        </div>
      `;
      })
      .join("");
  }

  nodeSearchResults.hidden = false;
}

/** Handle keydown on node search */
function handleNodeSearchKeydown(e: KeyboardEvent): void {
  if (e.key === "Enter") {
    e.preventDefault();
    // Select the first result
    const firstResult = nodeSearchResults?.querySelector(".search-result-item") as HTMLElement;
    if (firstResult) {
      const nodeId = firstResult.getAttribute("data-node-id");
      if (nodeId) {
        selectNode(nodeId);
        clearNodeSearch();
      }
    }
  } else if (e.key === "Escape") {
    clearNodeSearch();
    nodeSearchInput?.blur();
  }
}

/** Handle keydown on path inputs */
function handlePathKeydown(e: KeyboardEvent): void {
  if (e.key === "Enter") {
    e.preventDefault();
    findPath();
  }
}

/** Clear node search */
function clearNodeSearch(): void {
  if (nodeSearchInput) {
    nodeSearchInput.value = "";
  }
  if (nodeSearchResults) {
    nodeSearchResults.hidden = true;
    nodeSearchResults.innerHTML = "";
  }
}

/** Select a node by ID */
function selectNode(nodeId: string): void {
  const renderer = getRenderer();
  if (!renderer) return;

  const graph = renderer.sigma.getGraph();
  if (!graph.hasNode(nodeId)) return;

  const attrs = graph.getNodeAttributes(nodeId);

  // Select and focus on the node
  renderer.selectNode(nodeId);
  renderer.focusNode(nodeId);

  // Update detail panel
  updateDetailPanel(nodeId, attrs);
}

/** Find path between start and end nodes */
function findPath(): void {
  if (!pathStartInput || !pathEndInput || !pathResultsEl) return;

  const startQuery = pathStartInput.value.trim().toLowerCase();
  const endQuery = pathEndInput.value.trim().toLowerCase();

  if (!startQuery || !endQuery) {
    showPathError("Please enter both start and end nodes");
    return;
  }

  const renderer = getRenderer();
  if (!renderer) {
    showPathError("No graph loaded");
    return;
  }

  const graph = renderer.sigma.getGraph();

  // Find nodes matching the queries
  const startNode = findNodeByQuery(graph, startQuery);
  const endNode = findNodeByQuery(graph, endQuery);

  if (!startNode) {
    showPathError(`Start node "${pathStartInput.value}" not found`);
    return;
  }

  if (!endNode) {
    showPathError(`End node "${pathEndInput.value}" not found`);
    return;
  }

  if (startNode === endNode) {
    showPathError("Start and end nodes are the same");
    return;
  }

  // BFS to find shortest path
  const path = findShortestPath(graph, startNode, endNode);

  if (!path) {
    showPathError("No path found between these nodes");
    return;
  }

  // Display the path
  displayPath(graph, path);

  // Highlight the path on the graph
  renderer.highlightPath(path);
}

/** Find a node by query (label or ID) */
function findNodeByQuery(graph: ADGraphType, query: string): string | null {
  let found: string | null = null;

  graph.forEachNode((nodeId: string, attrs: ADNodeAttributes) => {
    if (found) return;
    const label = (attrs.label || nodeId).toLowerCase();
    if (label === query || nodeId.toLowerCase() === query) {
      found = nodeId;
    }
  });

  // If no exact match, try partial match
  if (!found) {
    graph.forEachNode((nodeId: string, attrs: ADNodeAttributes) => {
      if (found) return;
      const label = (attrs.label || nodeId).toLowerCase();
      if (label.includes(query) || nodeId.toLowerCase().includes(query)) {
        found = nodeId;
      }
    });
  }

  return found;
}

/** BFS shortest path */
function findShortestPath(graph: ADGraphType, start: string, end: string): string[] | null {
  const visited = new Set<string>();
  const parent = new Map<string, { node: string; edge: string }>();
  const queue: string[] = [start];
  visited.add(start);

  while (queue.length > 0) {
    const current = queue.shift()!;

    if (current === end) {
      // Reconstruct path
      const path: string[] = [end];
      let node = end;
      while (parent.has(node)) {
        const p = parent.get(node)!;
        path.unshift(p.node);
        node = p.node;
      }
      return path;
    }

    // Check outgoing edges
    graph.forEachOutEdge(current, (edge: string, _attrs: ADEdgeAttributes, _source: string, target: string) => {
      if (!visited.has(target)) {
        visited.add(target);
        parent.set(target, { node: current, edge });
        queue.push(target);
      }
    });
  }

  return null;
}

/** Display path results */
function displayPath(graph: ADGraphType, path: string[]): void {
  if (!pathResultsEl) return;

  const steps: string[] = [];

  for (let i = 0; i < path.length; i++) {
    const nodeId = path[i]!;
    const attrs = graph.getNodeAttributes(nodeId);
    const label = attrs.label || nodeId;
    const type = attrs.nodeType;
    const color = NODE_COLORS[type as ADNodeType] || "#6c757d";

    if (i < path.length - 1) {
      // Find the edge between this node and the next
      const nextNode = path[i + 1]!;
      let edgeType = "";
      graph.forEachEdge(nodeId, nextNode, (_edge: string, edgeAttrs: ADEdgeAttributes) => {
        edgeType = edgeAttrs.edgeType || edgeAttrs.label || "";
      });

      steps.push(`
        <div class="path-step" data-node-id="${escapeHtml(nodeId)}">
          <span class="node-badge" style="background-color: ${color}">${escapeHtml(type)}</span>
          <span class="path-step-node">${escapeHtml(label)}</span>
        </div>
        <div class="path-step-edge">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M19 12l-7 7-7-7"/>
          </svg>
          <span>${escapeHtml(edgeType)}</span>
        </div>
      `);
    } else {
      steps.push(`
        <div class="path-step" data-node-id="${escapeHtml(nodeId)}">
          <span class="node-badge" style="background-color: ${color}">${escapeHtml(type)}</span>
          <span class="path-step-node">${escapeHtml(label)}</span>
        </div>
      `);
    }
  }

  pathResultsEl.innerHTML = `
    <div class="path-length">${path.length} nodes, ${path.length - 1} hops</div>
    <div class="path-steps">${steps.join("")}</div>
  `;
  pathResultsEl.hidden = false;
}

/** Show path error */
function showPathError(message: string): void {
  if (!pathResultsEl) return;
  pathResultsEl.innerHTML = `<div class="path-error">${escapeHtml(message)}</div>`;
  pathResultsEl.hidden = false;
}

/** Escape HTML */
function escapeHtml(str: string): string {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

/** Set path start node from external source */
export function setPathStart(nodeId: string, label: string): void {
  if (pathStartInput) {
    pathStartInput.value = label || nodeId;
  }
}

/** Set path end node from external source */
export function setPathEnd(nodeId: string, label: string): void {
  if (pathEndInput) {
    pathEndInput.value = label || nodeId;
  }
}
