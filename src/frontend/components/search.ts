/**
 * Search and Path Finding
 *
 * Handles node search and path finding functionality.
 * Uses the /api/graph/search endpoint for autocomplete.
 */

import { getRenderer, loadGraphData } from "./graph-view";
import { updateDetailPanel } from "./sidebars";
import { NODE_COLORS } from "../graph/theme";
import type { ADNodeType, ADEdgeType, RawADGraph } from "../graph/types";
import { escapeHtml } from "../utils/html";
import { api, ApiClientError } from "../api/client";
import type { SearchResult, PathResponse } from "../api/types";
import { addToHistory } from "./query-history";

let nodeSearchInput: HTMLInputElement | null = null;
let nodeSearchResults: HTMLElement | null = null;
let pathStartInput: HTMLInputElement | null = null;
let pathStartResults: HTMLElement | null = null;
let pathEndInput: HTMLInputElement | null = null;
let pathEndResults: HTMLElement | null = null;
let pathResultsEl: HTMLElement | null = null;
let findPathBtn: HTMLElement | null = null;

/** Debounce timeout for search */
let searchDebounceTimer: ReturnType<typeof setTimeout> | null = null;
const SEARCH_DEBOUNCE_MS = 200;

/** Map inputs to their result containers for positioning */
const inputToResults = new Map<HTMLInputElement, HTMLElement>();

/** Initialize search functionality */
export function initSearch(): void {
  nodeSearchInput = document.getElementById("node-search") as HTMLInputElement;
  nodeSearchResults = document.getElementById("node-search-results");
  pathStartInput = document.getElementById("path-start") as HTMLInputElement;
  pathEndInput = document.getElementById("path-end") as HTMLInputElement;
  pathResultsEl = document.getElementById("path-results");
  findPathBtn = document.getElementById("find-path-btn");

  // Create result containers for path inputs if they don't exist
  pathStartResults = createResultsContainer(pathStartInput, "path-start-results");
  pathEndResults = createResultsContainer(pathEndInput, "path-end-results");

  // Register input-to-results mappings for positioning
  if (nodeSearchInput && nodeSearchResults) {
    inputToResults.set(nodeSearchInput, nodeSearchResults);
  }
  if (pathStartInput && pathStartResults) {
    inputToResults.set(pathStartInput, pathStartResults);
  }
  if (pathEndInput && pathEndResults) {
    inputToResults.set(pathEndInput, pathEndResults);
  }

  if (nodeSearchInput) {
    nodeSearchInput.addEventListener("input", () => handleSearch(nodeSearchInput!, nodeSearchResults!, "node"));
    nodeSearchInput.addEventListener("keydown", (e) => handleSearchKeydown(e, nodeSearchResults!, "node"));
    nodeSearchInput.addEventListener("blur", () => hideResultsDelayed(nodeSearchResults!));
  }

  if (pathStartInput && pathStartResults) {
    pathStartInput.addEventListener("input", () => handleSearch(pathStartInput!, pathStartResults!, "path-start"));
    pathStartInput.addEventListener("keydown", (e) => handleSearchKeydown(e, pathStartResults!, "path-start"));
    pathStartInput.addEventListener("blur", () => hideResultsDelayed(pathStartResults!));
  }

  if (pathEndInput && pathEndResults) {
    pathEndInput.addEventListener("input", () => handleSearch(pathEndInput!, pathEndResults!, "path-end"));
    pathEndInput.addEventListener("keydown", (e) => handleSearchKeydown(e, pathEndResults!, "path-end"));
    pathEndInput.addEventListener("blur", () => hideResultsDelayed(pathEndResults!));
  }

  // Reposition popovers on window resize
  window.addEventListener("resize", repositionAllPopovers);

  if (findPathBtn) {
    findPathBtn.addEventListener("click", findPath);
  }

  // Click handler for search results
  document.addEventListener("click", handleResultClick);
}

/** Create a results container next to an input if it doesn't exist */
function createResultsContainer(input: HTMLInputElement | null, id: string): HTMLElement | null {
  if (!input) return null;

  let container = document.getElementById(id);
  if (!container) {
    container = document.createElement("div");
    container.id = id;
    container.className = "search-results";
    container.hidden = true;
    input.parentElement?.appendChild(container);
  }
  return container;
}

/** Hide results after a short delay (allows click to register) */
function hideResultsDelayed(resultsEl: HTMLElement): void {
  setTimeout(() => {
    resultsEl.hidden = true;
  }, 150);
}

/** Position a results popover below its input */
function positionPopover(input: HTMLInputElement, resultsEl: HTMLElement): void {
  const rect = input.getBoundingClientRect();
  resultsEl.style.top = `${rect.bottom + 4}px`;
  resultsEl.style.left = `${rect.left}px`;
}

/** Reposition all visible popovers */
function repositionAllPopovers(): void {
  for (const [input, results] of inputToResults) {
    if (!results.hidden) {
      positionPopover(input, results);
    }
  }
}

/** Handle search input with debouncing */
function handleSearch(input: HTMLInputElement, resultsEl: HTMLElement, context: string): void {
  const query = input.value.trim();

  if (query.length < 2) {
    resultsEl.hidden = true;
    return;
  }

  // Debounce the search
  if (searchDebounceTimer) {
    clearTimeout(searchDebounceTimer);
  }

  searchDebounceTimer = setTimeout(() => {
    performSearch(input, query, resultsEl, context);
  }, SEARCH_DEBOUNCE_MS);
}

/** Perform the actual API search */
async function performSearch(
  input: HTMLInputElement,
  query: string,
  resultsEl: HTMLElement,
  context: string
): Promise<void> {
  try {
    const results = await api.get<SearchResult[]>(`/api/graph/search?q=${encodeURIComponent(query)}&limit=10`);

    if (results.length === 0) {
      resultsEl.innerHTML = '<div class="search-no-results">No nodes found</div>';
    } else {
      resultsEl.innerHTML = results
        .map((r) => {
          const color = NODE_COLORS[r.type as ADNodeType] || "#6c757d";
          return `
          <div class="search-result-item" data-node-id="${escapeHtml(r.id)}" data-node-label="${escapeHtml(r.label)}" data-context="${context}">
            <span class="node-badge" style="background-color: ${color}">${escapeHtml(r.type)}</span>
            <span class="node-name">${escapeHtml(r.label)}</span>
          </div>
        `;
        })
        .join("");
    }

    positionPopover(input, resultsEl);
    resultsEl.hidden = false;
  } catch (err) {
    console.error("Search error:", err);
    resultsEl.hidden = true;
  }
}

/** Handle keydown on search inputs */
function handleSearchKeydown(e: KeyboardEvent, resultsEl: HTMLElement, context: string): void {
  if (e.key === "Enter") {
    e.preventDefault();
    // Select the first result if visible
    const firstResult = resultsEl.querySelector(".search-result-item") as HTMLElement;
    if (firstResult && !resultsEl.hidden) {
      handleResultSelection(firstResult, context);
    } else if (context === "path-start" || context === "path-end") {
      // If no results shown, try to find path
      findPath();
    }
  } else if (e.key === "Escape") {
    resultsEl.hidden = true;
    (e.target as HTMLInputElement)?.blur();
  } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
    e.preventDefault();
    navigateResults(resultsEl, e.key === "ArrowDown" ? 1 : -1);
  }
}

/** Navigate through results with arrow keys */
function navigateResults(resultsEl: HTMLElement, direction: number): void {
  const items = resultsEl.querySelectorAll(".search-result-item");
  if (items.length === 0) return;

  const focused = resultsEl.querySelector(".search-result-item.focused");
  let index = -1;

  if (focused) {
    index = Array.from(items).indexOf(focused);
    focused.classList.remove("focused");
  }

  index += direction;
  if (index < 0) index = items.length - 1;
  if (index >= items.length) index = 0;

  items[index]?.classList.add("focused");
}

/** Handle click on search results */
function handleResultClick(e: Event): void {
  const target = e.target as HTMLElement;
  const resultItem = target.closest(".search-result-item") as HTMLElement;
  if (resultItem) {
    const context = resultItem.getAttribute("data-context") || "node";
    handleResultSelection(resultItem, context);
  }
}

/** Handle selection of a search result */
function handleResultSelection(resultItem: HTMLElement, context: string): void {
  const nodeId = resultItem.getAttribute("data-node-id");
  const nodeLabel = resultItem.getAttribute("data-node-label") || nodeId;
  const nodeType = resultItem.querySelector(".node-badge")?.textContent || "Unknown";

  if (!nodeId) return;

  switch (context) {
    case "node":
      loadSingleNode(nodeId, nodeLabel || nodeId, nodeType);
      clearSearch(nodeSearchInput, nodeSearchResults);
      break;
    case "path-start":
      if (pathStartInput) {
        pathStartInput.value = nodeLabel || "";
        pathStartInput.setAttribute("data-node-id", nodeId);
      }
      clearSearch(null, pathStartResults);
      break;
    case "path-end":
      if (pathEndInput) {
        pathEndInput.value = nodeLabel || "";
        pathEndInput.setAttribute("data-node-id", nodeId);
      }
      clearSearch(null, pathEndResults);
      break;
  }
}

/** Clear search input and results */
function clearSearch(input: HTMLInputElement | null, resultsEl: HTMLElement | null): void {
  if (input) {
    input.value = "";
  }
  if (resultsEl) {
    resultsEl.hidden = true;
    resultsEl.innerHTML = "";
  }
}

/** Load a single node as a trivial graph */
function loadSingleNode(nodeId: string, label: string, nodeType: string): void {
  // Create a minimal graph with just this one node
  const graph: RawADGraph = {
    nodes: [
      {
        id: nodeId,
        label: label,
        type: nodeType as ADNodeType,
        properties: {},
      },
    ],
    edges: [],
  };

  // Load the graph
  loadGraphData(graph);

  // After loading, select the node and show details
  const renderer = getRenderer();
  if (renderer) {
    renderer.selectNode(nodeId);
    const attrs = renderer.sigma.getGraph().getNodeAttributes(nodeId);
    updateDetailPanel(nodeId, attrs);
  }
}

/** Find path between start and end nodes using the API */
async function findPath(): Promise<void> {
  if (!pathStartInput || !pathEndInput || !pathResultsEl || !findPathBtn) return;

  // Get node IDs from data attributes or fall back to input values
  const startId = pathStartInput.getAttribute("data-node-id") || pathStartInput.value.trim();
  const endId = pathEndInput.getAttribute("data-node-id") || pathEndInput.value.trim();
  const startLabel = pathStartInput.value.trim();
  const endLabel = pathEndInput.value.trim();

  if (!startId || !endId) {
    showPathError("Please enter both start and end nodes");
    return;
  }

  if (startId === endId) {
    showPathError("Start and end nodes are the same");
    return;
  }

  // Hide any previous results and show loading state
  pathResultsEl.hidden = true;
  setButtonLoading(true);

  try {
    const data = await api.get<PathResponse>(
      `/api/graph/path?from=${encodeURIComponent(startId)}&to=${encodeURIComponent(endId)}`
    );

    if (!data.found) {
      showPathError("No path found between these nodes");
      // Add to history even when no path found
      addToHistory(`Path: ${startLabel} → ${endLabel}`, `find_path(${startId}, ${endId})`, 0);
      return;
    }

    // Load the path graph data into the view
    if (data.graph && data.graph.nodes.length > 0) {
      const pathGraph: RawADGraph = {
        nodes: data.graph.nodes.map((n) => ({
          id: n.id,
          label: n.label,
          type: n.type as ADNodeType,
          properties: n.properties ?? {},
        })),
        edges: data.graph.edges.map((e) => ({
          source: e.source,
          target: e.target,
          type: e.type as ADEdgeType,
        })),
      };
      loadGraphData(pathGraph);

      // Wait for next frame so sigma can compute positions, then highlight
      const nodeIds = data.path.map((step) => step.node.id);
      requestAnimationFrame(() => {
        const renderer = getRenderer();
        if (renderer) {
          renderer.highlightPath(nodeIds);
        }
      });

      // Add to query history
      addToHistory(`Path: ${startLabel} → ${endLabel}`, `find_path(${startId}, ${endId})`, data.path.length);
    }
  } catch (err) {
    console.error("Path finding error:", err);
    const message = err instanceof ApiClientError ? err.message : String(err);
    showPathError(`Path finding failed: ${message}`);
  } finally {
    setButtonLoading(false);
  }
}

/** Set the Find Path button loading state */
function setButtonLoading(loading: boolean): void {
  if (!findPathBtn) return;

  if (loading) {
    findPathBtn.setAttribute("disabled", "true");
    findPathBtn.classList.add("flex", "items-center", "justify-center", "gap-2");
    findPathBtn.innerHTML = '<span class="spinner-sm"></span>Finding...';
  } else {
    findPathBtn.removeAttribute("disabled");
    findPathBtn.classList.remove("flex", "items-center", "justify-center", "gap-2");
    findPathBtn.textContent = "Find Shortest Path";
  }
}

/** Show path error */
function showPathError(message: string): void {
  if (!pathResultsEl) return;
  pathResultsEl.innerHTML = `<div class="path-error">${escapeHtml(message)}</div>`;
  pathResultsEl.hidden = false;
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
