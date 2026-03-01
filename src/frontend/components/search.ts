/**
 * Search and Path Finding
 *
 * Handles node search and path finding functionality.
 * Uses the /api/graph/search endpoint for autocomplete.
 */

import { getRenderer, loadGraphData } from "./graph-view";
import { updateDetailPanel } from "./sidebars";
import { NODE_COLORS } from "../graph/theme";
import { getNodeIconPath } from "../graph/icons";
import type { ADNodeType, ADEdgeType, RawADGraph } from "../graph/types";
import { escapeHtml } from "../utils/html";
import { api, ApiClientError } from "../api/client";
import type { SearchResult, PathResponse } from "../api/types";
// Query history is now managed by the backend for path finding

/** DOM element references */
interface SearchElements {
  nodeSearchInput: HTMLInputElement | null;
  nodeSearchResults: HTMLElement | null;
  pathStartInput: HTMLInputElement | null;
  pathStartResults: HTMLElement | null;
  pathEndInput: HTMLInputElement | null;
  pathEndResults: HTMLElement | null;
  pathResultsEl: HTMLElement | null;
  findPathBtn: HTMLElement | null;
}

/** Component state */
interface SearchState {
  debounceTimer: ReturnType<typeof setTimeout> | null;
  searchAbortController: AbortController | null;
  inputToResults: Map<HTMLInputElement, HTMLElement>;
  pathStartNodeId: string | null;
  pathEndNodeId: string | null;
  /** Cache of recent search results for property lookup */
  searchResultsCache: Map<string, SearchResult>;
}

const elements: SearchElements = {
  nodeSearchInput: null,
  nodeSearchResults: null,
  pathStartInput: null,
  pathStartResults: null,
  pathEndInput: null,
  pathEndResults: null,
  pathResultsEl: null,
  findPathBtn: null,
};

const state: SearchState = {
  debounceTimer: null,
  searchAbortController: null,
  inputToResults: new Map(),
  pathStartNodeId: null,
  pathEndNodeId: null,
  searchResultsCache: new Map(),
};

const SEARCH_DEBOUNCE_MS = 200;

/** Reset component state (for testing) */
export function resetSearchState(): void {
  if (state.debounceTimer) {
    clearTimeout(state.debounceTimer);
  }
  if (state.searchAbortController) {
    state.searchAbortController.abort();
  }
  state.debounceTimer = null;
  state.searchAbortController = null;
  state.inputToResults.clear();
  state.pathStartNodeId = null;
  state.pathEndNodeId = null;
  state.searchResultsCache.clear();
  for (const key of Object.keys(elements) as (keyof SearchElements)[]) {
    elements[key] = null;
  }
}

/** Initialize search functionality */
export function initSearch(): void {
  elements.nodeSearchInput = document.getElementById("node-search") as HTMLInputElement;
  elements.pathStartInput = document.getElementById("path-start") as HTMLInputElement;
  elements.pathEndInput = document.getElementById("path-end") as HTMLInputElement;
  elements.pathResultsEl = document.getElementById("path-results");
  elements.findPathBtn = document.getElementById("find-path-btn");

  // Create/move result containers to body (portal pattern)
  elements.nodeSearchResults = createResultsContainer(elements.nodeSearchInput, "node-search-results");
  elements.pathStartResults = createResultsContainer(elements.pathStartInput, "path-start-results");
  elements.pathEndResults = createResultsContainer(elements.pathEndInput, "path-end-results");

  // Register input-to-results mappings for positioning
  if (elements.nodeSearchInput && elements.nodeSearchResults) {
    state.inputToResults.set(elements.nodeSearchInput, elements.nodeSearchResults);
  }
  if (elements.pathStartInput && elements.pathStartResults) {
    state.inputToResults.set(elements.pathStartInput, elements.pathStartResults);
  }
  if (elements.pathEndInput && elements.pathEndResults) {
    state.inputToResults.set(elements.pathEndInput, elements.pathEndResults);
  }

  if (elements.nodeSearchInput) {
    elements.nodeSearchInput.addEventListener("input", () =>
      handleSearch(elements.nodeSearchInput!, elements.nodeSearchResults!, "node")
    );
    elements.nodeSearchInput.addEventListener("keydown", (e) =>
      handleSearchKeydown(e, elements.nodeSearchResults!, "node")
    );
    elements.nodeSearchInput.addEventListener("blur", () => hideResultsDelayed(elements.nodeSearchResults!));
  }

  if (elements.pathStartInput && elements.pathStartResults) {
    elements.pathStartInput.addEventListener("input", () =>
      handleSearch(elements.pathStartInput!, elements.pathStartResults!, "path-start")
    );
    elements.pathStartInput.addEventListener("keydown", (e) =>
      handleSearchKeydown(e, elements.pathStartResults!, "path-start")
    );
    elements.pathStartInput.addEventListener("blur", () => hideResultsDelayed(elements.pathStartResults!));
  }

  if (elements.pathEndInput && elements.pathEndResults) {
    elements.pathEndInput.addEventListener("input", () =>
      handleSearch(elements.pathEndInput!, elements.pathEndResults!, "path-end")
    );
    elements.pathEndInput.addEventListener("keydown", (e) =>
      handleSearchKeydown(e, elements.pathEndResults!, "path-end")
    );
    elements.pathEndInput.addEventListener("blur", () => hideResultsDelayed(elements.pathEndResults!));
  }

  // Reposition popovers on window resize
  window.addEventListener("resize", repositionAllPopovers);

  if (elements.findPathBtn) {
    elements.findPathBtn.addEventListener("click", findPath);
  }
}

/** Create a results container as a portal in document.body */
function createResultsContainer(input: HTMLInputElement | null, id: string): HTMLElement | null {
  if (!input) return null;

  let container = document.getElementById(id);
  if (!container) {
    container = document.createElement("div");
    container.id = id;
    container.className = "search-results";
    container.hidden = true;
    document.body.appendChild(container);
  } else if (container.parentElement !== document.body) {
    // Move existing container to body if it's not already there
    document.body.appendChild(container);
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
  resultsEl.style.top = `${rect.bottom}px`;
  resultsEl.style.left = `${rect.left}px`;
  resultsEl.style.minWidth = `${rect.width}px`;
}

/** Reposition all visible popovers */
function repositionAllPopovers(): void {
  for (const [input, results] of state.inputToResults) {
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

  // Cancel any in-flight search request
  if (state.searchAbortController) {
    state.searchAbortController.abort();
  }

  // Debounce the search
  if (state.debounceTimer) {
    clearTimeout(state.debounceTimer);
  }

  state.debounceTimer = setTimeout(() => {
    // Create a new abort controller for this search
    state.searchAbortController = new AbortController();
    performSearch(input, query, resultsEl, context, state.searchAbortController.signal);
  }, SEARCH_DEBOUNCE_MS);
}

/** Perform the actual API search */
async function performSearch(
  input: HTMLInputElement,
  query: string,
  resultsEl: HTMLElement,
  context: string,
  signal: AbortSignal
): Promise<void> {
  try {
    const results = await api.get<SearchResult[]>(`/api/graph/search?q=${encodeURIComponent(query)}&limit=10`, signal);

    // Cache results for property lookup when selected
    for (const r of results) {
      state.searchResultsCache.set(r.id, r);
    }

    if (results.length === 0) {
      resultsEl.innerHTML = '<div class="search-no-results">No nodes found</div>';
    } else {
      resultsEl.innerHTML = results
        .map((r) => {
          const nodeType = r.type as ADNodeType;
          const color = NODE_COLORS[nodeType] || "#6c757d";
          const iconPath = getNodeIconPath(nodeType);
          return `
          <div class="search-result-item" data-node-id="${escapeHtml(r.id)}" data-node-label="${escapeHtml(r.name)}" data-node-type="${escapeHtml(r.type)}" data-context="${context}">
            <span class="node-type-icon" style="background-color: ${color}" title="${escapeHtml(r.type)}">
              <svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${iconPath}</svg>
            </span>
            <span class="node-name">${escapeHtml(r.name)}</span>
          </div>
        `;
        })
        .join("");
    }

    positionPopover(input, resultsEl);
    resultsEl.hidden = false;
  } catch (err) {
    // Ignore aborted requests - they're expected when user types quickly
    if (err instanceof Error && err.name === "AbortError") {
      return;
    }
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

/**
 * Handle clicks for search results.
 * Called from the central document click handler in main.ts.
 * Returns true if the click was handled.
 */
export function handleSearchClicks(e: MouseEvent): boolean {
  const target = e.target as HTMLElement;
  const resultItem = target.closest(".search-result-item") as HTMLElement;
  if (resultItem) {
    const context = resultItem.getAttribute("data-context") || "node";
    handleResultSelection(resultItem, context);
    return true;
  }
  return false;
}

/** Handle selection of a search result */
function handleResultSelection(resultItem: HTMLElement, context: string): void {
  const nodeId = resultItem.getAttribute("data-node-id");
  const nodeLabel = resultItem.getAttribute("data-node-label") || nodeId;
  const nodeType = resultItem.getAttribute("data-node-type") || "Unknown";

  if (!nodeId) return;

  // Get cached properties for this node
  const cachedResult = state.searchResultsCache.get(nodeId);
  const properties = cachedResult?.properties ?? {};

  switch (context) {
    case "node":
      loadSingleNode(nodeId, nodeLabel || nodeId, nodeType, properties);
      clearSearch(elements.nodeSearchInput, elements.nodeSearchResults);
      break;
    case "path-start":
      if (elements.pathStartInput) {
        elements.pathStartInput.value = nodeLabel || "";
        state.pathStartNodeId = nodeId;
      }
      clearSearch(null, elements.pathStartResults);
      break;
    case "path-end":
      if (elements.pathEndInput) {
        elements.pathEndInput.value = nodeLabel || "";
        state.pathEndNodeId = nodeId;
      }
      clearSearch(null, elements.pathEndResults);
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
function loadSingleNode(nodeId: string, label: string, nodeType: string, properties: Record<string, unknown>): void {
  // Create a minimal graph with just this one node
  const graph: RawADGraph = {
    nodes: [
      {
        id: nodeId,
        name: label,
        type: nodeType as ADNodeType,
        properties,
      },
    ],
    relationships: [],
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
  const { pathStartInput, pathEndInput, pathResultsEl, findPathBtn } = elements;
  if (!pathStartInput || !pathEndInput || !pathResultsEl || !findPathBtn) return;

  // Get node IDs from state or fall back to input values
  const startId = state.pathStartNodeId || pathStartInput.value.trim();
  const endId = state.pathEndNodeId || pathEndInput.value.trim();

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
      // Query history is managed by the backend
      return;
    }

    // Load the path graph data into the view
    if (data.graph && data.graph.nodes.length > 0) {
      const pathGraph: RawADGraph = {
        nodes: data.graph.nodes.map((n) => ({
          id: n.id,
          name: n.name,
          type: n.type as ADNodeType,
          properties: n.properties,
        })),
        relationships: data.graph.relationships.map((e) => ({
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

      // Query history is managed by the backend
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
  if (!elements.findPathBtn) return;

  if (loading) {
    elements.findPathBtn.setAttribute("disabled", "true");
    elements.findPathBtn.classList.add("flex", "items-center", "justify-center", "gap-2");
    elements.findPathBtn.innerHTML = '<span class="spinner-sm"></span>Finding...';
  } else {
    elements.findPathBtn.removeAttribute("disabled");
    elements.findPathBtn.classList.remove("flex", "items-center", "justify-center", "gap-2");
    elements.findPathBtn.textContent = "Find Shortest Path";
  }
}

/** Show path error */
function showPathError(message: string): void {
  if (!elements.pathResultsEl) return;
  elements.pathResultsEl.innerHTML = `<div class="path-error">${escapeHtml(message)}</div>`;
  elements.pathResultsEl.hidden = false;
}

/** Set path start node from external source */
export function setPathStart(nodeId: string, label: string): void {
  state.pathStartNodeId = nodeId;
  if (elements.pathStartInput) {
    elements.pathStartInput.value = label || nodeId;
  }
}

/** Set path end node from external source */
export function setPathEnd(nodeId: string, label: string): void {
  state.pathEndNodeId = nodeId;
  if (elements.pathEndInput) {
    elements.pathEndInput.value = label || nodeId;
  }
}
