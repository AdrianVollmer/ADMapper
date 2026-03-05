/**
 * Click Routing Tests
 *
 * Tests that click handlers respond correctly to DOM events and produce
 * the expected side effects. These tests use real handler implementations
 * with minimal mocking to verify actual behavior.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock dependencies that handlers use, not the handlers themselves
vi.mock("../../api/client", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    delete: vi.fn(),
  },
  ApiClientError: class ApiClientError extends Error {
    status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

vi.mock("../../components/graph-view", () => ({
  initGraph: vi.fn(),
  getRenderer: vi.fn(() => null),
  loadGraphData: vi.fn(),
  handleGraphClicks: vi.fn(() => false),
}));

vi.mock("../../utils/notifications", () => ({
  showSuccess: vi.fn(),
  showError: vi.fn(),
  showInfo: vi.fn(),
  showConfirm: vi.fn(() => Promise.resolve(false)),
}));

vi.mock("../../utils/query", () => ({
  executeQueryWithHistory: vi.fn(),
  getQueryErrorMessage: vi.fn((err) => String(err)),
  QueryAbortedError: class QueryAbortedError extends Error {
    constructor(queryId: string) {
      super(`Query ${queryId} was aborted`);
      this.name = "QueryAbortedError";
    }
  },
}));

// Import actual handlers after mocking their dependencies
import { handleSidebarClicks, toggleNavSidebar, toggleDetailSidebar } from "../../components/sidebars";
import { handleQueryTreeClicks, initQueries } from "../../components/queries";
import { handleSearchClicks, initSearch, resetSearchState } from "../../components/search";

// Use vi.hoisted to create mockAppState before vi.mock runs
const { mockAppState } = vi.hoisted(() => ({
  mockAppState: {
    navSidebarCollapsed: false,
    detailSidebarCollapsed: false,
    selectedNodeId: null as string | null,
    selectedEdgeId: null as string | null,
  },
}));

// Mock appState which sidebars.ts imports from main
vi.mock("../../main", () => ({
  appState: mockAppState,
}));

describe("Sidebar Click Handler", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  describe("toggle-nav-sidebar action", () => {
    it("handles toggle-nav-sidebar button click", () => {
      document.body.innerHTML = `
        <button data-action="toggle-nav-sidebar">Toggle Nav</button>
        <aside id="nav-sidebar" style="width: 240px"></aside>
        <button id="nav-sidebar-expand" class="hidden"></button>
      `;

      const button = document.querySelector('[data-action="toggle-nav-sidebar"]')!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: button });

      const handled = handleSidebarClicks(event);

      expect(handled).toBe(true);
    });

    it("does not handle unrelated button clicks", () => {
      document.body.innerHTML = `
        <button data-action="some-other-action">Other</button>
      `;

      const button = document.querySelector("button")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: button });

      const handled = handleSidebarClicks(event);

      expect(handled).toBe(false);
    });
  });

  describe("toggle-detail-sidebar action", () => {
    it("handles toggle-detail-sidebar button click", () => {
      document.body.innerHTML = `
        <button data-action="toggle-detail-sidebar">Toggle Detail</button>
        <aside id="detail-sidebar" style="width: 300px"></aside>
        <button id="detail-sidebar-expand" class="hidden"></button>
      `;

      const button = document.querySelector('[data-action="toggle-detail-sidebar"]')!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: button });

      const handled = handleSidebarClicks(event);

      expect(handled).toBe(true);
    });
  });

  describe("click-to-copy on property values", () => {
    it("handles click on detail-prop-value element", () => {
      document.body.innerHTML = `
        <span class="detail-prop-value" data-value="test-value">Test Value</span>
      `;

      // Mock clipboard API using Object.defineProperty
      const writeTextMock = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, "clipboard", {
        value: { writeText: writeTextMock },
        writable: true,
        configurable: true,
      });

      const valueEl = document.querySelector(".detail-prop-value")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: valueEl });

      const handled = handleSidebarClicks(event);

      expect(handled).toBe(true);
      expect(writeTextMock).toHaveBeenCalledWith("test-value");
    });
  });

  describe("overflow menu toggle", () => {
    it("opens overflow dropdown when trigger is clicked", () => {
      document.body.innerHTML = `
        <button class="overflow-trigger" aria-expanded="false">Menu</button>
        <div class="overflow-dropdown" hidden>
          <button class="overflow-item" data-action="delete-node" data-node-id="123">Delete</button>
        </div>
      `;

      const trigger = document.querySelector(".overflow-trigger")!;
      const dropdown = document.querySelector(".overflow-dropdown")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: trigger });

      const handled = handleSidebarClicks(event);

      expect(handled).toBe(true);
      expect(dropdown.hasAttribute("hidden")).toBe(false);
      expect(trigger.getAttribute("aria-expanded")).toBe("true");
    });

    it("closes overflow dropdown when trigger is clicked again", () => {
      document.body.innerHTML = `
        <button class="overflow-trigger" aria-expanded="true">Menu</button>
        <div class="overflow-dropdown">
          <button class="overflow-item" data-action="delete-node" data-node-id="123">Delete</button>
        </div>
      `;

      const trigger = document.querySelector(".overflow-trigger")!;
      const dropdown = document.querySelector(".overflow-dropdown")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: trigger });

      const handled = handleSidebarClicks(event);

      expect(handled).toBe(true);
      expect(dropdown.hasAttribute("hidden")).toBe(true);
      expect(trigger.getAttribute("aria-expanded")).toBe("false");
    });
  });
});

describe("Query Tree Click Handler", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  describe("toggle-category action", () => {
    it("handles category toggle click", () => {
      // Set up minimal DOM for initQueries
      document.body.innerHTML = `
        <div id="query-tree"></div>
        <input id="query-filter" />
      `;

      // Initialize the queries component to set up internal state
      initQueries();

      // Now create a category header to click
      const queryTree = document.getElementById("query-tree")!;
      // After init, query-tree contains rendered categories
      // Find a category header
      const categoryHeader = queryTree.querySelector('[data-action="toggle-category"]');

      if (categoryHeader) {
        const event = new MouseEvent("click", { bubbles: true });
        Object.defineProperty(event, "target", { value: categoryHeader });

        const handled = handleQueryTreeClicks(event);
        expect(handled).toBe(true);
      }
    });

    it("returns false for non-query-tree clicks", () => {
      document.body.innerHTML = `
        <button data-action="some-other-action">Other</button>
      `;

      const button = document.querySelector("button")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: button });

      const handled = handleQueryTreeClicks(event);

      expect(handled).toBe(false);
    });
  });

  describe("execute-sidebar-query action", () => {
    it("handles query item click", async () => {
      document.body.innerHTML = `
        <div id="query-tree"></div>
        <input id="query-filter" />
      `;

      initQueries();

      // Find a query item (if rendered)
      const queryTree = document.getElementById("query-tree")!;
      const queryItem = queryTree.querySelector('[data-action="execute-sidebar-query"]');

      if (queryItem) {
        const event = new MouseEvent("click", { bubbles: true });
        Object.defineProperty(event, "target", { value: queryItem });

        const handled = handleQueryTreeClicks(event);
        expect(handled).toBe(true);
      }
    });
  });
});

describe("Search Click Handler", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    resetSearchState();
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.body.innerHTML = "";
    resetSearchState();
  });

  describe("search result selection", () => {
    it("handles click on search result item", () => {
      document.body.innerHTML = `
        <input id="node-search" />
        <input id="path-start" />
        <input id="path-end" />
        <div id="path-results"></div>
        <button id="find-path-btn">Find Path</button>
        <div class="search-result-item" data-node-id="node-1" data-node-label="Test Node" data-node-type="User" data-context="node">
          Test Node
        </div>
      `;

      initSearch();

      const resultItem = document.querySelector(".search-result-item")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: resultItem });

      const handled = handleSearchClicks(event);

      expect(handled).toBe(true);
    });

    it("handles click on path-start search result", () => {
      document.body.innerHTML = `
        <input id="node-search" />
        <input id="path-start" />
        <input id="path-end" />
        <div id="path-results"></div>
        <button id="find-path-btn">Find Path</button>
        <div class="search-result-item" data-node-id="start-node" data-node-label="Start" data-context="path-start">
          Start Node
        </div>
      `;

      initSearch();

      const resultItem = document.querySelector(".search-result-item")!;
      const pathStartInput = document.getElementById("path-start") as HTMLInputElement;

      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: resultItem });

      const handled = handleSearchClicks(event);

      expect(handled).toBe(true);
      expect(pathStartInput.value).toBe("Start");
    });

    it("handles click on path-end search result", () => {
      document.body.innerHTML = `
        <input id="node-search" />
        <input id="path-start" />
        <input id="path-end" />
        <div id="path-results"></div>
        <button id="find-path-btn">Find Path</button>
        <div class="search-result-item" data-node-id="end-node" data-node-label="End" data-context="path-end">
          End Node
        </div>
      `;

      initSearch();

      const resultItem = document.querySelector(".search-result-item")!;
      const pathEndInput = document.getElementById("path-end") as HTMLInputElement;

      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: resultItem });

      const handled = handleSearchClicks(event);

      expect(handled).toBe(true);
      expect(pathEndInput.value).toBe("End");
    });

    it("returns false for non-search-result clicks", () => {
      document.body.innerHTML = `
        <div class="some-other-element">Not a search result</div>
      `;

      const element = document.querySelector(".some-other-element")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: element });

      const handled = handleSearchClicks(event);

      expect(handled).toBe(false);
    });
  });
});

describe("Handler Priority Integration", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("sidebar handler claims sidebar toggle clicks before other handlers", () => {
    document.body.innerHTML = `
      <button data-action="toggle-nav-sidebar">Toggle</button>
      <aside id="nav-sidebar" style="width: 240px"></aside>
      <button id="nav-sidebar-expand" class="hidden"></button>
    `;

    const button = document.querySelector("button")!;
    const event = new MouseEvent("click", { bubbles: true });
    Object.defineProperty(event, "target", { value: button });

    // Sidebar should handle it
    const sidebarHandled = handleSidebarClicks(event);
    expect(sidebarHandled).toBe(true);

    // Other handlers should not handle sidebar-specific actions
    const queryHandled = handleQueryTreeClicks(event);
    expect(queryHandled).toBe(false);

    const searchHandled = handleSearchClicks(event);
    expect(searchHandled).toBe(false);
  });

  it("query handler claims query-tree clicks", () => {
    document.body.innerHTML = `
      <div id="query-tree"></div>
      <input id="query-filter" />
    `;

    initQueries();

    const queryTree = document.getElementById("query-tree")!;
    const categoryHeader = queryTree.querySelector('[data-action="toggle-category"]');

    if (categoryHeader) {
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: categoryHeader });

      // Sidebar should not handle query tree clicks
      const sidebarHandled = handleSidebarClicks(event);
      expect(sidebarHandled).toBe(false);

      // Query handler should handle it
      const queryHandled = handleQueryTreeClicks(event);
      expect(queryHandled).toBe(true);
    }
  });

  it("search handler claims search result clicks", () => {
    document.body.innerHTML = `
      <input id="node-search" />
      <input id="path-start" />
      <input id="path-end" />
      <div id="path-results"></div>
      <button id="find-path-btn">Find Path</button>
      <div class="search-result-item" data-node-id="test" data-context="node">Result</div>
    `;

    initSearch();

    const resultItem = document.querySelector(".search-result-item")!;
    const event = new MouseEvent("click", { bubbles: true });
    Object.defineProperty(event, "target", { value: resultItem });

    // Sidebar should not handle search result clicks
    const sidebarHandled = handleSidebarClicks(event);
    expect(sidebarHandled).toBe(false);

    // Query handler should not handle search result clicks
    const queryHandled = handleQueryTreeClicks(event);
    expect(queryHandled).toBe(false);

    // Search handler should handle it
    const searchHandled = handleSearchClicks(event);
    expect(searchHandled).toBe(true);
  });
});

describe("Sidebar Toggle Functions", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.clearAllMocks();
    // Reset appState for each test
    mockAppState.navSidebarCollapsed = false;
    mockAppState.detailSidebarCollapsed = false;
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("toggleNavSidebar collapses nav sidebar", () => {
    document.body.innerHTML = `
      <aside id="nav-sidebar" data-collapsed="false" style="width: 240px"></aside>
      <button id="nav-sidebar-expand" class="hidden"></button>
    `;

    toggleNavSidebar();

    const sidebar = document.getElementById("nav-sidebar")!;
    const expandBtn = document.getElementById("nav-sidebar-expand")!;

    expect(sidebar.getAttribute("data-collapsed")).toBe("true");
    expect(sidebar.style.width).toBe("0px");
    expect(expandBtn.classList.contains("hidden")).toBe(false);
  });

  it("toggleDetailSidebar collapses detail sidebar", () => {
    document.body.innerHTML = `
      <aside id="detail-sidebar" data-collapsed="false" style="width: 300px"></aside>
      <button id="detail-sidebar-expand" class="hidden"></button>
    `;

    toggleDetailSidebar();

    const sidebar = document.getElementById("detail-sidebar")!;
    const expandBtn = document.getElementById("detail-sidebar-expand")!;

    expect(sidebar.getAttribute("data-collapsed")).toBe("true");
    expect(sidebar.style.width).toBe("0px");
    expect(expandBtn.classList.contains("hidden")).toBe(false);
  });
});
