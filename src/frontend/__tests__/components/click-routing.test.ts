/**
 * Click Routing Tests
 *
 * Tests that document-level clicks are routed to the correct component handlers.
 * The order of handlers matters - more specific handlers must run before generic ones.
 *
 * Handler order (from main.ts):
 * 1. handleSidebarClicks - sidebar toggles, detail panel actions
 * 2. handleQueryTreeClicks - query tree expand/collapse, execute query
 * 3. handleSearchClicks - search result selection
 * 4. handleGraphClicks - generic data-action dispatch (catch-all)
 * 5. handleMenubarOutsideClick - close open menus
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock the component modules before importing
vi.mock("../../components/sidebars", () => ({
  initSidebars: vi.fn(),
  handleSidebarClicks: vi.fn(() => false),
}));

vi.mock("../../components/queries", () => ({
  initQueries: vi.fn(),
  handleQueryTreeClicks: vi.fn(() => false),
}));

vi.mock("../../components/search", () => ({
  initSearch: vi.fn(),
  handleSearchClicks: vi.fn(() => false),
}));

vi.mock("../../components/graph-view", () => ({
  initGraph: vi.fn(),
  handleGraphClicks: vi.fn(() => false),
}));

vi.mock("../../components/menubar", () => ({
  initMenuBar: vi.fn(),
  handleMenubarOutsideClick: vi.fn(),
}));

vi.mock("../../components/keyboard", () => ({
  initKeyboardShortcuts: vi.fn(),
}));

vi.mock("../../components/import", () => ({
  initImport: vi.fn(),
}));

vi.mock("../../components/query-history", () => ({
  initQueryHistory: vi.fn(),
}));

vi.mock("../../components/db-manager", () => ({
  initDbManager: vi.fn(),
}));

vi.mock("../../components/db-connect", () => ({
  initDbConnect: vi.fn(),
}));

vi.mock("../../components/run-query", () => ({
  initRunQuery: vi.fn(),
}));

vi.mock("../../components/manage-queries", () => ({
  initManageQueries: vi.fn(),
}));

vi.mock("../../components/query-activity", () => ({
  initQueryActivity: vi.fn(),
}));

// Import handlers after mocking
import { handleSidebarClicks } from "../../components/sidebars";
import { handleQueryTreeClicks } from "../../components/queries";
import { handleSearchClicks } from "../../components/search";
import { handleGraphClicks } from "../../components/graph-view";
import { handleMenubarOutsideClick } from "../../components/menubar";

describe("Click Routing", () => {
  let clickHandler: (e: MouseEvent) => void;

  beforeEach(() => {
    // Reset all mocks
    vi.clearAllMocks();

    // Reset DOM
    document.body.innerHTML = "";

    // Recreate the click handler logic from main.ts
    clickHandler = (e: MouseEvent) => {
      if (handleSidebarClicks(e)) return;
      if (handleQueryTreeClicks(e)) return;
      if (handleSearchClicks(e)) return;
      if (handleGraphClicks(e)) return;
      handleMenubarOutsideClick(e);
    };
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  describe("Handler priority", () => {
    it("calls handlers in correct order when none handle the click", () => {
      const callOrder: string[] = [];

      vi.mocked(handleSidebarClicks).mockImplementation(() => {
        callOrder.push("sidebar");
        return false;
      });
      vi.mocked(handleQueryTreeClicks).mockImplementation(() => {
        callOrder.push("queryTree");
        return false;
      });
      vi.mocked(handleSearchClicks).mockImplementation(() => {
        callOrder.push("search");
        return false;
      });
      vi.mocked(handleGraphClicks).mockImplementation(() => {
        callOrder.push("graph");
        return false;
      });
      vi.mocked(handleMenubarOutsideClick).mockImplementation(() => {
        callOrder.push("menubar");
      });

      document.body.innerHTML = "<div>Test</div>";
      const event = new MouseEvent("click", { bubbles: true });
      clickHandler(event);

      expect(callOrder).toEqual(["sidebar", "queryTree", "search", "graph", "menubar"]);
    });

    it("stops at sidebar handler when it returns true", () => {
      vi.mocked(handleSidebarClicks).mockReturnValue(true);

      const event = new MouseEvent("click", { bubbles: true });
      clickHandler(event);

      expect(handleSidebarClicks).toHaveBeenCalled();
      expect(handleQueryTreeClicks).not.toHaveBeenCalled();
      expect(handleSearchClicks).not.toHaveBeenCalled();
      expect(handleGraphClicks).not.toHaveBeenCalled();
      expect(handleMenubarOutsideClick).not.toHaveBeenCalled();
    });

    it("stops at query tree handler when it returns true", () => {
      vi.mocked(handleSidebarClicks).mockReturnValue(false);
      vi.mocked(handleQueryTreeClicks).mockReturnValue(true);

      const event = new MouseEvent("click", { bubbles: true });
      clickHandler(event);

      expect(handleSidebarClicks).toHaveBeenCalled();
      expect(handleQueryTreeClicks).toHaveBeenCalled();
      expect(handleSearchClicks).not.toHaveBeenCalled();
      expect(handleGraphClicks).not.toHaveBeenCalled();
    });

    it("stops at search handler when it returns true", () => {
      vi.mocked(handleSidebarClicks).mockReturnValue(false);
      vi.mocked(handleQueryTreeClicks).mockReturnValue(false);
      vi.mocked(handleSearchClicks).mockReturnValue(true);

      const event = new MouseEvent("click", { bubbles: true });
      clickHandler(event);

      expect(handleSearchClicks).toHaveBeenCalled();
      expect(handleGraphClicks).not.toHaveBeenCalled();
    });

    it("stops at graph handler when it returns true", () => {
      vi.mocked(handleSidebarClicks).mockReturnValue(false);
      vi.mocked(handleQueryTreeClicks).mockReturnValue(false);
      vi.mocked(handleSearchClicks).mockReturnValue(false);
      vi.mocked(handleGraphClicks).mockReturnValue(true);

      const event = new MouseEvent("click", { bubbles: true });
      clickHandler(event);

      expect(handleGraphClicks).toHaveBeenCalled();
      expect(handleMenubarOutsideClick).not.toHaveBeenCalled();
    });
  });

  describe("Query tree clicks (regression test for execute-sidebar-query)", () => {
    it("query tree handler runs before graph handler for query items", () => {
      // This test catches the bug where handleGraphClicks was catching
      // data-action="execute-sidebar-query" before handleQueryTreeClicks

      document.body.innerHTML = `
        <div class="query-item" data-action="execute-sidebar-query" data-query-id="test-query">
          Test Query
        </div>
      `;

      const queryItem = document.querySelector(".query-item")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: queryItem });

      // Simulate real behavior: query handler should claim this click
      vi.mocked(handleQueryTreeClicks).mockImplementation((e) => {
        const target = e.target as HTMLElement;
        return target.closest('[data-action="execute-sidebar-query"]') !== null;
      });

      clickHandler(event);

      expect(handleQueryTreeClicks).toHaveBeenCalled();
      expect(handleQueryTreeClicks).toHaveReturnedWith(true);
      // Graph handler should NOT be called because query handler claimed it
      expect(handleGraphClicks).not.toHaveBeenCalled();
    });

    it("graph handler runs for non-query data-action elements", () => {
      document.body.innerHTML = `
        <button data-action="zoom-in">Zoom In</button>
      `;

      const button = document.querySelector("button")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: button });

      // Query handler doesn't claim generic actions
      vi.mocked(handleQueryTreeClicks).mockReturnValue(false);
      // Graph handler claims data-action elements
      vi.mocked(handleGraphClicks).mockImplementation((e) => {
        const target = e.target as HTMLElement;
        return target.closest("[data-action]") !== null;
      });

      clickHandler(event);

      expect(handleQueryTreeClicks).toHaveBeenCalled();
      expect(handleGraphClicks).toHaveBeenCalled();
      expect(handleGraphClicks).toHaveReturnedWith(true);
    });
  });

  describe("Sidebar clicks", () => {
    it("sidebar handler claims toggle-nav-sidebar action", () => {
      document.body.innerHTML = `
        <button data-action="toggle-nav-sidebar">Toggle</button>
      `;

      const button = document.querySelector("button")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: button });

      vi.mocked(handleSidebarClicks).mockImplementation((e) => {
        const target = e.target as HTMLElement;
        const action = target.closest("[data-action]")?.getAttribute("data-action");
        return action === "toggle-nav-sidebar" || action === "toggle-detail-sidebar";
      });

      clickHandler(event);

      expect(handleSidebarClicks).toHaveReturnedWith(true);
      expect(handleQueryTreeClicks).not.toHaveBeenCalled();
    });
  });

  describe("Search clicks", () => {
    it("search handler claims search result item clicks", () => {
      document.body.innerHTML = `
        <div class="search-result-item" data-node-id="node-1">Result</div>
      `;

      const item = document.querySelector(".search-result-item")!;
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: item });

      vi.mocked(handleSearchClicks).mockImplementation((e) => {
        const target = e.target as HTMLElement;
        return target.closest(".search-result-item") !== null;
      });

      clickHandler(event);

      expect(handleSearchClicks).toHaveReturnedWith(true);
      expect(handleGraphClicks).not.toHaveBeenCalled();
    });
  });
});
