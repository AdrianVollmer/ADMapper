/**
 * Manage Queries Tests
 *
 * Tests for the manage-queries component including:
 * - Schema validation
 * - Category/query filtering
 * - Query counting
 * - Modal interactions and CRUD operations
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Store original module
let manageQueriesModule: typeof import("../../components/manage-queries");

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key];
    }),
    clear: vi.fn(() => {
      store = {};
    }),
    _setStore: (newStore: Record<string, string>) => {
      store = newStore;
    },
  };
})();

Object.defineProperty(window, "localStorage", { value: localStorageMock });

describe("Manage Queries", () => {
  beforeEach(async () => {
    vi.clearAllMocks();
    localStorageMock.clear();
    localStorageMock._setStore({});

    // Clean up DOM completely
    document.body.innerHTML = "";

    // Re-import module fresh to reset module-level state
    vi.resetModules();
    manageQueriesModule = await import("../../components/manage-queries");

    // Register Escape handler (normally done by main.ts init).
    // Inline minimal version to avoid importing main.ts (which pulls in WebGL).
    // Uses the outer `manageQueriesModule` variable which is reassigned each beforeEach.
    const mod = manageQueriesModule;
    document.addEventListener("keydown", (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      const modal = document.querySelector<HTMLElement>(".modal-overlay:not([hidden])");
      if (!modal) return;
      if (modal.id === "manage-queries-modal" && mod.handleEscapeKey) {
        mod.handleEscapeKey();
      } else {
        modal.hidden = true;
      }
    });
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  describe("initManageQueries", () => {
    it("creates modal element in DOM", () => {
      manageQueriesModule.initManageQueries();

      const modal = document.getElementById("manage-queries-modal");
      expect(modal).not.toBeNull();
      expect(modal?.classList.contains("modal-overlay")).toBe(true);
      expect(modal?.hasAttribute("hidden")).toBe(true);
    });

    it("modal contains header, body, and footer", () => {
      manageQueriesModule.initManageQueries();

      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.querySelector(".modal-header")).not.toBeNull();
      expect(modal?.querySelector("#manage-queries-body")).not.toBeNull();
      expect(modal?.querySelector("#manage-queries-footer")).not.toBeNull();
    });

    it("modal has close button", () => {
      manageQueriesModule.initManageQueries();

      const closeBtn = document.querySelector('[data-action="close"]');
      expect(closeBtn).not.toBeNull();
    });
  });

  describe("loadCustomQueries", () => {
    it("returns default empty queries when localStorage is empty", async () => {
      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({
        version: 1,
        categories: [],
      });
    });

    it("loads valid queries from localStorage", async () => {
      const storedQueries = {
        version: 1,
        categories: [
          {
            id: "test-cat",
            name: "Test Category",
            queries: [
              {
                id: "test-query",
                name: "Test Query",
                query: "MATCH (n) RETURN n",
              },
            ],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(storedQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual(storedQueries);
    });

    it("returns default queries for invalid JSON", async () => {
      localStorageMock._setStore({
        admapper_custom_queries: "not valid json",
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({
        version: 1,
        categories: [],
      });
    });

    it("returns default queries for invalid schema", async () => {
      const invalidQueries = {
        // missing version
        categories: [],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(invalidQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({
        version: 1,
        categories: [],
      });
    });

    it("returns default queries when category is missing required fields", async () => {
      const invalidQueries = {
        version: 1,
        categories: [
          {
            // missing id and name
            queries: [],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(invalidQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({
        version: 1,
        categories: [],
      });
    });

    it("returns default queries when query is missing required fields", async () => {
      const invalidQueries = {
        version: 1,
        categories: [
          {
            id: "cat-1",
            name: "Category",
            queries: [
              {
                id: "q-1",
                name: "Query",
                // missing query field
              },
            ],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(invalidQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({
        version: 1,
        categories: [],
      });
    });
  });

  describe("getCustomCategories", () => {
    it("returns empty array when no custom queries", async () => {
      const result = await manageQueriesModule.getCustomCategories();

      expect(result).toEqual([]);
    });

    it("returns categories from storage", async () => {
      const storedQueries = {
        version: 1,
        categories: [
          {
            id: "cat-1",
            name: "Category 1",
            queries: [],
          },
          {
            id: "cat-2",
            name: "Category 2",
            queries: [],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(storedQueries),
      });

      const result = await manageQueriesModule.getCustomCategories();

      expect(result).toHaveLength(2);
      expect(result[0]?.name).toBe("Category 1");
      expect(result[1]?.name).toBe("Category 2");
    });
  });

  describe("openManageQueries", () => {
    beforeEach(() => {
      manageQueriesModule.initManageQueries();
    });

    it("shows the modal", async () => {
      await manageQueriesModule.openManageQueries();

      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.hasAttribute("hidden")).toBe(false);
    });

    it("renders tree view with toolbar", async () => {
      await manageQueriesModule.openManageQueries();

      const toolbar = document.querySelector(".query-manager-toolbar");
      expect(toolbar).not.toBeNull();
    });

    it("renders filter input", async () => {
      await manageQueriesModule.openManageQueries();

      const filterInput = document.getElementById("query-manager-filter");
      expect(filterInput).not.toBeNull();
      expect(filterInput?.getAttribute("placeholder")).toBe("Filter queries...");
    });

    it("renders add category button", async () => {
      await manageQueriesModule.openManageQueries();

      const addCategoryBtn = document.querySelector('[data-action="add-category"]');
      expect(addCategoryBtn).not.toBeNull();
      expect(addCategoryBtn?.textContent).toContain("Category");
    });

    it("renders load example button", async () => {
      await manageQueriesModule.openManageQueries();

      const loadExampleBtn = document.querySelector('[data-action="load-example"]');
      expect(loadExampleBtn).not.toBeNull();
    });

    it("renders save all button", async () => {
      await manageQueriesModule.openManageQueries();

      const saveBtn = document.querySelector('[data-action="save-all"]');
      expect(saveBtn).not.toBeNull();
    });

    it("shows empty state when no queries", async () => {
      await manageQueriesModule.openManageQueries();

      const emptyState = document.querySelector(".query-manager-empty");
      expect(emptyState).not.toBeNull();
      expect(emptyState?.textContent).toContain("No custom queries yet");
    });

    it("shows file location hint", async () => {
      await manageQueriesModule.openManageQueries();

      const hint = document.querySelector(".query-manager-hint");
      expect(hint).not.toBeNull();
    });
  });

  describe("closeManageQueries", () => {
    beforeEach(() => {
      manageQueriesModule.initManageQueries();
    });

    it("hides the modal", async () => {
      await manageQueriesModule.openManageQueries();
      manageQueriesModule.closeManageQueries();

      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.hasAttribute("hidden")).toBe(true);
    });
  });

  describe("Modal interactions", () => {
    beforeEach(() => {
      manageQueriesModule.initManageQueries();
    });

    it("closes modal when clicking backdrop", async () => {
      await manageQueriesModule.openManageQueries();

      const modal = document.getElementById("manage-queries-modal");
      modal?.dispatchEvent(
        new MouseEvent("click", {
          bubbles: true,
        })
      );

      expect(modal?.hasAttribute("hidden")).toBe(true);
    });

    it("closes modal when clicking close button", async () => {
      await manageQueriesModule.openManageQueries();

      const closeBtn = document.querySelector('[data-action="close"]') as HTMLElement;
      closeBtn?.click();

      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.hasAttribute("hidden")).toBe(true);
    });

    it("loads example queries when clicking load example", async () => {
      await manageQueriesModule.openManageQueries();

      const loadBtn = document.querySelector('[data-action="load-example"]') as HTMLElement;
      loadBtn?.click();

      // After loading example, should show categories
      const categoryHeaders = document.querySelectorAll(".query-manager-category-header");
      expect(categoryHeaders.length).toBeGreaterThan(0);
    });

    it("opens create category form when clicking add category", async () => {
      await manageQueriesModule.openManageQueries();

      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();

      const title = document.getElementById("manage-queries-title");
      expect(title?.textContent).toBe("Create Category");

      const nameInput = document.getElementById("category-name-input");
      expect(nameInput).not.toBeNull();
    });

    it("returns to tree view when canceling category edit", async () => {
      await manageQueriesModule.openManageQueries();

      // Open create category form
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();

      // Cancel
      const cancelBtn = document.querySelector('[data-action="cancel-edit"]') as HTMLElement;
      cancelBtn?.click();

      const title = document.getElementById("manage-queries-title");
      expect(title?.textContent).toBe("Manage Custom Queries");
    });
  });

  describe("Category CRUD operations", () => {
    beforeEach(() => {
      manageQueriesModule.initManageQueries();
    });

    it("creates a new category", async () => {
      await manageQueriesModule.openManageQueries();

      // Open create category form
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();

      // Fill in name
      const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
      nameInput.value = "Test Category";

      // Save
      const saveBtn = document.querySelector('[data-action="save-category"]') as HTMLElement;
      saveBtn?.click();

      // Should be back to tree view with new category
      const categoryHeaders = document.querySelectorAll(".query-manager-category-header");
      expect(categoryHeaders.length).toBe(1);

      const categoryName = document.querySelector(".query-manager-category-name");
      expect(categoryName?.textContent).toBe("Test Category");
    });

    it("shows validation error when category name is empty", async () => {
      await manageQueriesModule.openManageQueries();

      // Open create category form
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();

      // Try to save without name
      const saveBtn = document.querySelector('[data-action="save-category"]') as HTMLElement;
      saveBtn?.click();

      // Should show error
      const error = document.querySelector(".query-error");
      expect(error).not.toBeNull();
      expect(error?.textContent).toContain("Name is required");
    });

    it("edits an existing category", async () => {
      await manageQueriesModule.openManageQueries();

      // First create a category
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();
      const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
      nameInput.value = "Original Name";
      const saveBtn = document.querySelector('[data-action="save-category"]') as HTMLElement;
      saveBtn?.click();

      // Now edit it - get the edit button that's inside the category header
      const editBtn = document.querySelector(
        '.query-manager-category-header [data-action="edit-category"]'
      ) as HTMLElement;
      editBtn?.click();

      const editInput = document.getElementById("category-name-input") as HTMLInputElement;
      expect(editInput.value).toBe("Original Name");

      editInput.value = "Updated Name";
      const saveEditBtn = document.querySelector('[data-action="save-category"]') as HTMLElement;
      saveEditBtn?.click();

      const categoryName = document.querySelector(".query-manager-category-name");
      expect(categoryName?.textContent).toBe("Updated Name");
    });

    it("deletes a category", async () => {
      await manageQueriesModule.openManageQueries();

      // Create a category
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();
      const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
      nameInput.value = "To Delete";
      const saveBtn = document.querySelector('[data-action="save-category"]') as HTMLElement;
      saveBtn?.click();

      // Verify category exists
      expect(document.querySelectorAll(".query-manager-category-header").length).toBe(1);

      // Delete it (no queries, so no confirmation needed)
      const deleteBtn = document.querySelector(
        '.query-manager-category-header [data-action="delete-category"]'
      ) as HTMLElement;
      deleteBtn?.click();

      // Category should be gone
      expect(document.querySelectorAll(".query-manager-category-header").length).toBe(0);
    });
  });

  describe("Query CRUD operations", () => {
    beforeEach(async () => {
      manageQueriesModule.initManageQueries();
      await manageQueriesModule.openManageQueries();

      // Create a category first
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();
      const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
      nameInput.value = "Test Category";
      const saveBtn = document.querySelector('[data-action="save-category"]') as HTMLElement;
      saveBtn?.click();
    });

    it("creates a new query in a category", async () => {
      // Click add query button inside the category
      const addQueryBtn = document.querySelector('[data-action="add-query"]') as HTMLElement;
      addQueryBtn?.click();

      const title = document.getElementById("manage-queries-title");
      expect(title?.textContent).toBe("Create Query");

      // Fill in form
      const nameInput = document.getElementById("query-name-input") as HTMLInputElement;
      nameInput.value = "Test Query";

      const descInput = document.getElementById("query-desc-input") as HTMLInputElement;
      descInput.value = "A test query";

      const queryInput = document.getElementById("query-cypher-input") as HTMLTextAreaElement;
      queryInput.value = "MATCH (n) RETURN n";

      // Save
      const saveBtn = document.querySelector('[data-action="save-query"]') as HTMLElement;
      saveBtn?.click();

      // Verify query appears in tree
      const queryName = document.querySelector(".query-manager-query-name");
      expect(queryName?.textContent).toBe("Test Query");

      const queryDesc = document.querySelector(".query-manager-query-desc");
      expect(queryDesc?.textContent).toBe("A test query");
    });

    it("shows validation error when query name is empty", async () => {
      const addQueryBtn = document.querySelector('[data-action="add-query"]') as HTMLElement;
      addQueryBtn?.click();

      // Try to save without filling form
      const saveBtn = document.querySelector('[data-action="save-query"]') as HTMLElement;
      saveBtn?.click();

      const error = document.querySelector(".query-error");
      expect(error).not.toBeNull();
      expect(error?.textContent).toContain("Name is required");
    });

    it("shows validation error when query cypher is empty", async () => {
      const addQueryBtn = document.querySelector('[data-action="add-query"]') as HTMLElement;
      addQueryBtn?.click();

      // Fill name but not query
      const nameInput = document.getElementById("query-name-input") as HTMLInputElement;
      nameInput.value = "Test Query";

      const saveBtn = document.querySelector('[data-action="save-query"]') as HTMLElement;
      saveBtn?.click();

      const error = document.querySelector(".query-error");
      expect(error).not.toBeNull();
      expect(error?.textContent).toContain("Query is required");
    });

    it("edits an existing query", async () => {
      // Create a query first
      const addQueryBtn = document.querySelector('[data-action="add-query"]') as HTMLElement;
      addQueryBtn?.click();
      (document.getElementById("query-name-input") as HTMLInputElement).value = "Original";
      (document.getElementById("query-cypher-input") as HTMLTextAreaElement).value = "MATCH (n) RETURN n";
      (document.querySelector('[data-action="save-query"]') as HTMLElement)?.click();

      // Edit it - get the edit button inside the query row
      const editBtn = document.querySelector('.query-manager-query [data-action="edit-query"]') as HTMLElement;
      editBtn?.click();

      const nameInput = document.getElementById("query-name-input") as HTMLInputElement;
      expect(nameInput.value).toBe("Original");

      nameInput.value = "Updated";
      (document.querySelector('[data-action="save-query"]') as HTMLElement)?.click();

      const queryName = document.querySelector(".query-manager-query-name");
      expect(queryName?.textContent).toBe("Updated");
    });

    it("duplicates a query", async () => {
      // Create a query first
      const addQueryBtn = document.querySelector('[data-action="add-query"]') as HTMLElement;
      addQueryBtn?.click();
      (document.getElementById("query-name-input") as HTMLInputElement).value = "Original Query";
      (document.getElementById("query-desc-input") as HTMLInputElement).value = "Description";
      (document.getElementById("query-cypher-input") as HTMLTextAreaElement).value = "MATCH (n) RETURN n";
      (document.querySelector('[data-action="save-query"]') as HTMLElement)?.click();

      // Duplicate it - get the duplicate button inside the query row
      const duplicateBtn = document.querySelector(
        '.query-manager-query [data-action="duplicate-query"]'
      ) as HTMLElement;
      duplicateBtn?.click();

      // Should open create form with copied values
      const title = document.getElementById("manage-queries-title");
      expect(title?.textContent).toBe("Create Query");

      const nameInput = document.getElementById("query-name-input") as HTMLInputElement;
      expect(nameInput.value).toBe("Original Query (copy)");

      const descInput = document.getElementById("query-desc-input") as HTMLInputElement;
      expect(descInput.value).toBe("Description");

      const queryInput = document.getElementById("query-cypher-input") as HTMLTextAreaElement;
      expect(queryInput.value).toBe("MATCH (n) RETURN n");
    });

    it("deletes a query", async () => {
      // Create a query first
      const addQueryBtn = document.querySelector('[data-action="add-query"]') as HTMLElement;
      addQueryBtn?.click();
      (document.getElementById("query-name-input") as HTMLInputElement).value = "To Delete";
      (document.getElementById("query-cypher-input") as HTMLTextAreaElement).value = "MATCH (n) RETURN n";
      (document.querySelector('[data-action="save-query"]') as HTMLElement)?.click();

      // Verify query exists
      expect(document.querySelectorAll(".query-manager-query").length).toBe(1);

      // Delete it - get the delete button inside the query row
      const deleteBtn = document.querySelector('.query-manager-query [data-action="delete-query"]') as HTMLElement;
      deleteBtn?.click();

      // Query should be gone
      expect(document.querySelectorAll(".query-manager-query").length).toBe(0);
    });
  });

  describe("Filtering", () => {
    beforeEach(async () => {
      manageQueriesModule.initManageQueries();
      await manageQueriesModule.openManageQueries();

      // Load example to have some queries to filter
      const loadBtn = document.querySelector('[data-action="load-example"]') as HTMLElement;
      loadBtn?.click();
    });

    it("filters queries by name", () => {
      const filterInput = document.getElementById("query-manager-filter") as HTMLInputElement;
      filterInput.value = "Admin";
      filterInput.dispatchEvent(new Event("input"));

      const queries = document.querySelectorAll(".query-manager-query");
      // Should only show queries matching "Admin"
      expect(queries.length).toBeGreaterThan(0);

      for (const query of queries) {
        const name = query.querySelector(".query-manager-query-name")?.textContent;
        const desc = query.querySelector(".query-manager-query-desc")?.textContent || "";
        expect(name?.toLowerCase().includes("admin") || desc.toLowerCase().includes("admin")).toBe(true);
      }
    });

    it("shows empty state when no queries match filter", () => {
      const filterInput = document.getElementById("query-manager-filter") as HTMLInputElement;
      filterInput.value = "zzznomatchzzz";
      filterInput.dispatchEvent(new Event("input"));

      const emptyState = document.querySelector(".query-manager-empty");
      expect(emptyState).not.toBeNull();
      expect(emptyState?.textContent).toContain("No queries match");
    });

    it("shows query count", () => {
      const countEl = document.querySelector(".query-manager-count");
      expect(countEl).not.toBeNull();
      expect(countEl?.textContent).toMatch(/\d+ quer(y|ies)/);
    });
  });

  describe("Category expand/collapse", () => {
    beforeEach(async () => {
      manageQueriesModule.initManageQueries();
      await manageQueriesModule.openManageQueries();

      // Load example to have categories
      const loadBtn = document.querySelector('[data-action="load-example"]') as HTMLElement;
      loadBtn?.click();
    });

    it("toggles category expansion when clicking expand button", () => {
      // Initially expanded (example queries have expanded: true)
      let content = document.querySelector(".query-manager-category-content");
      expect(content).not.toBeNull();

      // Click the expand button (not the whole header)
      const toggleBtn = document.querySelector('[data-action="toggle-category"]') as HTMLElement;
      toggleBtn?.click();

      // Should be collapsed - content should not exist
      content = document.querySelector(".query-manager-category-content");
      expect(content).toBeNull();

      // Click to expand again
      const toggleBtn2 = document.querySelector('[data-action="toggle-category"]') as HTMLElement;
      toggleBtn2?.click();

      // Should be expanded
      content = document.querySelector(".query-manager-category-content");
      expect(content).not.toBeNull();
    });

    it("shows expand icon rotation based on state", () => {
      // Initially expanded
      let icon = document.querySelector(".query-expand-icon");
      expect(icon?.classList.contains("expanded")).toBe(true);

      // Collapse
      const toggleBtn = document.querySelector('[data-action="toggle-category"]') as HTMLElement;
      toggleBtn?.click();

      // Icon should not have expanded class
      icon = document.querySelector(".query-expand-icon");
      expect(icon?.classList.contains("expanded")).toBe(false);
    });
  });

  describe("Keyboard shortcuts", () => {
    beforeEach(() => {
      manageQueriesModule.initManageQueries();
    });

    it("closes modal on Escape key in tree view", async () => {
      await manageQueriesModule.openManageQueries();

      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));

      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.hasAttribute("hidden")).toBe(true);
    });

    it("returns to tree view on Escape key in edit view", async () => {
      await manageQueriesModule.openManageQueries();

      // Open create category
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();

      // Press Escape
      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));

      // Should be back in tree view, modal still open
      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.hasAttribute("hidden")).toBe(false);

      const title = document.getElementById("manage-queries-title");
      expect(title?.textContent).toBe("Manage Custom Queries");
    });

    it("saves category on Enter key", async () => {
      await manageQueriesModule.openManageQueries();

      // Open create category
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();

      // Fill name
      const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
      nameInput.value = "Enter Test";

      // Press Enter
      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter" }));

      // Should be saved
      const categoryName = document.querySelector(".query-manager-category-name");
      expect(categoryName?.textContent).toBe("Enter Test");
    });
  });

  describe("Saving to storage", () => {
    beforeEach(() => {
      manageQueriesModule.initManageQueries();
    });

    it("saves queries to localStorage when clicking Save All", async () => {
      await manageQueriesModule.openManageQueries();

      // Create a category
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();
      (document.getElementById("category-name-input") as HTMLInputElement).value = "Saved Category";
      (document.querySelector('[data-action="save-category"]') as HTMLElement)?.click();

      // Click Save All
      const saveAllBtn = document.querySelector('[data-action="save-all"]') as HTMLElement;
      saveAllBtn?.click();

      // Wait for async save
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Verify localStorage was called
      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        "admapper_custom_queries",
        expect.stringContaining("Saved Category")
      );
    });

    it("dispatches custom-queries-changed event after save", async () => {
      await manageQueriesModule.openManageQueries();

      const eventHandler = vi.fn();
      window.addEventListener("custom-queries-changed", eventHandler);

      // Create a category and save
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();
      (document.getElementById("category-name-input") as HTMLInputElement).value = "Test";
      (document.querySelector('[data-action="save-category"]') as HTMLElement)?.click();
      (document.querySelector('[data-action="save-all"]') as HTMLElement)?.click();

      // Wait for async operations
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(eventHandler).toHaveBeenCalled();

      window.removeEventListener("custom-queries-changed", eventHandler);
    });

    it("closes modal after successful save", async () => {
      await manageQueriesModule.openManageQueries();

      // Create and save
      const addBtn = document.querySelector('[data-action="add-category"]') as HTMLElement;
      addBtn?.click();
      (document.getElementById("category-name-input") as HTMLInputElement).value = "Test";
      (document.querySelector('[data-action="save-category"]') as HTMLElement)?.click();
      (document.querySelector('[data-action="save-all"]') as HTMLElement)?.click();

      // Wait for async operations
      await new Promise((resolve) => setTimeout(resolve, 50));

      const modal = document.getElementById("manage-queries-modal");
      expect(modal?.hasAttribute("hidden")).toBe(true);
    });
  });

  describe("Schema validation edge cases", () => {
    it("accepts queries with optional description", async () => {
      const storedQueries = {
        version: 1,
        categories: [
          {
            id: "cat-1",
            name: "Category",
            queries: [
              {
                id: "q-1",
                name: "Query Without Description",
                query: "MATCH (n) RETURN n",
                // no description field
              },
            ],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(storedQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result.categories[0]?.queries?.[0]?.name).toBe("Query Without Description");
    });

    it("accepts categories with subcategories", async () => {
      const storedQueries = {
        version: 1,
        categories: [
          {
            id: "parent",
            name: "Parent Category",
            subcategories: [
              {
                id: "child",
                name: "Child Category",
                queries: [
                  {
                    id: "q-1",
                    name: "Nested Query",
                    query: "MATCH (n) RETURN n",
                  },
                ],
              },
            ],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(storedQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result.categories[0]?.subcategories?.[0]?.name).toBe("Child Category");
    });

    it("rejects categories with empty id", async () => {
      const invalidQueries = {
        version: 1,
        categories: [
          {
            id: "", // empty string
            name: "Category",
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(invalidQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({ version: 1, categories: [] });
    });

    it("rejects queries with empty id", async () => {
      const invalidQueries = {
        version: 1,
        categories: [
          {
            id: "cat-1",
            name: "Category",
            queries: [
              {
                id: "", // empty string
                name: "Query",
                query: "MATCH (n) RETURN n",
              },
            ],
          },
        ],
      };
      localStorageMock._setStore({
        admapper_custom_queries: JSON.stringify(invalidQueries),
      });

      const result = await manageQueriesModule.loadCustomQueries();

      expect(result).toEqual({ version: 1, categories: [] });
    });
  });
});
