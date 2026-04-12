/**
 * Edit Node/Edge Tests
 *
 * Tests for the edit node and edge modal functionality,
 * including property parsing, form population, and API interactions.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock dependencies
vi.mock("../../api/client", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    putNoContent: vi.fn(),
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

vi.mock("../../main", () => ({
  appState: {
    navSidebarCollapsed: false,
    detailSidebarCollapsed: false,
    selectedNodeId: null as string | null,
    selectedEdgeId: null as string | null,
  },
}));

import { openEditNode, openEditEdge } from "../../components/add-node-edge";
import { api } from "../../api/client";

describe("Edit Node/Edge", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Register Escape handler (normally done by main.ts init).
    // Inline minimal version to avoid importing main.ts (which pulls in WebGL).
    document.addEventListener("keydown", (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      const modal = document.querySelector<HTMLElement>(".modal-overlay:not([hidden])");
      if (modal) modal.hidden = true;
    });
  });

  describe("openEditNode", () => {
    it("creates and shows the edit node modal", () => {
      openEditNode("node-1", { name: "TestUser", enabled: true });

      const modal = document.getElementById("edit-node-modal");
      expect(modal).toBeTruthy();
      expect(modal!.hidden).toBe(false);
    });

    it("populates form with existing properties", () => {
      openEditNode("node-1", { name: "TestUser", enabled: "true", domain: "CORP.LOCAL" });

      const container = document.getElementById("edit-node-properties");
      expect(container).toBeTruthy();

      const rows = container!.querySelectorAll(".edit-property-row");
      expect(rows.length).toBe(3);

      // Check first property row
      const firstKey = rows[0]!.querySelector(".edit-prop-key") as HTMLInputElement;
      const firstValue = rows[0]!.querySelector(".edit-prop-value") as HTMLInputElement;
      expect(firstKey.value).toBe("name");
      expect(firstValue.value).toBe("TestUser");
    });

    it("shows empty row when properties are empty", () => {
      openEditNode("node-1", {});

      const container = document.getElementById("edit-node-properties")!;
      const rows = container.querySelectorAll(".edit-property-row");
      expect(rows.length).toBe(1);

      const keyInput = rows[0]!.querySelector(".edit-prop-key") as HTMLInputElement;
      expect(keyInput.value).toBe("");
    });

    it("has Save and Cancel buttons", () => {
      openEditNode("node-1", { name: "TestUser" });

      const modal = document.getElementById("edit-node-modal")!;
      const saveBtn = modal.querySelector('[data-action="submit-edit-node"]');
      const cancelBtn = modal.querySelector('[data-action="close"]');
      expect(saveBtn).toBeTruthy();
      expect(cancelBtn).toBeTruthy();
      expect(saveBtn!.textContent!.trim()).toBe("Save");
    });

    it("closes on cancel button click", () => {
      openEditNode("node-1", { name: "TestUser" });

      const modal = document.getElementById("edit-node-modal")!;
      const cancelBtn = modal.querySelector('[data-action="close"]') as HTMLElement;
      cancelBtn.click();

      expect(modal.hidden).toBe(true);
    });

    it("adds a new property row when add button is clicked", () => {
      openEditNode("node-1", { name: "TestUser" });

      const addBtn = document.querySelector('[data-action="add-property"]') as HTMLElement;
      addBtn.click();

      const container = document.getElementById("edit-node-properties")!;
      const rows = container.querySelectorAll(".edit-property-row");
      expect(rows.length).toBe(2);
    });

    it("removes a property row when remove button is clicked", () => {
      openEditNode("node-1", { name: "TestUser", domain: "CORP.LOCAL" });

      const container = document.getElementById("edit-node-properties")!;
      expect(container.querySelectorAll(".edit-property-row").length).toBe(2);

      const removeBtn = container.querySelector('[data-action="remove-property"]') as HTMLElement;
      removeBtn.click();

      expect(container.querySelectorAll(".edit-property-row").length).toBe(1);
    });

    it("calls api.put on save", async () => {
      vi.mocked(api.putNoContent).mockResolvedValueOnce(undefined);

      openEditNode("node-1", { name: "TestUser" });

      const modal = document.getElementById("edit-node-modal")!;
      const saveBtn = modal.querySelector('[data-action="submit-edit-node"]') as HTMLElement;
      saveBtn.click();

      await vi.waitFor(() => {
        expect(api.putNoContent).toHaveBeenCalledWith(
          "/api/graph/nodes/node-1",
          expect.objectContaining({
            properties: expect.objectContaining({ name: "TestUser" }),
          })
        );
      });
    });

    it("closes modal after successful save", async () => {
      vi.mocked(api.putNoContent).mockResolvedValueOnce(undefined);

      openEditNode("node-1", { name: "TestUser" });

      const modal = document.getElementById("edit-node-modal")!;
      const saveBtn = modal.querySelector('[data-action="submit-edit-node"]') as HTMLElement;
      saveBtn.click();

      await vi.waitFor(() => {
        expect(modal.hidden).toBe(true);
      });
    });

    it("shows error on failed save", async () => {
      vi.mocked(api.putNoContent).mockRejectedValueOnce(new Error("Network error"));

      openEditNode("node-1", { name: "TestUser" });

      const modal = document.getElementById("edit-node-modal")!;
      const saveBtn = modal.querySelector('[data-action="submit-edit-node"]') as HTMLElement;
      saveBtn.click();

      await vi.waitFor(() => {
        const errorEl = document.getElementById("edit-node-error");
        expect(errorEl!.hidden).toBe(false);
        expect(errorEl!.textContent).toContain("Network error");
      });
    });

    it("reuses existing modal on second open", () => {
      openEditNode("node-1", { name: "User1" });
      const modal1 = document.getElementById("edit-node-modal");

      // Close
      const cancelBtn = modal1!.querySelector('[data-action="close"]') as HTMLElement;
      cancelBtn.click();

      // Open again with different properties
      openEditNode("node-2", { name: "User2" });
      const modal2 = document.getElementById("edit-node-modal");

      expect(modal1).toBe(modal2);
      expect(modal2!.hidden).toBe(false);

      // Check it shows new properties
      const rows = document.querySelectorAll("#edit-node-properties .edit-property-row");
      const keyInput = rows[0]!.querySelector(".edit-prop-key") as HTMLInputElement;
      expect(keyInput.value).toBe("name");
      const valInput = rows[0]!.querySelector(".edit-prop-value") as HTMLInputElement;
      expect(valInput.value).toBe("User2");
    });
  });

  describe("openEditEdge", () => {
    it("creates and shows the edit edge modal", () => {
      openEditEdge("edge-1", "node-a", "node-b", "MemberOf", { since: "2024" });

      const modal = document.getElementById("edit-edge-modal");
      expect(modal).toBeTruthy();
      expect(modal!.hidden).toBe(false);
    });

    it("shows relationship type in modal title", () => {
      openEditEdge("edge-1", "node-a", "node-b", "AdminTo", {});

      const modal = document.getElementById("edit-edge-modal")!;
      const title = modal.querySelector(".modal-title")!;
      expect(title.textContent).toContain("AdminTo");
    });

    it("populates form with edge properties", () => {
      openEditEdge("edge-1", "node-a", "node-b", "MemberOf", { since: "2024", notes: "test" });

      const container = document.getElementById("edit-edge-properties")!;
      const rows = container.querySelectorAll(".edit-property-row");
      expect(rows.length).toBe(2);
    });

    it("calls api.put with correct URL on save", async () => {
      vi.mocked(api.putNoContent).mockResolvedValueOnce(undefined);

      openEditEdge("edge-1", "source-id", "target-id", "MemberOf", { notes: "test" });

      const modal = document.getElementById("edit-edge-modal")!;
      const saveBtn = modal.querySelector('[data-action="submit-edit-edge"]') as HTMLElement;
      saveBtn.click();

      await vi.waitFor(() => {
        expect(api.putNoContent).toHaveBeenCalledWith(
          "/api/graph/relationships/source-id/target-id/MemberOf",
          expect.objectContaining({
            properties: expect.objectContaining({ notes: "test" }),
          })
        );
      });
    });

    it("closes on Escape key", () => {
      openEditEdge("edge-1", "node-a", "node-b", "MemberOf", {});

      const modal = document.getElementById("edit-edge-modal")!;
      expect(modal.hidden).toBe(false);

      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
      expect(modal.hidden).toBe(true);
    });
  });

  describe("property value parsing", () => {
    it("parses string values correctly in saved properties", async () => {
      vi.mocked(api.putNoContent).mockResolvedValueOnce(undefined);

      openEditNode("parse-test", {});

      // Set up form with various value types
      const container = document.getElementById("edit-node-properties")!;
      container.innerHTML = "";

      const values = [
        ["stringProp", "hello world"],
        ["numberProp", "42"],
        ["boolTrue", "true"],
        ["boolFalse", "false"],
        ["nullProp", "null"],
      ];

      for (const [key, value] of values) {
        const row = document.createElement("div");
        row.className = "edit-property-row";
        row.innerHTML = `
          <input type="text" class="form-input edit-prop-key" value="${key}" />
          <input type="text" class="form-input edit-prop-value" value="${value}" />
          <button type="button" class="btn-icon-sm danger" data-action="remove-property"></button>
        `;
        container.appendChild(row);
      }

      const modal = document.getElementById("edit-node-modal")!;
      const saveBtn = modal.querySelector('[data-action="submit-edit-node"]') as HTMLElement;
      saveBtn.click();

      await vi.waitFor(() => {
        expect(api.putNoContent).toHaveBeenCalledWith(
          "/api/graph/nodes/parse-test",
          expect.objectContaining({
            properties: {
              stringProp: "hello world",
              numberProp: 42,
              boolTrue: true,
              boolFalse: false,
              nullProp: null,
            },
          })
        );
      });
    });

    it("formats object properties as JSON strings", () => {
      openEditNode("obj-test", { nested: { a: 1, b: 2 } });

      const container = document.getElementById("edit-node-properties")!;
      const valueInput = container.querySelector(".edit-prop-value") as HTMLInputElement;
      expect(valueInput.value).toBe('{"a":1,"b":2}');
    });

    it("formats array properties as JSON strings", () => {
      openEditNode("arr-test", { tags: ["admin", "tier0"] });

      const container = document.getElementById("edit-node-properties")!;
      const valueInput = container.querySelector(".edit-prop-value") as HTMLInputElement;
      expect(valueInput.value).toBe('["admin","tier0"]');
    });
  });

  describe("modal closes on overlay click", () => {
    it("closes edit node modal on overlay click", () => {
      openEditNode("overlay-test", { name: "Test" });

      const modal = document.getElementById("edit-node-modal")!;
      expect(modal.hidden).toBe(false);

      // Simulate click on overlay (the modal element itself)
      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: modal });
      modal.dispatchEvent(event);

      expect(modal.hidden).toBe(true);
    });

    it("closes edit edge modal on overlay click", () => {
      openEditEdge("overlay-edge", "a", "b", "MemberOf", {});

      const modal = document.getElementById("edit-edge-modal")!;
      expect(modal.hidden).toBe(false);

      const event = new MouseEvent("click", { bubbles: true });
      Object.defineProperty(event, "target", { value: modal });
      modal.dispatchEvent(event);

      expect(modal.hidden).toBe(true);
    });
  });
});
