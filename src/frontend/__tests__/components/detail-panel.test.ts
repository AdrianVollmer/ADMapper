/**
 * Detail Panel Tests
 *
 * Integration tests for updateDetailPanel to verify that the full property
 * list is always fetched from the backend, regardless of what partial data
 * is already stored in the graph node attributes.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("../../api/client", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    postNoContent: vi.fn(),
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

const { mockAppState } = vi.hoisted(() => ({
  mockAppState: {
    navSidebarCollapsed: false,
    detailSidebarCollapsed: false,
    selectedNodeId: null as string | null,
    selectedEdgeId: null as string | null,
  },
}));

vi.mock("../../main", () => ({
  appState: mockAppState,
}));

import { updateDetailPanel } from "../../components/sidebars";
import { api } from "../../api/client";

const BASE_NODE_ATTRS = {
  label: "jsmith",
  nodeType: "User",
  x: 0,
  y: 0,
  size: 6,
  color: "#4299e1",
  image: "/icons/user.svg",
};

function setupDom() {
  document.body.innerHTML = `<div id="detail-content"></div>`;
}

function mockApiResponses(fullProperties: Record<string, unknown>) {
  vi.mocked(api.get).mockImplementation((url: string) => {
    if (/\/api\/graph\/node\/[^/]+$/.test(url)) {
      return Promise.resolve({ id: "user1", name: "jsmith", type: "User", properties: fullProperties });
    }
    if (url.includes("/status")) {
      return Promise.resolve({
        owned: false,
        isEnterpriseAdmin: false,
        isDomainAdmin: false,
        tier: -1,
        hasPathToHighTier: false,
      });
    }
    if (url.includes("/counts")) {
      return Promise.resolve({ incoming: 0, outgoing: 0, adminTo: 0, memberOf: 0, members: 0 });
    }
    return Promise.resolve({});
  });
}

describe("updateDetailPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockAppState.selectedNodeId = null;
    setupDom();
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("always fetches full properties from backend, even when attrs.properties is populated", async () => {
    // This is the regression scenario: the graph API returns nodes with only the
    // three summary status fields (owned/enabled/tier) in attrs.properties.
    // The sidebar must still call the node detail endpoint to get all properties.
    const summaryOnlyAttrs = {
      ...BASE_NODE_ATTRS,
      properties: { owned: true, enabled: true, tier: 1 },
    };

    const fullProperties = {
      owned: true,
      enabled: true,
      tier: 1,
      distinguishedname: "CN=jsmith,DC=corp,DC=local",
      objectid: "S-1-5-21-123-456",
      admincount: false,
    };

    mockApiResponses(fullProperties);

    updateDetailPanel("user1", summaryOnlyAttrs);

    // The node detail endpoint must be called regardless of attrs.properties content
    expect(api.get).toHaveBeenCalledWith("/api/graph/node/user1");

    // Wait for the async fetch to resolve and update the DOM
    await vi.waitFor(() => {
      const content = document.getElementById("node-properties-content");
      // More than 3 properties should be shown once full data arrives
      expect(content?.querySelectorAll(".detail-prop").length).toBeGreaterThan(3);
    });

    const content = document.getElementById("node-properties-content");
    // Full properties include values beyond the 3 summary status fields
    expect(content?.innerHTML).toContain("CN=jsmith,DC=corp,DC=local"); // distinguishedname value
    expect(content?.innerHTML).toContain("S-1-5-21-123-456"); // objectid value
  });

  it("fetches full properties when attrs.properties is undefined", async () => {
    // Nodes without any cached properties also trigger a fetch
    const noPropsAttrs = { ...BASE_NODE_ATTRS };

    const fullProperties = {
      enabled: false,
      distinguishedname: "CN=jsmith,DC=corp,DC=local",
    };

    mockApiResponses(fullProperties);

    updateDetailPanel("user1", noPropsAttrs);

    expect(api.get).toHaveBeenCalledWith("/api/graph/node/user1");

    await vi.waitFor(() => {
      const content = document.getElementById("node-properties-content");
      expect(content?.innerHTML).toContain("CN=jsmith,DC=corp,DC=local");
    });
  });

  it("renders loading spinner initially before fetch completes", () => {
    // Properties fetch is async; the panel must show a loading state immediately
    vi.mocked(api.get).mockReturnValue(new Promise(() => {})); // never resolves

    const attrs = {
      ...BASE_NODE_ATTRS,
      properties: { owned: false, enabled: true, tier: 2 },
    };

    updateDetailPanel("user1", attrs);

    const content = document.getElementById("node-properties-content");
    expect(content?.innerHTML).toContain("Loading properties");
  });

  it("renders all properties returned by backend, not just status fields", async () => {
    const attrs = {
      ...BASE_NODE_ATTRS,
      properties: { owned: false, enabled: true, tier: 0 },
    };

    const fullProperties: Record<string, unknown> = {
      owned: false,
      enabled: true,
      tier: 0,
      distinguishedname: "CN=admin,CN=Users,DC=corp,DC=local",
      objectid: "S-1-5-21-999",
      admincount: true,
      lastlogon: "2024-01-15T10:30:00Z",
      pwdlastset: "2023-06-01T08:00:00Z",
    };

    mockApiResponses(fullProperties);

    updateDetailPanel("user1", attrs);

    await vi.waitFor(() => {
      const content = document.getElementById("node-properties-content");
      expect(content?.querySelectorAll(".detail-prop").length).toBeGreaterThan(3);
    });

    const content = document.getElementById("node-properties-content")!;
    // All backend-provided values should appear, not just the 3 summary status fields
    expect(content.innerHTML).toContain("CN=admin,CN=Users,DC=corp,DC=local"); // distinguishedname
    expect(content.innerHTML).toContain("S-1-5-21-999"); // objectid
    expect(content.innerHTML).toContain("2024-01-15T10:30:00Z"); // lastlogon
    expect(content.innerHTML).toContain("2023-06-01T08:00:00Z"); // pwdlastset
  });
});
