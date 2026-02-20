/**
 * Integration tests for the API client.
 *
 * Uses MSW to mock server responses and test error handling.
 */

import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { api, ApiClientError } from "../../api/client";
import type { GraphData, SearchResult, PathResponse, QueryResponse, QueryHistoryResponse } from "../../api/types";

// Create MSW server
const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

// ============================================================================
// GET requests
// ============================================================================

describe("api.get", () => {
  it("returns JSON data on success", async () => {
    server.use(
      http.get("/api/test", () => {
        return HttpResponse.json({ message: "hello" });
      })
    );

    const result = await api.get<{ message: string }>("/api/test");
    expect(result.message).toBe("hello");
  });

  it("throws ApiClientError on 404", async () => {
    server.use(
      http.get("/api/missing", () => {
        return new HttpResponse("Not found", { status: 404 });
      })
    );

    await expect(api.get("/api/missing")).rejects.toMatchObject({
      status: 404,
      message: "Not found",
    });
  });

  it("throws ApiClientError on 500", async () => {
    server.use(
      http.get("/api/error", () => {
        return new HttpResponse("Internal server error", { status: 500 });
      })
    );

    await expect(api.get("/api/error")).rejects.toMatchObject({
      status: 500,
      message: "Internal server error",
    });
  });

  it("handles empty error response body", async () => {
    server.use(
      http.get("/api/empty-error", () => {
        return new HttpResponse(null, { status: 503 });
      })
    );

    try {
      await api.get("/api/empty-error");
      expect.fail("Should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(ApiClientError);
      expect((err as ApiClientError).status).toBe(503);
    }
  });
});

// ============================================================================
// POST requests
// ============================================================================

describe("api.post", () => {
  it("sends JSON body and returns response", async () => {
    server.use(
      http.post("/api/submit", async ({ request }) => {
        const body = await request.json();
        return HttpResponse.json({ received: body });
      })
    );

    const result = await api.post<{ received: unknown }>("/api/submit", {
      name: "test",
    });
    expect(result.received).toEqual({ name: "test" });
  });

  it("throws ApiClientError on 400 Bad Request", async () => {
    server.use(
      http.post("/api/validate", () => {
        return new HttpResponse("Invalid input: name required", { status: 400 });
      })
    );

    await expect(api.post("/api/validate", { invalid: true })).rejects.toMatchObject({
      status: 400,
      message: "Invalid input: name required",
    });
  });
});

// ============================================================================
// DELETE requests
// ============================================================================

describe("api.delete", () => {
  it("succeeds on 204 No Content", async () => {
    server.use(
      http.delete("/api/items/123", () => {
        return new HttpResponse(null, { status: 204 });
      })
    );

    await expect(api.delete("/api/items/123")).resolves.toBeUndefined();
  });

  it("throws ApiClientError on 404", async () => {
    server.use(
      http.delete("/api/items/missing", () => {
        return new HttpResponse("Item not found", { status: 404 });
      })
    );

    await expect(api.delete("/api/items/missing")).rejects.toMatchObject({
      status: 404,
    });
  });
});

// ============================================================================
// Graph endpoints
// ============================================================================

describe("GET /api/graph/all", () => {
  it("returns graph data", async () => {
    const graphData: GraphData = {
      nodes: [
        { id: "user-1", name: "admin@corp.local", type: "User" },
        { id: "group-1", name: "Domain Admins", type: "Group" },
      ],
      edges: [{ source: "user-1", target: "group-1", type: "MemberOf" }],
    };

    server.use(
      http.get("/api/graph/all", () => {
        return HttpResponse.json(graphData);
      })
    );

    const result = await api.get<GraphData>("/api/graph/all");
    expect(result.nodes).toHaveLength(2);
    expect(result.edges).toHaveLength(1);
  });

  it("handles server error", async () => {
    server.use(
      http.get("/api/graph/all", () => {
        return new HttpResponse("Database connection failed", { status: 500 });
      })
    );

    await expect(api.get("/api/graph/all")).rejects.toMatchObject({
      status: 500,
      message: "Database connection failed",
    });
  });
});

// ============================================================================
// Search endpoint
// ============================================================================

describe("GET /api/graph/search", () => {
  it("returns matching nodes", async () => {
    server.use(
      http.get("/api/graph/search", ({ request }) => {
        const url = new URL(request.url);
        const query = url.searchParams.get("q");

        if (query === "admin") {
          return HttpResponse.json([
            { id: "user-1", name: "admin@corp.local", type: "User" },
            { id: "group-1", name: "Domain Admins", type: "Group" },
          ]);
        }

        return HttpResponse.json([]);
      })
    );

    const results = await api.get<SearchResult[]>("/api/graph/search?q=admin");
    expect(results).toHaveLength(2);
    expect(results[0]!.name).toBe("admin@corp.local");
  });

  it("returns empty array for no matches", async () => {
    server.use(
      http.get("/api/graph/search", () => {
        return HttpResponse.json([]);
      })
    );

    const results = await api.get<SearchResult[]>("/api/graph/search?q=nonexistent");
    expect(results).toEqual([]);
  });
});

// ============================================================================
// Path finding endpoint
// ============================================================================

describe("GET /api/graph/path", () => {
  it("returns path when found", async () => {
    const pathResponse: PathResponse = {
      found: true,
      path: [
        { node: { id: "a", name: "A", type: "User" }, edge_type: "MemberOf" },
        { node: { id: "b", name: "B", type: "Group" } },
      ],
      graph: { nodes: [], edges: [] },
    };

    server.use(
      http.get("/api/graph/path", () => {
        return HttpResponse.json(pathResponse);
      })
    );

    const result = await api.get<PathResponse>("/api/graph/path?from=a&to=b");
    expect(result.found).toBe(true);
    expect(result.path).toHaveLength(2);
  });

  it("returns found=false when no path exists", async () => {
    server.use(
      http.get("/api/graph/path", () => {
        return HttpResponse.json({
          found: false,
          path: [],
          graph: { nodes: [], edges: [] },
        });
      })
    );

    const result = await api.get<PathResponse>("/api/graph/path?from=a&to=z");
    expect(result.found).toBe(false);
    expect(result.path).toEqual([]);
  });
});

// ============================================================================
// Query endpoint
// ============================================================================

describe("POST /api/graph/query", () => {
  it("executes valid query", async () => {
    server.use(
      http.post("/api/graph/query", () => {
        return HttpResponse.json({
          results: {
            headers: ["x"],
            rows: [[2]],
          },
        });
      })
    );

    const result = await api.post<QueryResponse>("/api/graph/query", {
      query: "MATCH (n:Node) RETURN n + 1",
      extract_graph: false,
    });

    expect(result.results.rows[0]![0]).toBe(2);
  });

  it("returns 400 for invalid syntax", async () => {
    server.use(
      http.post("/api/graph/query", () => {
        return new HttpResponse("Parse error: unexpected token", { status: 400 });
      })
    );

    await expect(
      api.post("/api/graph/query", {
        query: "invalid syntax",
        extract_graph: false,
      })
    ).rejects.toMatchObject({
      status: 400,
      message: "Parse error: unexpected token",
    });
  });

  it("returns graph when extract_graph=true", async () => {
    server.use(
      http.post("/api/graph/query", () => {
        return HttpResponse.json({
          results: { headers: ["id"], rows: [["user-1"]] },
          graph: {
            nodes: [{ id: "user-1", label: "admin", type: "User" }],
            edges: [],
          },
        });
      })
    );

    const result = await api.post<QueryResponse>("/api/graph/query", {
      query: "MATCH (n:Node) WHERE n.node_type = 'User' RETURN n.object_id",
      extract_graph: true,
    });

    expect(result.graph).toBeDefined();
    expect(result.graph!.nodes).toHaveLength(1);
  });
});

// ============================================================================
// Query history endpoints
// ============================================================================

describe("Query History API", () => {
  it("GET /api/query-history returns paginated results", async () => {
    server.use(
      http.get("/api/query-history", () => {
        return HttpResponse.json({
          entries: [
            {
              id: "1",
              name: "Test Query",
              query: "MATCH (n:Node) RETURN n",
              timestamp: Date.now(),
              result_count: 1,
            },
          ],
          total: 1,
          page: 1,
          per_page: 10,
        });
      })
    );

    const result = await api.get<QueryHistoryResponse>("/api/query-history?page=1&per_page=10");
    expect(result.entries).toHaveLength(1);
    expect(result.total).toBe(1);
  });

  it("POST /api/query-history adds entry", async () => {
    server.use(
      http.post("/api/query-history", async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({
          id: "new-id",
          ...body,
          timestamp: Date.now(),
        });
      })
    );

    const result = await api.post("/api/query-history", {
      name: "New Query",
      query: "MATCH (n:Node) RETURN n",
      result_count: 1,
    });

    expect(result).toHaveProperty("id", "new-id");
  });

  it("DELETE /api/query-history/:id removes entry", async () => {
    server.use(
      http.delete("/api/query-history/123", () => {
        return new HttpResponse(null, { status: 204 });
      })
    );

    await expect(api.delete("/api/query-history/123")).resolves.toBeUndefined();
  });
});

// ============================================================================
// Network error handling
// ============================================================================

describe("network errors", () => {
  it("throws on network failure", async () => {
    server.use(
      http.get("/api/network-fail", () => {
        return HttpResponse.error();
      })
    );

    await expect(api.get("/api/network-fail")).rejects.toThrow();
  });
});

// ============================================================================
// ApiClientError class
// ============================================================================

describe("ApiClientError", () => {
  it("has correct properties", () => {
    const error = new ApiClientError(404, "Not found");
    expect(error.status).toBe(404);
    expect(error.message).toBe("Not found");
    expect(error.name).toBe("ApiClientError");
  });

  it("converts to ApiError", () => {
    const error = new ApiClientError(500, "Server error");
    const apiError = error.toApiError();
    expect(apiError).toEqual({ status: 500, message: "Server error" });
  });

  it("is instanceof Error", () => {
    const error = new ApiClientError(400, "Bad request");
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(ApiClientError);
  });
});
