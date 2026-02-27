/**
 * Tests for the API client's error handling and HTTP behavior.
 *
 * Uses MSW to mock server responses. These tests verify that the API client:
 * - Correctly throws ApiClientError for non-2xx responses
 * - Handles empty response bodies
 * - Sends JSON bodies in POST requests
 * - Handles network failures gracefully
 *
 * Note: Endpoint-specific integration tests (graph data, search, queries, etc.)
 * are covered by the E2E test suite in /e2e which runs against a real backend.
 */

import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { api, ApiClientError } from "../../api/client";

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
