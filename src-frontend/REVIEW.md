# TypeScript Code Review

## Executive Summary

The ADMapper TypeScript frontend is a modular graph visualization application built with Sigma.js. While the overall architecture is sound, there are opportunities to improve code quality through deduplication, better error handling, and a centralized API layer. This review identifies concrete refactorings and a test plan.

---

## Current Issues

### Code Duplication

| Issue | Files | Impact |
|-------|-------|--------|
| `escapeHtml()` duplicated 4 times | search.ts:378, queries.ts:542, query-history.ts:496, sidebars.ts:358 | Maintenance burden |
| Demo graph generation duplicated | graph/demo.ts:12-85, components/graph-view.ts:143-222 | Confusing, divergence risk |
| API fetch + error handling pattern | 8+ locations across components | Inconsistent error UX |
| Query execution logic | queries.ts:473-520, query-history.ts:169-204 | Bug fix in one missed in other |

### Error Handling Gaps

| File | Lines | Issue |
|------|-------|-------|
| import.ts | 283-312 | `refreshGraphData()` silently logs errors, graph not updated |
| search.ts | 318 | `data.path.map()` assumes structure exists without validation |
| queries.ts | 489-500 | Uses `alert()` for errors - blocks UI |
| queries.ts | 510-512 | TODO comment - incomplete graph loading feature |

### Type Safety Issues

| File | Lines | Issue |
|------|-------|-------|
| ADGraphRenderer.ts | 205 | `as any` cast for edge curve program |
| actions.ts | 51-52 | `@ts-expect-error` for Tauri global |
| Multiple files | - | API responses not validated against interfaces |

### Testability Concerns

1. **Tight DOM coupling** - Components query specific element IDs directly
2. **No API abstraction** - `fetch()` called inline in 8+ places
3. **Mixed concerns** - Event handlers interleaved with business logic
4. **No dependency injection** - Hard to mock for testing

---

## Refactoring Plan

### 1. Create shared utilities module

**Files to create:** `src-frontend/utils/html.ts`, `src-frontend/utils/dom.ts`

Extract duplicated helper functions:

```typescript
// utils/html.ts
export function escapeHtml(str: string): string {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

// utils/dom.ts
export function getElement<T extends HTMLElement>(id: string): T | null {
  return document.getElementById(id) as T | null;
}

export function requireElement<T extends HTMLElement>(id: string): T {
  const el = document.getElementById(id) as T | null;
  if (!el) throw new Error(`Element #${id} not found`);
  return el;
}
```

Remove duplicates from: search.ts, queries.ts, query-history.ts, sidebars.ts

**Priority:** High
**Risk:** Low

---

### 2. Create centralized API client

**File to create:** `src-frontend/api/client.ts`

```typescript
// api/client.ts
export interface ApiError {
  status: number;
  message: string;
}

export class ApiClient {
  async get<T>(url: string): Promise<T> {
    const response = await fetch(url);
    if (!response.ok) {
      const text = await response.text();
      throw { status: response.status, message: text || response.statusText };
    }
    return response.json();
  }

  async post<T>(url: string, body: unknown): Promise<T> {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw { status: response.status, message: text || response.statusText };
    }
    return response.json();
  }

  async delete(url: string): Promise<void> {
    const response = await fetch(url, { method: "DELETE" });
    if (!response.ok) {
      const text = await response.text();
      throw { status: response.status, message: text || response.statusText };
    }
  }
}

export const api = new ApiClient();
```

Update all fetch calls to use this client.

**Priority:** High
**Risk:** Medium (touches many files)

---

### 3. Define API response types

**File to create:** `src-frontend/api/types.ts`

```typescript
// api/types.ts
export interface GraphNode {
  id: string;
  label: string;
  type: string;
  properties?: Record<string, unknown>;
}

export interface GraphEdge {
  source: string;
  target: string;
  type: string;
  properties?: Record<string, unknown>;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface PathStep {
  node: GraphNode;
  edge_type: string | null;
}

export interface PathResponse {
  found: boolean;
  path: PathStep[];
  graph: GraphData;
}

export interface QueryResult {
  headers: string[];
  rows: unknown[][];
}

export interface QueryResponse {
  results: QueryResult;
  graph?: GraphData;
}

export interface QueryHistoryEntry {
  id: string;
  name: string;
  query: string;
  result_count: number | null;
  created_at: string;
}

export interface PaginatedResponse<T> {
  entries: T[];
  total: number;
  page: number;
  per_page: number;
}
```

Use these types throughout the codebase to catch API contract mismatches at compile time.

**Priority:** High
**Risk:** Low

---

### 4. Remove demo graph duplication

**Action:** Delete `graph/demo.ts` and use `graph-view.ts:generateDemoGraph()` as the single source.

The demo.ts file appears unused (no imports found). If it is used, consolidate into one location.

**Priority:** Medium
**Risk:** Low

---

### 5. Extract query execution logic

**File to modify:** `components/queries.ts`

Create a shared function for query execution that both queries.ts and query-history.ts can use:

```typescript
// queries.ts - export this function
export async function executeQuery(
  query: string,
  extractGraph: boolean = false
): Promise<QueryResponse> {
  return api.post<QueryResponse>("/api/graph/query", {
    query,
    extract_graph: extractGraph,
  });
}
```

Update query-history.ts to import and use this function instead of duplicating the logic.

**Priority:** Medium
**Risk:** Low

---

### 6. Improve error display

**File to create:** `src-frontend/utils/errors.ts`

Replace `alert()` calls with a toast/notification system:

```typescript
// utils/errors.ts
export function showError(message: string): void {
  // Use existing status bar or create toast
  const statusBar = document.getElementById("status-bar");
  if (statusBar) {
    statusBar.textContent = `Error: ${message}`;
    statusBar.classList.add("error");
    setTimeout(() => statusBar.classList.remove("error"), 5000);
  } else {
    console.error(message);
  }
}

export function showSuccess(message: string): void {
  const statusBar = document.getElementById("status-bar");
  if (statusBar) {
    statusBar.textContent = message;
  }
}
```

Replace alert() in queries.ts:489 and similar locations.

**Priority:** Medium
**Risk:** Low

---

### 7. Add API response validation

For critical paths, validate API responses before using them:

```typescript
// api/validation.ts
export function isPathResponse(data: unknown): data is PathResponse {
  return (
    typeof data === "object" &&
    data !== null &&
    "found" in data &&
    "path" in data &&
    Array.isArray((data as PathResponse).path)
  );
}
```

Use in search.ts:300-320 before accessing `data.path`:

```typescript
const data = await api.get<unknown>(`/api/graph/path?from=${from}&to=${to}`);
if (!isPathResponse(data)) {
  throw new Error("Invalid path response from server");
}
```

**Priority:** Medium
**Risk:** Low

---

### 8. Fix silent error in refreshGraphData

**File:** `components/import.ts:283-312`

Current code logs errors but doesn't notify the user:

```typescript
// Current (problematic)
} catch (error) {
  console.error("Failed to refresh graph data:", error);
}

// Fixed
} catch (error) {
  console.error("Failed to refresh graph data:", error);
  showError("Failed to load graph data. Please refresh the page.");
}
```

**Priority:** High
**Risk:** Low

---

### 9. Simplify keyboard shortcut matching

**File:** `components/keyboard.ts:95-110`

Current code is repetitive. Simplify:

```typescript
// Current (verbose)
function matchesShortcut(e: KeyboardEvent, shortcut: KeyboardShortcut): boolean {
  if (shortcut.ctrl && !e.ctrlKey) return false;
  if (shortcut.alt && !e.altKey) return false;
  if (shortcut.shift && !e.shiftKey) return false;
  if (shortcut.meta && !e.metaKey) return false;
  // ...
}

// Simplified
function matchesShortcut(e: KeyboardEvent, shortcut: KeyboardShortcut): boolean {
  const modifiers = ["ctrl", "alt", "shift", "meta"] as const;
  for (const mod of modifiers) {
    if (shortcut[mod] && !e[`${mod}Key`]) return false;
  }
  return e.key.toLowerCase() === shortcut.key.toLowerCase();
}
```

**Priority:** Low
**Risk:** Low

---

### 10. Remove unnecessary abstractions

**File:** `graph/theme.ts:128-140`

The `getThemeColors()` wrapper adds indirection without value. Consider inlining where used or keeping the direct record access.

Similarly, review `graph-view.ts:108-125` - `setLayout()` is a thin wrapper that could be inlined into the action handler.

**Priority:** Low
**Risk:** Low

---

## Test Plan

### Unit Tests

Test pure functions in isolation. These don't require DOM or network.

**File to create:** `src-frontend/__tests__/utils.test.ts`

| Function | File | Test Cases |
|----------|------|------------|
| `escapeHtml()` | utils/html.ts | Empty string, special chars (<, >, &, ", '), nested tags, unicode |
| `assignEdgeCurvatures()` | graph/ADGraph.ts:94-126 | Single edge, bidirectional edges, 3+ parallel edges, self-loops |
| `applyHierarchicalLayout()` | graph/layout.ts:119-157 | Linear chain, tree, diamond, cycle, isolated nodes |
| `matchesShortcut()` | keyboard.ts:95-110 | All modifier combos, case sensitivity, special keys |
| `mapNodeType()` | import.ts:315-340 | All BH types (User, Computer, Group, Domain, GPO, OU, Container) |
| `mapEdgeType()` | import.ts:345-370 | All edge types (MemberOf, AdminTo, HasSession, etc.) |

**Example test:**

```typescript
// __tests__/graph.test.ts
import { describe, it, expect } from "vitest";
import Graph from "graphology";
import { assignEdgeCurvatures } from "../graph/ADGraph";

describe("assignEdgeCurvatures", () => {
  it("assigns no curvature to single edges", () => {
    const graph = new Graph({ multi: true, type: "directed" });
    graph.addNode("a");
    graph.addNode("b");
    graph.addEdge("a", "b", { type: "MemberOf" });

    assignEdgeCurvatures(graph);

    const attrs = graph.getEdgeAttributes(graph.edges()[0]);
    expect(attrs.curvature).toBe(0);
  });

  it("assigns opposite curvatures to bidirectional edges", () => {
    const graph = new Graph({ multi: true, type: "directed" });
    graph.addNode("a");
    graph.addNode("b");
    const e1 = graph.addEdge("a", "b", { type: "MemberOf" });
    const e2 = graph.addEdge("b", "a", { type: "AdminTo" });

    assignEdgeCurvatures(graph);

    const c1 = graph.getEdgeAttribute(e1, "curvature");
    const c2 = graph.getEdgeAttribute(e2, "curvature");
    expect(c1).not.toBe(c2);
    expect(c1 + c2).toBeCloseTo(0); // Opposite curvatures
  });

  it("distributes curvatures evenly for multiple parallel edges", () => {
    const graph = new Graph({ multi: true, type: "directed" });
    graph.addNode("a");
    graph.addNode("b");
    graph.addEdge("a", "b", { type: "MemberOf" });
    graph.addEdge("a", "b", { type: "AdminTo" });
    graph.addEdge("a", "b", { type: "HasSession" });

    assignEdgeCurvatures(graph);

    const curvatures = graph.edges().map((e) => graph.getEdgeAttribute(e, "curvature"));
    // All different
    expect(new Set(curvatures).size).toBe(3);
  });
});
```

---

### Integration Tests

Test API interactions with real server responses. Use a test server or MSW (Mock Service Worker) to intercept requests.

**File to create:** `src-frontend/__tests__/api.integration.test.ts`

| Scenario | Endpoint | Test Cases |
|----------|----------|------------|
| Graph loading | GET /api/graph/all | Empty graph, populated graph, server error (500), timeout |
| Search | GET /api/graph/search | No results, partial match, special characters, server error |
| Path finding | GET /api/graph/path | Direct path, multi-hop, no path exists, invalid node ID, server error |
| Query execution | POST /api/graph/query | Valid CozoDB, syntax error, extract_graph=true, empty results, server error |
| Query history | GET/POST/DELETE /api/query-history | CRUD operations, pagination, concurrent modifications |
| Import | POST /api/import | Single file, multiple files, invalid format, upload error, SSE progress |

**Example integration test:**

```typescript
// __tests__/api.integration.test.ts
import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { api } from "../api/client";

const server = setupServer();

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("API Client", () => {
  describe("search", () => {
    it("returns matching nodes", async () => {
      server.use(
        http.get("/api/graph/search", ({ request }) => {
          const url = new URL(request.url);
          const query = url.searchParams.get("q");
          if (query === "admin") {
            return HttpResponse.json([
              { id: "user-1", label: "admin@corp.local", type: "User" },
            ]);
          }
          return HttpResponse.json([]);
        })
      );

      const results = await api.get<GraphNode[]>("/api/graph/search?q=admin");
      expect(results).toHaveLength(1);
      expect(results[0].label).toBe("admin@corp.local");
    });

    it("handles server errors gracefully", async () => {
      server.use(
        http.get("/api/graph/search", () => {
          return HttpResponse.json({ error: "Database error" }, { status: 500 });
        })
      );

      await expect(api.get("/api/graph/search?q=test")).rejects.toMatchObject({
        status: 500,
      });
    });

    it("handles network failures", async () => {
      server.use(
        http.get("/api/graph/search", () => {
          return HttpResponse.error();
        })
      );

      await expect(api.get("/api/graph/search?q=test")).rejects.toThrow();
    });
  });

  describe("path finding", () => {
    it("returns path with edges", async () => {
      server.use(
        http.get("/api/graph/path", () => {
          return HttpResponse.json({
            found: true,
            path: [
              { node: { id: "a", label: "A", type: "User" }, edge_type: "MemberOf" },
              { node: { id: "b", label: "B", type: "Group" }, edge_type: null },
            ],
            graph: { nodes: [], edges: [] },
          });
        })
      );

      const result = await api.get<PathResponse>("/api/graph/path?from=a&to=b");
      expect(result.found).toBe(true);
      expect(result.path).toHaveLength(2);
    });

    it("handles no path found", async () => {
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
      expect(result.path).toHaveLength(0);
    });
  });

  describe("query execution", () => {
    it("executes valid CozoDB queries", async () => {
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
        query: "?[x] := x = 1 + 1",
        extract_graph: false,
      });
      expect(result.results.rows[0][0]).toBe(2);
    });

    it("returns 400 for invalid syntax", async () => {
      server.use(
        http.post("/api/graph/query", () => {
          return HttpResponse.json(
            { error: "Parse error: unexpected token" },
            { status: 400 }
          );
        })
      );

      await expect(
        api.post("/api/graph/query", { query: "invalid syntax", extract_graph: false })
      ).rejects.toMatchObject({ status: 400 });
    });
  });
});
```

---

### E2E Tests (Optional)

For critical user flows, consider Playwright tests:

1. **Import flow**: Upload BloodHound file, verify progress, verify graph loads
2. **Search flow**: Type query, click result, verify node selected
3. **Path finding**: Select two nodes, find path, verify path highlighted
4. **Query panel**: Run query, verify results table, verify graph extraction

---

## Implementation Order

1. **Create utils/html.ts** - Quick win, removes 4 duplicates
2. **Create api/types.ts** - Foundation for type safety
3. **Create api/client.ts** - Foundation for consistent error handling
4. **Fix refreshGraphData error handling** - High-impact bug fix
5. **Set up test framework** - Vitest + MSW
6. **Write unit tests** - Start with assignEdgeCurvatures, layout
7. **Write integration tests** - Cover API layer
8. **Extract query execution** - Medium impact deduplication
9. **Add API validation** - Prevent runtime errors from API changes
10. **Remove demo.ts duplicate** - Cleanup

---

## Dependencies

For testing:
```json
{
  "devDependencies": {
    "vitest": "^1.0.0",
    "@vitest/coverage-v8": "^1.0.0",
    "msw": "^2.0.0",
    "happy-dom": "^12.0.0"
  }
}
```

Vitest config:
```typescript
// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "happy-dom",
    include: ["src-frontend/__tests__/**/*.test.ts"],
    coverage: {
      provider: "v8",
      include: ["src-frontend/**/*.ts"],
      exclude: ["src-frontend/__tests__/**"],
    },
  },
});
```
