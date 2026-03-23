/**
 * Centralized API Client
 *
 * Provides consistent error handling and response parsing for all API calls.
 * Automatically detects Tauri environment and uses IPC commands instead of HTTP.
 */

import type { ApiError } from "./types";

// Tauri types
declare global {
  interface Window {
    __TAURI__?: {
      core: {
        invoke: <T>(cmd: string, args?: Record<string, unknown>) => Promise<T>;
      };
      event: {
        listen: <T>(event: string, handler: (event: { payload: T }) => void) => Promise<() => void>;
      };
    };
    __TAURI_PLUGIN_DIALOG__?: {
      open: (options: {
        multiple?: boolean;
        directory?: boolean;
        filters?: Array<{ name: string; extensions: string[] }>;
        title?: string;
      }) => Promise<string | string[] | null>;
      save: (options: {
        defaultPath?: string;
        filters?: Array<{ name: string; extensions: string[] }>;
        title?: string;
      }) => Promise<string | null>;
    };
  }
}

/**
 * Check if running in Tauri environment.
 */
export function isRunningInTauri(): boolean {
  return typeof window !== "undefined" && !!window.__TAURI__;
}

// Internal alias for backward compatibility within this module
const isTauri = isRunningInTauri;

/**
 * Custom error class for API errors.
 * Contains the HTTP status code and error message.
 */
export class ApiClientError extends Error {
  constructor(
    public readonly status: number,
    message: string
  ) {
    super(message);
    this.name = "ApiClientError";
  }

  toApiError(): ApiError {
    return {
      status: this.status,
      message: this.message,
    };
  }
}

/**
 * URL to Tauri command mapping.
 * Maps HTTP method + URL pattern to Tauri command name.
 */
const COMMAND_MAPPING: Record<string, string> = {
  // Database
  "GET /api/database/status": "database_status",
  "GET /api/database/supported": "database_supported",
  "POST /api/database/connect": "database_connect",
  "POST /api/database/disconnect": "database_disconnect",
  // Graph stats
  "GET /api/graph/stats": "graph_stats",
  "GET /api/graph/detailed-stats": "graph_detailed_stats",
  "POST /api/graph/clear": "graph_clear",
  "POST /api/graph/clear-disabled": "graph_clear_disabled",
  // Graph data
  "GET /api/graph/nodes": "graph_nodes",
  "GET /api/graph/relationships": "graph_edges",
  "GET /api/graph/all": "graph_all",
  "GET /api/graph/search": "graph_search",
  // Node operations
  "GET /api/graph/node/:id": "node_get",
  "GET /api/graph/node/:id/counts": "node_counts",
  "GET /api/graph/node/:id/connections/:direction": "node_connections",
  "GET /api/graph/node/:id/status": "node_status",
  "POST /api/graph/node/:id/owned": "node_set_owned",
  // Path finding
  "GET /api/graph/path": "graph_path",
  "GET /api/graph/paths-to-da": "paths_to_domain_admins",
  // Insights
  "GET /api/graph/insights": "graph_insights",
  "GET /api/graph/relationship-types": "graph_edge_types",
  "GET /api/graph/node-types": "graph_node_types",
  "GET /api/graph/choke-points": "graph_choke_points",
  // Tier management
  "POST /api/graph/batch-set-tier": "batch_set_tier",
  "GET /api/graph/tier-violations": "tier_violations",
  "POST /api/graph/compute-effective-tiers": "compute_effective_tiers",
  // Mutations
  "POST /api/graph/node": "add_node",
  "POST /api/graph/relationship": "add_edge",
  "DELETE /api/graph/nodes/:id": "delete_node",
  "DELETE /api/graph/relationships/:source/:target/:rel_type": "delete_edge",
  // Query
  "POST /api/graph/query": "graph_query",
  // Query history
  "GET /api/query-history": "get_query_history",
  "POST /api/query-history": "add_query_history",
  "DELETE /api/query-history/:id": "delete_query_history",
  "POST /api/query-history/clear": "clear_query_history",
  // Settings
  "GET /api/settings": "get_settings",
  "PUT /api/settings": "update_settings",
  // File browser
  "GET /api/browse": "browse_directory",
  // Data generation
  "POST /api/graph/generate": "generate_data",
  // Health
  "GET /api/health": "health_check",
  // Query activity
  "GET /api/query/activity": "get_query_activity",
};

/**
 * Normalize a URL for command lookup.
 * Replaces path parameters with placeholders.
 */
function normalizeUrl(url: string): string {
  // Remove query string - split always returns at least one element
  const normalized = url.split("?")[0] ?? url;

  // Replace path params with placeholders
  // /api/graph/node/abc123 -> /api/graph/node/:id
  return normalized
    .replace(/\/api\/graph\/node\/([^/]+)\/connections\/([^/]+)/, "/api/graph/node/:id/connections/:direction")
    .replace(/\/api\/graph\/node\/([^/]+)\/counts/, "/api/graph/node/:id/counts")
    .replace(/\/api\/graph\/node\/([^/]+)\/status/, "/api/graph/node/:id/status")
    .replace(/\/api\/graph\/node\/([^/]+)\/owned/, "/api/graph/node/:id/owned")
    .replace(/\/api\/graph\/node\/([^/]+)$/, "/api/graph/node/:id")
    .replace(/\/api\/graph\/nodes\/([^/]+)$/, "/api/graph/nodes/:id")
    .replace(
      /\/api\/graph\/relationships\/([^/]+)\/([^/]+)\/([^/]+)$/,
      "/api/graph/relationships/:source/:target/:rel_type"
    )
    .replace(/\/api\/query-history\/([^/]+)$/, "/api/query-history/:id");
}

/**
 * Commands where the body should be wrapped in a named parameter
 * instead of spreading flat into args.
 */
const BODY_WRAPPER_MAP: Record<string, string> = {
  update_settings: "settings",
};

/**
 * Extract arguments from URL and body for Tauri command.
 */
function extractArgs(url: string, body?: unknown, command?: string): Record<string, unknown> {
  const args: Record<string, unknown> = {};

  // Extract query params
  const queryStart = url.indexOf("?");
  if (queryStart !== -1) {
    const params = new URLSearchParams(url.slice(queryStart + 1));
    for (const [key, value] of params) {
      // Convert known numeric params
      if (key === "limit" || key === "page" || key === "per_page") {
        args[key === "per_page" ? "per_page" : key] = parseInt(value, 10);
      } else {
        args[key] = value;
      }
    }
  }

  // Extract path params
  // Node ID from /api/graph/node/:id/...
  const nodeMatch = url.match(/\/api\/graph\/node\/([^/?]+)/);
  if (nodeMatch?.[1]) {
    args.id = decodeURIComponent(nodeMatch[1]);
  }

  // Node ID from /api/graph/nodes/:id (delete endpoint)
  const nodesMatch = url.match(/\/api\/graph\/nodes\/([^/?]+)$/);
  if (nodesMatch?.[1]) {
    args.id = decodeURIComponent(nodesMatch[1]);
  }

  // Edge params from /api/graph/relationships/:source/:target/:rel_type (delete endpoint)
  const edgeMatch = url.match(/\/api\/graph\/relationships\/([^/]+)\/([^/]+)\/([^/?]+)$/);
  if (edgeMatch?.[1] && edgeMatch?.[2] && edgeMatch?.[3]) {
    args.source = decodeURIComponent(edgeMatch[1]);
    args.target = decodeURIComponent(edgeMatch[2]);
    args.rel_type = decodeURIComponent(edgeMatch[3]);
  }

  // Direction from /connections/:direction
  const dirMatch = url.match(/\/connections\/([^/?]+)/);
  if (dirMatch?.[1]) {
    args.direction = decodeURIComponent(dirMatch[1]);
  }

  // Query history ID
  const historyMatch = url.match(/\/api\/query-history\/([^/?]+)$/);
  if (historyMatch?.[1]) {
    args.id = decodeURIComponent(historyMatch[1]);
  }

  // Merge body (for POST/PUT)
  if (body && typeof body === "object") {
    // Some commands expect the body wrapped in a named parameter
    const wrapper = command ? BODY_WRAPPER_MAP[command] : undefined;
    if (wrapper) {
      args[wrapper] = body;
    } else {
      Object.assign(args, body);
    }
  }

  return args;
}

/**
 * Invoke a Tauri command based on HTTP method and URL.
 */
async function invokeFromUrl<T>(method: string, url: string, body?: unknown): Promise<T> {
  const { invoke } = window.__TAURI__!.core;

  const normalized = normalizeUrl(url);
  const key = `${method} ${normalized}`;
  const command = COMMAND_MAPPING[key];

  if (!command) {
    throw new ApiClientError(501, `No Tauri command mapping for: ${key}`);
  }

  const args = extractArgs(url, body, command);

  try {
    return await invoke<T>(command, args);
  } catch (e) {
    // Tauri commands return error strings
    throw new ApiClientError(500, String(e));
  }
}

/**
 * Centralized API client for making HTTP requests.
 * All methods throw ApiClientError on failure.
 * All methods accept an optional AbortSignal for request cancellation.
 */
export class ApiClient {
  /**
   * Make a GET request and parse JSON response.
   * @param url - The URL to fetch
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async get<T>(url: string, signal?: AbortSignal): Promise<T> {
    if (isTauri()) {
      return invokeFromUrl<T>("GET", url);
    }

    const response = await fetch(url, { signal: signal ?? null });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Make a POST request with JSON body.
   * @param url - The URL to post to
   * @param body - The request body (will be JSON stringified)
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async post<T>(url: string, body: unknown, signal?: AbortSignal): Promise<T> {
    if (isTauri()) {
      return invokeFromUrl<T>("POST", url, body);
    }

    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: signal ?? null,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Make a PUT request with JSON body.
   * @param url - The URL to put to
   * @param body - The request body (will be JSON stringified)
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async put<T>(url: string, body: unknown, signal?: AbortSignal): Promise<T> {
    if (isTauri()) {
      return invokeFromUrl<T>("PUT", url, body);
    }

    const response = await fetch(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: signal ?? null,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Make a POST request with JSON body, expecting no content response.
   * @param url - The URL to post to
   * @param body - Optional request body (will be JSON stringified)
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async postNoContent(url: string, body?: unknown, signal?: AbortSignal): Promise<void> {
    if (isTauri()) {
      await invokeFromUrl<unknown>("POST", url, body);
      return;
    }

    const init: RequestInit = {
      method: "POST",
      headers: body ? { "Content-Type": "application/json" } : {},
      signal: signal ?? null,
    };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
    }
    const response = await fetch(url, init);

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }
  }

  /**
   * Make a DELETE request.
   * @param url - The URL to delete
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async delete(url: string, signal?: AbortSignal): Promise<void> {
    if (isTauri()) {
      await invokeFromUrl<unknown>("DELETE", url);
      return;
    }

    const response = await fetch(url, { method: "DELETE", signal: signal ?? null });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }
  }

  /**
   * Upload files using multipart form data.
   * Note: File uploads are not supported in Tauri mode via IPC.
   * Use the HTTP API with a local server for file uploads.
   * @param url - The URL to upload to
   * @param files - The files to upload
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async uploadFiles<T>(url: string, files: FileList | File[], signal?: AbortSignal): Promise<T> {
    if (isTauri()) {
      throw new ApiClientError(501, "File uploads are not supported in Tauri mode. Use the import dialog.");
    }

    const formData = new FormData();
    for (const file of files) {
      formData.append("files", file);
    }

    const response = await fetch(url, {
      method: "POST",
      body: formData,
      signal: signal ?? null,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }
}

/** Singleton API client instance */
export const api = new ApiClient();
