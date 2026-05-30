/**
 * Tauri API compatibility tests.
 *
 * Ensures that all API calls go through the API client (which handles
 * Tauri IPC routing), and that no component bypasses it with raw fetch().
 *
 * Raw fetch() calls to /api/* endpoints will silently fail in Tauri desktop
 * mode because Tauri serves the frontend from embedded assets, not an HTTP
 * server. The API client detects Tauri and routes through IPC instead.
 */

/// <reference types="node" />

import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";

/** Files that are allowed to use raw fetch() for /api/ endpoints */
const ALLOWED_RAW_FETCH: Record<string, string> = {
  // The API client itself uses fetch for HTTP mode
  "api/client.ts": "API client implementation",
  // Multipart upload is HTTP-only (Tauri uses native import dialog)
  "components/import.ts": "Multipart upload (not supported in Tauri)",
  // Health check already has explicit Tauri branch (uses invoke)
  "main.ts": "Health check with explicit Tauri/HTTP branching",
};

/**
 * Recursively find all .ts files in a directory.
 */
function findTsFiles(dir: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory() && entry.name !== "__tests__" && entry.name !== "node_modules") {
      files.push(...findTsFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts")) {
      files.push(fullPath);
    }
  }
  return files;
}

describe("Tauri API compatibility", () => {
  // Resolve from the project root (vitest runs from workspace root)
  const frontendDir = resolve(process.cwd(), "src/frontend");

  it("no component uses raw fetch() for API endpoints", () => {
    const tsFiles = findTsFiles(frontendDir);
    const violations: string[] = [];

    for (const file of tsFiles) {
      const relPath = relative(frontendDir, file);

      // Skip allowed files
      if (Object.keys(ALLOWED_RAW_FETCH).some((allowed) => relPath === allowed)) {
        continue;
      }

      const content = readFileSync(file, "utf-8");
      const lines = content.split("\n");

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i]!;
        // Match fetch() calls that reference /api/ paths
        if (/\bfetch\s*\(/.test(line) && /["'`]\/api\//.test(line)) {
          violations.push(`${relPath}:${i + 1}: ${line.trim()}`);
        }
      }
    }

    if (violations.length > 0) {
      const message = [
        "Raw fetch() calls to /api/ endpoints found. These will fail in Tauri desktop mode.",
        "Use the api client (import { api } from '../api/client') instead.",
        "",
        "Violations:",
        ...violations.map((v) => `  ${v}`),
        "",
        "To allow a file, add it to ALLOWED_RAW_FETCH in tauri-compat.test.ts.",
      ].join("\n");

      expect.fail(message);
    }
  });

  it("subscribe() calls include filterKey in params", () => {
    // When a channel has a filterKey (e.g., "query_id"), the params passed to
    // subscribe() must include that key. Otherwise, Tauri event filtering silently
    // fails and all events are delivered to all listeners, causing data corruption
    // when multiple subscriptions run in parallel.
    const transportPath = join(frontendDir, "api/transport.ts");
    const transportContent = readFileSync(transportPath, "utf-8");

    // Extract channel definitions with filterKey.
    // Split on "export const" to isolate each channel, then check for filterKey.
    const channels: Record<string, string> = {};
    const blocks = transportContent.split(/(?=export const \w+:\s*ChannelDefinition)/);
    for (const block of blocks) {
      const nameMatch = block.match(/^export const (\w+):\s*ChannelDefinition/);
      const filterMatch = block.match(/filterKey:\s*"(\w+)"/);
      if (nameMatch && filterMatch) {
        channels[nameMatch[1]!] = filterMatch[1]!;
      }
    }

    expect(Object.keys(channels).length).toBeGreaterThan(0);

    // Find all subscribe() calls that reference these channels
    const tsFiles = findTsFiles(frontendDir);
    const violations: string[] = [];

    for (const file of tsFiles) {
      const relPath = relative(frontendDir, file);
      const content = readFileSync(file, "utf-8");

      for (const [channelName, filterKey] of Object.entries(channels)) {
        // Find subscribe(CHANNEL_NAME, { ... }, ...) calls
        const regex = new RegExp(`subscribe\\(\\s*${channelName}\\s*,\\s*\\{([^}]*)\\}`, "g");
        let match;
        while ((match = regex.exec(content)) !== null) {
          const paramsContent = match[1]!;
          // Check that the filterKey (snake_case) is present in the params object
          if (!paramsContent.includes(filterKey)) {
            const lineNum = content.slice(0, match.index).split("\n").length;
            violations.push(
              `${relPath}:${lineNum}: subscribe(${channelName}) missing "${filterKey}" in params (has: {${paramsContent.trim()}})`
            );
          }
        }
      }
    }

    if (violations.length > 0) {
      const message = [
        "subscribe() calls missing filterKey in params. In Tauri mode, events are",
        "broadcast globally and must be filtered client-side. Missing the filterKey",
        "causes all events to be delivered to all listeners.",
        "",
        "Violations:",
        ...violations.map((v) => `  ${v}`),
        "",
        "Fix: add the snake_case filterKey to params, e.g., { queryId, query_id: queryId }",
      ].join("\n");

      expect.fail(message);
    }
  });

  it("extractArgs produces snake_case keys that toCamelCaseKeys converts correctly", () => {
    // The update_edge and delete_edge commands use rel_type from URL path params.
    // Tauri v2 expects camelCase args (rel_type -> relType).
    // Verify the client.ts contains toCamelCaseKeys conversion.
    const clientPath = join(frontendDir, "api/client.ts");
    const content = readFileSync(clientPath, "utf-8");

    expect(content).toContain("toCamelCaseKeys");
    expect(content).toContain("camelArgs");

    // Verify the conversion function exists and handles underscore patterns
    expect(content).toMatch(/replace\(.*_\(\[a-z\]\)/);
  });

  it("all API command mappings have valid format", () => {
    // Read the client.ts file to extract COMMAND_MAPPING
    const clientPath = join(frontendDir, "api/client.ts");
    const content = readFileSync(clientPath, "utf-8");

    // Extract command mapping entries
    const mappingMatch = content.match(/COMMAND_MAPPING[^{]*\{([^}]+)\}/s);
    expect(mappingMatch).toBeTruthy();

    const mappingContent = mappingMatch![1]!;
    const entries = mappingContent.match(/"[^"]+"\s*:\s*"[^"]+"/g) ?? [];

    // Verify at least a reasonable number of mappings exist
    expect(entries.length).toBeGreaterThan(15);

    // Verify all entries have valid format
    for (const entry of entries) {
      const match = entry.match(/"(GET|POST|PUT|DELETE) (\/api\/[^"]+)"\s*:\s*"([^"]+)"/);
      expect(match, `Invalid mapping format: ${entry}`).toBeTruthy();
    }
  });

  it("all api client calls have a Tauri command mapping", () => {
    // This test catches the class of bug where a frontend API call has no entry
    // in COMMAND_MAPPING, causing "No Tauri command mapping" errors at runtime
    // in the desktop app. Adding any new api.* call requires a corresponding
    // COMMAND_MAPPING entry.

    const clientPath = join(frontendDir, "api/client.ts");
    const clientContent = readFileSync(clientPath, "utf-8");

    // Parse COMMAND_MAPPING keys from client.ts
    const mappingMatch = clientContent.match(/COMMAND_MAPPING[^{]*\{([^}]+)\}/s);
    expect(mappingMatch).toBeTruthy();
    const mappingContent = mappingMatch![1]!;
    const mappingKeys = new Set<string>();
    for (const entry of mappingContent.matchAll(/"((?:GET|POST|PUT|DELETE) \/api\/[^"]+)"/g)) {
      mappingKeys.add(entry[1]!);
    }

    // Endpoints that are intentionally not in COMMAND_MAPPING because they have
    // no Tauri equivalent (SSE streams, or Tauri mode handles them differently).
    const KNOWN_GAPS: Record<string, string> = {
      "POST /api/query/abort/:id": "Query abort is a no-op in Tauri mode (graph_query is synchronous)",
      "GET /api/query/progress/:id": "SSE progress stream — not used via api client in Tauri mode",
      "POST /api/graph/import": "Multipart upload — Tauri uses native file dialog instead",
      "GET /api/import/progress/:id": "SSE import progress — not used via api client in Tauri mode",
    };

    // Normalize a URL extracted from source code to a COMMAND_MAPPING key pattern.
    // Replaces ${...} template expressions and known path-param patterns with placeholders.
    function normalizeExtracted(method: string, rawUrl: string): string {
      // Strip query string
      const base = rawUrl.split("?")[0] ?? rawUrl;
      // Replace template literal interpolations ${...} with a placeholder
      const noInterp = base.replace(/\$\{[^}]*\}/g, ":param");
      // Apply the same normalization rules as client.ts normalizeUrl()
      const normalized = noInterp
        .replace(/\/api\/graph\/node\/:param\/connections\/:param/, "/api/graph/node/:id/connections/:direction")
        .replace(/\/api\/graph\/node\/:param\/counts/, "/api/graph/node/:id/counts")
        .replace(/\/api\/graph\/node\/:param\/status/, "/api/graph/node/:id/status")
        .replace(/\/api\/graph\/node\/:param\/owned/, "/api/graph/node/:id/owned")
        .replace(/\/api\/graph\/node\/:param$/, "/api/graph/node/:id")
        .replace(/\/api\/graph\/nodes\/:param$/, "/api/graph/nodes/:id")
        .replace(
          /\/api\/graph\/relationships\/:param\/:param\/:param$/,
          "/api/graph/relationships/:source/:target/:rel_type"
        )
        .replace(/\/api\/query-history\/:param$/, "/api/query-history/:id")
        .replace(/\/api\/query\/abort\/:param$/, "/api/query/abort/:id")
        .replace(/\/api\/query\/progress\/:param$/, "/api/query/progress/:id")
        .replace(/\/api\/graph\/import\/:param$/, "/api/graph/import/:id");
      return `${method} ${normalized}`;
    }

    // Scan frontend files for api.get/post/put/delete/postNoContent calls
    const tsFiles = findTsFiles(frontendDir);
    const violations: string[] = [];

    // HTTP method mapping
    const METHOD_MAP: Record<string, string> = {
      get: "GET",
      post: "POST",
      postNoContent: "POST",
      put: "PUT",
      delete: "DELETE",
    };

    for (const file of tsFiles) {
      const relPath = relative(frontendDir, file);
      // Skip the API client itself (defines the mapping) and transport (SSE/WebSocket)
      if (relPath === "api/client.ts" || relPath === "api/transport.ts") continue;

      const content = readFileSync(file, "utf-8");
      const lines = content.split("\n");

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i]!;
        // Match: api.METHOD(`/api/...`) or api.METHOD("/api/...")
        const callMatch = line.match(/\bapi\.(get|post|postNoContent|put|delete)\s*[<(]/);
        if (!callMatch) continue;

        const method = METHOD_MAP[callMatch[1]!]!;

        // Extract the URL argument (first string/template literal after the call)
        const afterCall = line.slice(line.indexOf(callMatch[0]!) + callMatch[0]!.length);
        const urlMatch =
          afterCall.match(/[<(][^"'`]*["'`](\/api\/[^"'`]*)["'`]/) ?? afterCall.match(/["'`](\/api\/[^"'`]*)["'`]/);
        if (!urlMatch) continue;

        const rawUrl = urlMatch[1]!;
        const key = normalizeExtracted(method, rawUrl);

        if (!mappingKeys.has(key) && !Object.prototype.hasOwnProperty.call(KNOWN_GAPS, key)) {
          violations.push(`${relPath}:${i + 1}: ${method} ${rawUrl}  →  key "${key}" missing from COMMAND_MAPPING`);
        }
      }
    }

    if (violations.length > 0) {
      const message = [
        "API calls found with no entry in COMMAND_MAPPING (client.ts).",
        "These will throw 'No Tauri command mapping' errors in desktop (Tauri) mode.",
        "",
        "Add the missing entry to COMMAND_MAPPING and implement a corresponding",
        "#[tauri::command] fn in src/backend/src/tauri_commands.rs.",
        "",
        "If the endpoint intentionally has no Tauri equivalent, add it to KNOWN_GAPS",
        "in tauri-compat.test.ts with an explanation.",
        "",
        "Violations:",
        ...violations.map((v) => `  ${v}`),
      ].join("\n");

      expect.fail(message);
    }
  });
});
