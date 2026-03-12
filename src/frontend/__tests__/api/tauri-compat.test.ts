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
        const regex = new RegExp(
          `subscribe\\(\\s*${channelName}\\s*,\\s*\\{([^}]*)\\}`,
          "g"
        );
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
});
