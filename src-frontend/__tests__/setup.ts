/**
 * Vitest Setup
 *
 * Runs before each test file.
 */

import { beforeAll, afterEach, afterAll } from "vitest";

// Mock fetch for tests that don't use MSW
const originalFetch = globalThis.fetch;

beforeAll(() => {
  // Any global setup
});

afterEach(() => {
  // Reset any global state between tests
});

afterAll(() => {
  // Restore original fetch
  globalThis.fetch = originalFetch;
});
