/**
 * Tests for run-query history navigation helpers.
 */

import { describe, it, expect } from "vitest";
import { filterForegroundQueryTexts, navigateHistoryIndex } from "../../utils/query";
import type { QueryHistoryEntry } from "../../api/types";

function makeEntry(query: string, background = false): QueryHistoryEntry {
  return {
    id: Math.random().toString(36).slice(2),
    name: "test",
    query,
    timestamp: Date.now(),
    result_count: null,
    status: "completed",
    started_at: Date.now(),
    duration_ms: null,
    error: null,
    background,
  };
}

// ============================================================================
// filterForegroundQueryTexts
// ============================================================================

describe("filterForegroundQueryTexts", () => {
  it("returns query strings for foreground entries in order", () => {
    const entries = [makeEntry("MATCH (n) RETURN n"), makeEntry("MATCH (u:User) RETURN u")];
    expect(filterForegroundQueryTexts(entries)).toEqual(["MATCH (n) RETURN n", "MATCH (u:User) RETURN u"]);
  });

  it("excludes background entries", () => {
    const entries = [
      makeEntry("foreground-1"),
      makeEntry("background-1", true),
      makeEntry("foreground-2"),
      makeEntry("background-2", true),
    ];
    expect(filterForegroundQueryTexts(entries)).toEqual(["foreground-1", "foreground-2"]);
  });

  it("returns empty array when all entries are background", () => {
    const entries = [makeEntry("q1", true), makeEntry("q2", true)];
    expect(filterForegroundQueryTexts(entries)).toEqual([]);
  });

  it("returns empty array for empty input", () => {
    expect(filterForegroundQueryTexts([])).toEqual([]);
  });
});

// ============================================================================
// navigateHistoryIndex
// ============================================================================

describe("navigateHistoryIndex", () => {
  it("moves older (increases index) when there are more entries", () => {
    expect(navigateHistoryIndex(0, "older", 5)).toBe(1);
    expect(navigateHistoryIndex(2, "older", 5)).toBe(3);
  });

  it("clamps at last index when navigating older from the end", () => {
    expect(navigateHistoryIndex(4, "older", 5)).toBe(4);
  });

  it("moves newer (decreases index) when not at the start", () => {
    expect(navigateHistoryIndex(3, "newer", 5)).toBe(2);
    expect(navigateHistoryIndex(1, "newer", 5)).toBe(0);
  });

  it("clamps at 0 when navigating newer from the start", () => {
    expect(navigateHistoryIndex(0, "newer", 5)).toBe(0);
  });

  it("handles single-entry history", () => {
    expect(navigateHistoryIndex(0, "older", 1)).toBe(0);
    expect(navigateHistoryIndex(0, "newer", 1)).toBe(0);
  });
});
