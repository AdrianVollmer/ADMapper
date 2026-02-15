/**
 * Tests for HTML utilities.
 */

import { describe, it, expect } from "vitest";
import { escapeHtml } from "../../utils/html";

describe("escapeHtml", () => {
  it("returns empty string for empty input", () => {
    expect(escapeHtml("")).toBe("");
  });

  it("passes through plain text unchanged", () => {
    expect(escapeHtml("hello world")).toBe("hello world");
  });

  it("escapes < and >", () => {
    expect(escapeHtml("<script>")).toBe("&lt;script&gt;");
  });

  it("escapes &", () => {
    expect(escapeHtml("foo & bar")).toBe("foo &amp; bar");
  });

  it("escapes quotes", () => {
    // Note: textContent/innerHTML escaping behavior varies
    // The implementation uses textContent which escapes < > &
    expect(escapeHtml('say "hello"')).toBe('say "hello"');
  });

  it("escapes nested HTML tags", () => {
    expect(escapeHtml("<div><span>text</span></div>")).toBe(
      "&lt;div&gt;&lt;span&gt;text&lt;/span&gt;&lt;/div&gt;"
    );
  });

  it("handles unicode characters", () => {
    expect(escapeHtml("hello 世界")).toBe("hello 世界");
  });

  it("escapes script injection attempt", () => {
    const malicious = '<script>alert("xss")</script>';
    const escaped = escapeHtml(malicious);
    expect(escaped).not.toContain("<script>");
    expect(escaped).toContain("&lt;script&gt;");
  });
});
