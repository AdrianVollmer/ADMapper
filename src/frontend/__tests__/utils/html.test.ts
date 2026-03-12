/**
 * Tests for HTML utilities.
 */

import { describe, it, expect } from "vitest";
import { escapeHtml, redactUrlCredentials } from "../../utils/html";

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

  it("escapes double quotes", () => {
    expect(escapeHtml('say "hello"')).toBe("say &quot;hello&quot;");
  });

  it("escapes single quotes", () => {
    expect(escapeHtml("it's")).toBe("it&#39;s");
  });

  it("escapes nested HTML tags", () => {
    expect(escapeHtml("<div><span>text</span></div>")).toBe("&lt;div&gt;&lt;span&gt;text&lt;/span&gt;&lt;/div&gt;");
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

describe("redactUrlCredentials", () => {
  it("redacts password in neo4j URL", () => {
    expect(redactUrlCredentials("neo4j://user:secretpass@localhost:7687")).toBe("neo4j://user:****@localhost:7687");
  });

  it("redacts password in neo4j+s URL", () => {
    expect(redactUrlCredentials("neo4j+s://admin:p4ssw0rd@host.example.com:7687/db")).toBe(
      "neo4j+s://admin:****@host.example.com:7687/db"
    );
  });

  it("redacts password in falkordb URL", () => {
    expect(redactUrlCredentials("falkordb://user:pass@redis:6379")).toBe("falkordb://user:****@redis:6379");
  });

  it("leaves URL without credentials unchanged", () => {
    expect(redactUrlCredentials("crustdb:///path/to/db")).toBe("crustdb:///path/to/db");
  });

  it("leaves URL with only username unchanged", () => {
    expect(redactUrlCredentials("neo4j://user@localhost:7687")).toBe("neo4j://user@localhost:7687");
  });

  it("handles special characters in password", () => {
    // Note: @ in passwords must be URL-encoded as %40 per RFC 3986
    expect(redactUrlCredentials("neo4j://user:p%40ss:word!@localhost:7687")).toBe("neo4j://user:****@localhost:7687");
  });
});
