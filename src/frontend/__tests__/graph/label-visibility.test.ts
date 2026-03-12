/**
 * Tests for label visibility and obfuscation.
 *
 * Verifies that label masking uses '#' characters (not XOR cipher or
 * other transformations that produce unreadable output).
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  getLabelParts,
  setLabelVisibilityMode,
  cycleLabelVisibility,
  getLabelVisibilityMode,
  getLabelVisibilityName,
} from "../../graph/label-visibility";

afterEach(() => {
  setLabelVisibilityMode("normal");
});

describe("getLabelParts", () => {
  it("returns full label in normal mode", () => {
    setLabelVisibilityMode("normal");
    const parts = getLabelParts("USER@DOMAIN.LOCAL");
    expect(parts).toEqual({ clear: "USER@DOMAIN.LOCAL", blurred: "" });
  });

  it("returns null for hidden mode", () => {
    setLabelVisibilityMode("hidden");
    expect(getLabelParts("USER@DOMAIN.LOCAL")).toBeNull();
  });

  it("returns null for null/undefined/empty input", () => {
    expect(getLabelParts(null)).toBeNull();
    expect(getLabelParts(undefined)).toBeNull();
    expect(getLabelParts("")).toBeNull();
  });

  describe("blur-domain mode", () => {
    it("masks only the domain part with '#' characters", () => {
      setLabelVisibilityMode("blur-domain");
      const parts = getLabelParts("admin@CORP.LOCAL");
      expect(parts!.clear).toBe("admin@");
      expect(parts!.blurred).toBe("##########");
      expect(parts!.blurred).toMatch(/^#+$/);
      expect(parts!.blurred.length).toBe("CORP.LOCAL".length);
    });

    it("shows full label when no '@' present", () => {
      setLabelVisibilityMode("blur-domain");
      const parts = getLabelParts("DC01.corp.local");
      expect(parts).toEqual({ clear: "DC01.corp.local", blurred: "" });
    });
  });

  describe("blur-all mode", () => {
    it("masks entire label with '#' characters", () => {
      setLabelVisibilityMode("blur-all");
      const parts = getLabelParts("admin@CORP.LOCAL");
      expect(parts!.clear).toBe("");
      expect(parts!.blurred).toMatch(/^#+$/);
      expect(parts!.blurred.length).toBe("admin@CORP.LOCAL".length);
    });

    it("never contains readable characters", () => {
      setLabelVisibilityMode("blur-all");
      const labels = [
        "DOMAIN ADMINS@CORP.LOCAL",
        "S-1-5-21-1234567890-512",
        "DC01.corp.local",
        "user with spaces",
      ];
      for (const label of labels) {
        const parts = getLabelParts(label);
        expect(parts!.blurred).toBe("#".repeat(label.length));
      }
    });
  });
});

describe("cycleLabelVisibility", () => {
  it("cycles through all modes in order", () => {
    setLabelVisibilityMode("normal");
    expect(cycleLabelVisibility()).toBe("blur-domain");
    expect(cycleLabelVisibility()).toBe("blur-all");
    expect(cycleLabelVisibility()).toBe("hidden");
    expect(cycleLabelVisibility()).toBe("normal");
  });
});

describe("getLabelVisibilityName", () => {
  it("returns human-readable names", () => {
    setLabelVisibilityMode("normal");
    expect(getLabelVisibilityName()).toBe("Labels: Normal");
    setLabelVisibilityMode("blur-all");
    expect(getLabelVisibilityName()).toBe("Labels: All Hidden");
  });
});

describe("mode state", () => {
  it("get returns what was set", () => {
    setLabelVisibilityMode("blur-all");
    expect(getLabelVisibilityMode()).toBe("blur-all");
    setLabelVisibilityMode("normal");
    expect(getLabelVisibilityMode()).toBe("normal");
  });
});
