import { describe, it, expect, beforeEach } from "vitest";
import { getShowNodeBadges, setShowNodeBadges, toggleShowNodeBadges } from "../../components/settings";

describe("node badges toggle", () => {
  beforeEach(() => {
    setShowNodeBadges(false);
  });

  it("is off by default", () => {
    expect(getShowNodeBadges()).toBe(false);
  });

  it("toggles on when called once", () => {
    toggleShowNodeBadges();
    expect(getShowNodeBadges()).toBe(true);
  });

  it("toggles back off when called twice", () => {
    toggleShowNodeBadges();
    toggleShowNodeBadges();
    expect(getShowNodeBadges()).toBe(false);
  });

  it("returns the new state", () => {
    const result = toggleShowNodeBadges();
    expect(result).toBe(true);
  });
});
