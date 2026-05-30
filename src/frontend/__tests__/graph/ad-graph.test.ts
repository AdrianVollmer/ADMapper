import { describe, it, expect } from "vitest";
import { loadGraph } from "../../graph/ADGraph";
import type { RawADGraph } from "../../graph/types";

function makeGraph(nodeProps: Record<string, unknown>): RawADGraph {
  return {
    nodes: [{ id: "n1", name: "Alice", type: "User", ...nodeProps }],
    relationships: [],
  };
}

describe("rawNodeToAttributes — status fields", () => {
  it("stores owned=true in properties when node is owned", () => {
    const g = loadGraph(makeGraph({ owned: true }));
    expect(g.getNodeAttribute("n1", "properties")).toMatchObject({ owned: true });
  });

  it("stores owned=false in properties when node is not owned", () => {
    const g = loadGraph(makeGraph({ owned: false }));
    expect(g.getNodeAttribute("n1", "properties")).toMatchObject({ owned: false });
  });

  it("stores enabled=false in properties when account is disabled", () => {
    const g = loadGraph(makeGraph({ enabled: false }));
    expect(g.getNodeAttribute("n1", "properties")).toMatchObject({ enabled: false });
  });

  it("stores tier=0 in properties when tier is set", () => {
    const g = loadGraph(makeGraph({ tier: 0 }));
    expect(g.getNodeAttribute("n1", "properties")).toMatchObject({ tier: 0 });
  });

  it("omits owned from properties when field is absent", () => {
    const g = loadGraph(makeGraph({}));
    const props = g.getNodeAttribute("n1", "properties") ?? {};
    expect(props).not.toHaveProperty("owned");
  });

  it("preserves existing node.properties while adding status fields", () => {
    const g = loadGraph(makeGraph({ properties: { objectid: "S-1" }, owned: true, tier: 1 }));
    const props = g.getNodeAttribute("n1", "properties")!;
    expect(props["objectid"]).toBe("S-1");
    expect(props["owned"]).toBe(true);
    expect(props["tier"]).toBe(1);
  });
});
