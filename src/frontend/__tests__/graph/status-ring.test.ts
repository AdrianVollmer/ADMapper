import { describe, it, expect, vi, beforeEach } from "vitest";
import { drawStatusRing, drawTierBadge } from "../../graph/status-ring";

function makeMockCtx() {
  const state = { strokeStyle: "", fillStyle: "", lineWidth: 0, font: "", textAlign: "", textBaseline: "" };
  return {
    beginPath: vi.fn(),
    arc: vi.fn(),
    stroke: vi.fn(),
    fill: vi.fn(),
    fillText: vi.fn(),
    get strokeStyle() {
      return state.strokeStyle;
    },
    set strokeStyle(v: string) {
      state.strokeStyle = v;
    },
    get fillStyle() {
      return state.fillStyle;
    },
    set fillStyle(v: string) {
      state.fillStyle = v;
    },
    get lineWidth() {
      return state.lineWidth;
    },
    set lineWidth(v: number) {
      state.lineWidth = v;
    },
    get font() {
      return state.font;
    },
    set font(v: string) {
      state.font = v;
    },
    get textAlign() {
      return state.textAlign;
    },
    set textAlign(v: string) {
      state.textAlign = v;
    },
    get textBaseline() {
      return state.textBaseline;
    },
    set textBaseline(v: string) {
      state.textBaseline = v;
    },
  } as unknown as CanvasRenderingContext2D;
}

describe("drawStatusRing", () => {
  let ctx: CanvasRenderingContext2D;
  beforeEach(() => {
    ctx = makeMockCtx();
  });

  it("draws nothing when no status applies", () => {
    drawStatusRing(ctx, 100, 100, 20, {}, "dark");
    expect(ctx.arc).not.toHaveBeenCalled();
  });

  it("draws nothing for tier-only nodes (tier has no ring)", () => {
    drawStatusRing(ctx, 100, 100, 20, { tier: 0 }, "dark");
    expect(ctx.arc).not.toHaveBeenCalled();
  });

  it("draws one arc for an owned node", () => {
    drawStatusRing(ctx, 100, 100, 20, { owned: true }, "dark");
    expect(ctx.arc).toHaveBeenCalledTimes(1);
    expect(ctx.stroke).toHaveBeenCalledTimes(1);
  });

  it("uses red for owned nodes", () => {
    const calls: string[] = [];
    Object.defineProperty(ctx, "strokeStyle", {
      set(v: string) {
        calls.push(v);
      },
      get() {
        return "";
      },
    });
    drawStatusRing(ctx, 100, 100, 20, { owned: true }, "dark");
    expect(calls).toContain("#ef4444");
  });

  it("uses grey for disabled nodes", () => {
    const calls: string[] = [];
    Object.defineProperty(ctx, "strokeStyle", {
      set(v: string) {
        calls.push(v);
      },
      get() {
        return "";
      },
    });
    drawStatusRing(ctx, 100, 100, 20, { enabled: false }, "dark");
    expect(calls).toContain("#6b7280");
  });

  it("owned takes priority over disabled", () => {
    const calls: string[] = [];
    Object.defineProperty(ctx, "strokeStyle", {
      set(v: string) {
        calls.push(v);
      },
      get() {
        return "";
      },
    });
    drawStatusRing(ctx, 100, 100, 20, { owned: true, enabled: false }, "dark");
    expect(calls).toContain("#ef4444");
    expect(calls).not.toContain("#6b7280");
  });

  it("draws ring at size+2", () => {
    drawStatusRing(ctx, 100, 100, 20, { owned: true }, "dark");
    const arcCalls = (ctx.arc as ReturnType<typeof vi.fn>).mock.calls;
    expect(arcCalls[0]![2]).toBe(22); // radius = size + 2
  });
});

describe("drawTierBadge", () => {
  let ctx: CanvasRenderingContext2D;
  beforeEach(() => {
    ctx = makeMockCtx();
  });

  it("draws nothing when tier is not set", () => {
    drawTierBadge(ctx, 100, 100, 20, {});
    expect(ctx.arc).not.toHaveBeenCalled();
  });

  it("draws a circle and text for tier 0", () => {
    drawTierBadge(ctx, 100, 100, 20, { tier: 0 });
    expect(ctx.arc).toHaveBeenCalledTimes(1);
    expect(ctx.fillText).toHaveBeenCalledWith("0", expect.any(Number), expect.any(Number));
  });

  it("positions badge at top-left of node (x - size*0.7, y - size*0.7)", () => {
    drawTierBadge(ctx, 100, 100, 20, { tier: 1 });
    const arcCall = (ctx.arc as ReturnType<typeof vi.fn>).mock.calls[0]!;
    expect(arcCall[0]).toBeCloseTo(86); // 100 - 20*0.7
    expect(arcCall[1]).toBeCloseTo(86); // 100 - 20*0.7
  });
});
