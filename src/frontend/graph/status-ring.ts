/** Node status information subset needed for ring drawing. */
export interface NodeStatusData {
  owned?: boolean;
  enabled?: boolean | null;
  tier?: number | null;
}

/**
 * Draw a status ring around a node.
 *
 * The ring is drawn slightly outside the node boundary so it doesn't overlap
 * the icon. Only owned (red) and disabled (grey) nodes get a ring; tier is
 * shown via the badge only.
 *
 * Priority: owned (red) > disabled (grey).
 */
export function drawStatusRing(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  size: number,
  status: NodeStatusData,
  _theme: "light" | "dark"
): void {
  let ringColor: string | null = null;

  if (status.owned) {
    ringColor = "#ef4444";
  } else if (status.enabled === false) {
    ringColor = "#6b7280";
  }

  if (!ringColor) return;

  ctx.beginPath();
  ctx.arc(x, y, size + 2, 0, Math.PI * 2);
  ctx.strokeStyle = ringColor;
  ctx.lineWidth = 3;
  ctx.stroke();
}

/**
 * Draw a tier number badge at the top-left of a node.
 *
 * Same visual style as the collapse count badge (top-right).
 * Gold background for tier 0, grey for tier 1+.
 */
export function drawTierBadge(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  size: number,
  status: NodeStatusData
): void {
  if (status.tier === undefined || status.tier === null) return;

  const tier = status.tier;
  const badgeText = String(tier);
  const badgeSize = Math.max(12, size * 0.6);
  const badgeX = x - size * 0.7;
  const badgeY = y - size * 0.7;

  const bgColor = tier === 0 ? "#f59e0b" : "#6b7280";
  const textColor = tier === 0 ? "#000000" : "#ffffff";

  ctx.beginPath();
  ctx.arc(badgeX, badgeY, badgeSize / 2, 0, Math.PI * 2);
  ctx.fillStyle = bgColor;
  ctx.fill();

  ctx.font = `bold ${badgeSize * 0.7}px sans-serif`;
  ctx.fillStyle = textColor;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(badgeText, badgeX, badgeY);
}
