/**
 * Query Activity Tracking
 *
 * Subscribes to query activity events and controls the query indicator in the menubar.
 * Uses the unified transport abstraction for consistent behavior in both HTTP and Tauri modes.
 */

import { subscribe, QUERY_ACTIVITY_CHANNEL, type Unsubscribe } from "../api/transport";

/** Unsubscribe function for current connection */
let unsubscribe: Unsubscribe | null = null;

/** Current number of active queries */
let activeQueryCount = 0;

/** Initialize query activity tracking */
export function initQueryActivity(): void {
  // Delay connection slightly to ensure page is fully loaded
  setTimeout(connectToActivityStream, 100);
}

/** Connect to the query activity stream */
function connectToActivityStream(): void {
  // Clean up existing connection
  if (unsubscribe) {
    unsubscribe();
    unsubscribe = null;
  }

  // Note: fetchInitial: false because the endpoint is SSE-only (it sends initial state on connect)
  unsubscribe = subscribe(
    QUERY_ACTIVITY_CHANNEL,
    {}, // No params needed for this channel
    (data) => {
      activeQueryCount = data.active;
      updateQueryIndicator(activeQueryCount > 0);
    },
    () => {
      // Connection lost - reset indicator (transport handles reconnection)
      updateQueryIndicator(false);
    },
    { fetchInitial: false, autoReconnect: true, reconnectDelay: 5000 }
  );
}

/** Update the query indicator in the menubar */
function updateQueryIndicator(running: boolean): void {
  const indicator = document.getElementById("query-indicator");
  if (!indicator) return;

  // Get the child SVGs
  const staticSvg = indicator.querySelector(".query-indicator-static") as HTMLElement | null;
  const animatedSvg = indicator.querySelector(".query-indicator-animated") as HTMLElement | null;

  if (running) {
    indicator.classList.add("running");
    indicator.title = `${activeQueryCount} query${activeQueryCount === 1 ? "" : "ies"} running - Click for history`;
    // Apply inline styles as fallback for CSS
    if (staticSvg) staticSvg.style.display = "none";
    if (animatedSvg) animatedSvg.style.display = "block";
  } else {
    indicator.classList.remove("running");
    indicator.title = "Query History";
    // Reset inline styles
    if (staticSvg) staticSvg.style.display = "";
    if (animatedSvg) animatedSvg.style.display = "";
  }
}

/** Get the current number of active queries */
export function getActiveQueryCount(): number {
  return activeQueryCount;
}

/** Check if any queries are currently running */
export function hasActiveQueries(): boolean {
  return activeQueryCount > 0;
}
