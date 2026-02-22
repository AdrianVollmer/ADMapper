/**
 * Query Activity Tracking
 *
 * Subscribes to query activity events (via SSE or Tauri) and controls
 * the query indicator in the menubar.
 */

import { subscribeToQueryActivity, type Unsubscribe } from "../api/events";

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

  unsubscribe = subscribeToQueryActivity(
    (data) => {
      activeQueryCount = data.active;
      updateQueryIndicator(activeQueryCount > 0);
    },
    () => {
      // Connection lost, try to reconnect after a delay
      if (unsubscribe) {
        unsubscribe();
        unsubscribe = null;
      }
      // Reset indicator to idle state
      updateQueryIndicator(false);
      // Reconnect after 5 seconds
      setTimeout(connectToActivityStream, 5000);
    }
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
