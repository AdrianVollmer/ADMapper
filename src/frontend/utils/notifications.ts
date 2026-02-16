/**
 * User Notification Utilities
 *
 * Provides non-blocking notifications for user feedback.
 */

/** Show an error notification to the user */
export function showError(message: string): void {
  // Try to use a status bar if available
  const statusBar = document.getElementById("status-bar");
  if (statusBar) {
    statusBar.textContent = `Error: ${message}`;
    statusBar.classList.add("error");
    setTimeout(() => {
      statusBar.classList.remove("error");
      statusBar.textContent = "";
    }, 5000);
    return;
  }

  // Fall back to console for non-critical errors
  console.error(`[ADMapper] ${message}`);
}

/** Show a success notification to the user */
export function showSuccess(message: string): void {
  const statusBar = document.getElementById("status-bar");
  if (statusBar) {
    statusBar.textContent = message;
    setTimeout(() => {
      statusBar.textContent = "";
    }, 3000);
    return;
  }

  console.log(`[ADMapper] ${message}`);
}

/** Show an info notification to the user */
export function showInfo(message: string): void {
  const statusBar = document.getElementById("status-bar");
  if (statusBar) {
    statusBar.textContent = message;
    return;
  }

  console.log(`[ADMapper] ${message}`);
}
