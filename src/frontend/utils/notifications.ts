/**
 * User Notification Utilities
 *
 * Provides toast notifications for user feedback.
 * Styles are defined in main.css under "Toast Notifications".
 */

/** Toast container element */
let toastContainer: HTMLElement | null = null;

/** Initialize toast container */
function getToastContainer(): HTMLElement {
  if (!toastContainer) {
    toastContainer = document.createElement("div");
    toastContainer.id = "toast-container";
    document.body.appendChild(toastContainer);
  }
  return toastContainer;
}

/** Show a toast notification */
function showToast(message: string, type: "success" | "error" | "info", duration: number = 4000): void {
  const container = getToastContainer();

  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.textContent = message;

  // Click to dismiss
  toast.addEventListener("click", () => {
    toast.remove();
  });

  container.appendChild(toast);

  // Auto-remove after duration
  setTimeout(() => {
    toast.classList.add("toast-exit");
    setTimeout(() => toast.remove(), 200);
  }, duration);
}

/** Show an error notification to the user */
export function showError(message: string): void {
  showToast(message, "error", 6000);
}

/** Show a success notification to the user */
export function showSuccess(message: string): void {
  showToast(message, "success", 4000);
}

/** Show an info notification to the user */
export function showInfo(message: string): void {
  showToast(message, "info", 4000);
}
