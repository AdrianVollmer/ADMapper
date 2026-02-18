/**
 * User Notification Utilities
 *
 * Provides toast notifications for user feedback.
 */

/** Toast container element */
let toastContainer: HTMLElement | null = null;

/** Initialize toast container */
function getToastContainer(): HTMLElement {
  if (!toastContainer) {
    toastContainer = document.createElement("div");
    toastContainer.id = "toast-container";
    toastContainer.style.cssText = `
      position: fixed;
      bottom: 20px;
      right: 20px;
      z-index: 10000;
      display: flex;
      flex-direction: column;
      gap: 8px;
      pointer-events: none;
    `;
    document.body.appendChild(toastContainer);
  }
  return toastContainer;
}

/** Show a toast notification */
function showToast(message: string, type: "success" | "error" | "info", duration: number = 4000): void {
  const container = getToastContainer();

  const toast = document.createElement("div");
  toast.style.cssText = `
    padding: 12px 16px;
    border-radius: 6px;
    color: white;
    font-size: 14px;
    max-width: 400px;
    pointer-events: auto;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    animation: slideIn 0.2s ease-out;
    cursor: pointer;
  `;

  // Set background color based on type
  switch (type) {
    case "success":
      toast.style.backgroundColor = "#22c55e";
      break;
    case "error":
      toast.style.backgroundColor = "#ef4444";
      break;
    case "info":
      toast.style.backgroundColor = "#3b82f6";
      break;
  }

  toast.textContent = message;

  // Click to dismiss
  toast.addEventListener("click", () => {
    toast.remove();
  });

  container.appendChild(toast);

  // Auto-remove after duration
  setTimeout(() => {
    toast.style.animation = "slideOut 0.2s ease-in forwards";
    setTimeout(() => toast.remove(), 200);
  }, duration);
}

/** Add toast animations to document */
function ensureAnimations(): void {
  if (document.getElementById("toast-animations")) return;

  const style = document.createElement("style");
  style.id = "toast-animations";
  style.textContent = `
    @keyframes slideIn {
      from { transform: translateX(100%); opacity: 0; }
      to { transform: translateX(0); opacity: 1; }
    }
    @keyframes slideOut {
      from { transform: translateX(0); opacity: 1; }
      to { transform: translateX(100%); opacity: 0; }
    }
  `;
  document.head.appendChild(style);
}

// Initialize animations on module load
ensureAnimations();

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
