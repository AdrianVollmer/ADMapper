/**
 * User Notification Utilities
 *
 * Provides toast notifications for user feedback.
 * Styles are defined in main.css under "Toast Notifications".
 */

import { escapeHtml } from "./html";

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

/** Options for confirmation dialog */
export interface ConfirmOptions {
  /** Title shown in the modal header */
  title?: string;
  /** Text for the confirm button */
  confirmText?: string;
  /** Text for the cancel button */
  cancelText?: string;
  /** Use danger styling for confirm button */
  danger?: boolean;
}

/**
 * Show a confirmation dialog and return user's choice.
 * Replaces native confirm() with a styled HTML modal.
 */
export function showConfirm(message: string, options: ConfirmOptions = {}): Promise<boolean> {
  const { title = "Confirm", confirmText = "Confirm", cancelText = "Cancel", danger = false } = options;

  return new Promise((resolve) => {
    // Create modal overlay
    const overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML = `
      <div class="modal-content" style="max-width: 400px;">
        <div class="modal-header">
          <h2 class="modal-title">${escapeHtml(title)}</h2>
        </div>
        <div class="modal-body px-6 py-4">
          <p class="text-gray-300">${escapeHtml(message)}</p>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" data-action="cancel">${escapeHtml(cancelText)}</button>
          <button class="btn ${danger ? "btn-danger" : "btn-primary"}" data-action="confirm">${escapeHtml(confirmText)}</button>
        </div>
      </div>
    `;

    // Handle clicks
    const handleClick = (e: Event) => {
      const target = e.target as HTMLElement;
      const action = target.closest("[data-action]")?.getAttribute("data-action");

      if (action === "confirm") {
        cleanup();
        resolve(true);
      } else if (action === "cancel" || target === overlay) {
        cleanup();
        resolve(false);
      }
    };

    // Handle escape key
    const handleKeydown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        cleanup();
        resolve(false);
      } else if (e.key === "Enter") {
        cleanup();
        resolve(true);
      }
    };

    // Cleanup function
    const cleanup = () => {
      overlay.remove();
      document.removeEventListener("keydown", handleKeydown);
    };

    overlay.addEventListener("click", handleClick);
    document.addEventListener("keydown", handleKeydown);
    document.body.appendChild(overlay);

    // Focus the confirm button
    const confirmBtn = overlay.querySelector("[data-action='confirm']") as HTMLButtonElement;
    confirmBtn?.focus();
  });
}