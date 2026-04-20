/**
 * User Notification Utilities
 *
 * Provides toast notifications for user feedback.
 * Styles are defined in main.css under "Toast Notifications".
 */

import { escapeHtml } from "./html";
import { createModal } from "./modal";

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

/** Maximum length for toast messages before truncation */
const MAX_TOAST_LENGTH = 200;

/** Show a toast notification */
function showToast(message: string, type: "success" | "error" | "info", duration: number = 4000): void {
  const container = getToastContainer();

  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  const truncated = message.length > MAX_TOAST_LENGTH ? message.slice(0, MAX_TOAST_LENGTH) + "…" : message;
  toast.textContent = truncated;

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
    let resolved = false;

    const finish = (result: boolean) => {
      if (resolved) return;
      resolved = true;
      modal.close();
      modal.overlay.remove();
      document.removeEventListener("keydown", handleKeydown);
      resolve(result);
    };

    const modal = createModal({
      id: "confirm-dialog-" + Date.now(),
      title: escapeHtml(title),
      contentStyle: "max-width: 400px;",
      buttons: [
        { label: escapeHtml(cancelText), action: "cancel", className: "btn btn-secondary" },
        {
          label: escapeHtml(confirmText),
          action: "confirm",
          className: `btn ${danger ? "btn-danger" : "btn-primary"}`,
        },
      ],
      onClick(action) {
        if (action === "confirm") {
          finish(true);
        } else if (action === "cancel" || action === "close") {
          finish(false);
        }
      },
    });

    // Populate the body
    modal.body.className = "modal-body px-6 py-4";
    modal.body.innerHTML = `<p class="text-gray-300">${escapeHtml(message)}</p>`;

    // Handle escape/enter key
    const handleKeydown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        finish(false);
      } else if (e.key === "Enter") {
        finish(true);
      }
    };

    document.addEventListener("keydown", handleKeydown);
    modal.open();

    // Focus the confirm button
    const confirmBtn = modal.overlay.querySelector("[data-action='confirm']") as HTMLButtonElement;
    confirmBtn?.focus();
  });
}
