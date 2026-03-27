/**
 * Shared Modal Utility
 *
 * Eliminates the copy-pasted modal creation boilerplate across components.
 * Each caller provides a title, optional buttons, and optional configuration;
 * this utility handles overlay, header (with optional expand button), close
 * button, footer, and backdrop-click-to-close.
 *
 * NOTE: Escape-key handling is NOT included here — issue 15 consolidated
 * that into main.ts.
 */

/** A button to render in the modal footer */
export interface ModalButton {
  /** Button label text */
  label: string;
  /** data-action value for click routing */
  action: string;
  /** CSS class — defaults to "btn btn-secondary" */
  className?: string;
}

/** Options for createModal */
export interface ModalOptions {
  /** The DOM id for the overlay element */
  id: string;
  /** Modal title shown in the header */
  title: string;
  /**
   * Size class added to .modal-content.
   * Use "modal-lg" for wide modals, or omit for default width.
   * You may also pass a custom inline style via `contentStyle`.
   */
  sizeClass?: string;
  /** Optional inline style applied to .modal-content (e.g. "max-width: 480px;") */
  contentStyle?: string;
  /** Whether to show the expand/collapse toggle button in the header */
  expandable?: boolean;
  /** Footer buttons (rendered left-to-right). Omit for no footer buttons. */
  buttons?: ModalButton[];
  /** Click handler for data-action elements and backdrop clicks */
  onClick?: (action: string, event: Event) => void;
}

/** The object returned by createModal */
export interface ModalHandle {
  /** The root overlay element (append to document.body yourself or it's already appended) */
  overlay: HTMLDivElement;
  /** The .modal-body element — populate this with your content */
  body: HTMLElement;
  /** The .modal-footer element — update innerHTML to change buttons dynamically */
  footer: HTMLElement;
  /** Hide the modal */
  close: () => void;
  /** Show the modal */
  open: () => void;
}

/** SVG markup for the close (X) icon */
const CLOSE_ICON_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
  <path d="M18 6L6 18M6 6l12 12"/>
</svg>`;

/** SVG markup for the expand icon */
const EXPAND_ICON_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="expand-icon">
  <path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/>
</svg>`;

/** SVG markup for the collapse icon */
const COLLAPSE_ICON_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="collapse-icon" style="display:none">
  <path d="M4 14h6v6M20 10h-6V4M14 10l7-7M3 21l7-7"/>
</svg>`;

/**
 * Create a modal with standard boilerplate.
 *
 * The returned overlay is already appended to document.body and starts hidden.
 */
export function createModal(options: ModalOptions): ModalHandle {
  const { id, title, sizeClass, contentStyle, expandable, buttons, onClick } = options;

  const overlay = document.createElement("div");
  overlay.id = id;
  overlay.className = "modal-overlay";
  overlay.hidden = true;

  // Build modal-content class list
  const contentClasses = ["modal-content", sizeClass].filter(Boolean).join(" ");
  const styleAttr = contentStyle ? ` style="${contentStyle}"` : "";

  // Build header actions
  let headerActions = "";
  if (expandable) {
    headerActions += `
      <button class="modal-close" data-action="toggle-expand" aria-label="Expand">
        ${EXPAND_ICON_SVG}
        ${COLLAPSE_ICON_SVG}
      </button>`;
  }
  headerActions += `
    <button class="modal-close" data-action="close" aria-label="Close">
      ${CLOSE_ICON_SVG}
    </button>`;

  // Wrap header actions — use the flex wrapper when there are multiple buttons
  const headerActionsHtml = expandable
    ? `<div class="modal-header-actions">${headerActions}</div>`
    : headerActions;

  // Build footer buttons
  const footerButtonsHtml = (buttons ?? [])
    .map((btn) => {
      const cls = btn.className ?? "btn btn-secondary";
      return `<button class="${cls}" data-action="${btn.action}">${btn.label}</button>`;
    })
    .join("\n        ");

  overlay.innerHTML = `
    <div class="${contentClasses}"${styleAttr}>
      <div class="modal-header">
        <h2 class="modal-title">${title}</h2>
        ${headerActionsHtml}
      </div>
      <div class="modal-body"></div>
      <div class="modal-footer">
        ${footerButtonsHtml}
      </div>
    </div>
  `;

  const body = overlay.querySelector(".modal-body") as HTMLElement;
  const footer = overlay.querySelector(".modal-footer") as HTMLElement;

  // Expand/collapse state
  let expanded = false;

  // Central click handler
  overlay.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;

    // Backdrop click
    if (target === overlay) {
      overlay.hidden = true;
      onClick?.("close", e);
      return;
    }

    const actionEl = target.closest("[data-action]") as HTMLElement | null;
    if (!actionEl) return;

    const action = actionEl.getAttribute("data-action")!;

    // Handle built-in expand/collapse
    if (action === "toggle-expand") {
      expanded = !expanded;
      const content = overlay.querySelector(".modal-content") as HTMLElement;
      content.classList.toggle("modal-expanded", expanded);
      const expandIcon = overlay.querySelector(".expand-icon") as HTMLElement;
      const collapseIcon = overlay.querySelector(".collapse-icon") as HTMLElement;
      if (expandIcon && collapseIcon) {
        expandIcon.style.display = expanded ? "none" : "";
        collapseIcon.style.display = expanded ? "" : "none";
      }
      return;
    }

    // Handle close action
    if (action === "close") {
      overlay.hidden = true;
    }

    // Delegate to caller
    onClick?.(action, e);
  });

  document.body.appendChild(overlay);

  return {
    overlay,
    body,
    footer,
    close: () => {
      overlay.hidden = true;
    },
    open: () => {
      overlay.hidden = false;
    },
  };
}
