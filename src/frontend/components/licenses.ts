/**
 * Licenses Modal
 *
 * Displays third-party license information in a scrollable modal,
 * loaded from a static JSON file generated at build time.
 */

import { escapeHtml } from "../utils/html";

interface LicenseEntry {
  name: string;
  url: string;
  licenseType: string;
  description: string;
  licenseText: string;
}

let modal: HTMLDivElement | null = null;
let licensesData: LicenseEntry[] | null = null;

/** Fetch license data (cached after first load) */
async function loadLicenses(): Promise<LicenseEntry[]> {
  if (licensesData) return licensesData;

  const resp = await fetch("licenses.json");
  if (!resp.ok) throw new Error(`Failed to load licenses: ${resp.status}`);
  licensesData = (await resp.json()) as LicenseEntry[];
  return licensesData;
}

/** Build the HTML for a single license entry */
function renderEntry(entry: LicenseEntry): string {
  return `
    <details class="license-entry">
      <summary class="license-summary">
        <span class="license-name">${escapeHtml(entry.name)}</span>
        <span class="license-type">${escapeHtml(entry.licenseType)}</span>
      </summary>
      <div class="license-detail">
        <p class="license-description">${escapeHtml(entry.description)}</p>
        <a href="${escapeHtml(entry.url)}" target="_blank" rel="noopener noreferrer"
           class="license-url">${escapeHtml(entry.url)}</a>
        <pre class="license-text">${escapeHtml(entry.licenseText)}</pre>
      </div>
    </details>
  `;
}

/** Create the modal element */
function createModal(): HTMLDivElement {
  const el = document.createElement("div");
  el.id = "licenses-modal";
  el.className = "modal-overlay";
  el.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">Third-Party Licenses</h2>
        <button class="modal-close" data-action="close-licenses" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <p class="text-sm text-gray-400">Loading...</p>
      </div>
      <div class="modal-footer">
        <button class="btn btn-primary" data-action="close-licenses">Close</button>
      </div>
    </div>
  `;
  document.body.appendChild(el);

  el.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    if (target === el || target.closest("[data-action='close-licenses']")) {
      el.hidden = true;
    }
  });

  return el;
}

/** Open the licenses modal */
export async function openLicenses(): Promise<void> {
  if (!modal) {
    modal = createModal();
  }
  modal.hidden = false;

  const body = modal.querySelector(".modal-body")!;

  if (licensesData) return; // Already loaded

  try {
    const entries = await loadLicenses();
    body.innerHTML = entries.map(renderEntry).join("");
  } catch {
    body.innerHTML = `<p class="text-sm text-red-400">Failed to load license data.</p>`;
  }
}
