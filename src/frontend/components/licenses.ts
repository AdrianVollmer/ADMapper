/**
 * Licenses Modal
 *
 * Displays third-party license information in a scrollable modal,
 * loaded from a static JSON file generated at build time.
 */

import { escapeHtml } from "../utils/html";
import { createModal, type ModalHandle } from "../utils/modal";

interface LicenseEntry {
  name: string;
  url: string;
  licenseType: string;
  description: string;
  licenseText: string;
}

let modal: ModalHandle | null = null;
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

/** Open the licenses modal */
export async function openLicenses(): Promise<void> {
  if (!modal) {
    modal = createModal({
      id: "licenses-modal",
      title: "Third-Party Licenses",
      sizeClass: "modal-lg",
      buttons: [{ label: "Close", action: "close", className: "btn btn-primary" }],
    });
  }
  modal.open();

  if (licensesData) return; // Already loaded

  modal.body.innerHTML = `<p class="text-sm text-gray-400">Loading...</p>`;

  try {
    const entries = await loadLicenses();
    modal.body.innerHTML = entries.map(renderEntry).join("");
  } catch {
    modal.body.innerHTML = `<p class="text-sm text-red-400">Failed to load license data.</p>`;
  }
}
