/**
 * BloodHound Import Handler
 *
 * Handles file selection, upload, and progress tracking for BloodHound data import.
 * In desktop mode, uses native file dialogs and IPC. In headless mode, uses HTTP upload.
 */

import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";
import { showError as showNotification } from "../utils/notifications";
import { executeQuery, QueryAbortedError } from "../utils/query";
import { subscribe, IMPORT_PROGRESS_CHANNEL, type ImportProgressEvent, type Unsubscribe } from "../api/transport";
import { isRunningInTauri } from "../api/client";
import { invalidateTypeCache } from "./add-node-edge";

// DOM element references
let fileInput: HTMLInputElement | null = null;
let modal: HTMLElement | null = null;
let progressFill: HTMLElement | null = null;
let progressPercent: HTMLElement | null = null;
let progressFiles: HTMLElement | null = null;
let currentFileEl: HTMLElement | null = null;
let stageEl: HTMLElement | null = null;
let nodesCountEl: HTMLElement | null = null;
let edgesCountEl: HTMLElement | null = null;
let errorEl: HTMLElement | null = null;
let doneBtn: HTMLElement | null = null;
let cancelBtn: HTMLElement | null = null;

// State
let unsubscribe: Unsubscribe | null = null;

/** Initialize the import handler */
export function initImport(): void {
  fileInput = document.getElementById("bloodhound-file-input") as HTMLInputElement;
  modal = document.getElementById("import-modal");
  progressFill = document.getElementById("import-progress-fill");
  progressPercent = document.getElementById("import-progress-percent");
  progressFiles = document.getElementById("import-progress-files");
  currentFileEl = document.getElementById("import-current-file");
  stageEl = document.getElementById("import-stage");
  nodesCountEl = document.getElementById("import-nodes-count");
  edgesCountEl = document.getElementById("import-edges-count");
  errorEl = document.getElementById("import-error");
  doneBtn = document.getElementById("import-done-btn");
  cancelBtn = document.getElementById("import-cancel-btn");

  if (!fileInput) {
    console.error("BloodHound file input not found");
    return;
  }

  // File selection handler
  fileInput.addEventListener("change", handleFileSelect);

  // Modal buttons
  doneBtn?.addEventListener("click", handleDone);
  cancelBtn?.addEventListener("click", handleCancel);
}

/** Trigger the file picker dialog */
export async function triggerBloodHoundImport(): Promise<void> {
  // In Tauri mode, use native file dialog
  if (isRunningInTauri()) {
    await triggerTauriImport();
    return;
  }

  // In HTTP mode, use HTML file input
  if (!fileInput) {
    fileInput = document.querySelector<HTMLInputElement>("#bloodhound-file-input");
  }
  if (fileInput) {
    fileInput.click();
  } else {
    // Last resort: create the input dynamically
    fileInput = document.createElement("input");
    fileInput.type = "file";
    fileInput.accept = ".json,.zip";
    fileInput.multiple = true;
    fileInput.style.display = "none";
    fileInput.addEventListener("change", handleFileSelect);
    document.body.appendChild(fileInput);
    fileInput.click();
  }
}

/** Trigger import using Tauri native file dialog */
async function triggerTauriImport(): Promise<void> {
  try {
    // Use Tauri's dialog plugin for native file selection
    const result = await window.__TAURI_PLUGIN_DIALOG__?.open({
      multiple: true,
      filters: [{ name: "BloodHound Data", extensions: ["json", "zip"] }],
      title: "Select BloodHound Files",
    });

    if (!result) return; // User cancelled

    // Normalize to array
    const paths = Array.isArray(result) ? result : [result];
    if (paths.length === 0) return;

    // Show modal and reset progress
    showModal();
    resetProgress();

    // Call Tauri command to import files - get job ID first
    const response = await window.__TAURI__!.core.invoke<{ job_id: string; status: string }>("import_from_paths", {
      paths,
    });

    // Subscribe to progress events using the real job ID
    unsubscribe = subscribe(
      IMPORT_PROGRESS_CHANNEL,
      { jobId: response.job_id, job_id: response.job_id },
      (progress: ImportProgressEvent) => {
        updateProgressUI(progress);
        if (progress.status === "completed") {
          unsubscribe?.();
          unsubscribe = null;
          showCompleted();
          loadDomainAdmins();
        } else if (progress.status === "failed") {
          unsubscribe?.();
          unsubscribe = null;
          showError(progress.error || "Import failed");
        }
      },
      () => {
        unsubscribe?.();
        unsubscribe = null;
      }
    );
  } catch (err) {
    showError(err instanceof Error ? err.message : String(err));
  }
}

/** Handle file selection */
async function handleFileSelect(event: Event): Promise<void> {
  const input = event.target as HTMLInputElement;
  const files = input.files;

  if (!files || files.length === 0) {
    return;
  }

  // Show modal
  showModal();
  resetProgress();

  try {
    // Build form data
    const formData = new FormData();
    for (const file of files) {
      formData.append("files", file, file.name);
    }

    // Start upload
    const response = await fetch("/api/import", {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `Upload failed: ${response.status}`);
    }

    const result = await response.json();

    // Subscribe to progress updates
    subscribeToProgressUpdates(result.job_id);
  } catch (err) {
    showError(err instanceof Error ? err.message : "Import failed");
  }

  // Clear the input so the same file can be selected again
  input.value = "";
}

/** Subscribe to progress updates (SSE or Tauri events) */
function subscribeToProgressUpdates(jobId: string): void {
  if (unsubscribe) {
    unsubscribe();
  }

  unsubscribe = subscribe(
    IMPORT_PROGRESS_CHANNEL,
    { jobId, job_id: jobId },
    (progress: ImportProgressEvent) => {
      updateProgressUI(progress);

      if (progress.status === "completed") {
        unsubscribe?.();
        unsubscribe = null;
        showCompleted();
        loadDomainAdmins();
      } else if (progress.status === "failed") {
        unsubscribe?.();
        unsubscribe = null;
        showError(progress.error || "Import failed");
      }
    },
    () => {
      // Connection closed/error, clean up
      unsubscribe?.();
      unsubscribe = null;
    }
  );
}

/** Update the progress UI */
function updateProgressUI(progress: ImportProgressEvent): void {
  // Use byte-weighted progress when available, fall back to file count
  const rawPercent =
    progress.bytes_total > 0
      ? Math.round((progress.bytes_processed / progress.bytes_total) * 100)
      : progress.total_files > 0
        ? Math.round((progress.files_processed / progress.total_files) * 100)
        : 0;

  // Cap at 95% while still running — post-processing can hold at 100% for a while
  const percent = progress.status === "running" ? Math.min(rawPercent, 95) : rawPercent;

  if (progressFill) {
    progressFill.style.width = `${percent}%`;
  }

  if (progressPercent) {
    progressPercent.textContent = `${percent}%`;
  }

  if (progressFiles) {
    progressFiles.textContent = `${progress.files_processed} / ${progress.total_files} files`;
  }

  if (currentFileEl) {
    // Fall back to stage when no file is being processed (e.g. post-processing)
    currentFileEl.textContent = progress.current_file || progress.stage || "-";
  }

  if (stageEl) {
    // Only show stage separately when a file is also shown; avoid duplicating post-processing text
    stageEl.innerHTML = progress.current_file && progress.stage ? progress.stage : "&nbsp;";
  }

  if (nodesCountEl) {
    nodesCountEl.textContent = progress.nodes_imported.toLocaleString();
  }

  if (edgesCountEl) {
    edgesCountEl.textContent = progress.edges_imported.toLocaleString();
  }
}

/** Reset progress display */
function resetProgress(): void {
  if (progressFill) {
    progressFill.style.width = "0%";
    progressFill.classList.remove("progress-fill--done");
  }
  if (progressPercent) progressPercent.textContent = "0%";
  if (progressFiles) progressFiles.textContent = "0 / 0 files";
  if (currentFileEl) currentFileEl.textContent = "-";
  if (stageEl) {
    stageEl.innerHTML = "&nbsp;";
  }
  if (nodesCountEl) nodesCountEl.textContent = "0";
  if (edgesCountEl) edgesCountEl.textContent = "0";
  if (errorEl) {
    errorEl.hidden = true;
    errorEl.textContent = "";
  }
  if (doneBtn) doneBtn.hidden = true;
  if (cancelBtn) cancelBtn.hidden = false;
}

/** Show the modal */
function showModal(): void {
  if (modal) {
    modal.hidden = false;
  }
}

/** Hide the modal */
function hideModal(): void {
  if (modal) {
    modal.hidden = true;
  }
}

/** Show completed state */
function showCompleted(): void {
  if (progressFill) {
    progressFill.style.width = "100%";
    progressFill.classList.add("progress-fill--done");
  }
  if (progressPercent) progressPercent.textContent = "100%";
  if (doneBtn) doneBtn.hidden = false;
  if (cancelBtn) cancelBtn.hidden = true;
  // New types may have been introduced by the import
  invalidateTypeCache();
}

/** Show error */
function showError(message: string): void {
  if (errorEl) {
    errorEl.textContent = message;
    errorEl.hidden = false;
  }
  if (doneBtn) doneBtn.hidden = false;
  if (cancelBtn) cancelBtn.hidden = true;
}

/** Handle done button click */
function handleDone(): void {
  hideModal();
  cleanup();
}

/** Handle cancel button click */
function handleCancel(): void {
  hideModal();
  cleanup();
}

/** Cleanup subscription and state */
function cleanup(): void {
  if (unsubscribe) {
    unsubscribe();
    unsubscribe = null;
  }
}

/** Query to find all members of Domain Admin groups (SID ends with -512) */
const DOMAIN_ADMINS_QUERY = `
MATCH p = (m)-[:MemberOf]->(g:Group)
WHERE g.objectid ENDS WITH '-512'
RETURN p
`;

/** Load Domain Admin members after import */
async function loadDomainAdmins(): Promise<void> {
  try {
    const result = await executeQuery(DOMAIN_ADMINS_QUERY, { extractGraph: true });

    if (result.graph && result.graph.nodes.length > 0) {
      // Convert to RawADGraph format
      const graph: RawADGraph = {
        nodes: result.graph.nodes,
        relationships: result.graph.relationships,
      };

      loadGraphData(graph);
    } else {
      // No Domain Admins found - show empty graph
      loadGraphData({ nodes: [], relationships: [] });
    }
  } catch (err) {
    // Silently ignore if query was aborted
    if (err instanceof QueryAbortedError) {
      return;
    }
    console.error("Failed to load Domain Admins:", err);
    showNotification("Failed to load Domain Admin members. Use the query panel to explore the data.");
  }
}
