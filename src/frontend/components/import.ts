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

    // Register the Tauri event listener BEFORE starting the import.
    // Fast failures (e.g. parse errors) can complete before a post-invoke
    // subscribe() registers the listener, leaving the dialog stuck.
    // Awaiting event.listen() guarantees we catch every event.
    let currentJobId: string | null = null;
    const unlistenFn = await window.__TAURI__!.event.listen<ImportProgressEvent>(IMPORT_PROGRESS_CHANNEL.name, (e) => {
      const progress = e.payload;
      // Once job_id is known, filter out events from unrelated imports
      if (currentJobId !== null && progress.job_id !== currentJobId) return;

      updateProgressUI(progress);
      if (progress.status === "completed") {
        unlistenFn();
        unsubscribe = null;
        showCompleted(progress.failed_files);
        loadDomainAdmins();
      } else if (progress.status === "failed") {
        unlistenFn();
        unsubscribe = null;
        showError(progress.error || "Import failed", progress.failed_files);
      }
    });
    unsubscribe = () => unlistenFn();

    // Now start the import — listener is already in place
    const response = await window.__TAURI__!.core.invoke<{ job_id: string; status: string }>("import_from_paths", {
      paths,
    });
    currentJobId = response.job_id;
  } catch (err) {
    unsubscribe?.();
    unsubscribe = null;
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
        showCompleted(progress.failed_files);
        loadDomainAdmins();
      } else if (progress.status === "failed") {
        unsubscribe?.();
        unsubscribe = null;
        showError(progress.error || "Import failed", progress.failed_files);
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

  // Progress is split 50/50 between file parsing (3%–50%) and edge flush (50%–99%).
  let percent: number;
  if (progress.status === "completed") {
    percent = 100;
  } else if (progress.edges_total && progress.edges_total > 0 && rawPercent >= 100) {
    // Files done; edge flush in progress — animate from 50% → 99%
    const edgeFraction = progress.edges_imported / progress.edges_total;
    percent = Math.round(50 + edgeFraction * 49);
  } else {
    // File parsing phase: scale rawPercent (0–100) into 3%–50%.
    percent = Math.max(3, Math.round(3 + (rawPercent / 100) * 47));
  }

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
    edgesCountEl.textContent =
      progress.edges_total && progress.edges_total > 0
        ? `${progress.edges_imported.toLocaleString()} / ${progress.edges_total.toLocaleString()}`
        : progress.edges_imported.toLocaleString();
  }
}

/** Reset progress display */
function resetProgress(): void {
  if (progressFill) {
    progressFill.style.width = "3%";
    progressFill.classList.remove("progress-fill--done");
  }
  if (progressPercent) progressPercent.textContent = "3%";
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

/** Show completed state, optionally with a list of files that failed */
function showCompleted(failedFiles?: Array<{ filename: string; error: string }>): void {
  if (progressFill) {
    progressFill.style.width = "100%";
    progressFill.classList.add("progress-fill--done");
  }
  if (progressPercent) progressPercent.textContent = "100%";
  if (doneBtn) doneBtn.hidden = false;
  if (cancelBtn) cancelBtn.hidden = true;
  // New types may have been introduced by the import
  invalidateTypeCache();

  if (failedFiles && failedFiles.length > 0) {
    renderFailedFiles(failedFiles);
  }
}

/** Show a fatal import error, optionally listing per-file failures */
function showError(message: string, failedFiles?: Array<{ filename: string; error: string }>): void {
  if (errorEl) {
    if (failedFiles && failedFiles.length > 0) {
      renderFailedFiles(failedFiles);
    } else {
      errorEl.textContent = message;
      errorEl.hidden = false;
    }
  }
  if (doneBtn) doneBtn.hidden = false;
  if (cancelBtn) cancelBtn.hidden = true;
}

/** Render per-file failures into the error element */
function renderFailedFiles(failedFiles: Array<{ filename: string; error: string }>): void {
  if (!errorEl) return;
  errorEl.innerHTML = "";

  const header = document.createElement("div");
  header.textContent = `${failedFiles.length} file${failedFiles.length === 1 ? "" : "s"} could not be imported:`;
  errorEl.appendChild(header);

  const list = document.createElement("ul");
  list.style.marginTop = "0.4em";
  list.style.paddingLeft = "1.2em";
  for (const { filename, error } of failedFiles) {
    const item = document.createElement("li");
    const name = document.createElement("strong");
    name.textContent = filename;
    const reason = document.createElement("span");
    reason.textContent = ` — ${error}`;
    item.appendChild(name);
    item.appendChild(reason);
    list.appendChild(item);
  }
  errorEl.appendChild(list);
  errorEl.hidden = false;
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
