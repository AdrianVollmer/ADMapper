/**
 * BloodHound Import Handler
 *
 * Handles file selection, upload, and progress tracking for BloodHound data import.
 */

import { loadGraphData } from "./graph-view";
import type { RawADGraph, ADNodeType, ADEdgeType } from "../graph/types";

/** Import progress from server */
interface ImportProgress {
  job_id: string;
  status: "running" | "completed" | "failed";
  current_file: string | null;
  files_processed: number;
  total_files: number;
  nodes_imported: number;
  edges_imported: number;
  error: string | null;
}

/** Graph node from API */
interface ApiGraphNode {
  id: string;
  label: string;
  type: string;
  properties: Record<string, unknown>;
}

/** Graph edge from API */
interface ApiGraphEdge {
  source: string;
  target: string;
  type: string;
}

/** Full graph response from API */
interface ApiGraph {
  nodes: ApiGraphNode[];
  edges: ApiGraphEdge[];
}

// DOM element references
let fileInput: HTMLInputElement | null = null;
let modal: HTMLElement | null = null;
let progressFill: HTMLElement | null = null;
let progressPercent: HTMLElement | null = null;
let progressFiles: HTMLElement | null = null;
let currentFileEl: HTMLElement | null = null;
let nodesCountEl: HTMLElement | null = null;
let edgesCountEl: HTMLElement | null = null;
let errorEl: HTMLElement | null = null;
let doneBtn: HTMLElement | null = null;
let cancelBtn: HTMLElement | null = null;

// State
let eventSource: EventSource | null = null;
let currentJobId: string | null = null;

/** Initialize the import handler */
export function initImport(): void {
  fileInput = document.getElementById("bloodhound-file-input") as HTMLInputElement;
  modal = document.getElementById("import-modal");
  progressFill = document.getElementById("import-progress-fill");
  progressPercent = document.getElementById("import-progress-percent");
  progressFiles = document.getElementById("import-progress-files");
  currentFileEl = document.getElementById("import-current-file");
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
export function triggerBloodHoundImport(): void {
  // Fallback: try to find element if not initialized
  if (!fileInput) {
    fileInput = document.getElementById("bloodhound-file-input") as HTMLInputElement;
  }
  fileInput?.click();
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
    currentJobId = result.job_id;

    // Subscribe to progress updates
    subscribeToProgress(result.job_id);
  } catch (err) {
    showError(err instanceof Error ? err.message : "Import failed");
  }

  // Clear the input so the same file can be selected again
  input.value = "";
}

/** Subscribe to SSE progress updates */
function subscribeToProgress(jobId: string): void {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource(`/api/import/progress/${jobId}`);

  eventSource.onmessage = (event) => {
    try {
      const progress: ImportProgress = JSON.parse(event.data);
      updateProgressUI(progress);

      if (progress.status === "completed") {
        eventSource?.close();
        eventSource = null;
        showCompleted();
        refreshGraphData();
      } else if (progress.status === "failed") {
        eventSource?.close();
        eventSource = null;
        showError(progress.error || "Import failed");
      }
    } catch (err) {
      console.error("Failed to parse progress:", err);
    }
  };

  eventSource.onerror = () => {
    // SSE connection closed, check if import completed
    eventSource?.close();
    eventSource = null;
  };
}

/** Update the progress UI */
function updateProgressUI(progress: ImportProgress): void {
  const percent =
    progress.total_files > 0
      ? Math.round((progress.files_processed / progress.total_files) * 100)
      : 0;

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
    currentFileEl.textContent = progress.current_file || "-";
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
  if (progressFill) progressFill.style.width = "0%";
  if (progressPercent) progressPercent.textContent = "0%";
  if (progressFiles) progressFiles.textContent = "0 / 0 files";
  if (currentFileEl) currentFileEl.textContent = "-";
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
  if (progressFill) progressFill.style.width = "100%";
  if (progressPercent) progressPercent.textContent = "100%";
  if (doneBtn) doneBtn.hidden = false;
  if (cancelBtn) cancelBtn.hidden = true;
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

/** Cleanup event source and state */
function cleanup(): void {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  currentJobId = null;
}

/** Refresh graph data from server */
async function refreshGraphData(): Promise<void> {
  try {
    const response = await fetch("/api/graph/all");
    if (!response.ok) {
      console.error("Failed to fetch graph data");
      return;
    }

    const data: ApiGraph = await response.json();

    // Convert to RawADGraph format
    const graph: RawADGraph = {
      nodes: data.nodes.map((n) => ({
        id: n.id,
        label: n.label,
        type: mapNodeType(n.type),
        properties: n.properties,
      })),
      edges: data.edges.map((e) => ({
        source: e.source,
        target: e.target,
        type: mapEdgeType(e.type),
      })),
    };

    // Load into graph view
    loadGraphData(graph);
  } catch (err) {
    console.error("Failed to refresh graph:", err);
  }
}

/** Map API node type to ADNodeType */
function mapNodeType(type: string): ADNodeType {
  const known: ADNodeType[] = [
    "User",
    "Group",
    "Computer",
    "Domain",
    "GPO",
    "OU",
    "Container",
    "CertTemplate",
    "EnterpriseCA",
    "RootCA",
    "AIACA",
    "NTAuthStore",
  ];
  return known.includes(type as ADNodeType) ? (type as ADNodeType) : "Unknown";
}

/** Map API edge type to ADEdgeType */
function mapEdgeType(type: string): ADEdgeType {
  const known: ADEdgeType[] = [
    "MemberOf",
    "HasSession",
    "AdminTo",
    "CanRDP",
    "CanPSRemote",
    "ExecuteDCOM",
    "AllowedToDelegate",
    "AllowedToAct",
    "AddMember",
    "ForceChangePassword",
    "GenericAll",
    "GenericWrite",
    "WriteOwner",
    "WriteDacl",
    "Owns",
    "Contains",
    "GPLink",
    "TrustedBy",
    "DCSync",
    "GetChanges",
    "GetChangesAll",
    "AllExtendedRights",
    "AddKeyCredentialLink",
    "AddAllowedToAct",
    "ReadLAPSPassword",
    "ReadGMSAPassword",
    "GetChangesInFilteredSet",
    "WriteSPN",
    "WriteAccountRestrictions",
    "LocalGroupMember",
    "ACE",
  ];
  return known.includes(type as ADEdgeType) ? (type as ADEdgeType) : "Unknown";
}
