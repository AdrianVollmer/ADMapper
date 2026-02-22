/**
 * BloodHound Import Handler
 *
 * Handles file selection, upload, and progress tracking for BloodHound data import.
 */

import { loadGraphData } from "./graph-view";
import type { RawADGraph, ADNodeType, ADEdgeType } from "../graph/types";
import type { ImportProgress } from "../api/types";
import { showError as showNotification } from "../utils/notifications";
import { executeQuery } from "../utils/query";
import { subscribeToImportProgress, type Unsubscribe } from "../api/events";

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
let unsubscribe: Unsubscribe | null = null;

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

  unsubscribe = subscribeToImportProgress(
    jobId,
    (progress: ImportProgress) => {
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
function updateProgressUI(progress: ImportProgress): void {
  const percent = progress.total_files > 0 ? Math.round((progress.files_processed / progress.total_files) * 100) : 0;

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

/** Cleanup subscription and state */
function cleanup(): void {
  if (unsubscribe) {
    unsubscribe();
    unsubscribe = null;
  }
}

/** Query to find all members of Domain Admin groups (SID ends with -512) */
const DOMAIN_ADMINS_QUERY = `
MATCH (m)-[e:Edge]->(g:Group)
WHERE g.object_id ENDS WITH '-512'
AND e.edge_type = 'MemberOf'
RETURN m, e, g
`;

/** Load Domain Admin members after import */
async function loadDomainAdmins(): Promise<void> {
  try {
    const result = await executeQuery(DOMAIN_ADMINS_QUERY, { extractGraph: true });

    if (result.graph && result.graph.nodes.length > 0) {
      // Convert to RawADGraph format
      const graph: RawADGraph = {
        nodes: result.graph.nodes.map((n) => ({
          id: n.id,
          name: n.name,
          type: mapNodeType(n.type),
          properties: n.properties,
        })),
        edges: result.graph.edges.map((e) => ({
          source: e.source,
          target: e.target,
          type: mapEdgeType(e.type),
        })),
      };

      loadGraphData(graph);
    } else {
      // No Domain Admins found - show empty graph
      loadGraphData({ nodes: [], edges: [] });
    }
  } catch (err) {
    console.error("Failed to load Domain Admins:", err);
    showNotification("Failed to load Domain Admin members. Use the query panel to explore the data.");
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
