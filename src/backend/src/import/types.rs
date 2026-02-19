//! Types for import progress tracking.

use serde::{Deserialize, Serialize};

/// Status of an import job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ImportStatus {
    /// Import is in progress
    Running,
    /// Import completed successfully
    Completed,
    /// Import failed with an error
    Failed,
}

/// Progress information for an import job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportProgress {
    /// Unique job ID
    pub job_id: String,
    /// Current status
    pub status: ImportStatus,
    /// Current file being processed
    pub current_file: Option<String>,
    /// Number of files processed
    pub files_processed: usize,
    /// Total number of files
    pub total_files: usize,
    /// Number of nodes imported
    pub nodes_imported: usize,
    /// Number of edges imported
    pub edges_imported: usize,
    /// Error message if status is Failed
    pub error: Option<String>,
}

impl ImportProgress {
    pub fn new(job_id: String) -> Self {
        Self {
            job_id,
            status: ImportStatus::Running,
            current_file: None,
            files_processed: 0,
            total_files: 0,
            nodes_imported: 0,
            edges_imported: 0,
            error: None,
        }
    }

    pub fn with_total_files(mut self, total: usize) -> Self {
        self.total_files = total;
        self
    }

    pub fn set_current_file(&mut self, file: String) {
        self.current_file = Some(file);
    }

    pub fn complete(&mut self) {
        self.status = ImportStatus::Completed;
        self.current_file = None;
    }

    /// Mark the import as failed with an error message.
    ///
    /// Note: Currently errors during individual file imports are logged but
    /// don't fail the entire import. This method exists for future use when
    /// stricter error handling may be needed.
    pub fn fail(&mut self, error: String) {
        self.status = ImportStatus::Failed;
        self.error = Some(error);
    }
}
