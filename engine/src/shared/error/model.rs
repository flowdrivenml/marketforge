use std::{io, path::PathBuf};

use thiserror::Error;

use crate::shared::validation::IntegrityCategory;

#[derive(Debug, Error)]
pub enum MarketForgeError {
    #[error("conversion interrupted")]
    ConversionInterrupted,
    #[error("native trade identity: {reason}")]
    TradeIdentityViolation {
        source_index: usize,
        previous_source_index: Option<usize>,
        reason: String,
    },
    #[error("{source}; reconciliation context: {details}")]
    ReconciliationFailure {
        #[source]
        source: Box<MarketForgeError>,
        details: Box<serde_json::Value>,
    },
    #[error("preparation job={job_id}, task={task_id}, stream={stream}, source={}, member={member:?}: {source}",path.display())]
    PreparationFailure {
        job_id: u64,
        task_id: u64,
        stream: String,
        path: PathBuf,
        member: Option<String>,
        #[source]
        source: Box<MarketForgeError>,
    },
    #[error("not attempted: {reason}")]
    NotAttempted { reason: String },
    #[error("stream {stream}, stage={stage}: {source}")]
    StreamFailure {
        stream: String,
        stage: &'static str,
        #[source]
        source: Box<MarketForgeError>,
    },
    #[error("{processing}; additionally report persistence failed: {report}")]
    ReportPersistence {
        #[source]
        processing: Box<MarketForgeError>,
        report: Box<MarketForgeError>,
    },
    #[error(transparent)]
    ResourceLimit(#[from] ResourceLimitError),
    #[error("invalid canonical data: {0}")]
    InvalidCanonical(String),

    #[error("record integrity violation ({category:?}): {message}")]
    RecordIntegrity {
        category: IntegrityCategory,
        message: String,
    },

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("failed to open archive {path}: {message}")]
    ArchiveOpen { path: PathBuf, message: String },

    #[error("archive member not found in {path}: {member}")]
    ArchiveMemberNotFound { path: PathBuf, member: String },

    #[error("archive {path} does not contain a unique data member")]
    AmbiguousArchiveMembers { path: PathBuf },

    #[error("failed to open source {path}: {source}")]
    SourceOpen {
        path: PathBuf,

        #[source]
        source: io::Error,
    },

    #[error("failed to read source {path}: {source}")]
    SourceRead {
        path: PathBuf,

        #[source]
        source: io::Error,
    },
}

pub type Result<T> = std::result::Result<T, MarketForgeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_integrity_error_preserves_category() {
        let error = MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::InvalidRecord,
            message: "trade price must be positive".to_owned(),
        };

        match error {
            MarketForgeError::RecordIntegrity { category, message } => {
                assert_eq!(category, IntegrityCategory::InvalidRecord);
                assert_eq!(message, "trade price must be positive");
            }

            _ => panic!("expected structured integrity error"),
        }
    }

    #[test]
    fn structured_integrity_error_formats_correctly() {
        let error = MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::TransformationFailure,
            message: "quantity conversion failed".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "record integrity violation (TransformationFailure): quantity conversion failed"
        );
    }
}

/// Execution constraint; never a recoverable malformed market record.
#[derive(Debug, Clone, Error)]
#[error(
    "resource limit: {resource}, permitted={permitted}, requested={requested}, stage={stage}, scope={scope}"
)]
pub struct ResourceLimitError {
    pub resource: &'static str,
    pub permitted: u64,
    pub requested: u64,
    pub stage: &'static str,
    pub scope: String,
}

impl MarketForgeError {
    /// Preserve typed causes underneath contextual execution/preparation wrappers.
    pub fn processing_cause(&self) -> &Self {
        match self {
            Self::PreparationFailure { source, .. }
            | Self::StreamFailure { source, .. }
            | Self::ReconciliationFailure { source, .. } => source.processing_cause(),
            Self::ReportPersistence { processing, .. } => processing.processing_cause(),
            _ => self,
        }
    }
}
