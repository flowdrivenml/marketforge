use std::{io, path::PathBuf};

use thiserror::Error;

use crate::job::IntegrityCategory;

#[derive(Debug, Error)]
pub enum MarketForgeError {
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
