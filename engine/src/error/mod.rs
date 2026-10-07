use std::{io, path::PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MarketForgeError {
    #[error("invalid canonical data: {0}")]
    InvalidCanonical(String),

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
