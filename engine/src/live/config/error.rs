use std::{io, path::PathBuf};

/// Errors encountered while loading or validating TOML configuration files.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Configuration file could not be read.
    #[error("failed to read configuration file '{}': {source}", path.display())]
    Read {
        path: PathBuf,

        #[source]
        source: io::Error,
    },

    /// Configuration file contains invalid TOML or cannot be deserialized
    /// into the requested Rust configuration type.
    #[error("failed to parse configuration file '{}': {source}", path.display())]
    Parse {
        path: PathBuf,

        #[source]
        source: toml::de::Error,
    },

    /// Configuration was successfully parsed but failed semantic validation.
    #[error("invalid configuration in '{}': {reason}", path.display())]
    Validation { path: PathBuf, reason: String },

    /// Failed to enumerate the configuration directory.
    #[error("failed to read configuration directory '{}': {source}", path.display())]
    Directory {
        path: PathBuf,

        #[source]
        source: io::Error,
    },

    /// Multiple configuration files declare the same profile.
    #[error("duplicate exchange profile '{profile}' in '{}'", path.display())]
    DuplicateProfile { profile: String, path: PathBuf },
}
