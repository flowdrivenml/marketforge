use std::path::Path;

use super::{GeneralConfig, validation};
use crate::live::config::{error::ConfigError, loader::load_toml};

// ============================================================
// GENERAL CONFIGURATION IMPLEMENTATION
// ============================================================

impl GeneralConfig {
    /// Load a general configuration from a TOML file.
    ///
    /// The configuration is deserialized and semantically
    /// validated before being returned.
    ///
    /// No filesystem state is modified.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        let config: Self = load_toml(path).map_err(|error| match error {
            ConfigError::Parse { path, source } => {
                // TOML errors can contain complete input lines and rejected values.
                let message = match source.span() {
                    Some(span) => format!(
                        "invalid general TOML at byte {} (input redacted)",
                        span.start
                    ),
                    None => "invalid general TOML (input redacted)".into(),
                };
                ConfigError::Parse {
                    path,
                    source: serde::de::Error::custom(message),
                }
            }
            other => other,
        })?;

        config
            .validate()
            .map_err(|reason| ConfigError::Validation {
                path: path.to_path_buf(),
                reason,
            })?;

        Ok(config)
    }

    /// Validate the configuration's semantic constraints.
    ///
    /// This can also be called after constructing or modifying
    /// a configuration programmatically.
    pub fn validate(&self) -> Result<(), String> {
        validation::validate(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_general_toml_does_not_echo_credentials() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let private = "synthetic-secret-in-invalid-general-config";
        std::fs::write(file.path(), format!("schema_version = \"{private}\"")).unwrap();
        let error = GeneralConfig::load(file.path()).unwrap_err();
        assert!(matches!(error, ConfigError::Parse { .. }));
        assert!(!format!("{error:?} {error}").contains(private));
        assert!(error.to_string().contains("invalid general TOML at byte"));
    }
}
