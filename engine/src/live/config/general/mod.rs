mod model;
mod validation;

// ============================================================
// PUBLIC EXPORTS
// ============================================================

pub use model::{
    ControlConfig, DefaultsConfig, GeneralConfig, LogLevel, LoggingConfig, MetricsConfig,
    PrometheusConfig, PublicationConfig, RecordingConfig, RecoveryDefaultsConfig, RuntimeConfig,
};

use std::path::Path;

use super::{error::ConfigError, loader::load_toml};

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

        let config: Self = load_toml(path)?;

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
