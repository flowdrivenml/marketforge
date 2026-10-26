use super::exchange::ExchangeId;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{error::ConfigError, exchange::ExchangeConfig};

/// Immutable collection of validated exchange configurations.
///
/// All profiles are loaded during initialization.
/// The store performs no filesystem operations afterward.
#[derive(Debug, Clone, Default)]
pub struct ExchangeConfigStore {
    profiles: BTreeMap<String, Arc<ExchangeConfig>>,
}

impl ExchangeConfigStore {
    /// Create an empty configuration store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load every TOML exchange profile from a directory.
    ///
    /// Only regular files with a `.toml` extension are loaded.
    /// Subdirectories and unrelated files are ignored.
    ///
    /// Loading fails if any profile is invalid or duplicated.
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        let entries = fs::read_dir(path).map_err(|source| ConfigError::Directory {
            path: path.to_path_buf(),
            source,
        })?;

        let mut paths = Vec::<PathBuf>::new();

        for entry in entries {
            let entry = entry.map_err(|source| ConfigError::Directory {
                path: path.to_path_buf(),
                source,
            })?;

            let file_type = entry.file_type().map_err(|source| ConfigError::Directory {
                path: entry.path(),
                source,
            })?;

            if !file_type.is_file() {
                continue;
            }

            let file_path = entry.path();

            if file_path.extension().is_some_and(|ext| ext == "toml") {
                paths.push(file_path);
            }
        }

        // Deterministic loading and error reporting.
        paths.sort();

        if paths.is_empty() {
            return Err(ConfigError::Validation {
                path: path.to_path_buf(),
                reason: "no exchange TOML profiles found".into(),
            });
        }

        let mut store = Self::new();

        for file_path in paths {
            let config = ExchangeConfig::load(&file_path)?;

            let profile = config.exchange.profile.clone();

            if store.profiles.contains_key(&profile) {
                return Err(ConfigError::DuplicateProfile {
                    profile,
                    path: file_path,
                });
            }

            store.profiles.insert(profile, Arc::new(config));
        }

        Ok(store)
    }

    /// Retrieve a profile by its unique name.
    ///
    /// Example: "binance_spot".
    pub fn get(&self, profile: &str) -> Option<&ExchangeConfig> {
        self.profiles.get(profile).map(Arc::as_ref)
    }

    /// Retrieve an Arc for sharing with an asynchronous component.
    ///
    /// Cloning the Arc does not clone the configuration.
    pub fn get_shared(&self, profile: &str) -> Option<Arc<ExchangeConfig>> {
        self.profiles.get(profile).map(Arc::clone)
    }

    /// Retrieve a profile using exchange and market identifiers.
    ///
    /// Example: get_market("binance", "spot").
    pub fn get_market(&self, exchange: &str, market: &str) -> Option<&ExchangeConfig> {
        let profile = format!("{exchange}_{market}");
        self.get(&profile)
    }

    /// Return all configurations belonging to an exchange.

    pub fn by_exchange(&self, exchange: ExchangeId) -> impl Iterator<Item = &ExchangeConfig> {
        self.profiles
            .values()
            .map(Arc::as_ref)
            .filter(move |config| config.exchange.id == exchange)
    }

    /// Iterate over all loaded configurations.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ExchangeConfig)> {
        self.profiles
            .iter()
            .map(|(name, config)| (name.as_str(), config.as_ref()))
    }

    /// Iterate over available profile names.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.profiles.keys().map(String::as_str)
    }

    /// Number of loaded exchange profiles.
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Whether the store contains no profiles.
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Check whether a profile exists.
    pub fn contains(&self, profile: &str) -> bool {
        self.profiles.contains_key(profile)
    }
}

#[cfg(test)]
mod tests;
