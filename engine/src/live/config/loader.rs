use std::path::Path;

use serde::de::DeserializeOwned;

use super::error::ConfigError;

/// Loads a TOML configuration file and deserializes it into type `T`.
///
/// The caller provides the file path and expected configuration type.
///
/// This function performs no semantic validation, caching, or runtime
/// configuration management.
pub fn load_toml<T>(path: impl AsRef<Path>) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    let path = path.as_ref();

    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    parse_toml(&contents).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

/// Deserializes TOML content already available in memory.
///
/// Useful for tests, embedded configurations, and API-provided content.
///
/// Performs no filesystem operations.
pub fn parse_toml<T>(content: &str) -> Result<T, toml::de::Error>
where
    T: DeserializeOwned,
{
    toml::from_str(content)
}
