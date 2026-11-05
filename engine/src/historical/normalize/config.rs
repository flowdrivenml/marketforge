use std::{fs, path::Path};

use serde::Deserialize;

use crate::{
    historical::job::{IntegrityPolicy, ResourceConfig},
    shared::error::{MarketForgeError, Result},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingConfig {
    pub resources: ResourceConfig,
    pub integrity_policy: IntegrityPolicy,
}

pub const RUNTIME_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeDocument {
    schema_version: u32,
    resources: ResourceConfig,
    integrity_policy: IntegrityPolicy,
}

/// Canonicalize a real project directory, without changing the process CWD.
pub fn project_root(root: impl AsRef<Path>) -> Result<std::path::PathBuf> {
    let root = root.as_ref();
    let resolved = root.canonicalize().map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!(
            "invalid project root {}: {e}",
            root.display()
        ))
    })?;
    if !resolved.is_dir() {
        return invalid(&format!(
            "invalid project root {}: must be a directory",
            root.display()
        ));
    }
    Ok(resolved)
}

pub fn resolve_path(root: &Path, path: &Path) -> std::path::PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

pub fn load_processing_config(
    path: impl AsRef<Path>,
    root: impl AsRef<Path>,
) -> Result<ProcessingConfig> {
    let root = project_root(root)?;
    let path = resolve_path(&root, path.as_ref());
    let result = (|| -> Result<ProcessingConfig> {
        let contents = fs::read_to_string(&path)
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        let document: RuntimeDocument = toml::from_str(&contents)
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        if document.schema_version != RUNTIME_SCHEMA_VERSION {
            return invalid(&format!(
                "schema_version: unsupported version {}; expected {RUNTIME_SCHEMA_VERSION}",
                document.schema_version
            ));
        }
        let mut config = ProcessingConfig {
            resources: document.resources,
            integrity_policy: document.integrity_policy,
        };
        validate_processing_config(&config)?;
        config.resources.scratch_path = resolve_path(&root, &config.resources.scratch_path);
        if config.resources.scratch_path.exists() && !config.resources.scratch_path.is_dir() {
            return invalid("resources.scratch_path: existing path must be a directory");
        }
        Ok(config)
    })();
    result.map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!(
            "normalization configuration {}: {e}",
            path.display()
        ))
    })
}

pub fn validate_processing_config(config: &ProcessingConfig) -> Result<()> {
    validate_resources(&config.resources)?;
    validate_integrity_policy(&config.integrity_policy)
}

pub use crate::historical::job::{validate_integrity_policy, validate_resources};

fn invalid<T>(message: &str) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.to_owned()))
}

#[cfg(test)]
mod tests;
