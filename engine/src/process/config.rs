use std::{fs, path::Path};

use serde::Deserialize;

use crate::{
    error::{MarketForgeError, Result},
    job::ResourceConfig,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingConfig {
    pub resources: ResourceConfig,
}

pub fn load_processing_config(
    path: impl AsRef<Path>,
    project_root: impl AsRef<Path>,
) -> Result<ProcessingConfig> {
    let path = path.as_ref();

    let contents = fs::read_to_string(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to read processing configuration {}: {error}",
            path.display()
        ))
    })?;

    let mut config: ProcessingConfig = serde_json::from_str(&contents).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "invalid processing configuration {}: {error}",
            path.display()
        ))
    })?;

    validate_resources(&config.resources)?;

    if config.resources.scratch_path.is_relative() {
        config.resources.scratch_path = project_root.as_ref().join(&config.resources.scratch_path);
    }

    Ok(config)
}

pub fn validate_resources(resources: &ResourceConfig) -> Result<()> {
    if resources.workers == 0 {
        return invalid("worker count must be greater than zero");
    }

    if resources.memory_budget_bytes == 0 {
        return invalid("memory budget must be greater than zero");
    }

    if resources.scratch_budget_bytes == 0 {
        return invalid("scratch budget must be greater than zero");
    }

    if resources.parquet.row_group_target_bytes == 0 {
        return invalid("Parquet row-group target must be greater than zero");
    }

    if resources.parquet.file_target_bytes == 0 {
        return invalid("Parquet file target must be greater than zero");
    }

    if resources.parquet.row_group_target_bytes > resources.parquet.file_target_bytes {
        return invalid("Parquet row-group target cannot exceed file target");
    }

    Ok(())
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.to_owned()))
}
