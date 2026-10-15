use std::{fs, path::Path};

use serde::Deserialize;

use crate::{
    error::{MarketForgeError, Result},
    job::{IntegrityAction, IntegrityPolicy, IntegrityRule, IntegrityWindowRule, ResourceConfig},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingConfig {
    pub resources: ResourceConfig,
    pub integrity_policy: IntegrityPolicy,
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
    validate_integrity_policy(&config.integrity_policy)?;

    if config.resources.scratch_path.is_relative() {
        config.resources.scratch_path = project_root.as_ref().join(&config.resources.scratch_path);
    }

    Ok(config)
}

// -----------------------------------------------------------------------------
// Resource validation
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Integrity policy validation
// -----------------------------------------------------------------------------

pub fn validate_integrity_policy(policy: &IntegrityPolicy) -> Result<()> {
    validate_rule("parse_failure", &policy.parse_failure)?;
    validate_rule("invalid_record", &policy.invalid_record)?;
    validate_rule("sequence_gap", &policy.sequence_gap)?;
    validate_rule("timestamp_regression", &policy.timestamp_regression)?;
    validate_rule("missing_snapshot", &policy.missing_snapshot)?;
    validate_rule("invalid_book", &policy.invalid_book)?;
    validate_rule("transformation_failure", &policy.transformation_failure)?;

    Ok(())
}

fn validate_rule(name: &str, rule: &IntegrityRule) -> Result<()> {
    if let Some(rate) = rule.max_rate {
        validate_rate(name, rate)?;
    }

    if let Some(window) = &rule.windows.daily {
        validate_window(name, "daily", window)?;
    }

    if let Some(window) = &rule.windows.hourly {
        validate_window(name, "hourly", window)?;
    }

    if matches!(rule.action, IntegrityAction::Degrade)
        && rule.max_count.is_none()
        && rule.max_rate.is_none()
        && rule.windows.daily.is_none()
        && rule.windows.hourly.is_none()
    {
        return invalid(&format!(
            "{name}: degrade policy requires at least one threshold"
        ));
    }

    Ok(())
}

fn validate_window(category: &str, window_name: &str, window: &IntegrityWindowRule) -> Result<()> {
    validate_rate(&format!("{category}.{window_name}"), window.max_rate)
}

fn validate_rate(name: &str, rate: f64) -> Result<()> {
    if !rate.is_finite() || !(0.0..=1.0).contains(&rate) {
        return invalid(&format!("{name}: integrity rate must be between 0 and 1"));
    }

    Ok(())
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.to_owned()))
}
