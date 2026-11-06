//! Shared legacy/runtime checks; no I/O or normalization dependencies.
use super::{IntegrityAction, IntegrityPolicy, IntegrityRule, IntegrityWindowRule, ResourceConfig};
use crate::shared::error::{MarketForgeError, Result};

// -----------------------------------------------------------------------------
// Resource validation
// -----------------------------------------------------------------------------

pub fn validate_resources(resources: &ResourceConfig) -> Result<()> {
    if resources.scratch_path.as_os_str().is_empty() {
        return invalid("resources.scratch_path: must not be empty");
    }
    if resources.workers == 0 {
        return invalid("resources.workers: must be greater than zero");
    }

    if resources.memory_budget_bytes == 0 {
        return invalid("resources.memory_budget_bytes: must be greater than zero");
    }

    if resources.scratch_budget_bytes == 0 {
        return invalid("resources.scratch_budget_bytes: must be greater than zero");
    }

    if resources.parquet.row_group_target_bytes == 0 {
        return invalid("resources.parquet.row_group_target_bytes: must be greater than zero");
    }

    if resources.parquet.file_target_bytes == 0 {
        return invalid("resources.parquet.file_target_bytes: must be greater than zero");
    }

    if resources.parquet.row_group_target_bytes > resources.parquet.file_target_bytes {
        return invalid(
            "resources.parquet.row_group_target_bytes: cannot exceed file_target_bytes",
        );
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
            "integrity_policy.{name}: degrade policy requires at least one threshold"
        ));
    }

    Ok(())
}

fn validate_window(category: &str, window_name: &str, window: &IntegrityWindowRule) -> Result<()> {
    validate_rate(&format!("{category}.{window_name}"), window.max_rate)
}

fn validate_rate(name: &str, rate: f64) -> Result<()> {
    if !rate.is_finite() || !(0.0..=1.0).contains(&rate) {
        return invalid(&format!(
            "integrity_policy.{name}: integrity rate must be between 0 and 1"
        ));
    }

    Ok(())
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.to_owned()))
}
