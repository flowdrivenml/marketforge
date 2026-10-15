use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityPolicy,
    process::metrics::{
        IntegrityCategory, IntegrityDiagnostic, IntegrityScope, ScopedIntegrityMetrics,
        TaskMetrics, enforce_immediate_integrity,
    },
};

pub fn record_integrity_error(
    task_id: u64,
    scope: &IntegrityScope,
    metrics: &mut TaskMetrics,
    category: IntegrityCategory,
    source_record: u64,
    event_timestamp_ns: Option<i64>,
    message: String,
) -> Result<()> {
    metrics.counters.records_rejected = metrics
        .counters
        .records_rejected
        .checked_add(1)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("records_rejected counter overflow".to_owned())
        })?;

    metrics.integrity.record(IntegrityDiagnostic {
        category,
        task_id,
        source_record: Some(source_record),
        event_timestamp_ns,
        message,
    })?;

    metrics
        .scoped_integrity
        .record(scope, category, true, event_timestamp_ns)?;

    Ok(())
}

pub fn enforce_integrity(
    policy: &IntegrityPolicy,
    metrics: &TaskMetrics,
    previous_job_integrity: &ScopedIntegrityMetrics,
    category: IntegrityCategory,
    message: &str,
) -> Result<()> {
    let mut combined = previous_job_integrity.clone();

    combined.merge(&metrics.scoped_integrity)?;

    let evaluation = enforce_immediate_integrity(policy, &combined, category)?;

    if evaluation.is_failed() {
        return Err(MarketForgeError::RecordIntegrity {
            category,
            message: format!("immediate integrity policy violation: {message}"),
        });
    }

    Ok(())
}
