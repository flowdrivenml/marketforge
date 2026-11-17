use {
    crate::historical::job::IntegrityPolicy,
    crate::historical::normalize::metrics::IntegrityCategory,
    crate::historical::normalize::metrics::IntegrityDiagnostic,
    crate::historical::normalize::metrics::IntegrityScope,
    crate::historical::normalize::metrics::ScopedIntegrityMetrics,
    crate::historical::normalize::metrics::TaskMetrics,
    crate::historical::normalize::metrics::enforce_immediate_integrity,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
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
    // Immediate rules consume global observations only. Preserve the existing
    // merge validation, but do not clone a task's detailed observation maps.
    previous_job_integrity.validate_merge(&metrics.scoped_integrity)?;
    let mut combined = ScopedIntegrityMetrics {
        global: previous_job_integrity.global.clone(),
        ..Default::default()
    };
    combined.global.merge(&metrics.scoped_integrity.global)?;

    let evaluation = enforce_immediate_integrity(policy, &combined, category)?;

    if evaluation.is_failed() {
        return Err(MarketForgeError::RecordIntegrity {
            category,
            message: format!("immediate integrity policy violation: {message}"),
        });
    }

    Ok(())
}

/// Merge completed checks exactly once; error helpers record the failing category.
pub(super) fn record_trace(
    metrics: &mut TaskMetrics,
    scope: &IntegrityScope,
    trace: &crate::shared::validation::IntegrityObservationTrace,
    error: Option<&MarketForgeError>,
) -> Result<()> {
    let failed = match error {
        Some(MarketForgeError::RecordIntegrity { category, .. }) => Some(*category),
        _ => None,
    };
    metrics.scoped_integrity.record_successes(
        scope,
        trace.successful(failed),
        trace.event_timestamp_ns,
    )
}

pub(crate) fn csv_source_error(
    task: &crate::historical::job::WorkTask,
    error: csv::Error,
) -> MarketForgeError {
    let message = error.to_string();
    match error.into_kind() {
        csv::ErrorKind::Io(source) => {
            // Routed scratch iterators carry their original engine error through
            // csv::Error. Do not relabel a scratch failure as source corruption.
            if source.get_ref().is_some_and(|e| e.is::<MarketForgeError>()) {
                return *source
                    .into_inner()
                    .unwrap()
                    .downcast::<MarketForgeError>()
                    .unwrap();
            }
            crate::shared::historical::limits::source_error(task.input_path.clone(), source)
        }
        _ => MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::ParseFailure,
            message,
        },
    }
}
