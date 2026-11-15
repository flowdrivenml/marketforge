mod counters;
mod integrity;
mod policy;
mod report;
mod retained;
pub(crate) const REPORT_BUFFER_BYTES: u64 = 8 * 1024 * 1024;
/// Match the spool's frame/decode overlap bound without imposing a new fixed
/// limit on valid stream/path identities. JSON escaping is at most six bytes
/// per identity byte; 8 KiB covers scalar keys, counters and frame overhead.
pub(crate) fn report_buffer_bytes(identity: u64) -> crate::shared::error::Result<u64> {
    identity
        .checked_mul(6)
        .and_then(|n| n.checked_add(8192))
        .and_then(|n| n.checked_mul(128))
        .map(|n| n.max(REPORT_BUFFER_BYTES))
        .ok_or_else(|| {
            crate::shared::error::MarketForgeError::InvalidConfiguration(
                "report buffer estimate overflow".into(),
            )
        })
}
mod windows;

pub use crate::shared::validation::IntegrityCategory;
pub use counters::ProcessingCounters;
pub use integrity::{
    DEFAULT_MAX_DIAGNOSTICS, IntegrityCounters, IntegrityDiagnostic, IntegrityMetrics,
    MAX_DIAGNOSTIC_MESSAGE_BYTES,
};

pub use report::{ProcessingMetricsReport, TaskMetrics, TaskMetricsReport};

pub use windows::{
    CategoryObservation, CategoryStatistics, DAY_NS, HOUR_NS, IntegrityScope,
    ScopedIntegrityMetrics, WindowGranularity,
};

pub use policy::{
    IntegrityEvaluation, IntegrityPolicyViolation, IntegrityStatus, IntegrityThreshold,
    MAX_POLICY_VIOLATIONS, enforce_immediate_integrity, evaluate_integrity_policy,
};

pub use crate::historical::normalize::reconcile::SourceObservation;
