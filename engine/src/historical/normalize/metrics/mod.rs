mod counters;
mod integrity;
mod policy;
mod report;
mod windows;

pub use crate::job::IntegrityCategory;
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
