use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MarketForgeError, Result},
    job::{IntegrityAction, IntegrityPolicy, IntegrityRule, IntegrityWindowRule},
};

use super::windows::{
    CategoryObservation, CategoryStatistics, IntegrityScope, ScopedIntegrityMetrics,
};

use crate::job::IntegrityCategory;
pub const MAX_POLICY_VIOLATIONS: usize = 100;
// -----------------------------------------------------------------------------
// Policy evaluation status
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityStatus {
    Clean,
    Degraded,
    Failed,
}

// -----------------------------------------------------------------------------
// Threshold violation
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityThreshold {
    FailAction,
    MaximumCount,
    GlobalRate,
    FileRate,
    StreamRate,
    DailyRate,
    HourlyRate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityPolicyViolation {
    pub category: IntegrityCategory,
    pub threshold: IntegrityThreshold,

    pub task_id: Option<u64>,
    pub stream_id: Option<String>,
    pub source_file: Option<String>,

    pub window_start_ns: Option<i64>,

    pub eligible: u64,
    pub violations: u64,

    pub message: String,
}

// -----------------------------------------------------------------------------
// Evaluation result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityEvaluation {
    pub status: IntegrityStatus,

    pub violations: Vec<IntegrityPolicyViolation>,

    pub total_policy_violations: u64,
}

// -----------------------------------------------------------------------------
// Immediate integrity enforcement
// -----------------------------------------------------------------------------

pub fn enforce_immediate_integrity(
    policy: &IntegrityPolicy,
    metrics: &ScopedIntegrityMetrics,
    category: IntegrityCategory,
) -> Result<IntegrityEvaluation> {
    let mut evaluation = IntegrityEvaluation::default();

    if !policy.enabled {
        return Ok(evaluation);
    }

    let rule = rule_for_category(policy, category);
    let observation = metrics.global.get(category);

    if observation.violations == 0 {
        return Ok(evaluation);
    }

    // Fatal categories are rejected immediately.
    if rule.action == IntegrityAction::Fail {
        record_failure(
            &mut evaluation,
            category,
            IntegrityThreshold::FailAction,
            None,
            None,
            observation,
            "integrity policy requires immediate failure".to_owned(),
        )?;

        return Ok(evaluation);
    }

    // A tolerated violation degrades the dataset.
    evaluation.status = IntegrityStatus::Degraded;

    // Absolute limits are safe to enforce immediately because
    // violation counts cannot decrease during processing.
    if let Some(max_count) = rule.max_count {
        if observation.violations > max_count {
            record_failure(
                &mut evaluation,
                category,
                IntegrityThreshold::MaximumCount,
                None,
                None,
                observation,
                format!(
                    "integrity count exceeded: {} > {}",
                    observation.violations, max_count,
                ),
            )?;
        }
    }

    Ok(evaluation)
}

impl Default for IntegrityEvaluation {
    fn default() -> Self {
        Self {
            status: IntegrityStatus::Clean,
            violations: Vec::new(),
            total_policy_violations: 0,
        }
    }
}

impl IntegrityEvaluation {
    pub fn record_violation(&mut self, violation: IntegrityPolicyViolation) -> Result<()> {
        self.total_policy_violations =
            self.total_policy_violations.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "policy violation counter overflow".to_owned(),
                )
            })?;

        self.status = IntegrityStatus::Failed;

        if self.violations.len() < MAX_POLICY_VIOLATIONS {
            self.violations.push(violation);
        }

        Ok(())
    }

    pub fn is_failed(&self) -> bool {
        self.status == IntegrityStatus::Failed
    }

    pub fn is_degraded(&self) -> bool {
        self.status == IntegrityStatus::Degraded
    }
}

// -----------------------------------------------------------------------------
// Policy evaluator
// -----------------------------------------------------------------------------

pub fn evaluate_integrity_policy(
    policy: &IntegrityPolicy,
    metrics: &ScopedIntegrityMetrics,
) -> Result<IntegrityEvaluation> {
    let mut evaluation = IntegrityEvaluation::default();

    if !policy.enabled {
        let has_violations = [
            IntegrityCategory::ParseFailure,
            IntegrityCategory::InvalidRecord,
            IntegrityCategory::SequenceGap,
            IntegrityCategory::TimestampRegression,
            IntegrityCategory::MissingSnapshot,
            IntegrityCategory::InvalidBook,
            IntegrityCategory::TransformationFailure,
        ]
        .into_iter()
        .any(|category| metrics.global.get(category).violations > 0);

        if has_violations {
            evaluation.status = IntegrityStatus::Degraded;
        }

        return Ok(evaluation);
    }

    for category in all_categories() {
        let rule = rule_for_category(policy, category);

        let global = metrics.global.get(category);

        if global.violations == 0 {
            continue;
        }

        // Any violation under Fail action is fatal.
        if rule.action == IntegrityAction::Fail {
            evaluation.status = IntegrityStatus::Failed;

            evaluation.record_violation(IntegrityPolicyViolation {
                category,
                threshold: IntegrityThreshold::FailAction,
                task_id: None,
                stream_id: None,
                source_file: None,
                window_start_ns: None,
                eligible: global.eligible,
                violations: global.violations,
                message: "integrity policy requires immediate failure".to_owned(),
            })?;

            continue;
        }

        // A tolerated violation marks the dataset degraded.
        if evaluation.status == IntegrityStatus::Clean {
            evaluation.status = IntegrityStatus::Degraded;
        }

        // ---------------------------------------------------------
        // Absolute count
        // ---------------------------------------------------------

        if let Some(max_count) = rule.max_count {
            if global.violations > max_count {
                record_failure(
                    &mut evaluation,
                    category,
                    IntegrityThreshold::MaximumCount,
                    None,
                    None,
                    global,
                    format!(
                        "integrity count exceeded: {} > {}",
                        global.violations, max_count
                    ),
                )?;
            }
        }

        // ---------------------------------------------------------
        // Global rate
        // ---------------------------------------------------------

        if let Some(max_rate) = rule.max_rate {
            if rate_exceeded(global, max_rate, rule.minimum_samples)? {
                record_failure(
                    &mut evaluation,
                    category,
                    IntegrityThreshold::GlobalRate,
                    None,
                    None,
                    global,
                    "global integrity rate exceeded".to_owned(),
                )?;
            }

            // -----------------------------------------------------
            // Per-file rates
            // -----------------------------------------------------

            for (scope, stats) in &metrics.files {
                let observation = stats.get(category);

                if rate_exceeded(observation, max_rate, rule.minimum_samples)? {
                    record_failure(
                        &mut evaluation,
                        category,
                        IntegrityThreshold::FileRate,
                        Some(scope),
                        None,
                        observation,
                        "source-file integrity rate exceeded".to_owned(),
                    )?;
                }
            }

            // -----------------------------------------------------
            // Per-stream rates
            // -----------------------------------------------------

            let streams = aggregate_streams(&metrics.files)?;

            for (stream_id, stats) in streams {
                let observation = stats.get(category);

                if rate_exceeded(observation, max_rate, rule.minimum_samples)? {
                    evaluation.status = IntegrityStatus::Failed;

                    evaluation.record_violation(IntegrityPolicyViolation {
                        category,
                        threshold: IntegrityThreshold::StreamRate,
                        task_id: None,
                        stream_id: Some(stream_id),
                        source_file: None,
                        window_start_ns: None,
                        eligible: observation.eligible,
                        violations: observation.violations,
                        message: "stream integrity rate exceeded".to_owned(),
                    })?;
                }
            }
        }

        // ---------------------------------------------------------
        // Daily windows
        // ---------------------------------------------------------

        if let Some(window_rule) = &rule.windows.daily {
            evaluate_windows(
                &mut evaluation,
                category,
                &metrics.daily,
                window_rule,
                IntegrityThreshold::DailyRate,
            )?;
        }

        // ---------------------------------------------------------
        // Hourly windows
        // ---------------------------------------------------------

        if let Some(window_rule) = &rule.windows.hourly {
            evaluate_windows(
                &mut evaluation,
                category,
                &metrics.hourly,
                window_rule,
                IntegrityThreshold::HourlyRate,
            )?;
        }
    }

    Ok(evaluation)
}

// -----------------------------------------------------------------------------
// Window evaluation
// -----------------------------------------------------------------------------

fn evaluate_windows(
    evaluation: &mut IntegrityEvaluation,
    category: IntegrityCategory,
    windows: &BTreeMap<(IntegrityScope, i64), CategoryStatistics>,
    rule: &IntegrityWindowRule,
    threshold: IntegrityThreshold,
) -> Result<()> {
    for ((scope, start_ns), stats) in windows {
        let observation = stats.get(category);

        if rate_exceeded(observation, rule.max_rate, rule.minimum_samples)? {
            record_failure(
                evaluation,
                category,
                threshold,
                Some(scope),
                Some(*start_ns),
                observation,
                "integrity time-window rate exceeded".to_owned(),
            )?;
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Exact threshold comparison
// -----------------------------------------------------------------------------

fn rate_exceeded(
    observation: &CategoryObservation,
    max_rate: f64,
    minimum_samples: u64,
) -> Result<bool> {
    if observation.eligible == 0 || observation.eligible < minimum_samples {
        return Ok(false);
    }

    if !max_rate.is_finite() || !(0.0..=1.0).contains(&max_rate) {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "invalid integrity rate threshold: {max_rate}"
        )));
    }

    let threshold = Decimal::from_str_exact(&max_rate.to_string()).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "invalid integrity threshold {max_rate}: {error}"
        ))
    })?;

    let violations = Decimal::from(observation.violations);
    let eligible = Decimal::from(observation.eligible);

    let limit = eligible.checked_mul(threshold).ok_or_else(|| {
        MarketForgeError::InvalidConfiguration("integrity rate comparison overflow".to_owned())
    })?;

    Ok(violations > limit)
}

// -----------------------------------------------------------------------------
// Stream aggregation
// -----------------------------------------------------------------------------

fn aggregate_streams(
    files: &BTreeMap<IntegrityScope, CategoryStatistics>,
) -> Result<BTreeMap<String, CategoryStatistics>> {
    let mut streams = BTreeMap::<String, CategoryStatistics>::new();

    for (scope, stats) in files {
        let entry = streams.entry(scope.stream_id.clone()).or_default();

        entry.merge(stats)?;
    }

    Ok(streams)
}

// -----------------------------------------------------------------------------
// Failure recording
// -----------------------------------------------------------------------------

fn record_failure(
    evaluation: &mut IntegrityEvaluation,
    category: IntegrityCategory,
    threshold: IntegrityThreshold,
    scope: Option<&IntegrityScope>,
    window_start_ns: Option<i64>,
    observation: &CategoryObservation,
    message: String,
) -> Result<()> {
    evaluation.record_violation(IntegrityPolicyViolation {
        category,
        threshold,

        task_id: scope.map(|scope| scope.task_id),
        stream_id: scope.map(|scope| scope.stream_id.clone()),
        source_file: scope.map(|scope| scope.source_file.clone()),

        window_start_ns,

        eligible: observation.eligible,
        violations: observation.violations,

        message,
    })
}

// -----------------------------------------------------------------------------
// Category mapping
// -----------------------------------------------------------------------------

fn all_categories() -> [IntegrityCategory; 7] {
    [
        IntegrityCategory::ParseFailure,
        IntegrityCategory::InvalidRecord,
        IntegrityCategory::SequenceGap,
        IntegrityCategory::TimestampRegression,
        IntegrityCategory::MissingSnapshot,
        IntegrityCategory::InvalidBook,
        IntegrityCategory::TransformationFailure,
    ]
}

fn rule_for_category(policy: &IntegrityPolicy, category: IntegrityCategory) -> &IntegrityRule {
    match category {
        IntegrityCategory::ParseFailure => &policy.parse_failure,
        IntegrityCategory::InvalidRecord => &policy.invalid_record,
        IntegrityCategory::SequenceGap => &policy.sequence_gap,
        IntegrityCategory::TimestampRegression => &policy.timestamp_regression,
        IntegrityCategory::MissingSnapshot => &policy.missing_snapshot,
        IntegrityCategory::InvalidBook => &policy.invalid_book,
        IntegrityCategory::TransformationFailure => &policy.transformation_failure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_equal_to_threshold_is_allowed() {
        let observation = CategoryObservation {
            eligible: 1_000,
            violations: 1,
        };

        assert!(!rate_exceeded(&observation, 0.001, 1).unwrap());
    }

    #[test]
    fn rate_above_threshold_fails() {
        let observation = CategoryObservation {
            eligible: 1_000,
            violations: 2,
        };

        assert!(rate_exceeded(&observation, 0.001, 1).unwrap());
    }

    #[test]
    fn rate_below_minimum_samples_is_deferred() {
        let observation = CategoryObservation {
            eligible: 100,
            violations: 10,
        };

        assert!(!rate_exceeded(&observation, 0.001, 1_000).unwrap());
    }

    #[test]
    fn rejects_invalid_thresholds() {
        let observation = CategoryObservation {
            eligible: 1_000,
            violations: 1,
        };

        assert!(rate_exceeded(&observation, -0.1, 1).is_err());
        assert!(rate_exceeded(&observation, 1.1, 1).is_err());
        assert!(rate_exceeded(&observation, f64::NAN, 1).is_err());
    }

    #[test]
    fn handles_large_counters() {
        let observation = CategoryObservation {
            eligible: u64::MAX,
            violations: 1,
        };

        assert!(!rate_exceeded(&observation, 0.001, 1).unwrap());
    }
}
