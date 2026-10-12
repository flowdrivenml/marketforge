#![cfg(feature = "process")]

use marketforge_engine::job::IntegrityAction;
use marketforge_engine::process::metrics::enforce_immediate_integrity;
use marketforge_engine::process::{
    load_processing_config,
    metrics::{
        IntegrityCategory, IntegrityScope, IntegrityStatus, IntegrityThreshold,
        ScopedIntegrityMetrics, evaluate_integrity_policy,
    },
};
use std::path::Path;

fn policy() -> marketforge_engine::job::IntegrityPolicy {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();

    load_processing_config(root.join("data/.jobs/processing.json"), root)
        .unwrap()
        .integrity_policy
}

fn scope() -> IntegrityScope {
    IntegrityScope {
        task_id: 120,
        stream_id: "okx:BTCUSDT:trades".to_owned(),
        source_file: "BTCUSDT-2026-09-01.zip".to_owned(),
    }
}

fn record(
    metrics: &mut ScopedIntegrityMetrics,
    category: IntegrityCategory,
    valid: u64,
    invalid: u64,
) {
    let scope = scope();

    for _ in 0..valid {
        metrics.record(&scope, category, false, None).unwrap();
    }

    for _ in 0..invalid {
        metrics.record(&scope, category, true, None).unwrap();
    }
}

#[test]
fn clean_dataset_passes() {
    let metrics = ScopedIntegrityMetrics::default();

    let result = evaluate_integrity_policy(&policy(), &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Clean);
}

#[test]
fn tolerated_errors_degrade_dataset() {
    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::InvalidRecord, 10_000, 1);

    let result = evaluate_integrity_policy(&policy(), &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Degraded);
}

#[test]
fn fatal_category_fails_immediately() {
    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::SequenceGap, 0, 1);

    let result = evaluate_integrity_policy(&policy(), &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Failed);
}

#[test]
fn global_rate_threshold_fails() {
    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::InvalidRecord, 9_000, 100);

    let result = evaluate_integrity_policy(&policy(), &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Failed);

    assert!(
        result
            .violations
            .iter()
            .any(|violation| { violation.threshold == IntegrityThreshold::GlobalRate })
    );
}

#[test]
fn minimum_samples_defers_rate_check() {
    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::InvalidRecord, 10, 1);

    let result = evaluate_integrity_policy(&policy(), &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Degraded);
}

#[test]
fn absolute_count_threshold_fails() {
    let mut policy = policy();

    policy.invalid_record.max_count = Some(2);

    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::InvalidRecord, 100, 3);

    let result = evaluate_integrity_policy(&policy, &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Failed);

    assert!(
        result
            .violations
            .iter()
            .any(|violation| { violation.threshold == IntegrityThreshold::MaximumCount })
    );
}

#[test]
fn policy_violation_reports_are_bounded() {
    use marketforge_engine::process::metrics::MAX_POLICY_VIOLATIONS;

    let mut policy = policy();

    policy.invalid_record.windows.hourly = Some(marketforge_engine::job::IntegrityWindowRule {
        max_rate: 0.0,
        minimum_samples: 1,
    });

    let mut metrics = ScopedIntegrityMetrics::default();

    let scope = scope();

    let first_hour = 1_788_220_800_000_000_000;
    let hour_ns = 3_600_000_000_000;

    for hour in 0..150 {
        metrics
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                true,
                Some(first_hour + hour * hour_ns),
            )
            .unwrap();
    }

    let result = evaluate_integrity_policy(&policy, &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Failed);

    assert!(result.total_policy_violations > 100);

    assert_eq!(result.violations.len(), MAX_POLICY_VIOLATIONS);
}

#[test]
fn disabled_enforcement_accepts_violations_as_degraded() {
    let mut policy = policy();

    policy.enabled = false;

    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::SequenceGap, 0, 1);

    let result = evaluate_integrity_policy(&policy, &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Degraded);
    assert!(!result.is_failed());
}

#[test]
fn disabled_enforcement_preserves_clean_status() {
    let mut policy = policy();

    policy.enabled = false;

    let metrics = ScopedIntegrityMetrics::default();

    let result = evaluate_integrity_policy(&policy, &metrics).unwrap();

    assert_eq!(result.status, IntegrityStatus::Clean);
}

#[test]
fn immediate_enforcement_defers_rate_thresholds() {
    let mut policy = policy();

    policy.invalid_record.action = IntegrityAction::Degrade;
    policy.invalid_record.max_count = Some(100);

    policy.invalid_record.max_rate = Some(0.001);
    policy.invalid_record.minimum_samples = 1;

    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::InvalidRecord, 9, 1);

    let immediate =
        enforce_immediate_integrity(&policy, &metrics, IntegrityCategory::InvalidRecord).unwrap();

    assert_eq!(immediate.status, IntegrityStatus::Degraded);

    let final_result = evaluate_integrity_policy(&policy, &metrics).unwrap();

    assert_eq!(final_result.status, IntegrityStatus::Failed);
}

#[test]
fn immediate_enforcement_rejects_count_threshold() {
    let mut policy = policy();

    policy.invalid_record.action = IntegrityAction::Degrade;
    policy.invalid_record.max_count = Some(1);

    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::InvalidRecord, 0, 2);

    let result =
        enforce_immediate_integrity(&policy, &metrics, IntegrityCategory::InvalidRecord).unwrap();

    assert_eq!(result.status, IntegrityStatus::Failed);

    assert!(
        result
            .violations
            .iter()
            .any(|violation| { violation.threshold == IntegrityThreshold::MaximumCount })
    );
}

#[test]
fn immediate_enforcement_rejects_fatal_category() {
    let policy = policy();

    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::SequenceGap, 0, 1);

    let result =
        enforce_immediate_integrity(&policy, &metrics, IntegrityCategory::SequenceGap).unwrap();

    assert_eq!(result.status, IntegrityStatus::Failed);
}

#[test]
fn immediate_enforcement_respects_disabled_policy() {
    let mut policy = policy();

    policy.enabled = false;

    let mut metrics = ScopedIntegrityMetrics::default();

    record(&mut metrics, IntegrityCategory::SequenceGap, 0, 1);

    let result =
        enforce_immediate_integrity(&policy, &metrics, IntegrityCategory::SequenceGap).unwrap();

    assert_eq!(result.status, IntegrityStatus::Clean);
}
