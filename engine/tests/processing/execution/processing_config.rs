#![cfg(feature = "process")]

use std::path::{Path, PathBuf};

use marketforge_engine::process::{load_processing_config, validate_integrity_policy};

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn load_config() -> marketforge_engine::process::ProcessingConfig {
    let root = project_root();

    load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load global processing configuration")
}

#[test]
fn loads_global_processing_configuration() {
    let config = load_config();

    assert!(config.resources.workers > 0);
    assert!(config.resources.memory_budget_bytes > 0);

    assert!(config.resources.scratch_path.is_absolute());

    validate_integrity_policy(&config.integrity_policy).expect("valid integrity policy");
}

#[test]
fn supports_global_integrity_thresholds() {
    let config = load_config();

    let rule = &config.integrity_policy.invalid_record;

    assert_eq!(rule.max_count, Some(10_000));
    assert_eq!(rule.max_rate, Some(0.001));
    assert_eq!(rule.minimum_samples, 1000);

    let daily = rule.windows.daily.as_ref().expect("daily window");

    assert_eq!(daily.max_rate, 0.005);
    assert_eq!(daily.minimum_samples, 1000);

    let hourly = rule.windows.hourly.as_ref().expect("hourly window");

    assert_eq!(hourly.max_rate, 0.02);
    assert_eq!(hourly.minimum_samples, 100);
}

#[test]
fn rejects_invalid_global_rate() {
    let mut config = load_config();

    config.integrity_policy.invalid_record.max_rate = Some(1.5);

    assert!(validate_integrity_policy(&config.integrity_policy).is_err());
}

#[test]
fn rejects_negative_rate() {
    let mut config = load_config();

    config.integrity_policy.parse_failure.max_rate = Some(-0.1);

    assert!(validate_integrity_policy(&config.integrity_policy).is_err());
}

#[test]
fn rejects_non_finite_rate() {
    let mut config = load_config();

    config.integrity_policy.invalid_record.max_rate = Some(f64::NAN);

    assert!(validate_integrity_policy(&config.integrity_policy).is_err());
}

#[test]
fn accepts_legacy_integrity_rules() {
    let json = r#"
    {
        "action": "fail",
        "max_count": 0
    }
    "#;

    let rule: marketforge_engine::job::IntegrityRule = serde_json::from_str(json).unwrap();

    assert_eq!(rule.max_count, Some(0));
    assert_eq!(rule.max_rate, None);
    assert_eq!(rule.minimum_samples, 0);
    assert!(rule.windows.daily.is_none());
    assert!(rule.windows.hourly.is_none());
}

#[test]
fn global_integrity_enforcement_is_enabled() {
    let config = load_config();

    assert!(config.integrity_policy.enabled);
}

#[test]
fn legacy_standard_profile_is_accepted() {
    let mut config = load_config();

    let mut value = serde_json::to_value(&config.integrity_policy).unwrap();

    value.as_object_mut().unwrap().remove("enabled");

    value["profile"] = serde_json::json!("standard");

    let policy: marketforge_engine::job::IntegrityPolicy = serde_json::from_value(value).unwrap();

    assert!(policy.enabled);

    config.integrity_policy = policy;
}

#[test]
fn legacy_strict_profile_is_accepted() {
    let config = load_config();

    let mut value = serde_json::to_value(&config.integrity_policy).unwrap();

    value.as_object_mut().unwrap().remove("enabled");

    value["profile"] = serde_json::json!("strict");

    let policy: marketforge_engine::job::IntegrityPolicy = serde_json::from_value(value).unwrap();

    assert!(policy.enabled);
}
