//! Configuration-only tests. Temporary files contain TOML, never market data.
use super::*;
use tempfile::TempDir;

const TOML: &str = include_str!("fixtures/runtime.toml");

fn load(text: &str) -> Result<ProcessingConfig> {
    let temp = TempDir::new().unwrap();
    fs::write(temp.path().join("normalize.toml"), text).unwrap();
    load_processing_config("normalize.toml", temp.path())
}

#[test]
fn runtime_toml_loads_and_resolves_relative_scratch() {
    let temp = TempDir::new().unwrap();
    fs::write(temp.path().join("runtime.toml"), TOML).unwrap();
    let config = load_processing_config("runtime.toml", temp.path()).unwrap();
    assert_eq!(
        config.resources.scratch_path,
        temp.path().join("data/.work/normalize")
    );
    assert_eq!(config.resources.workers, 1);
    assert!(config.integrity_policy.enabled);
    let absolute = temp.path().join("absolute-scratch");
    let text = TOML.replace("data/.work/normalize", absolute.to_str().unwrap());
    assert_eq!(load(&text).unwrap().resources.scratch_path, absolute);
    assert!(!temp.path().join("data").exists());
}

#[test]
fn invalid_toml_reports_file_and_field() {
    for (old, new, expected) in [
        ("schema_version = 1", "schema_version = 2", "schema_version"),
        ("schema_version = 1", "", "schema_version"),
        ("workers = 1", "", "workers"),
        ("workers = 1", "workers = -1", "workers"),
        ("workers = 1", "workers = 0", "resources.workers"),
        (
            "memory_budget_bytes = 1073741824",
            "memory_budget_bytes = 0",
            "memory_budget_bytes",
        ),
        (
            "scratch_budget_bytes = 1073741824",
            "scratch_budget_bytes = 0",
            "scratch_budget_bytes",
        ),
        (
            "scratch_path = \"data/.work/normalize\"",
            "scratch_path = \"\"",
            "scratch_path",
        ),
        (
            "row_group_target_bytes = 67108864",
            "row_group_target_bytes = 0",
            "row_group_target_bytes",
        ),
        (
            "file_target_bytes = 268435456",
            "file_target_bytes = 1",
            "row_group_target_bytes",
        ),
        ("action = \"fail\"", "action = \"degrade\"", "degrade"),
        ("action = \"fail\"", "action = \"ignore\"", "ignore"),
        ("workers = 1", "workers = 1\nunknown = 2", "unknown"),
        (
            "schema_version = 1",
            "schema_version = 1\nunknown = true",
            "unknown",
        ),
        (
            "row_group_target_bytes = 67108864",
            "row_group_target_bytes = 67108864\nunknown = true",
            "unknown",
        ),
        (
            "max_count = 0",
            "max_rate = 1.1",
            "integrity_policy.parse_failure",
        ),
        (
            "max_count = 0",
            "max_rate = nan",
            "integrity_policy.parse_failure",
        ),
    ] {
        // Degrade with max_count is valid; remove thresholds for the invalid case.
        let mut text = TOML.replace(old, new);
        if new == "action = \"degrade\"" {
            text = text.replace("max_count = 0", "");
        }
        let error = load(&text).unwrap_err().to_string();
        assert!(
            error.contains("normalize.toml") && error.contains(expected),
            "{expected}: {error}"
        );
    }
}

#[test]
fn roots_and_absolute_paths_are_explicit() {
    let temp = TempDir::new().unwrap();
    let file = temp.path().join("file");
    fs::write(&file, "").unwrap();
    assert!(project_root(&file).is_err());
    assert!(project_root(temp.path().join("missing")).is_err());
    assert_eq!(resolve_path(temp.path(), &file), file);
    assert_eq!(
        resolve_path(temp.path(), Path::new("relative")),
        temp.path().join("relative")
    );
}

#[test]
fn required_sections_nested_unknown_fields_and_scratch_files_fail() {
    for field in ["resources", "integrity_policy"] {
        let mut document: toml::Value = toml::from_str(TOML).unwrap();
        document.as_table_mut().unwrap().remove(field);
        let error = load(&toml::to_string(&document).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains(field), "{error}");
    }
    for path in [
        vec!["integrity_policy"],
        vec!["integrity_policy", "parse_failure"],
    ] {
        let mut document: toml::Value = toml::from_str(TOML).unwrap();
        let mut table = &mut document;
        for key in path {
            table = table.get_mut(key).unwrap();
        }
        table
            .as_table_mut()
            .unwrap()
            .insert("unknown".into(), toml::Value::Boolean(true));
        assert!(
            load(&toml::to_string(&document).unwrap())
                .unwrap_err()
                .to_string()
                .contains("unknown")
        );
    }
    let temp = TempDir::new().unwrap();
    let scratch = temp.path().join("file");
    fs::write(&scratch, "configuration-test").unwrap();
    let text = TOML.replace("data/.work/normalize", scratch.to_str().unwrap());
    assert!(
        load(&text)
            .unwrap_err()
            .to_string()
            .contains("resources.scratch_path")
    );
}

#[test]
fn authoritative_config_preserves_effective_settings() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let text = fs::read_to_string(root.join("data/.config/historical/general.toml")).unwrap();
    let document: RuntimeDocument = toml::from_str(&text).unwrap();
    let config = load_processing_config("data/.config/historical/general.toml", root).unwrap();
    let mut expected = document.resources;
    expected.scratch_path = root.join(expected.scratch_path);
    assert_eq!(config.resources, expected);
    assert_eq!(config.integrity_policy, document.integrity_policy);
}

#[test]
fn optional_policy_thresholds_and_existing_enforcement_are_preserved() {
    use crate::historical::normalize::metrics::{
        IntegrityCategory, IntegrityScope, IntegrityStatus, ScopedIntegrityMetrics,
        enforce_immediate_integrity,
    };
    let text = TOML.replace("max_count = 0", "max_count = 1000\nmax_rate = 0.1\nminimum_samples = 100\nwindows.hourly = { max_rate = 0.2, minimum_samples = 10 }\nwindows.daily = { max_rate = 0.3, minimum_samples = 20 }");
    let mut config = load(&text).unwrap();
    assert_eq!(
        config
            .integrity_policy
            .parse_failure
            .windows
            .hourly
            .as_ref()
            .unwrap()
            .minimum_samples,
        10
    );
    assert_eq!(
        config
            .integrity_policy
            .parse_failure
            .windows
            .daily
            .as_ref()
            .unwrap()
            .max_rate,
        0.3
    );
    let scope = IntegrityScope {
        task_id: 1,
        stream_id: "test".into(),
        source_file: "synthetic".into(),
    };
    let mut metrics = ScopedIntegrityMetrics::default();
    metrics
        .record(&scope, IntegrityCategory::ParseFailure, true, None)
        .unwrap();
    assert_eq!(
        enforce_immediate_integrity(
            &config.integrity_policy,
            &metrics,
            IntegrityCategory::ParseFailure
        )
        .unwrap()
        .status,
        IntegrityStatus::Failed
    );
    config.integrity_policy.parse_failure.action = super::super::IntegrityAction::Degrade;
    assert_eq!(
        enforce_immediate_integrity(
            &config.integrity_policy,
            &metrics,
            IntegrityCategory::ParseFailure
        )
        .unwrap()
        .status,
        IntegrityStatus::Degraded
    );
}

#[test]
fn malformed_and_out_of_range_values_are_rejected() {
    assert!(load("[invalid").is_err());
    for (old, new) in [
        ("file_target_bytes = 268435456", "file_target_bytes = 0"),
        ("workers = 1", "workers = 18446744073709551616"),
        (
            "memory_budget_bytes = 1073741824",
            "memory_budget_bytes = -1",
        ),
        (
            "scratch_budget_bytes = 1073741824",
            "scratch_budget_bytes = -1",
        ),
        (
            "max_count = 0",
            "windows.hourly = { max_rate = -0.1, minimum_samples = 1 }",
        ),
    ] {
        assert!(load(&TOML.replace(old, new)).is_err(), "{new}");
    }
}

#[test]
fn normalization_limits_default_validate_and_serialize() {
    let config = load(TOML).unwrap();
    assert_eq!(
        config.resources.normalization_limits,
        super::super::limits::NormalizationLimits::default()
    );
    assert_eq!(
        serde_json::to_value(&config).unwrap()["resources"]["normalization_limits"]["open_files"],
        64
    );
    for field in [
        "workbook_bytes",
        "workbook_xml_bytes",
        "workbook_rows",
        "record_bytes",
        "depth_levels",
        "metric_entries",
        "open_files",
    ] {
        let text = TOML.replace(
            "[resources.parquet]",
            &format!("[resources.normalization_limits]\n{field} = 0\n[resources.parquet]"),
        );
        assert!(load(&text).unwrap_err().to_string().contains(field));
    }
    let text = TOML.replace(
        "[resources.parquet]",
        "[resources.normalization_limits]\nunknown_limit = 1\n[resources.parquet]",
    );
    assert!(
        load(&text)
            .unwrap_err()
            .to_string()
            .contains("unknown_limit")
    );
}

#[test]
fn performance_settings_default_validate_and_roundtrip() {
    let config = load(TOML).unwrap();
    assert_eq!(config.diagnostics.mode, DiagnosticsMode::Disabled);
    assert_eq!(config.diagnostics.interval_ms, 1000);
    assert_eq!(config.merge_fan_in, 8);
    assert_eq!(config.scratch_format, ScratchFormat::Json);
    for setting in [
        "merge_fan_in = 1",
        "merge_fan_in = 65",
        "scratch_format = \"unknown\"",
        "scratch_codec = \"unknown\"",
        "[diagnostics]\ninterval_ms = 0",
        "[diagnostics]\nmode = \"unknown\"",
    ] {
        assert!(
            load(&TOML.replace(
                "schema_version = 1",
                &format!("schema_version = 1\n{setting}")
            ))
            .is_err(),
            "{setting}"
        );
    }
    let text=TOML.replace("schema_version = 1","schema_version = 1\nmerge_fan_in = 3\nscratch_format = \"typed_trades\"\nscratch_codec = \"uncompressed\"\n[diagnostics]\nmode = \"detailed\"\ninterval_ms = 100");
    let config = load(&text).unwrap();
    let value = serde_json::to_value(&config).unwrap();
    let decoded: ProcessingConfig = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.merge_fan_in, 3);
    assert_eq!(decoded.scratch_codec, ScratchCodec::Uncompressed);
    assert_eq!(decoded.diagnostics.mode, DiagnosticsMode::Detailed);
    let typed_events = load(&text.replace("typed_trades", "typed_events")).unwrap();
    assert_eq!(typed_events.scratch_format, ScratchFormat::TypedEvents);
}

#[test]
fn shared_runtime_preserves_operation_specific_validation() {
    let root = TempDir::new().unwrap();
    let path = Path::new("runtime.toml");
    fs::write(
        root.path().join(path),
        format!("{TOML}\n[merge]\npartitions_per_worker = 0\n"),
    )
    .unwrap();
    let normalization = load_processing_config(path, root.path()).unwrap();
    let (conversion, parquet) = load_conversion_runtime_settings(path, root.path()).unwrap();
    assert_eq!(conversion.workers, normalization.resources.workers);
    assert_eq!(
        parquet.row_group_target_mb * 1024 * 1024,
        normalization.resources.parquet.row_group_target_bytes
    );
    assert!(load_merge_runtime_settings(path, root.path()).is_err());
}
