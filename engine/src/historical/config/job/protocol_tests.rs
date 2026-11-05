//! Pure wire/configuration tests. Never call an executor or inspect source archives.
use super::*;
use crate::historical::normalize::config::load_processing_config;
use std::{fs, path::PathBuf};
use tempfile::TempDir;

const EXAMPLE: &str = include_str!("../../../../../docs/historical/normalize-job-v3.example.json");
const RUNTIME: &str = include_str!("../../../../../data/.config/historical/normalize.toml");

struct Fixture {
    root: TempDir,
    catalog: FormatCatalog,
    runtime: ProcessingConfig,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        // Copy configuration only to establish a relocated project and catalog.
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical");
        fn copy(source: &Path, dest: &Path) {
            fs::create_dir_all(dest).unwrap();
            for entry in fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    copy(&entry.path(), &dest.join(entry.file_name()));
                } else {
                    fs::copy(entry.path(), dest.join(entry.file_name())).unwrap();
                }
            }
        }
        let catalog_root = root.path().join("data/.config/historical");
        copy(&source.join("profiles"), &catalog_root.join("profiles"));
        copy(
            &source.join("raw_schemas"),
            &catalog_root.join("raw_schemas"),
        );
        fs::write(catalog_root.join("normalize.toml"), RUNTIME).unwrap();
        let catalog = FormatCatalog::load(catalog_root).unwrap();
        let runtime =
            load_processing_config("data/.config/historical/normalize.toml", root.path()).unwrap();
        Self {
            root,
            catalog,
            runtime,
        }
    }
    fn load(&self, value: &Value) -> Result<ProcessingJob> {
        fs::write(
            self.root.path().join("job.json"),
            serde_json::to_vec(value).unwrap(),
        )
        .unwrap();
        load_normalization_job("job.json", &self.catalog, &self.runtime, self.root.path())
    }
}
fn example() -> Value {
    serde_json::from_str(EXAMPLE).unwrap()
}

#[test]
fn v3_example_deserializes_resolves_and_uses_global_runtime() {
    let fixture = Fixture::new();
    let wire: NormalizationJobV3 = serde_json::from_str(EXAMPLE).unwrap();
    assert_eq!(wire.protocol_version, 3);
    let job = fixture.load(&example()).unwrap();
    assert_eq!(job.operation, ProcessingOperation::Process);
    assert_eq!(job.protocol_version, PROCESSING_PROTOCOL_VERSION);
    assert_eq!(job.resources, fixture.runtime.resources);
    assert_eq!(job.integrity_policy, fixture.runtime.integrity_policy);
    assert_eq!(
        job.tasks[0].input_path,
        fixture.root.path().join("data/raw/BTCUSDT.csv.gz")
    );
    assert_eq!(
        job.output.dataset_path,
        fixture.root.path().join("data/normalized/501")
    );
    assert_eq!(
        job.output.staging_path,
        fixture
            .root
            .path()
            .join("data/normalized/.501.staging.1001")
    );
    let profile = fixture.catalog.get("bybit.spot.spot.bybit-t1.v1").unwrap();
    assert_eq!(job.tasks[0].normalizations, profile.normalizations);
    assert_eq!(
        job.tasks[0].raw_schema["marketforge_profile"]["sha256"],
        profile.fingerprint
    );
    assert!(!job.tasks[0].input_path.exists());
    assert!(!job.output.staging_path.exists());
    assert!(!job.resources.scratch_path.exists());
    // A loader lacking an explicit runtime must not fabricate settings for v3.
    assert!(
        load_document_with_catalog(fixture.root.path().join("job.json"), &fixture.catalog)
            .unwrap_err()
            .to_string()
            .contains("explicit runtime")
    );
}

#[test]
fn v3_unknown_fields_required_fields_and_operations_fail() {
    let fixture = Fixture::new();
    for pointer in [
        "/resources",
        "/integrity_policy",
        "/typo",
        "/tasks/0/typo",
        "/tasks/0/instrument/typo",
        "/output/typo",
        "/ordering/typo",
        "/streams/0/typo",
    ] {
        let mut value = example();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        value
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.into(), serde_json::json!({}));
        assert!(serde_json::from_value::<NormalizationJobV3>(value.clone()).is_err());
        let error = fixture.load(&value).unwrap_err().to_string();
        assert!(error.contains("unknown field"), "{error}");
    }
    for field in [
        "tasks",
        "job_id",
        "dataset_id",
        "operation",
        "streams",
        "ordering",
        "output",
    ] {
        let mut value = example();
        value.as_object_mut().unwrap().remove(field);
        assert!(fixture.load(&value).is_err(), "{field}");
    }
    let mut value = example();
    value["operation"] = "process".into();
    assert!(
        fixture
            .load(&value)
            .unwrap_err()
            .to_string()
            .contains("normalize")
    );
    value["protocol_version"] = 4.into();
    assert!(
        fixture
            .load(&value)
            .unwrap_err()
            .to_string()
            .contains("protocol_version")
    );
}

#[test]
fn v3_checks_fingerprints_profiles_instruments_and_streams() {
    let fixture = Fixture::new();
    let mut pinned = example();
    pinned["tasks"][0]["profile_fingerprint"] = fixture
        .catalog
        .get("bybit.spot.spot.bybit-t1.v1")
        .unwrap()
        .fingerprint
        .clone()
        .into();
    fixture.load(&pinned).unwrap();
    for (pointer, replacement, expected) in [
        (
            "/tasks/0/profile_fingerprint",
            Value::from("stale"),
            "fingerprint mismatch",
        ),
        (
            "/tasks/0/format_profile",
            Value::from("unknown"),
            "unknown Historical profile",
        ),
        (
            "/tasks/0/format_profile",
            Value::from("bybit.option.option.bybit-t3.v1"),
            "unsupported profile",
        ),
        (
            "/tasks/0/instrument/tick_size",
            Value::from("0"),
            "tick size",
        ),
        ("/tasks/0/exchange", Value::from("okx"), "incompatible"),
        (
            "/tasks/0/stream_id",
            Value::from("absent"),
            "unknown stream",
        ),
        (
            "/tasks/0/source_compression",
            Value::from("zip"),
            "unsupported source container",
        ),
    ] {
        let mut value = example();
        *value.pointer_mut(pointer).unwrap() = replacement;
        let error = fixture.load(&value).unwrap_err().to_string();
        assert!(error.contains(expected), "{pointer}: {error}");
    }
    let mut value = example();
    for field in [
        "profile_fingerprint",
        "archive_member",
        "source_compression",
    ] {
        value["tasks"][0].as_object_mut().unwrap().remove(field);
    }
    fixture.load(&value).unwrap();
}

#[test]
fn absolute_job_input_output_paths_remain_unchanged() {
    let fixture = Fixture::new();
    let mut value = example();
    for (pointer, name) in [
        ("/tasks/0/input_path", "absolute-input"),
        ("/output/staging_path", "absolute-stage"),
        ("/output/dataset_path", "absolute-output"),
    ] {
        *value.pointer_mut(pointer).unwrap() =
            fixture.root.path().join(name).to_str().unwrap().into();
    }
    let job = fixture.load(&value).unwrap();
    assert_eq!(
        job.tasks[0].input_path,
        fixture.root.path().join("absolute-input")
    );
    assert_eq!(
        job.output.staging_path,
        fixture.root.path().join("absolute-stage")
    );
    assert_eq!(
        job.output.dataset_path,
        fixture.root.path().join("absolute-output")
    );
    let path = fixture.root.path().join("job.json");
    load_normalization_job(
        &path,
        &fixture.catalog,
        &fixture.runtime,
        fixture.root.path(),
    )
    .unwrap();
}

#[test]
fn legacy_v1_v2_validate_embedded_fields_but_global_settings_win() {
    let fixture = Fixture::new();
    let mut v2 = example();
    v2["protocol_version"] = 2.into();
    v2["operation"] = "process".into();
    v2["resources"] = serde_json::to_value(&fixture.runtime.resources).unwrap();
    v2["resources"]["workers"] = 7.into();
    v2["resources"]["memory_budget_bytes"] = 2147483648_u64.into();
    v2["resources"]["scratch_budget_bytes"] = 2147483648_u64.into();
    v2["resources"]["scratch_path"] = "ignored-legacy-scratch".into();
    v2["resources"]["parquet"]["row_group_target_bytes"] = 33554432_u64.into();
    v2["resources"]["parquet"]["file_target_bytes"] = 134217728_u64.into();
    v2["integrity_policy"] = serde_json::to_value(&fixture.runtime.integrity_policy).unwrap();
    v2["integrity_policy"]["enabled"] = false.into();
    v2["integrity_policy"]["parse_failure"]["action"] = "degrade".into();
    v2["integrity_policy"]["parse_failure"]["max_count"] = 100.into();
    let job = fixture.load(&v2).unwrap();
    assert_eq!(job.resources, fixture.runtime.resources);
    assert_eq!(job.integrity_policy, fixture.runtime.integrity_policy);
    let mut v1 = serde_json::to_value(job).unwrap();
    v1["resources"]["workers"] = 9.into();
    v1["integrity_policy"]["enabled"] = false.into();
    let job = fixture.load(&v1).unwrap();
    assert_eq!(job.resources, fixture.runtime.resources);
    assert_eq!(job.integrity_policy, fixture.runtime.integrity_policy);
    for mut legacy in [v1, v2] {
        let valid = legacy.clone();
        legacy["resources"]["workers"] = 0.into();
        assert!(
            fixture
                .load(&legacy)
                .unwrap_err()
                .to_string()
                .contains("workers")
        );
        legacy = valid.clone();
        legacy["integrity_policy"]["parse_failure"]["max_rate"] = 1.1.into();
        assert!(
            fixture
                .load(&legacy)
                .unwrap_err()
                .to_string()
                .contains("parse_failure")
        );
        legacy = valid;
        if legacy["protocol_version"] == 1 {
            legacy["tasks"][0]["raw_schema"]["marketforge_profile"]["sha256"] = "stale".into();
        } else {
            legacy["tasks"][0]["profile_fingerprint"] = "stale".into();
        }
        assert!(
            fixture
                .load(&legacy)
                .unwrap_err()
                .to_string()
                .contains("fingerprint")
        );
    }
}
