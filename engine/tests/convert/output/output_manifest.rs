use marketforge_engine::{
    job::load_convert_job,
    process::{
        convert::output::{
            ConversionManifest, validate_converted_trades, validate_source_preservation,
        },
        merge::PhysicalMergeManifest,
    },
};
use std::sync::OnceLock;
use std::{fs, path::PathBuf};

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_IDENTITY: u64 = 11_435_942;
const EXPECTED_CONVERTED: u64 = 1_180_489;
const EXPECTED_UNAVAILABLE: u64 = 11_215;
const EXPECTED_REFERENCES: u64 = 2_308_230;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}
fn fixture() -> ConversionManifest {
    static MANIFEST: OnceLock<ConversionManifest> = OnceLock::new();

    MANIFEST.get_or_init(build_fixture).clone()
}
fn build_fixture() -> ConversionManifest {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let staging = root.join("data/converted/multihop-USDT");

    let validation = validate_converted_trades(&staging, job.conversion.max_staleness_ns).unwrap();

    let preservation = validate_source_preservation(&job.input_merge, &source, &staging).unwrap();

    ConversionManifest::build(
        &job,
        &source,
        &validation,
        &preservation,
        EXPECTED_EVENTS,
        &staging,
    )
    .unwrap()
}

// -----------------------------------------------------------------------------
// Manifest construction
// -----------------------------------------------------------------------------

#[test]
fn builds_manifest_from_real_dataset() {
    let manifest = fixture();

    assert_eq!(manifest.protocol_version, 1);
    assert_eq!(manifest.conversion_schema_version, 1);

    assert_eq!(manifest.job_id, 940004);
    assert_eq!(manifest.input.merge_job_id, 930005);

    assert_eq!(manifest.statistics.events_written, EXPECTED_EVENTS);
    assert_eq!(manifest.statistics.identity, EXPECTED_IDENTITY);
    assert_eq!(manifest.statistics.converted, EXPECTED_CONVERTED);
    assert_eq!(manifest.statistics.unavailable, EXPECTED_UNAVAILABLE);

    assert_eq!(manifest.integrity.references_checked, EXPECTED_REFERENCES);
    assert_eq!(manifest.integrity.source_events_compared, EXPECTED_EVENTS);

    manifest.validate().unwrap();
}

// -----------------------------------------------------------------------------
// Physical file inventory
// -----------------------------------------------------------------------------

#[test]
fn records_physical_file_inventory() {
    let manifest = fixture();

    assert_eq!(manifest.output.files_written, 1);
    assert_eq!(manifest.output.row_groups, 48);

    let file = &manifest.output.files[0];

    assert_eq!(file.path, PathBuf::from("part-000000.parquet"));
    assert_eq!(file.rows, EXPECTED_EVENTS);
    assert_eq!(file.row_groups, 48);
    assert!(file.size_bytes > 0);
}

// -----------------------------------------------------------------------------
// Conversion configuration
// -----------------------------------------------------------------------------

#[test]
fn preserves_conversion_configuration() {
    let manifest = fixture();

    assert_eq!(manifest.conversion.target_quote, "USDT");
    assert_eq!(manifest.conversion.max_staleness_ns, 5_000_000_000);
    assert_eq!(manifest.conversion.max_hops, 3);
    assert_eq!(manifest.conversion.references.len(), 3);
}

// -----------------------------------------------------------------------------
// JSON serialization
// -----------------------------------------------------------------------------

#[test]
fn serializes_and_deserializes_manifest() {
    let manifest = fixture();

    let json = serde_json::to_vec_pretty(&manifest).unwrap();

    let decoded: ConversionManifest = serde_json::from_slice(&json).unwrap();

    decoded.validate().unwrap();

    assert_eq!(decoded.job_id, manifest.job_id);
    assert_eq!(
        decoded.statistics.events_written,
        manifest.statistics.events_written
    );
    assert_eq!(decoded.output.files_written, manifest.output.files_written);
}

// -----------------------------------------------------------------------------
// Reject invalid accounting
// -----------------------------------------------------------------------------

#[test]
fn rejects_incorrect_event_count() {
    let mut manifest = fixture();

    manifest.statistics.events_written += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_incorrect_status_accounting() {
    let mut manifest = fixture();

    manifest.statistics.converted += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_incomplete_integrity() {
    let mut manifest = fixture();

    manifest.integrity.source_preservation = false;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_incorrect_file_count() {
    let mut manifest = fixture();

    manifest.output.files_written += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_incorrect_row_group_count() {
    let mut manifest = fixture();

    manifest.output.row_groups += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_incorrect_source_preservation_count() {
    let mut manifest = fixture();

    manifest.integrity.source_events_compared -= 1;

    assert!(manifest.validate().is_err());
}

// -----------------------------------------------------------------------------
// Manifest writing
// -----------------------------------------------------------------------------

#[test]
fn writes_manifest_without_overwriting() {
    let manifest = fixture();

    let temporary = tempfile::tempdir().unwrap();

    let path = manifest.write(temporary.path()).unwrap();

    assert!(path.is_file());

    let decoded: ConversionManifest = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

    decoded.validate().unwrap();

    // Existing manifests must never be silently overwritten.
    assert!(manifest.write(temporary.path()).is_err());
}
