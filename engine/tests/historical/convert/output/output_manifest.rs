use std::{fs, path::PathBuf};

use marketforge_engine::historical::{
    convert::{
        execution::materialize_trade_conversion,
        output::{ConversionExecutionReceipt, ConversionManifest, publish_conversion},
        plan_conversion, resolve_conversion_catalog,
    },
    merge::UnifiedMergeMode,
};

const EXPECTED_EVENTS: u64 = 6;

fn fixture() -> ConversionManifest {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = crate::conversion_fixture::canonical_job();
    job.input_merge = root.join("trades");
    job.output.dataset_path = root.join("output");
    let source = crate::market::create(&job.input_merge, UnifiedMergeMode::Trades, false);
    let catalog = crate::market::catalog(root);
    let resolved = resolve_conversion_catalog(&job, &source, &catalog, root).unwrap();
    let plan = plan_conversion(&job, &resolved).unwrap();
    let stage = root.join("staging");
    let metrics =
        materialize_trade_conversion(&job, &source, &plan, &stage, &crate::market::parquet())
            .unwrap();
    ConversionExecutionReceipt::new(job.job_id, &metrics)
        .unwrap()
        .write(&stage)
        .unwrap();
    let published = publish_conversion(&job, &source, &stage).unwrap();
    serde_json::from_reader(fs::File::open(published.manifest_path).unwrap()).unwrap()
}

// -----------------------------------------------------------------------------
// Manifest construction
// -----------------------------------------------------------------------------

#[test]
fn builds_manifest_from_complete_trade_dataset() {
    let manifest = fixture();

    assert_eq!(manifest.protocol_version, 1);
    assert_eq!(manifest.conversion_schema_version, 1);

    assert_eq!(manifest.job_id, 265083950276773788);
    assert_eq!(manifest.input.merge_job_id, 901);

    assert_eq!(manifest.statistics.events_written, EXPECTED_EVENTS);
    assert_eq!(manifest.statistics.identity, 3);
    assert_eq!(manifest.statistics.converted, 2);
    assert_eq!(manifest.statistics.unavailable, 1);

    assert_eq!(manifest.integrity.references_checked, 3);
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

    let file = &manifest.output.files[0];

    assert_eq!(file.path, PathBuf::from("part-000000.parquet"));
    assert_eq!(file.rows, EXPECTED_EVENTS);
    assert!(file.row_groups > 0);
    assert_eq!(manifest.output.row_groups, file.row_groups);
    assert!(file.size_bytes > 0);
}

// -----------------------------------------------------------------------------
// Conversion configuration
// -----------------------------------------------------------------------------

#[test]
fn preserves_conversion_configuration() {
    let manifest = fixture();

    assert_eq!(manifest.conversion.target_quote, "USDT");
    assert_eq!(manifest.conversion.max_staleness_ns, 60_000_000_000);
    assert_eq!(manifest.conversion.max_hops, 2);
    assert_eq!(manifest.conversion.references.len(), 2);
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
