use marketforge_engine::historical::merge::{
    PhysicalInitialReconstruction, PhysicalReconstructionAvailability,
};
use marketforge_engine::{
    job::MergeTimeRange,
    process::merge::{
        MergeInputDataset, MergeTimelineFile, PHYSICAL_MERGE_MANIFEST_VERSION,
        PHYSICAL_MERGE_SCHEMA_VERSION, PhysicalMergeManifest, UnifiedMergeMode, UnifiedMergeStream,
        UnifiedMergeStreamKind, ValidatedPhysicalDataset,
    },
};

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn input(dataset_id: u64) -> MergeInputDataset {
    MergeInputDataset {
        dataset_id,
        dataset_path: format!("/data/canonical/{dataset_id}"),
        manifest_version: 1,
    }
}

fn stream(dataset_id: u64, rank: u32, kind: UnifiedMergeStreamKind) -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: rank,
        dataset_id,
        kind,
        exchange: "bybit".into(),
        instrument_id: 688,
        symbol: "BTCUSDT".into(),
        stream_id: format!("bybit:BTCUSDT:{dataset_id}"),
        base_currency: Some("BTC".into()),
        quote_currency: Some("USDT".into()),
    }
}

fn file(partition_id: u32, rows: u64, first: i64, last: i64) -> MergeTimelineFile {
    MergeTimelineFile {
        path: format!("timeline/partition-{partition_id:06}/part-000000.parquet"),
        size_bytes: 4096,
        partition_id: Some(partition_id),
        rows: Some(rows),
        start_timestamp_ns: Some(first),
        end_timestamp_ns: Some(last),
    }
}

fn validated() -> ValidatedPhysicalDataset {
    ValidatedPhysicalDataset {
        events: 5,
        trade_events: 3,
        depth_events: 2,
        canonical_rows: 9,
        files_written: 2,
        row_groups: 2,
        first_timestamp_ns: Some(100),
        last_timestamp_ns: Some(500),
        files: vec![file(0, 3, 100, 300), file(1, 2, 400, 500)],
    }
}

fn manifest() -> PhysicalMergeManifest {
    PhysicalMergeManifest::new(
        999,
        UnifiedMergeMode::Combined,
        vec![input(88), input(89)],
        MergeTimeRange {
            start_ns: 0,
            end_ns: 1000,
        },
        vec![
            stream(88, 0, UnifiedMergeStreamKind::Trade),
            stream(89, 1, UnifiedMergeStreamKind::Depth),
        ],
        validated(),
        vec![], // Boundary links
        vec![], // Skipped initializations
        vec![], // Physical checkpoints
        vec![PhysicalInitialReconstruction {
            dataset_id: 89,
            availability: PhysicalReconstructionAvailability::Unavailable,
            first_physical_checkpoint_ns: None,
            prior_canonical_checkpoint_ns: None,
        }],
    )
    .unwrap()
}

// -----------------------------------------------------------------------------
// Serialization
// -----------------------------------------------------------------------------

#[test]
fn physical_manifest_json_roundtrip() {
    let expected = manifest();

    let json = serde_json::to_string_pretty(&expected).unwrap();

    let restored: PhysicalMergeManifest = serde_json::from_str(&json).unwrap();

    restored.validate().unwrap();

    assert_eq!(restored.protocol_version, PHYSICAL_MERGE_MANIFEST_VERSION);
    assert_eq!(
        restored.physical_schema_version,
        PHYSICAL_MERGE_SCHEMA_VERSION
    );

    assert_eq!(restored.job_id, 999);
    assert_eq!(restored.mode, UnifiedMergeMode::Combined);

    assert_eq!(restored.events_written, 5);
    assert_eq!(restored.trade_events, 3);
    assert_eq!(restored.depth_events, 2);
    assert_eq!(restored.canonical_rows, 9);

    assert_eq!(restored.files_written, 2);
    assert_eq!(restored.row_groups, 2);
}

#[test]
fn physical_manifest_preserves_file_metadata() {
    let manifest = manifest();

    assert_eq!(manifest.files.len(), 2);

    assert_eq!(manifest.files[0].partition_id, Some(0));
    assert_eq!(manifest.files[0].rows, Some(3));

    assert_eq!(manifest.files[0].start_timestamp_ns, Some(100));
    assert_eq!(manifest.files[0].end_timestamp_ns, Some(300));

    assert_eq!(manifest.files[1].partition_id, Some(1));
    assert_eq!(manifest.files[1].rows, Some(2));
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

#[test]
fn rejects_unsupported_protocol_version() {
    let mut manifest = manifest();
    manifest.protocol_version += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_unsupported_schema_version() {
    let mut manifest = manifest();
    manifest.physical_schema_version += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_duplicate_input_datasets() {
    let mut manifest = manifest();

    manifest.input_datasets[1].dataset_id = 88;
    manifest.streams[1].dataset_id = 88;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_stream_rank_mismatch() {
    let mut manifest = manifest();
    manifest.streams[1].stream_rank = 5;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_event_count_mismatch() {
    let mut manifest = manifest();
    manifest.events_written += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_file_count_mismatch() {
    let mut manifest = manifest();
    manifest.files_written += 1;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_duplicate_physical_files() {
    let mut manifest = manifest();

    manifest.files[1].path = manifest.files[0].path.clone();

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_file_row_count_mismatch() {
    let mut manifest = manifest();
    manifest.files[0].rows = Some(4);

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_missing_file_metadata() {
    let mut manifest = manifest();
    manifest.files[0].rows = None;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_invalid_file_timestamp_range() {
    let mut manifest = manifest();

    manifest.files[0].start_timestamp_ns = Some(400);
    manifest.files[0].end_timestamp_ns = Some(300);

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_invalid_merge_time_range() {
    let mut manifest = manifest();

    manifest.time_range.start_ns = 1000;
    manifest.time_range.end_ns = 1000;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_invalid_combined_streams() {
    let mut manifest = manifest();

    manifest.streams[1].kind = UnifiedMergeStreamKind::Trade;

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_unknown_json_fields() {
    let manifest = manifest();

    let mut json = serde_json::to_value(&manifest).unwrap();

    json["unexpected_field"] = serde_json::json!(true);

    assert!(serde_json::from_value::<PhysicalMergeManifest>(json).is_err());
}
