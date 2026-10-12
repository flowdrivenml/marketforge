use std::{fs, path::Path};

use marketforge_engine::{
    job::MergeTimeRange,
    process::merge::{
        MergeInputDataset, MergeTimelineFile, PhysicalMergeManifest, UnifiedMergeMode,
        UnifiedMergeStream, UnifiedMergeStreamKind, ValidatedPhysicalDataset,
        prepare_physical_publication,
    },
};

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn manifest() -> PhysicalMergeManifest {
    let validated = ValidatedPhysicalDataset {
        events: 2,
        trade_events: 2,
        depth_events: 0,
        canonical_rows: 2,
        files_written: 1,
        row_groups: 1,
        first_timestamp_ns: Some(100),
        last_timestamp_ns: Some(200),
        files: vec![MergeTimelineFile {
            path: "timeline/partition-000000/part-000000.parquet".into(),
            size_bytes: 4096,
            partition_id: Some(0),
            rows: Some(2),
            start_timestamp_ns: Some(100),
            end_timestamp_ns: Some(200),
        }],
    };

    PhysicalMergeManifest::new(
        999,
        UnifiedMergeMode::Trades,
        vec![MergeInputDataset {
            dataset_id: 88,
            dataset_path: "/canonical/88".into(),
            manifest_version: 1,
        }],
        MergeTimeRange {
            start_ns: 0,
            end_ns: 1000,
        },
        vec![UnifiedMergeStream {
            stream_rank: 0,
            dataset_id: 88,
            kind: UnifiedMergeStreamKind::Trade,
            exchange: "bybit".into(),
            instrument_id: 688,
            symbol: "BTCUSDT".into(),
            stream_id: "bybit:BTCUSDT:trade".into(),
            base_currency: Some("BTC".into()),
            quote_currency: Some("USDT".into()),
        }],
        validated,
        vec![],
        vec![],
        vec![],
        vec![],
    )
    .unwrap()
}

fn create_staging(root: &Path) {
    let logical = root.join("logical/timeline/partition-000000");
    let physical = root.join("physical/timeline/partition-000000");

    fs::create_dir_all(&logical).unwrap();
    fs::create_dir_all(&physical).unwrap();

    fs::write(
        logical.join("part-000000.parquet"),
        b"logical-reference-data",
    )
    .unwrap();

    fs::write(
        physical.join("part-000000.parquet"),
        b"physical-market-data",
    )
    .unwrap();
}

#[test]
fn publishes_only_physical_timeline() {
    let dir = tempfile::tempdir().unwrap();

    create_staging(dir.path());

    let manifest = manifest();

    prepare_physical_publication(dir.path(), &manifest).unwrap();

    assert!(dir.path().join("manifest.json").exists());

    assert!(
        dir.path()
            .join("timeline/partition-000000/part-000000.parquet")
            .exists()
    );

    assert!(!dir.path().join("logical").exists());
    assert!(!dir.path().join("physical").exists());

    let bytes = fs::read(
        dir.path()
            .join("timeline/partition-000000/part-000000.parquet"),
    )
    .unwrap();

    assert_eq!(bytes, b"physical-market-data");

    let json = fs::read_to_string(dir.path().join("manifest.json")).unwrap();

    let restored: PhysicalMergeManifest = serde_json::from_str(&json).unwrap();

    restored.validate().unwrap();

    assert_eq!(restored.events_written, 2);
    assert_eq!(restored.files_written, 1);
}

#[test]
fn rejects_missing_physical_timeline() {
    let dir = tempfile::tempdir().unwrap();

    fs::create_dir_all(dir.path().join("logical")).unwrap();

    assert!(prepare_physical_publication(dir.path(), &manifest(),).is_err());

    assert!(!dir.path().join("manifest.json").exists());
}

#[test]
fn rejects_existing_publication_timeline() {
    let dir = tempfile::tempdir().unwrap();

    create_staging(dir.path());

    fs::create_dir_all(dir.path().join("timeline")).unwrap();

    assert!(prepare_physical_publication(dir.path(), &manifest(),).is_err());

    assert!(dir.path().join("logical").exists());
    assert!(dir.path().join("physical").exists());
}

#[test]
fn rejects_existing_publication_manifest() {
    let dir = tempfile::tempdir().unwrap();

    create_staging(dir.path());

    fs::write(dir.path().join("manifest.json"), b"existing").unwrap();

    assert!(prepare_physical_publication(dir.path(), &manifest(),).is_err());

    assert!(dir.path().join("logical").exists());
    assert!(dir.path().join("physical").exists());
}

#[test]
fn rejects_invalid_physical_manifest() {
    let dir = tempfile::tempdir().unwrap();

    create_staging(dir.path());

    let mut invalid_manifest = manifest();
    invalid_manifest.events_written = 999;

    assert!(prepare_physical_publication(dir.path(), &invalid_manifest,).is_err());

    assert!(!dir.path().join("manifest.json").exists());
    assert!(dir.path().join("logical").exists());
    assert!(dir.path().join("physical").exists());
}
