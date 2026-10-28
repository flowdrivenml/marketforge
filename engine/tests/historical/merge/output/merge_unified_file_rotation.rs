#![cfg(feature = "historical")]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int64Array, UInt8Array, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use {
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::merge::UnifiedTimelineRecord,
    marketforge_engine::historical::merge::UnifiedTimelineWriter,
    marketforge_engine::historical::merge::unified_timeline_schema,
};

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-unified-rotation-{}-{timestamp}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// -----------------------------------------------------------------------------
// Writer configuration
// -----------------------------------------------------------------------------

fn rotation_config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 1,
        compression_level: 3,
    }
}

// -----------------------------------------------------------------------------
// Deterministic record generator
// -----------------------------------------------------------------------------

fn record(index: u64) -> UnifiedTimelineRecord {
    // Vary values deterministically to reduce compression efficiency.
    let mixed = index.wrapping_mul(0x9E3779B97F4A7C15).rotate_left(17);

    UnifiedTimelineRecord {
        event_timestamp_ns: 1_788_220_800_000_000_000 + i64::try_from(index).unwrap(),

        stream_rank: 0,
        event_kind: 0,

        dataset_id: 111,
        task_id: 0,
        event_ordinal: index,

        canonical_row_offset: mixed,
        canonical_row_count: 1,

        event_boundary: None,
        source_operation: None,
    }
}

// -----------------------------------------------------------------------------
// Read physical output
// -----------------------------------------------------------------------------

fn read_output(directory: &Path) -> Vec<UnifiedTimelineRecord> {
    let mut files = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect::<Vec<_>>();

    files.sort();

    let mut records = Vec::new();

    for path in files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();

        assert_eq!(
            builder.schema().as_ref(),
            unified_timeline_schema().as_ref()
        );

        let reader = builder.build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            macro_rules! column {
                ($name:expr, $type:ty) => {
                    batch
                        .column_by_name($name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<$type>()
                        .unwrap()
                };
            }

            let timestamps = column!("event_timestamp_ns", Int64Array);
            let ranks = column!("stream_rank", UInt32Array);
            let kinds = column!("event_kind", UInt8Array);

            let datasets = column!("dataset_id", UInt64Array);
            let tasks = column!("task_id", UInt64Array);
            let ordinals = column!("event_ordinal", UInt64Array);

            let offsets = column!("canonical_row_offset", UInt64Array);
            let counts = column!("canonical_row_count", UInt64Array);

            let boundaries = column!("event_boundary", UInt8Array);
            let operations = column!("source_operation", UInt8Array);

            use arrow_array::Array;

            for row in 0..batch.num_rows() {
                records.push(UnifiedTimelineRecord {
                    event_timestamp_ns: timestamps.value(row),
                    stream_rank: ranks.value(row),
                    event_kind: kinds.value(row),

                    dataset_id: datasets.value(row),
                    task_id: tasks.value(row),
                    event_ordinal: ordinals.value(row),

                    canonical_row_offset: offsets.value(row),
                    canonical_row_count: counts.value(row),

                    event_boundary: (!boundaries.is_null(row)).then(|| boundaries.value(row)),

                    source_operation: (!operations.is_null(row)).then(|| operations.value(row)),
                });
            }
        }
    }

    records
}

// -----------------------------------------------------------------------------
// File rotation test
// -----------------------------------------------------------------------------

#[test]
fn rotates_unified_parquet_files_without_losing_events() {
    let directory = TestDirectory::new();

    let mut writer = UnifiedTimelineWriter::new(directory.path(), &rotation_config())
        .expect("create unified writer");

    const EVENTS: u64 = 500_000;

    for index in 0..EVENTS {
        writer.write(record(index)).unwrap();
    }

    writer.finish().expect("finish unified writer");

    assert!(writer.is_finished());
    assert!(!writer.is_failed());

    assert_eq!(writer.metrics().events_written, EVENTS);

    let files_written = writer.metrics().files_written;

    assert!(
        files_written > 1,
        "expected multiple Parquet files, got {files_written}"
    );

    let records = read_output(directory.path());

    assert_eq!(records.len() as u64, EVENTS);

    for (index, actual) in records.iter().enumerate() {
        assert_eq!(
            *actual,
            record(index as u64),
            "record mismatch at index {index}"
        );
    }

    let files = fs::read_dir(directory.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();

    assert_eq!(files as u64, files_written);

    println!("\nMARKETFORGE — UNIFIED PARQUET ROTATION");
    println!("{}", "=".repeat(80));
    println!("Events       : {EVENTS}");
    println!("Parquet files: {files_written}");
    println!("Row identity : VERIFIED");
    println!("Ordering     : VERIFIED");
    println!("Rotation     : VERIFIED");
    println!("Result       : PASS");
}

// -----------------------------------------------------------------------------
// Empty writer finalization
// -----------------------------------------------------------------------------

#[test]
fn empty_writer_creates_no_files() {
    let directory = TestDirectory::new();

    let mut writer = UnifiedTimelineWriter::new(directory.path(), &rotation_config()).unwrap();

    writer.finish().unwrap();

    assert_eq!(writer.metrics().events_written, 0);
    assert_eq!(writer.metrics().files_written, 0);

    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

// -----------------------------------------------------------------------------
// Idempotent finalization
// -----------------------------------------------------------------------------

#[test]
fn repeated_finish_does_not_create_files() {
    let directory = TestDirectory::new();

    let mut writer = UnifiedTimelineWriter::new(directory.path(), &rotation_config()).unwrap();

    writer.write(record(0)).unwrap();

    writer.finish().unwrap();

    let metrics = writer.metrics().clone();

    writer.finish().unwrap();

    assert_eq!(*writer.metrics(), metrics);

    assert_eq!(writer.metrics().files_written, 1);
    assert_eq!(writer.metrics().events_written, 1);
}

// -----------------------------------------------------------------------------
// Reject writes after finalization
// -----------------------------------------------------------------------------

#[test]
fn rejects_writes_after_finish() {
    let directory = TestDirectory::new();

    let mut writer = UnifiedTimelineWriter::new(directory.path(), &rotation_config()).unwrap();

    writer.write(record(0)).unwrap();

    writer.finish().unwrap();

    assert!(writer.write(record(1)).is_err());
}
