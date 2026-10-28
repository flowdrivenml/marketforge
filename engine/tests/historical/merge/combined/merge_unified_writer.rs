#![cfg(feature = "historical")]

use std::{
    fs::File,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Array, Int64Array, UInt8Array, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use {
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::MergeEventRef,
    marketforge_engine::historical::merge::MergedEvent,
    marketforge_engine::historical::merge::ResolvedMergeInput,
    marketforge_engine::historical::merge::TradeEventRef,
    marketforge_engine::historical::merge::UnifiedTimelineRecord,
    marketforge_engine::historical::merge::UnifiedTimelineWriter,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::unified_timeline_schema,
    marketforge_engine::historical::normalize::manifest::DatasetManifest,
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
            "marketforge-unified-writer-{}-{timestamp}",
            std::process::id()
        ));

        std::fs::create_dir_all(&path).unwrap();

        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

fn parquet_config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 3,
    }
}

// -----------------------------------------------------------------------------
// Real depth dataset
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn resolve_depth_dataset(root: &Path, dataset_id: u64) -> ResolvedMergeInput {
    let dataset_path = root.join(format!("data/processed/{dataset_id}"));

    let manifest: DatasetManifest = serde_json::from_reader(
        File::open(dataset_path.join("manifest.json")).expect("open depth manifest"),
    )
    .expect("parse depth manifest");

    let canonical_files = manifest
        .files
        .iter()
        .map(|file| dataset_path.join(&file.path))
        .collect();

    let auxiliary_files = manifest
        .auxiliary_files
        .iter()
        .map(|file| dataset_path.join(&file.path))
        .collect();

    ResolvedMergeInput {
        dataset_id,
        dataset_path,
        manifest,
        canonical_files,
        auxiliary_files,
    }
}

fn real_depth_events(count: usize) -> Vec<UnifiedTimelineRecord> {
    let root = project_root();
    let dataset_id = 112;

    let input = resolve_depth_dataset(&root, dataset_id);

    let inventory = build_depth_inventory(&[input]).unwrap();
    let boundaries = reconcile_depth_boundaries(&inventory).unwrap();

    let mut stream = DepthStream::new(dataset_id, inventory, &boundaries).unwrap();

    let mut records = Vec::with_capacity(count);

    while records.len() < count {
        let depth = stream
            .next_event()
            .unwrap()
            .expect("insufficient real depth events");

        let merged = MergedEvent {
            stream_rank: 1,
            event: MergeEventRef::Depth(depth),
        };

        records.push(UnifiedTimelineRecord::from(&merged));
    }

    records
}

// -----------------------------------------------------------------------------
// Trade event helper
// -----------------------------------------------------------------------------

fn trade_record(timestamp_ns: i64, row_offset: u64) -> UnifiedTimelineRecord {
    let merged = MergedEvent {
        stream_rank: 0,
        event: MergeEventRef::Trade(TradeEventRef {
            dataset_id: 111,
            task_id: 0,
            event_ordinal: row_offset,
            canonical_row_offset: row_offset,
            event_timestamp_ns: timestamp_ns,
        }),
    };

    UnifiedTimelineRecord::from(&merged)
}

// -----------------------------------------------------------------------------
// Parquet round trip
// -----------------------------------------------------------------------------

#[test]
fn writes_and_reads_unified_timeline() {
    let directory = TestDirectory::new();

    let mut records = Vec::new();

    // Real canonical depth references.
    records.extend(real_depth_events(10_000));

    // Trade references.
    for index in 0..1_000_u64 {
        records.push(trade_record(
            1_788_220_800_000_000_000 + index as i64,
            index,
        ));
    }

    // Establish deterministic chronological order.
    records.sort_by_key(|record| {
        (
            record.event_timestamp_ns,
            record.stream_rank,
            record.task_id,
            record.event_ordinal,
        )
    });

    let mut writer = UnifiedTimelineWriter::new(directory.path(), &parquet_config())
        .expect("construct unified writer");

    for &record in &records {
        writer.write(record).expect("write unified event");
    }

    writer.finish().expect("finalize unified writer");

    assert!(writer.is_finished());
    assert!(!writer.is_failed());

    assert_eq!(writer.metrics().events_written, records.len() as u64);

    assert!(writer.metrics().files_written > 0);

    // -------------------------------------------------------------------------
    // Read persisted Parquet files
    // -------------------------------------------------------------------------

    let mut files = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect::<Vec<_>>();

    files.sort();

    assert_eq!(files.len() as u64, writer.metrics().files_written);

    let mut actual_rows = 0_usize;
    let mut trade_rows = 0_usize;
    let mut depth_rows = 0_usize;

    for path in files {
        let file = File::open(&path).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        assert_eq!(
            builder.schema().as_ref(),
            unified_timeline_schema().as_ref()
        );

        let reader = builder.build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let ranks = batch
                .column_by_name("stream_rank")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();

            let kinds = batch
                .column_by_name("event_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            let dataset_ids = batch
                .column_by_name("dataset_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let task_ids = batch
                .column_by_name("task_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let ordinals = batch
                .column_by_name("event_ordinal")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let offsets = batch
                .column_by_name("canonical_row_offset")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let counts = batch
                .column_by_name("canonical_row_count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let boundaries = batch
                .column_by_name("event_boundary")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            let operations = batch
                .column_by_name("source_operation")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            for row in 0..batch.num_rows() {
                let expected = &records[actual_rows];

                assert_eq!(timestamps.value(row), expected.event_timestamp_ns);

                assert_eq!(ranks.value(row), expected.stream_rank);

                assert_eq!(kinds.value(row), expected.event_kind);

                assert_eq!(dataset_ids.value(row), expected.dataset_id);

                assert_eq!(task_ids.value(row), expected.task_id);

                assert_eq!(ordinals.value(row), expected.event_ordinal);

                assert_eq!(offsets.value(row), expected.canonical_row_offset);

                assert_eq!(counts.value(row), expected.canonical_row_count);

                match expected.event_boundary {
                    Some(value) => {
                        assert!(!boundaries.is_null(row));
                        assert_eq!(boundaries.value(row), value);
                    }
                    None => assert!(boundaries.is_null(row)),
                }

                match expected.source_operation {
                    Some(value) => {
                        assert!(!operations.is_null(row));
                        assert_eq!(operations.value(row), value);
                    }
                    None => assert!(operations.is_null(row)),
                }

                match expected.event_kind {
                    0 => trade_rows += 1,
                    1 => depth_rows += 1,
                    kind => panic!("unexpected event kind: {kind}"),
                }

                actual_rows += 1;
            }
        }
    }

    assert_eq!(actual_rows, records.len());
    assert_eq!(trade_rows, 1_000);
    assert_eq!(depth_rows, 10_000);

    println!("\nMARKETFORGE — UNIFIED PARQUET ROUND TRIP");
    println!("{}", "=".repeat(80));
    println!("Trade events : {trade_rows}");
    println!("Depth events : {depth_rows}");
    println!("Total events : {actual_rows}");
    println!("Parquet files: {}", writer.metrics().files_written);
    println!("Schema       : VERIFIED");
    println!("Row identity : VERIFIED");
    println!("Nullability  : VERIFIED");
    println!("Result       : PASS");
}
