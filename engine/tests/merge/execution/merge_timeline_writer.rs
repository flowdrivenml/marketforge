#![cfg(feature = "process")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Int64Array, UInt64Array};
use marketforge_engine::{
    job::MergeParquetConfig,
    process::merge::{TimelineRecord, TimelineWriter},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-timeline-writer-{}-{id}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn writes_and_reads_timeline_parquet() {
    let directory = TestDirectory::new();

    let config = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 1024,
        compression_level: 3,
    };

    let mut writer = TimelineWriter::new(&directory.path, &config).expect("create timeline writer");

    for index in 0..10_000_u64 {
        writer
            .write(TimelineRecord {
                event_timestamp_ns: 1_000_000 + index as i64,
                stream_rank: (index % 4) as u32,

                dataset_id: 105,
                task_id: 0,
                event_ordinal: index + 1,

                canonical_row_offset: index,
                canonical_row_count: 1,

                event_boundary: 0,
                source_operation: 2,
            })
            .expect("write timeline record");
    }

    writer.finish().expect("finalize timeline writer");

    assert!(writer.is_finished());
    assert!(!writer.is_failed());

    assert_eq!(writer.metrics().events_written, 10_000);
    assert_eq!(writer.metrics().files_written, 1);

    let file = fs::File::open(directory.path.join("part-000000.parquet")).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut total_rows = 0;

    for batch in reader {
        let batch = batch.unwrap();

        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let dataset_ids = batch
            .column_by_name("dataset_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for index in 0..batch.num_rows() {
            assert_eq!(dataset_ids.value(index), 105);
            assert_eq!(timestamps.value(index), 1_000_000 + total_rows as i64);

            total_rows += 1;
        }
    }

    assert_eq!(total_rows, 10_000);

    println!("\nMARKETFORGE — TIMELINE WRITER");
    println!("Records : {}", total_rows);
    println!("Files   : {}", writer.metrics().files_written);
    println!("Parquet : VERIFIED");
    println!("Result  : PASS");
}
