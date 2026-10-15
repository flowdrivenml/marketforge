#![cfg(feature = "historical")]

use std::{fs::File, path::PathBuf, time::Instant};

use arrow_array::{Array, Int64Array, UInt64Array};

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{
        DepthEventSource, DepthStream, KWayDepthMerger, MergeManifest, build_depth_inventory,
        reconcile_depth_boundaries, resolve_merge_inputs,
    },
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// -----------------------------------------------------------------------------
// Project root
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Timeline event identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventIdentity {
    timestamp_ns: i64,
    dataset_id: u64,
    task_id: u64,
    event_ordinal: u64,
    canonical_row_offset: u64,
    canonical_row_count: u64,
}

// -----------------------------------------------------------------------------
// Read timeline Parquet
// -----------------------------------------------------------------------------

fn read_column_u64(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> u64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("invalid type for {name}"))
        .value(row)
}

fn read_column_i64(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> i64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("invalid type for {name}"))
        .value(row)
}

// -----------------------------------------------------------------------------
// Test
// -----------------------------------------------------------------------------

#[test]
#[ignore = "full-day integration validation"]
fn validates_full_parallel_merge_against_sequential() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let merged_root = job.output.dataset_path.clone();

    let manifest: MergeManifest = serde_json::from_reader(
        File::open(merged_root.join("manifest.json")).expect("open merge manifest"),
    )
    .expect("parse merge manifest");

    manifest.validate().expect("validate merge manifest");

    // -------------------------------------------------------------------------
    // Construct sequential reference
    // -------------------------------------------------------------------------

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve input datasets");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    let mut streams: Vec<Box<dyn DepthEventSource>> = Vec::new();

    for &dataset_id in &job.inputs {
        let tasks = inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect();

        streams.push(Box::new(
            DepthStream::new(dataset_id, tasks, &boundaries).expect("construct sequential stream"),
        ));
    }

    let mut sequential = KWayDepthMerger::new(streams).expect("construct sequential merger");

    // -------------------------------------------------------------------------
    // Compare persisted parallel output
    // -------------------------------------------------------------------------

    let start = Instant::now();

    let mut compared = 0_u64;
    let mut previous_timestamp = None;

    println!("\nMARKETFORGE — FULL-DAY MERGE EQUIVALENCE");
    println!("{}", "=".repeat(90));

    for file in &manifest.files {
        let path = merged_root.join(&file.path);

        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(&path).expect("open timeline Parquet"),
        )
        .expect("read Parquet metadata")
        .with_batch_size(8192)
        .build()
        .expect("build Parquet reader");

        for batch in reader {
            let batch = batch.expect("read timeline batch");

            for row in 0..batch.num_rows() {
                let actual = EventIdentity {
                    timestamp_ns: read_column_i64(&batch, "event_timestamp_ns", row),
                    dataset_id: read_column_u64(&batch, "dataset_id", row),
                    task_id: read_column_u64(&batch, "task_id", row),
                    event_ordinal: read_column_u64(&batch, "event_ordinal", row),
                    canonical_row_offset: read_column_u64(&batch, "canonical_row_offset", row),
                    canonical_row_count: read_column_u64(&batch, "canonical_row_count", row),
                };

                // Advance the sequential reference into the
                // requested merge interval.
                let expected = loop {
                    let event = sequential
                        .next_event()
                        .expect("read sequential event")
                        .expect("sequential reference exhausted early");

                    if event.event.event.event_timestamp_ns >= job.time_range.start_ns {
                        break event;
                    }
                };

                let reference = EventIdentity {
                    timestamp_ns: expected.event.event.event_timestamp_ns,
                    dataset_id: expected.event.dataset_id,
                    task_id: expected.event.task_id,
                    event_ordinal: expected.event.event.event_ordinal,
                    canonical_row_offset: expected.event.event.canonical_row_offset,
                    canonical_row_count: expected.event.event.canonical_row_count,
                };

                assert_eq!(
                    actual, reference,
                    "event mismatch at global index {compared}"
                );

                if let Some(previous) = previous_timestamp {
                    assert!(
                        actual.timestamp_ns >= previous,
                        "global timestamp regression at event {compared}"
                    );
                }

                assert!(
                    actual.timestamp_ns >= job.time_range.start_ns
                        && actual.timestamp_ns < job.time_range.end_ns,
                    "event outside requested interval"
                );

                previous_timestamp = Some(actual.timestamp_ns);

                compared += 1;
            }
        }

        if compared > 0 && compared % 1_000_000 < 8192 {
            println!("Compared {} events...", compared);
        }
    }

    // -------------------------------------------------------------------------
    // Verify no sequential events remain inside the interval
    // -------------------------------------------------------------------------

    while let Some(event) = sequential
        .next_event()
        .expect("read remaining sequential event")
    {
        if event.event.event.event_timestamp_ns >= job.time_range.end_ns {
            break;
        }

        panic!(
            "parallel output omitted sequential event at timestamp {}",
            event.event.event.event_timestamp_ns
        );
    }

    assert_eq!(
        compared, manifest.events_written,
        "parallel output row count differs from manifest"
    );

    assert_eq!(compared, 21_158_403);

    let elapsed = start.elapsed().as_secs_f64();

    println!("{}", "=".repeat(90));
    println!("Events compared : {}", compared);
    println!("Parquet files   : {}", manifest.files.len());
    println!("Elapsed time    : {:.3} s", elapsed);
    println!("Result          : PASS");
}
