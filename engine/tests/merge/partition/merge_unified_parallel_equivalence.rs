#![cfg(feature = "process")]

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Array, Int64Array, UInt8Array, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    job::{
        MergeJob, MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning,
        MergeResources, MergeTimeRange,
    },
    process::merge::{
        DepthBoundaryLink, DepthStream, DepthTaskInventory, KWayEventMerger, MergeEventSource,
        MergePartition, TradeStream, TradeTaskInventory, UnifiedPartitionResult,
        UnifiedTimelineRecord, build_depth_inventory, build_trade_inventory, depth_task_intervals,
        execute_unified_partitions_parallel, plan_unified_partition_inputs,
        reconcile_depth_boundaries, trade_task_intervals,
    },
};

// -----------------------------------------------------------------------------
// Shared test utilities
// -----------------------------------------------------------------------------

#[path = "../../common/mod.rs"]
mod common;

use common::datasets::{project_root, resolve_depth_dataset, resolve_trade_dataset};

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
            "marketforge-unified-parallel-{}-{timestamp}",
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
// Job configuration
// -----------------------------------------------------------------------------

fn test_job(dataset_ids: &[u64], output: &Path, start_ns: i64, end_ns: i64) -> MergeJob {
    MergeJob {
        job_id: 900_100,
        operation: MergeOperation::Merge,

        inputs: dataset_ids.to_vec(),
        input_paths: HashMap::new(),

        time_range: MergeTimeRange { start_ns, end_ns },

        resources: MergeResources {
            workers: 4,
            memory_limit_mb: 4096,
            max_open_files: 128,
        },

        partitioning: MergePartitioning {
            partitions_per_worker: 1,
        },

        parquet: MergeParquetConfig {
            row_group_target_mb: 1,
            file_target_mb: 4,
            compression_level: 3,
        },

        output: MergeOutput {
            dataset_path: output.to_path_buf(),
        },
    }
}

// -----------------------------------------------------------------------------
// Read unified Parquet output
// -----------------------------------------------------------------------------

fn read_unified_output(
    staging: &Path,
    results: &[UnifiedPartitionResult],
) -> Vec<UnifiedTimelineRecord> {
    let mut records = Vec::new();

    for result in results {
        for file in &result.files {
            let path = staging.join(&file.path);

            let reader = ParquetRecordBatchReaderBuilder::try_new(
                File::open(&path).expect("open unified Parquet"),
            )
            .expect("construct Parquet reader")
            .build()
            .expect("build Parquet reader");

            for batch in reader {
                let batch = batch.expect("read Parquet batch");

                macro_rules! column {
                    ($name:expr, $type:ty) => {
                        batch
                            .column_by_name($name)
                            .expect("missing unified column")
                            .as_any()
                            .downcast_ref::<$type>()
                            .expect("incorrect unified column type")
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
    }

    records
}

// -----------------------------------------------------------------------------
// Sequential reference
// -----------------------------------------------------------------------------

fn sequential_reference(
    dataset_order: &[u64],
    trade_inventory: &[TradeTaskInventory],
    depth_inventory: &[DepthTaskInventory],
    boundaries: &[DepthBoundaryLink],
    start_ns: i64,
    end_ns: i64,
) -> Vec<UnifiedTimelineRecord> {
    let mut streams: Vec<Box<dyn MergeEventSource>> = Vec::new();

    for &dataset_id in dataset_order {
        let trades = trade_inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect::<Vec<_>>();

        let depth = depth_inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect::<Vec<_>>();

        if !trades.is_empty() {
            streams.push(Box::new(
                TradeStream::new_range(dataset_id, trades, start_ns, end_ns)
                    .expect("construct trade stream"),
            ));
        } else {
            streams.push(Box::new(
                DepthStream::new(dataset_id, depth, boundaries).expect("construct depth stream"),
            ));
        }
    }

    let mut merger = KWayEventMerger::new(streams).expect("construct sequential merger");

    let mut records = Vec::new();

    while let Some(merged) = merger.next_event().expect("read merged event") {
        let timestamp = merged.event.timestamp_ns();

        if timestamp >= end_ns {
            break;
        }

        if timestamp >= start_ns {
            records.push(UnifiedTimelineRecord::from(&merged));
        }
    }

    records
}

// -----------------------------------------------------------------------------
// Execute and compare
// -----------------------------------------------------------------------------

fn assert_parallel_equivalence(trade_ids: &[u64], depth_ids: &[u64]) {
    let root = project_root();
    let staging = TestDirectory::new();

    let start_ns = 1_788_220_800_000_000_000_i64;
    let end_ns = start_ns + 240_000_000_000;

    let dataset_order = trade_ids
        .iter()
        .chain(depth_ids.iter())
        .copied()
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Build inventories
    // -------------------------------------------------------------------------

    let trade_inputs = trade_ids
        .iter()
        .map(|&id| resolve_trade_dataset(&root, id))
        .collect::<Vec<_>>();

    let depth_inputs = depth_ids
        .iter()
        .map(|&id| resolve_depth_dataset(&root, id))
        .collect::<Vec<_>>();

    let trade_inventory = build_trade_inventory(&trade_inputs).expect("build trade inventory");

    let depth_inventory = build_depth_inventory(&depth_inputs).expect("build depth inventory");

    let boundaries =
        reconcile_depth_boundaries(&depth_inventory).expect("reconcile depth boundaries");

    // -------------------------------------------------------------------------
    // Plan partitions
    // -------------------------------------------------------------------------

    let mut intervals = trade_task_intervals(&trade_inventory);

    intervals.extend(depth_task_intervals(&depth_inventory).expect("build depth intervals"));

    let partitions = (0..4)
        .map(|index| MergePartition {
            partition_id: index,
            start_ns: start_ns + i64::from(index) * 60_000_000_000,
            end_ns: start_ns + (i64::from(index) + 1) * 60_000_000_000,
        })
        .collect::<Vec<_>>();

    let plans =
        plan_unified_partition_inputs(&partitions, &intervals).expect("plan unified partitions");

    let job = test_job(&dataset_order, staging.path(), start_ns, end_ns);

    // -------------------------------------------------------------------------
    // Sequential reference
    // -------------------------------------------------------------------------

    let expected = sequential_reference(
        &dataset_order,
        &trade_inventory,
        &depth_inventory,
        &boundaries,
        start_ns,
        end_ns,
    );

    // -------------------------------------------------------------------------
    // Parallel execution
    // -------------------------------------------------------------------------

    let results = execute_unified_partitions_parallel(
        &job,
        plans,
        &trade_inventory,
        &depth_inventory,
        &boundaries,
        staging.path(),
    )
    .expect("execute parallel unified merge");

    // -------------------------------------------------------------------------
    // Compare persisted output
    // -------------------------------------------------------------------------

    let actual = read_unified_output(staging.path(), &results);

    assert_eq!(
        actual.len(),
        expected.len(),
        "parallel/sequential event count mismatch"
    );

    for (index, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            actual, expected,
            "parallel/sequential mismatch at event {index}"
        );
    }

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------

    let trade_events = actual.iter().filter(|event| event.event_kind == 0).count();

    let depth_events = actual.iter().filter(|event| event.event_kind == 1).count();

    println!("\nMARKETFORGE — UNIFIED PARALLEL EQUIVALENCE");
    println!("{}", "=".repeat(80));

    println!("Streams      : {}", dataset_order.len());
    println!("Partitions   : {}", results.len());
    println!("Trade events : {trade_events}");
    println!("Depth events : {depth_events}");
    println!("Total events : {}", actual.len());

    println!("Ordering     : VERIFIED");
    println!("Row identity : VERIFIED");
    println!("Equivalence  : VERIFIED");
    println!("Result       : PASS");
}

// -----------------------------------------------------------------------------
// Integration tests
// -----------------------------------------------------------------------------

#[test]
fn parallel_trade_merge_matches_sequential() {
    assert_parallel_equivalence(&[111, 109, 115], &[]);
}

#[test]
fn parallel_combined_merge_matches_sequential() {
    assert_parallel_equivalence(&[111, 109], &[105, 112]);
}
