#![cfg(feature = "process")]

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::process::merge::{DepthTaskInventory, TradeTaskInventory};
use marketforge_engine::{
    job::{
        MergeJob, MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning,
        MergeResources, MergeTimeRange,
    },
    process::merge::{
        MergePartition, UnifiedMergeStream, UnifiedMergeStreamKind, UnifiedPartitionResult,
        build_depth_inventory, build_trade_inventory, depth_task_intervals,
        execute_unified_partitions_parallel, plan_unified_partition_inputs,
        reconcile_depth_boundaries, trade_task_intervals, validate_unified_merge_output,
    },
};
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
            "marketforge-unified-validation-{}-{timestamp}",
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
        job_id: 900_400,
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
// Stream metadata
// -----------------------------------------------------------------------------

fn test_stream(rank: usize, dataset_id: u64, kind: UnifiedMergeStreamKind) -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: u32::try_from(rank).unwrap(),
        dataset_id,
        kind,

        exchange: "test".to_owned(),
        instrument_id: 0,
        symbol: format!("DATASET-{dataset_id}"),
        stream_id: format!("stream-{dataset_id}"),

        base_currency: None,
        quote_currency: None,
    }
}

// -----------------------------------------------------------------------------
// Execute real parallel merge
// -----------------------------------------------------------------------------

fn execute_real_merge(
    trade_ids: &[u64],
    depth_ids: &[u64],
) -> (
    TestDirectory,
    Vec<UnifiedPartitionResult>,
    Vec<UnifiedMergeStream>,
    Vec<TradeTaskInventory>,
    Vec<DepthTaskInventory>,
) {
    let root = project_root();
    let staging = TestDirectory::new();

    let start_ns = 1_788_220_800_000_000_000_i64;
    let end_ns = start_ns + 240_000_000_000;

    let dataset_order = trade_ids
        .iter()
        .chain(depth_ids.iter())
        .copied()
        .collect::<Vec<_>>();

    let job = test_job(&dataset_order, staging.path(), start_ns, end_ns);

    // -------------------------------------------------------------------------
    // Resolve datasets
    // -------------------------------------------------------------------------

    let trade_inputs = trade_ids
        .iter()
        .map(|&id| resolve_trade_dataset(&root, id))
        .collect::<Vec<_>>();

    let depth_inputs = depth_ids
        .iter()
        .map(|&id| resolve_depth_dataset(&root, id))
        .collect::<Vec<_>>();

    let trade_inventory = build_trade_inventory(&trade_inputs).unwrap();

    let depth_inventory = build_depth_inventory(&depth_inputs).unwrap();

    let boundaries = reconcile_depth_boundaries(&depth_inventory).unwrap();

    // -------------------------------------------------------------------------
    // Plan partitions
    // -------------------------------------------------------------------------

    let mut intervals = trade_task_intervals(&trade_inventory);

    intervals.extend(depth_task_intervals(&depth_inventory).unwrap());

    let partitions = (0..4)
        .map(|index| MergePartition {
            partition_id: index,
            start_ns: start_ns + i64::from(index) * 60_000_000_000,
            end_ns: start_ns + (i64::from(index) + 1) * 60_000_000_000,
        })
        .collect::<Vec<_>>();

    let plans = plan_unified_partition_inputs(&partitions, &intervals).unwrap();

    // -------------------------------------------------------------------------
    // Execute
    // -------------------------------------------------------------------------

    let results = execute_unified_partitions_parallel(
        &job,
        plans,
        &trade_inventory,
        &depth_inventory,
        &boundaries,
        staging.path(),
    )
    .expect("execute unified merge");

    let streams = dataset_order
        .iter()
        .enumerate()
        .map(|(rank, &dataset_id)| {
            let kind = if trade_ids.contains(&dataset_id) {
                UnifiedMergeStreamKind::Trade
            } else {
                UnifiedMergeStreamKind::Depth
            };

            test_stream(rank, dataset_id, kind)
        })
        .collect();

    (staging, results, streams, trade_inventory, depth_inventory)
}

// -----------------------------------------------------------------------------
// Positive validation
// -----------------------------------------------------------------------------

#[test]
fn validates_real_trade_output() {
    let (staging, results, streams, trade_inventory, depth_inventory) =
        execute_real_merge(&[111, 109, 115], &[]);

    let validated = validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .expect("validate trade output");

    assert_eq!(validated.trade_events, 1_376);
    assert_eq!(validated.depth_events, 0);
    assert_eq!(validated.output.events_written, 1_376);
    assert_eq!(validated.output.files_written, 4);

    println!("\nMARKETFORGE — PHYSICAL TRADE VALIDATION");
    println!("{}", "=".repeat(80));
    println!("Trade events : {}", validated.trade_events);
    println!("Parquet files: {}", validated.output.files_written);
    println!("Result       : PASS");
}

#[test]
fn validates_real_combined_output() {
    let (staging, results, streams, trade_inventory, depth_inventory) =
        execute_real_merge(&[111, 109], &[105, 112]);

    let validated = validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .expect("validate combined output");

    assert_eq!(validated.trade_events, 607);
    assert_eq!(validated.depth_events, 49_132);
    assert_eq!(validated.output.events_written, 49_739);
    assert_eq!(validated.output.files_written, 4);

    println!("\nMARKETFORGE — PHYSICAL COMBINED VALIDATION");
    println!("{}", "=".repeat(80));
    println!("Trade events : {}", validated.trade_events);
    println!("Depth events : {}", validated.depth_events);
    println!("Total events : {}", validated.output.events_written);
    println!("Parquet files: {}", validated.output.files_written);
    println!("Result       : PASS");
}

// -----------------------------------------------------------------------------
// Negative validation
// -----------------------------------------------------------------------------
#[test]
fn rejects_incorrect_partition_event_count() {
    let (staging, mut results, streams, trade_inventory, depth_inventory) =
        execute_real_merge(&[111, 109, 115], &[]);

    results[0].events_written += 1;

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .is_err()
    );
}

#[test]
fn rejects_incorrect_stream_identity() {
    let (staging, results, mut streams, trade_inventory, depth_inventory) =
        execute_real_merge(&[111, 109, 115], &[]);

    streams[0].dataset_id = 999_999;

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .is_err()
    );
}

#[test]
fn rejects_incorrect_file_size() {
    let (staging, mut results, streams, trade_inventory, depth_inventory) =
        execute_real_merge(&[111, 109, 115], &[]);

    results[0].files[0].size_bytes += 1;

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .is_err()
    );
}

#[test]
fn rejects_unsafe_output_path() {
    let (staging, mut results, streams, trade_inventory, depth_inventory) =
        execute_real_merge(&[111, 109, 115], &[]);

    results[0].files[0].path = "../outside.parquet".to_owned();

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .is_err()
    );
}
