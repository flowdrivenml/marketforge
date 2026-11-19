#![cfg(feature = "historical")]
use crate::publication::depth_publication;
use crate::publication::trade_publication;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::historical::merge::{DepthTaskInventory, TradeTaskInventory};
use {
    marketforge_engine::historical::job::MergeJob,
    marketforge_engine::historical::job::MergeOperation,
    marketforge_engine::historical::job::MergeOutput,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::MergePartitioning,
    marketforge_engine::historical::job::MergeResources,
    marketforge_engine::historical::job::MergeTimeRange,
    marketforge_engine::historical::merge::MergePartition,
    marketforge_engine::historical::merge::UnifiedMergeStream,
    marketforge_engine::historical::merge::UnifiedMergeStreamKind,
    marketforge_engine::historical::merge::UnifiedPartitionResult,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::build_trade_inventory,
    marketforge_engine::historical::merge::depth_task_intervals,
    marketforge_engine::historical::merge::execute_unified_partitions_parallel,
    marketforge_engine::historical::merge::plan_unified_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::trade_task_intervals,
    marketforge_engine::historical::merge::validate_unified_merge_output,
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
        primary_files: Default::default(),
        submission: None,
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
// Execute generated parallel merge
// -----------------------------------------------------------------------------

fn execute_generated_merge(
    trade_ids: &[u64],
    depth_ids: &[u64],
) -> (
    TestDirectory,
    Vec<UnifiedPartitionResult>,
    Vec<UnifiedMergeStream>,
    Vec<TradeTaskInventory>,
    Vec<DepthTaskInventory>,
) {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();
    let staging = TestDirectory::new();

    let start_ns = crate::publication::START;
    let end_ns = start_ns + 400;

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
        .map(|&id| trade_publication(&root, id))
        .collect::<Vec<_>>();

    let depth_inputs = depth_ids
        .iter()
        .map(|&id| depth_publication(&root, id))
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
            start_ns: start_ns + i64::from(index) * 100,
            end_ns: start_ns + (i64::from(index) + 1) * 100,
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
fn validates_generated_trade_output() {
    let (staging, results, streams, trade_inventory, depth_inventory) =
        execute_generated_merge(&[111, 109, 115], &[]);
    validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .unwrap();

    let validated = validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .expect("validate trade output");

    assert_eq!(validated.trade_events, 12);
    assert_eq!(validated.depth_events, 0);
    assert_eq!(validated.output.events_written, 12);
    assert_eq!(validated.output.files_written, 4);

    println!("\nMARKETFORGE — PHYSICAL TRADE VALIDATION");
    println!("{}", "=".repeat(80));
    println!("Trade events : {}", validated.trade_events);
    println!("Parquet files: {}", validated.output.files_written);
    println!("Result       : PASS");
}

#[test]
fn validates_generated_combined_output() {
    let (staging, results, streams, trade_inventory, depth_inventory) =
        execute_generated_merge(&[111, 109], &[105, 112]);

    let validated = validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .expect("validate combined output");

    assert_eq!(validated.trade_events, 8);
    assert_eq!(validated.depth_events, 8);
    assert_eq!(validated.output.events_written, 16);
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
        execute_generated_merge(&[111, 109, 115], &[]);
    validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .unwrap();

    results[0].events_written += 1;

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .unwrap_err()
        .to_string()
        .contains("physical event count mismatch")
    );
}

#[test]
fn rejects_incorrect_stream_identity() {
    let (staging, results, mut streams, trade_inventory, depth_inventory) =
        execute_generated_merge(&[111, 109, 115], &[]);
    validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .unwrap();

    streams[0].dataset_id = 999_999;

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .unwrap_err()
        .to_string()
        .contains("unified stream identity mismatch")
    );
}

#[test]
fn rejects_incorrect_file_size() {
    let (staging, mut results, streams, trade_inventory, depth_inventory) =
        execute_generated_merge(&[111, 109, 115], &[]);
    validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .unwrap();

    results[0].files[0].size_bytes += 1;

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .unwrap_err()
        .to_string()
        .contains("invalid unified file metadata")
    );
}

#[test]
fn rejects_unsafe_output_path() {
    let (staging, mut results, streams, trade_inventory, depth_inventory) =
        execute_generated_merge(&[111, 109, 115], &[]);
    validate_unified_merge_output(
        staging.path(),
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )
    .unwrap();

    results[0].files[0].path = "../outside.parquet".to_owned();

    assert!(
        validate_unified_merge_output(
            staging.path(),
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
        .unwrap_err()
        .to_string()
        .contains("unsafe unified output path")
    );
}
