#![cfg(feature = "historical")]

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use {
    marketforge_engine::historical::job::MergeJob,
    marketforge_engine::historical::job::MergeOperation,
    marketforge_engine::historical::job::MergeOutput,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::MergePartitioning,
    marketforge_engine::historical::job::MergeResources,
    marketforge_engine::historical::job::MergeTimeRange,
    marketforge_engine::historical::merge::MergeInputDataset,
    marketforge_engine::historical::merge::MergePartition,
    marketforge_engine::historical::merge::UnifiedMergeMode,
    marketforge_engine::historical::merge::UnifiedMergeStream,
    marketforge_engine::historical::merge::UnifiedMergeStreamKind,
    marketforge_engine::historical::merge::ValidatedMergeOutput,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::build_trade_inventory,
    marketforge_engine::historical::merge::build_unified_merge_manifest,
    marketforge_engine::historical::merge::depth_task_intervals,
    marketforge_engine::historical::merge::execute_unified_partitions_parallel,
    marketforge_engine::historical::merge::plan_unified_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::trade_task_intervals,
};

#[path = "../support/mod.rs"]
mod support;

use support::datasets::{project_root, resolve_depth_dataset, resolve_trade_dataset};

// -----------------------------------------------------------------------------
// Temporary staging directory
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-unified-manifest-{}-{timestamp}",
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
        job_id: 900_300,
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
// Construct stream metadata
// -----------------------------------------------------------------------------

fn test_stream(rank: usize, dataset_id: u64, kind: UnifiedMergeStreamKind) -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: u32::try_from(rank).unwrap(),
        dataset_id,
        kind,

        // Placeholder metadata for builder validation only.
        // Production execution must resolve these from authoritative sources.
        exchange: "test".to_owned(),
        instrument_id: 0,
        symbol: format!("DATASET-{dataset_id}"),
        stream_id: format!("stream-{dataset_id}"),

        base_currency: None,
        quote_currency: None,
    }
}

// -----------------------------------------------------------------------------
// Execute and construct manifest
// -----------------------------------------------------------------------------

fn run_manifest_test(trade_ids: &[u64], depth_ids: &[u64]) {
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
    // Build partition plans
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
    // Execute real parallel merge
    // -------------------------------------------------------------------------

    let results = execute_unified_partitions_parallel(
        &job,
        plans,
        &trade_inventory,
        &depth_inventory,
        &boundaries,
        staging.path(),
    )
    .expect("execute unified partitions");

    // -------------------------------------------------------------------------
    // Build input metadata
    // -------------------------------------------------------------------------

    let inputs = dataset_order
        .iter()
        .map(|&dataset_id| {
            let input = trade_inputs
                .iter()
                .chain(depth_inputs.iter())
                .find(|input| input.dataset_id == dataset_id)
                .expect("resolved dataset");

            MergeInputDataset {
                dataset_id,
                dataset_path: input.dataset_path.to_string_lossy().into_owned(),
                manifest_version: input.manifest.protocol_version,
            }
        })
        .collect::<Vec<_>>();

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
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Aggregate independently reported partition metrics
    // -------------------------------------------------------------------------

    let mut events_written = 0_u64;
    let mut files_written = 0_u64;

    let mut first_timestamp = None::<i64>;
    let mut last_timestamp = None::<i64>;

    let mut files = Vec::new();

    for result in &results {
        events_written += result.events_written;
        files_written += result.files_written;

        if let Some(first) = result.first_timestamp_ns {
            first_timestamp = Some(first_timestamp.map_or(first, |current| current.min(first)));
        }

        if let Some(last) = result.last_timestamp_ns {
            last_timestamp = Some(last_timestamp.map_or(last, |current| current.max(last)));
        }

        files.extend(result.files.iter().cloned());
    }

    let validated = ValidatedMergeOutput {
        events_written,
        files_written,
        start_timestamp_ns: first_timestamp,
        end_timestamp_ns: last_timestamp,
        files,
    };

    // -------------------------------------------------------------------------
    // Build unified manifest
    // -------------------------------------------------------------------------

    let manifest = build_unified_merge_manifest(
        &job,
        inputs,
        streams,
        &boundaries,
        &results,
        validated,
        Vec::new(),
        Vec::new(),
    )
    .expect("build unified manifest");

    manifest.validate().expect("validate unified manifest");

    // -------------------------------------------------------------------------
    // Assertions
    // -------------------------------------------------------------------------

    assert_eq!(manifest.job_id, job.job_id);
    assert_eq!(manifest.input_datasets.len(), dataset_order.len());
    assert_eq!(manifest.streams.len(), dataset_order.len());

    assert_eq!(manifest.events_written, events_written);
    assert_eq!(manifest.files_written, files_written);

    let expected_trade_events: u64 = results.iter().map(|result| result.trade_events).sum();

    let expected_depth_events: u64 = results.iter().map(|result| result.depth_events).sum();

    assert_eq!(manifest.trade_events, expected_trade_events);
    assert_eq!(manifest.depth_events, expected_depth_events);

    let expected_mode = match (!trade_ids.is_empty(), !depth_ids.is_empty()) {
        (true, true) => UnifiedMergeMode::Combined,
        (true, false) => UnifiedMergeMode::Trades,
        (false, true) => UnifiedMergeMode::Depth,
        (false, false) => unreachable!(),
    };

    assert_eq!(manifest.mode, expected_mode);

    println!("\nMARKETFORGE — UNIFIED MANIFEST BUILDER");
    println!("{}", "=".repeat(80));
    println!("Mode         : {:?}", manifest.mode);
    println!("Streams      : {}", manifest.streams.len());
    println!("Partitions   : {}", results.len());
    println!("Trade events : {}", manifest.trade_events);
    println!("Depth events : {}", manifest.depth_events);
    println!("Total events : {}", manifest.events_written);
    println!("Parquet files: {}", manifest.files_written);
    println!("Boundary links: {}", manifest.boundary_links.len());
    println!("Result       : PASS");
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn builds_real_trade_manifest() {
    run_manifest_test(&[111, 109, 115], &[]);
}

#[test]
fn builds_real_combined_manifest() {
    run_manifest_test(&[111, 109], &[105, 112]);
}
