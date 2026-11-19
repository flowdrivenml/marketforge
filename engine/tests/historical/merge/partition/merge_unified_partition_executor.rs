#![cfg(feature = "historical")]
use crate::publication::depth_publication;
use crate::publication::trade_publication;
use marketforge_engine::historical::merge::{
    MergePartition, build_depth_inventory, build_trade_inventory, depth_task_intervals,
    plan_unified_partition_inputs, trade_task_intervals,
};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use {
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::merge::UnifiedPartitionResult,
    marketforge_engine::historical::merge::execute_unified_partition,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
};

// -----------------------------------------------------------------------------
// Resolve real trade dataset
// -----------------------------------------------------------------------------

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
            "marketforge-unified-partition-{}-{timestamp}",
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
// Partition
// -----------------------------------------------------------------------------

fn test_partition() -> MergePartition {
    let start_ns = crate::publication::START;

    MergePartition {
        partition_id: 0,
        start_ns,
        end_ns: start_ns + 200,
    }
}

// -----------------------------------------------------------------------------
// Validate physical output
// -----------------------------------------------------------------------------

fn validate_partition_output(staging: &Path, result: &UnifiedPartitionResult) {
    assert_eq!(
        result.events_written,
        result.trade_events + result.depth_events
    );

    assert_eq!(result.files_written as usize, result.files.len());

    let mut physical_rows = 0_u64;

    for file in &result.files {
        let path = staging.join(&file.path);

        assert!(path.is_file());

        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(&path).unwrap(),
        )
        .unwrap();

        assert_eq!(
            reader.schema().as_ref(),
            marketforge_engine::historical::merge::unified_timeline_schema().as_ref()
        );

        physical_rows += reader.metadata().file_metadata().num_rows() as u64;
    }

    assert_eq!(physical_rows, result.events_written);

    if result.events_written > 0 {
        assert!(result.first_timestamp_ns.is_some());
        assert!(result.last_timestamp_ns.is_some());

        assert!(result.first_timestamp_ns.unwrap() >= result.start_ns);

        assert!(result.last_timestamp_ns.unwrap() < result.end_ns);
    }
}

// -----------------------------------------------------------------------------
// Trade-only partition
// -----------------------------------------------------------------------------

#[test]
fn executes_generated_trade_partition() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();
    let staging = TestDirectory::new();

    let dataset_order = [111_u64, 109, 115];

    let inputs = dataset_order
        .iter()
        .map(|&id| trade_publication(&root, id))
        .collect::<Vec<_>>();

    let trade_inventory = build_trade_inventory(&inputs).unwrap();

    let partition = test_partition();

    let intervals = trade_task_intervals(&trade_inventory);

    let plans = plan_unified_partition_inputs(&[partition], &intervals).unwrap();

    let result = execute_unified_partition(
        &plans[0],
        &trade_inventory,
        &[],
        &[],
        &dataset_order,
        &parquet_config(),
        staging.path(),
    )
    .expect("execute trade partition");

    assert!(result.trade_events > 0);
    assert_eq!(result.depth_events, 0);

    assert!(result.skipped_initializations.is_empty());

    validate_partition_output(staging.path(), &result);

    println!("\nMARKETFORGE — TRADE PARTITION");
    println!("{}", "=".repeat(80));
    println!("Trade events : {}", result.trade_events);
    println!("Parquet files: {}", result.files_written);
    println!("Result       : PASS");
}

// -----------------------------------------------------------------------------
// Combined trade/depth partition
// -----------------------------------------------------------------------------

#[test]
fn executes_generated_combined_partition() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();
    let staging = TestDirectory::new();

    let trade_datasets = [111_u64, 109];
    let depth_datasets = [105_u64, 112];

    let trade_inputs = trade_datasets
        .iter()
        .map(|&id| trade_publication(&root, id))
        .collect::<Vec<_>>();

    let depth_inputs = depth_datasets
        .iter()
        .map(|&id| depth_publication(&root, id))
        .collect::<Vec<_>>();

    let trade_inventory = build_trade_inventory(&trade_inputs).unwrap();

    let depth_inventory = build_depth_inventory(&depth_inputs).unwrap();

    let boundaries = reconcile_depth_boundaries(&depth_inventory).unwrap();

    let mut intervals = trade_task_intervals(&trade_inventory);

    intervals.extend(depth_task_intervals(&depth_inventory).unwrap());

    let partition = test_partition();

    let plans = plan_unified_partition_inputs(&[partition], &intervals).unwrap();

    let dataset_order = [111_u64, 109, 105, 112];

    let result = execute_unified_partition(
        &plans[0],
        &trade_inventory,
        &depth_inventory,
        &boundaries,
        &dataset_order,
        &parquet_config(),
        staging.path(),
    )
    .expect("execute combined partition");

    assert!(result.trade_events > 0);
    assert!(result.depth_events > 0);

    validate_partition_output(staging.path(), &result);

    println!("\nMARKETFORGE — COMBINED PARTITION");
    println!("{}", "=".repeat(80));
    println!("Trade events : {}", result.trade_events);
    println!("Depth events : {}", result.depth_events);
    println!("Total events : {}", result.events_written);
    println!("Parquet files: {}", result.files_written);
    println!("Result       : PASS");
}
