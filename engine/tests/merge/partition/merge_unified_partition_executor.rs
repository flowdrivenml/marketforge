#![cfg(feature = "process")]
use marketforge_engine::process::merge::ResolvedMergeInput;
use marketforge_engine::process::merge::{
    MergePartition, build_depth_inventory, build_trade_inventory, depth_task_intervals,
    plan_unified_partition_inputs, trade_task_intervals,
};
use marketforge_engine::{
    job::MergeParquetConfig,
    process::{
        manifest::DatasetManifest,
        merge::{UnifiedPartitionResult, execute_unified_partition, reconcile_depth_boundaries},
    },
};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

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

// -----------------------------------------------------------------------------
// Resolve real trade dataset
// -----------------------------------------------------------------------------

fn resolve_trade_dataset(root: &Path, dataset_id: u64) -> ResolvedMergeInput {
    let datasets = root.join("data/.work/full-trade-test/datasets");

    for entry in std::fs::read_dir(&datasets).expect("read trade datasets") {
        let entry = entry.expect("read dataset entry");

        let dataset_path = entry.path();
        let manifest_path = dataset_path.join("manifest.json");

        if !manifest_path.is_file() {
            continue;
        }

        let manifest: DatasetManifest =
            serde_json::from_reader(File::open(&manifest_path).unwrap())
                .expect("parse trade manifest");

        if manifest.dataset_id != dataset_id {
            continue;
        }

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

        return ResolvedMergeInput {
            dataset_id,
            dataset_path,
            manifest,
            canonical_files,
            auxiliary_files,
        };
    }

    panic!("trade dataset {dataset_id} not found");
}
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
    let start_ns = 1_788_220_800_000_000_000_i64;

    MergePartition {
        partition_id: 0,
        start_ns,
        end_ns: start_ns + 60_000_000_000,
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
            marketforge_engine::process::merge::unified_timeline_schema().as_ref()
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
fn executes_real_trade_partition() {
    let root = project_root();
    let staging = TestDirectory::new();

    let dataset_order = [111_u64, 109, 115];

    let inputs = dataset_order
        .iter()
        .map(|&id| resolve_trade_dataset(&root, id))
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
fn executes_real_combined_partition() {
    let root = project_root();
    let staging = TestDirectory::new();

    let trade_datasets = [111_u64, 109];
    let depth_datasets = [105_u64, 112];

    let trade_inputs = trade_datasets
        .iter()
        .map(|&id| resolve_trade_dataset(&root, id))
        .collect::<Vec<_>>();

    let depth_inputs = depth_datasets
        .iter()
        .map(|&id| resolve_depth_dataset(&root, id))
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
