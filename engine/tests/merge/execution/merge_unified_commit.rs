#![cfg(feature = "process")]

use std::collections::HashSet;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::process::merge::{physical_combined_schema, physical_trade_schema};
use marketforge_engine::{
    job::{
        MergeJob, MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning,
        MergeResources, MergeTimeRange,
    },
    process::merge::{PhysicalMergeManifest, UnifiedMergeMode, execute_unified_merge_job},
};

#[path = "../../common/mod.rs"]
mod common;

use common::datasets::{project_root, resolve_depth_dataset, resolve_trade_dataset};

// -----------------------------------------------------------------------------
// Temporary output directory
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-unified-commit-{}-{timestamp}",
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
// Job configuration
// -----------------------------------------------------------------------------

fn test_job(dataset_ids: &[u64], output: &Path, input_paths: HashMap<u64, PathBuf>) -> MergeJob {
    let start_ns = 1_788_220_800_000_000_000_i64;

    MergeJob {
        job_id: 900_500,
        operation: MergeOperation::Merge,

        inputs: dataset_ids.to_vec(),
        input_paths,

        time_range: MergeTimeRange {
            start_ns,
            end_ns: start_ns + 240_000_000_000,
        },

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
// Resolve custom dataset paths
// -----------------------------------------------------------------------------

fn input_paths(root: &Path, trade_ids: &[u64], depth_ids: &[u64]) -> HashMap<u64, PathBuf> {
    let mut paths = HashMap::new();

    for &dataset_id in trade_ids {
        let input = resolve_trade_dataset(root, dataset_id);
        paths.insert(dataset_id, input.dataset_path);
    }

    for &dataset_id in depth_ids {
        let input = resolve_depth_dataset(root, dataset_id);
        paths.insert(dataset_id, input.dataset_path);
    }

    paths
}

// -----------------------------------------------------------------------------
// Execute and validate committed dataset
// -----------------------------------------------------------------------------

fn assert_transactional_commit(
    trade_ids: &[u64],
    depth_ids: &[u64],
    expected_mode: UnifiedMergeMode,
    expected_trades: u64,
    expected_depth: u64,
) {
    let root = project_root();
    let temporary = TestDirectory::new();

    let dataset_path = temporary.path().join("merged-900500");

    let dataset_ids = trade_ids
        .iter()
        .chain(depth_ids.iter())
        .copied()
        .collect::<Vec<_>>();

    let paths = input_paths(&root, trade_ids, depth_ids);

    let job = test_job(&dataset_ids, &dataset_path, paths);

    // -------------------------------------------------------------------------
    // Execute complete production pipeline
    // -------------------------------------------------------------------------

    let result = execute_unified_merge_job(&job, &root).expect("execute unified merge job");

    // -------------------------------------------------------------------------
    // Transactional publication
    // -------------------------------------------------------------------------

    assert_eq!(result.dataset_path, dataset_path);
    assert!(dataset_path.is_dir());

    let staging = temporary
        .path()
        .join(format!(".merged-900500.staging-{}", job.job_id));

    assert!(!staging.exists(), "staging directory should be committed");

    assert!(!dataset_path.join("logical").exists());
    assert!(!dataset_path.join("physical").exists());

    assert!(dataset_path.join("timeline").is_dir());

    let entries = fs::read_dir(&dataset_path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<HashSet<_>>();

    assert_eq!(
        entries,
        HashSet::from(["manifest.json".to_owned(), "timeline".to_owned(),]),
    );

    // -------------------------------------------------------------------------
    // Manifest persistence
    // -------------------------------------------------------------------------

    let manifest_path = dataset_path.join("manifest.json");

    assert!(manifest_path.is_file());

    let manifest: PhysicalMergeManifest =
        serde_json::from_reader(File::open(&manifest_path).unwrap())
            .expect("deserialize physical manifest");

    manifest.validate().expect("validate physical manifest");

    assert_eq!(manifest.mode, expected_mode);

    assert_eq!(manifest.trade_events, expected_trades);
    assert_eq!(manifest.depth_events, expected_depth);

    assert_eq!(manifest.events_written, expected_trades + expected_depth);

    assert_eq!(manifest.files_written as usize, manifest.files.len());

    // -------------------------------------------------------------------------
    // Validate committed files
    // -------------------------------------------------------------------------

    for file in &manifest.files {
        let path = dataset_path.join(&file.path);

        assert!(
            path.is_file(),
            "missing committed Parquet file: {}",
            path.display()
        );

        assert_eq!(fs::metadata(&path).unwrap().len(), file.size_bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .expect("open committed physical Parquet");

        let expected_schema = match expected_mode {
            UnifiedMergeMode::Trades => physical_trade_schema(),
            UnifiedMergeMode::Combined => physical_combined_schema(),
            UnifiedMergeMode::Depth => unreachable!(),
        };

        assert_eq!(
            reader.schema().as_ref(),
            expected_schema.as_ref(),
            "committed file has incorrect physical schema"
        );
        assert!(
            file.rows.unwrap_or(0) > 0,
            "committed file has no validated rows"
        );
    }

    // -------------------------------------------------------------------------
    // Duplicate publication protection
    // -------------------------------------------------------------------------

    let duplicate = execute_unified_merge_job(&job, &root);

    assert!(
        duplicate.is_err(),
        "duplicate publication should be rejected"
    );

    // Original committed dataset must remain intact.
    assert!(manifest_path.is_file());

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------

    println!("\nMARKETFORGE — UNIFIED TRANSACTIONAL COMMIT");
    println!("{}", "=".repeat(80));

    println!("Mode           : {:?}", result.mode);
    println!("Streams        : {}", dataset_ids.len());
    println!("Partitions     : {}", result.partitions_completed);
    println!("Trade events   : {}", result.trade_events);
    println!("Depth events   : {}", result.depth_events);
    println!("Total events   : {}", result.events_written);
    println!("Parquet files  : {}", result.files_written);
    println!(
        "Skipped initializations: {}",
        result.skipped_initializations
    );

    println!("Manifest       : VERIFIED");
    println!("Transactional commit : VERIFIED");
    println!("Duplicate protection : VERIFIED");
    println!("Result         : PASS");
}

// -----------------------------------------------------------------------------
// Trade-only transaction
// -----------------------------------------------------------------------------

#[test]
fn commits_real_trade_merge() {
    assert_transactional_commit(&[111, 109, 115], &[], UnifiedMergeMode::Trades, 1_376, 0);
}

// -----------------------------------------------------------------------------
// Combined transaction
// -----------------------------------------------------------------------------

#[test]
fn commits_real_combined_merge() {
    assert_transactional_commit(
        &[111, 109],
        &[105, 112],
        UnifiedMergeMode::Combined,
        607,
        49_132,
    );
}
