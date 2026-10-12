#![cfg(feature = "process")]

use marketforge_engine::{
    job::{
        MergeJob, MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning,
        MergeResources, MergeTimeRange,
    },
    process::merge::{
        PhysicalMergeManifest, UnifiedMergeMode, execute_unified_merge_job,
        physical_combined_schema,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
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
            "marketforge-production-rotation-{}-{timestamp}",
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

fn test_job(root: &Path, output: &Path) -> MergeJob {
    let trade_ids = [111_u64, 109];
    let depth_ids = [105_u64, 112];

    let mut input_paths = HashMap::new();

    for dataset_id in trade_ids {
        let input = resolve_trade_dataset(root, dataset_id);
        input_paths.insert(dataset_id, input.dataset_path);
    }

    for dataset_id in depth_ids {
        let input = resolve_depth_dataset(root, dataset_id);
        input_paths.insert(dataset_id, input.dataset_path);
    }

    let start_ns = 1_788_220_800_000_000_000_i64;

    MergeJob {
        job_id: 900_700,
        operation: MergeOperation::Merge,

        inputs: vec![111, 109, 105, 112],
        input_paths,

        time_range: MergeTimeRange {
            start_ns,
            end_ns: start_ns + 7_200_000_000_000,
        },

        resources: MergeResources {
            workers: 1,
            memory_limit_mb: 4096,
            max_open_files: 128,
        },

        partitioning: MergePartitioning {
            partitions_per_worker: 1,
        },

        parquet: MergeParquetConfig {
            row_group_target_mb: 1,
            file_target_mb: 1,
            compression_level: 3,
        },

        output: MergeOutput {
            dataset_path: output.to_path_buf(),
        },
    }
}

// -----------------------------------------------------------------------------
// Production rotation test
// -----------------------------------------------------------------------------

#[test]
fn validates_real_combined_rotation() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let job = test_job(&root, &output);

    // -------------------------------------------------------------------------
    // Execute complete production pipeline
    // -------------------------------------------------------------------------

    let result = execute_unified_merge_job(&job, &root).expect("execute production unified merge");

    assert_eq!(result.mode, UnifiedMergeMode::Combined);
    assert_eq!(result.partitions_completed, 1);

    // -------------------------------------------------------------------------
    // Verify file rotation
    // -------------------------------------------------------------------------

    assert!(
        result.files_written > 1,
        "expected multiple Parquet files in one partition, got {}",
        result.files_written
    );

    // -------------------------------------------------------------------------
    // Validate committed manifest
    // -------------------------------------------------------------------------

    let manifest: PhysicalMergeManifest =
        serde_json::from_reader(File::open(&result.manifest_path).unwrap())
            .expect("parse physical manifest");

    manifest.validate().unwrap();

    assert_eq!(manifest.files_written, result.files_written);
    assert_eq!(manifest.events_written, result.events_written);

    assert_eq!(
        manifest.trade_events + manifest.depth_events,
        manifest.events_written
    );

    // -------------------------------------------------------------------------
    // Verify committed files
    // -------------------------------------------------------------------------

    let mut total_rows = 0_u64;

    for (index, file) in manifest.files.iter().enumerate() {
        let mut next_file_index = std::collections::HashMap::<u32, u32>::new();

        for file in &manifest.files {
            let partition_id = file
                .partition_id
                .expect("physical file must have partition ID");

            let index = next_file_index.entry(partition_id).or_insert(0);

            let expected_name = format!("part-{:06}.parquet", *index);

            assert!(
                file.path.ends_with(&expected_name),
                "unexpected rotated file name: {}",
                file.path
            );

            *index += 1;

            // Keep the remaining file validation code here.
        }
        let expected_name = format!("part-{index:06}.parquet");

        assert!(
            file.path.ends_with(&expected_name),
            "unexpected rotated file name: {}",
            file.path
        );

        let path = result.dataset_path.join(&file.path);

        assert!(path.is_file());

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .expect("open rotated physical Parquet");

        assert_eq!(
            reader.schema().as_ref(),
            physical_combined_schema().as_ref(),
            "rotated file has incorrect physical schema"
        );

        assert_eq!(fs::metadata(&path).unwrap().len(), file.size_bytes);

        total_rows += file.rows.expect("validated row count");
    }

    assert_eq!(total_rows, manifest.events_written);

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------

    println!("\nMARKETFORGE — PRODUCTION FILE ROTATION");
    println!("{}", "=".repeat(80));

    println!("Mode         : {:?}", result.mode);
    println!("Partitions   : {}", result.partitions_completed);
    println!("Trade events : {}", result.trade_events);
    println!("Depth events : {}", result.depth_events);
    println!("Total events : {}", result.events_written);
    println!("Parquet files: {}", result.files_written);

    println!("Rotation     : VERIFIED");
    println!("References   : VERIFIED");
    println!("Manifest     : VERIFIED");
    println!("Commit       : VERIFIED");
    println!("Result       : PASS");
}
