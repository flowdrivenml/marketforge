#![cfg(feature = "historical")]

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::{
    job::{
        MergeJob, MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning,
        MergeResources, MergeTimeRange,
    },
    process::merge::execute_unified_merge_job,
};

#[path = "../../common/mod.rs"]
mod common;

use common::datasets::{project_root, resolve_trade_dataset};

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
            "marketforge-unified-failure-{}-{timestamp}",
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
// Job fixture
// -----------------------------------------------------------------------------

fn test_job(root: &Path, output: &Path) -> MergeJob {
    let input = resolve_trade_dataset(root, 111);

    let start_ns = 1_788_220_800_000_000_000_i64;

    MergeJob {
        job_id: 900_600,
        operation: MergeOperation::Merge,

        inputs: vec![111],

        input_paths: HashMap::from([(111, input.dataset_path)]),

        time_range: MergeTimeRange {
            start_ns,
            end_ns: start_ns + 60_000_000_000,
        },

        resources: MergeResources {
            workers: 2,
            memory_limit_mb: 2048,
            max_open_files: 64,
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
// Assertions
// -----------------------------------------------------------------------------

fn staging_path(output: &Path, job_id: u64) -> PathBuf {
    output.parent().unwrap().join(format!(
        ".{}.staging-{job_id}",
        output.file_name().unwrap().to_string_lossy()
    ))
}

fn assert_not_published(output: &Path) {
    assert!(
        !output.exists(),
        "failed merge published an output dataset: {}",
        output.display()
    );
}

// -----------------------------------------------------------------------------
// Missing input dataset
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_input_dataset() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let mut job = test_job(&root, &output);

    job.input_paths
        .insert(111, temporary.path().join("nonexistent-dataset"));

    assert!(execute_unified_merge_job(&job, &root).is_err());

    assert_not_published(&output);

    assert!(!staging_path(&output, job.job_id).exists());
}

// -----------------------------------------------------------------------------
// Corrupted source manifest
// -----------------------------------------------------------------------------

#[test]
fn rejects_corrupted_source_manifest() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let mut job = test_job(&root, &output);

    let corrupted = temporary.path().join("corrupted-input");

    fs::create_dir_all(&corrupted).unwrap();

    fs::write(corrupted.join("manifest.json"), b"{ invalid json").unwrap();

    job.input_paths.insert(111, corrupted);

    assert!(execute_unified_merge_job(&job, &root).is_err());

    assert_not_published(&output);
}

// -----------------------------------------------------------------------------
// Missing processing job
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_processing_job() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let job = test_job(&root, &output);

    // The input dataset exists, but the temporary project root
    // contains no processing job metadata.
    let fake_root = temporary.path().join("fake-project");

    fs::create_dir_all(&fake_root).unwrap();

    assert!(execute_unified_merge_job(&job, &fake_root).is_err());

    assert_not_published(&output);
}

// -----------------------------------------------------------------------------
// Existing committed output
// -----------------------------------------------------------------------------

#[test]
fn rejects_existing_output_without_modification() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    fs::create_dir_all(&output).unwrap();

    let marker = output.join("existing-data.txt");

    fs::write(&marker, b"DO NOT MODIFY").unwrap();

    let job = test_job(&root, &output);

    assert!(execute_unified_merge_job(&job, &root).is_err());

    assert_eq!(fs::read(&marker).unwrap(), b"DO NOT MODIFY");
}

// -----------------------------------------------------------------------------
// Existing staging directory
// -----------------------------------------------------------------------------

#[test]
fn rejects_existing_staging_directory() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let job = test_job(&root, &output);

    let staging = staging_path(&output, job.job_id);

    fs::create_dir_all(&staging).unwrap();

    let marker = staging.join("existing-staging.txt");

    fs::write(&marker, b"DO NOT MODIFY").unwrap();

    let result = execute_unified_merge_job(&job, &root);

    assert!(
        result.is_err(),
        "existing staging directory should be rejected"
    );

    assert_not_published(&output);

    assert_eq!(fs::read(&marker).unwrap(), b"DO NOT MODIFY");
}

// -----------------------------------------------------------------------------
// Insufficient memory budget
// -----------------------------------------------------------------------------

#[test]
fn rejects_insufficient_memory_budget() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let mut job = test_job(&root, &output);

    job.resources.memory_limit_mb = 1;

    assert!(execute_unified_merge_job(&job, &root).is_err());

    assert_not_published(&output);
}

// -----------------------------------------------------------------------------
// Insufficient open-file budget
// -----------------------------------------------------------------------------

#[test]
fn rejects_insufficient_file_budget() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let output = temporary.path().join("merged");

    let mut job = test_job(&root, &output);

    job.resources.max_open_files = 1;

    assert!(execute_unified_merge_job(&job, &root).is_err());

    assert_not_published(&output);
}
