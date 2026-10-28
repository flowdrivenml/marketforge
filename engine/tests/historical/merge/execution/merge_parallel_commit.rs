#![cfg(feature = "historical")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use {
    marketforge_engine::historical::job::load_merge_job,
    marketforge_engine::historical::merge::execute_parallel_merge_job,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

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
// Temporary test directory
// -----------------------------------------------------------------------------

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-parallel-commit-{}-{id}",
            std::process::id(),
        ));

        fs::create_dir_all(&path).expect("create temporary directory");

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// -----------------------------------------------------------------------------
// Integration test
// -----------------------------------------------------------------------------

#[test]
fn executes_and_commits_parallel_merge() {
    let root = project_root();

    let mut job =
        load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    // -------------------------------------------------------------------------
    // Temporary two-hour configuration
    // -------------------------------------------------------------------------

    job.job_id = 900003;

    job.time_range.end_ns = job.time_range.start_ns + 7_200_000_000_000;

    job.resources.workers = 4;
    job.partitioning.partitions_per_worker = 2;

    let temporary = TestDirectory::new();

    job.output.dataset_path = temporary.path.join("merged-900003");

    // -------------------------------------------------------------------------
    // Execute parallel merge
    // -------------------------------------------------------------------------

    let result = execute_parallel_merge_job(&job, &root).expect("execute parallel merge");

    // -------------------------------------------------------------------------
    // Verify execution result
    // -------------------------------------------------------------------------

    assert_eq!(result.job_id, 900003);

    assert_eq!(result.partitions_completed, 8);
    assert_eq!(result.events_written, 1_415_631);
    assert_eq!(result.files_written, 8);

    assert!(result.dataset_path.is_dir());
    assert!(result.manifest_path.is_file());

    // -------------------------------------------------------------------------
    // Verify committed manifest
    // -------------------------------------------------------------------------

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&result.manifest_path).expect("read manifest"))
            .expect("parse manifest");

    assert_eq!(manifest["job_id"], 900003);
    assert_eq!(manifest["events_written"], 1_415_631);
    assert_eq!(manifest["files_written"], 8);

    assert_eq!(manifest["input_datasets"].as_array().unwrap().len(), 4,);

    assert_eq!(manifest["initial_checkpoints"].as_array().unwrap().len(), 4,);

    // -------------------------------------------------------------------------
    // Verify committed files
    // -------------------------------------------------------------------------

    for file in manifest["files"].as_array().unwrap() {
        let relative_path = file["path"].as_str().expect("file path");

        let path = result.dataset_path.join(relative_path);

        assert!(path.is_file(), "missing committed file: {}", path.display());

        assert!(file["rows"].as_u64().unwrap() > 0);
    }

    // -------------------------------------------------------------------------
    // Verify staging was consumed
    // -------------------------------------------------------------------------

    let staging = temporary.path.join(".merged-900003.staging-900003");

    assert!(!staging.exists(), "staging directory remains after commit");

    // -------------------------------------------------------------------------
    // Verify duplicate execution is rejected
    // -------------------------------------------------------------------------

    let duplicate = execute_parallel_merge_job(&job, &root);

    assert!(
        duplicate.is_err(),
        "duplicate merge execution unexpectedly succeeded"
    );

    println!("\nMARKETFORGE — PARALLEL MERGE COMMIT");
    println!("{}", "=".repeat(80));

    println!("Workers         : {}", job.resources.workers);
    println!("Partitions      : {}", result.partitions_completed);
    println!("Merged events   : {}", result.events_written);
    println!("Parquet files   : {}", result.files_written);

    println!("Skipped snapshots: {}", result.skipped_initializations);

    println!("Dataset         : {}", result.dataset_path.display());

    println!("Transactional commit : VERIFIED");
    println!("Duplicate protection : VERIFIED");
    println!("Result               : PASS");
}
