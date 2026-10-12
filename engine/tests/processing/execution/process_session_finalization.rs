#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_processing_job,
    process::{
        ProcessingStatus, failure::PROCESSING_FAILURE_FILENAME, load_processing_config,
        session::execute_processing_session,
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILES: [&str; 2] = [
    "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json",
    "okx-future-inverse-BTC-USD-261225-trade-20260901-20260904-d122.json",
];

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn session_commits_successful_jobs_and_preserves_failures() {
    let root = project_root();

    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory = std::env::temp_dir().join(format!(
        "marketforge-session-finalization-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).unwrap();

    let mut jobs = Vec::new();

    for (index, filename) in JOB_FILES.iter().enumerate() {
        let mut job = load_processing_job(root.join("data/.jobs/process").join(filename)).unwrap();

        job.output.staging_path = directory.join(format!("job-{index}/staging"));

        job.output.dataset_path = directory.join(format!("job-{index}/dataset"));

        jobs.push(job);
    }

    // Force a failure in the second job.
    jobs[1].tasks[1].input_path = directory.join("missing-source.zip");

    let mut config =
        load_processing_config(root.join("data/.jobs/processing.json"), &root).unwrap();

    config.resources.workers = 4;

    config.resources.parquet.row_group_target_bytes = 16 * 1024;
    config.resources.parquet.file_target_bytes = 64 * 1024;

    let result = execute_processing_session(&jobs, &config).expect("execute processing session");

    assert_eq!(result.jobs.len(), 2);
    assert_eq!(result.total_tasks, 6);

    // -------------------------------------------------------------------------
    // Successful job
    // -------------------------------------------------------------------------

    assert_eq!(result.jobs[0].status, ProcessingStatus::Complete,);

    assert!(jobs[0].output.dataset_path.is_dir());

    assert!(jobs[0].output.dataset_path.join("manifest.json").is_file());

    assert!(!jobs[0].output.staging_path.exists());

    // -------------------------------------------------------------------------
    // Failed job
    // -------------------------------------------------------------------------

    assert_eq!(result.jobs[1].status, ProcessingStatus::Failed,);

    assert!(!jobs[1].output.dataset_path.exists());

    assert!(
        jobs[1]
            .output
            .staging_path
            .join(PROCESSING_FAILURE_FILENAME)
            .is_file()
    );

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — GLOBAL SESSION FINALIZATION");
    println!("{}", "=".repeat(80));

    println!("Jobs       : {}", result.jobs.len());
    println!("Tasks      : {}", result.total_tasks);
    println!("Workers    : {}", config.resources.workers);

    println!("Job 0      : {:?}", result.jobs[0].status);

    println!("Job 1      : {:?}", result.jobs[1].status);

    println!("RESULT     : PASS");

    fs::remove_dir_all(directory).unwrap();
}
