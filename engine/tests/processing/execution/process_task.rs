#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_processing_job,
    process::{load_processing_config, task::execute_trade_task},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn test_directory() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory =
        std::env::temp_dir().join(format!("marketforge-task-test-{}-{id}", std::process::id()));

    fs::create_dir_all(&directory).expect("create test directory");

    directory
}

#[test]
fn executes_trade_task_into_isolated_output() {
    let root = project_root();

    let job = load_processing_job(root.join("data/.jobs/process").join(JOB_FILE))
        .expect("load processing job");

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = test_directory();

    let mut parquet = config.resources.parquet.clone();

    // Small targets keep the test fast.
    parquet.row_group_target_bytes = 16 * 1024;
    parquet.file_target_bytes = 64 * 1024;

    let task = &job.tasks[0];

    let execution = execute_trade_task(task, &config.integrity_policy, &parquet, &directory);

    assert!(
        execution.is_success(),
        "task execution failed: {:?}",
        execution.error,
    );

    assert_eq!(execution.metrics.counters.tasks_completed, 1,);

    assert_eq!(execution.metrics.counters.tasks_failed, 0,);

    assert_eq!(execution.metrics.counters.records_read, 4201,);

    assert_eq!(execution.metrics.counters.records_matched, 116,);

    assert_eq!(execution.metrics.counters.events_written, 116,);

    assert!(execution.output_path.is_dir());

    let parquet_files: Vec<PathBuf> = fs::read_dir(&execution.output_path)
        .expect("read task output")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect();

    assert!(!parquet_files.is_empty());

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — ISOLATED TASK TEST");
    println!("{}", "=".repeat(80));

    println!("Task ID         : {}", execution.task_id);
    println!(
        "Records read    : {}",
        execution.metrics.counters.records_read
    );
    println!(
        "Records matched : {}",
        execution.metrics.counters.records_matched
    );
    println!(
        "Trades written  : {}",
        execution.metrics.counters.events_written
    );
    println!("Parquet files   : {}", parquet_files.len());
    println!("Output          : {}", execution.output_path.display());
    println!("RESULT          : PASS");

    fs::remove_dir_all(directory).expect("remove test directory");
}

#[test]
fn failed_task_preserves_metrics() {
    let root = project_root();

    let mut job = load_processing_job(root.join("data/.jobs/process").join(JOB_FILE))
        .expect("load processing job");

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = test_directory();

    // Force source failure without changing normalization.
    job.tasks[0].input_path = directory.join("missing-source.zip");

    let execution = execute_trade_task(
        &job.tasks[0],
        &config.integrity_policy,
        &config.resources.parquet,
        &directory,
    );

    assert!(!execution.is_success());

    assert_eq!(execution.metrics.counters.tasks_failed, 1,);

    assert_eq!(execution.metrics.counters.tasks_completed, 0,);

    assert_eq!(execution.metrics.counters.records_read, 0,);

    assert!(execution.error.is_some());

    println!("\nMARKETFORGE — FAILED ISOLATED TASK");
    println!("Task ID : {}", execution.task_id);
    println!("Error   : {:?}", execution.error);
    println!("RESULT  : PASS");

    fs::remove_dir_all(directory).expect("remove test directory");
}
