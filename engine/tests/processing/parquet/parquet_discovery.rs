#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_processing_job,
    process::{load_processing_config, manifest::inspect_parquet_files, task::execute_trade_task},
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

    let directory = std::env::temp_dir().join(format!(
        "marketforge-discovery-test-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).expect("create test directory");

    directory
}

#[test]
fn discovers_parquet_files_from_multiple_tasks() {
    let root = project_root();

    let job = load_processing_job(root.join("data/.jobs/process").join(JOB_FILE))
        .expect("load processing job");

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = test_directory();

    let mut parquet = config.resources.parquet.clone();

    parquet.row_group_target_bytes = 16 * 1024;
    parquet.file_target_bytes = 64 * 1024;

    let mut expected_rows = 0u64;

    for task in &job.tasks {
        let execution = execute_trade_task(task, &config.integrity_policy, &parquet, &directory);

        assert!(
            execution.is_success(),
            "task {} failed: {:?}",
            task.task_id.0,
            execution.error,
        );

        expected_rows += execution.metrics.counters.events_written;
    }

    let files = inspect_parquet_files(&directory).expect("discover task Parquet files");

    let actual_rows: u64 = files.iter().map(|file| file.rows).sum();

    assert_eq!(actual_rows, expected_rows);

    assert_eq!(actual_rows, 162);

    assert!(!files.is_empty());

    for file in &files {
        assert!(
            file.path.starts_with("tasks/"),
            "unexpected file path: {}",
            file.path,
        );

        assert!(file.path.ends_with(".parquet"));

        assert!(directory.join(&file.path).is_file());
    }

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — NESTED PARQUET DISCOVERY");
    println!("{}", "=".repeat(80));

    println!("Tasks processed : {}", job.tasks.len());
    println!("Parquet files   : {}", files.len());
    println!("Expected rows   : {expected_rows}");
    println!("Actual rows     : {actual_rows}");

    for file in &files {
        println!(
            "  {} | rows={} | bytes={}",
            file.path, file.rows, file.size_bytes,
        );
    }

    println!("RESULT          : PASS");

    fs::remove_dir_all(directory).expect("remove test directory");
}
