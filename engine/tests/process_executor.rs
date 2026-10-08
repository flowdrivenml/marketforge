#![cfg(feature = "process")]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::process::manifest::DatasetManifest;
use marketforge_engine::{
    job::load_processing_job,
    process::{ProcessingStatus, execute_processing_job, load_processing_config},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

// -----------------------------------------------------------------------------
// Test paths
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn job_path() -> PathBuf {
    project_root().join("data/.jobs/process").join(JOB_FILE)
}

fn config_path() -> PathBuf {
    project_root().join("data/.jobs/processing.json")
}

fn test_directory() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory = std::env::temp_dir().join(format!(
        "marketforge-executor-test-{}-{id}",
        std::process::id()
    ));

    fs::create_dir_all(&directory).expect("create test directory");

    directory
}

// -----------------------------------------------------------------------------
// Multi-task executor integration test
// -----------------------------------------------------------------------------

#[test]
fn processes_multiple_trade_tasks_into_staging() {
    // -----------------------------------------------------------------
    // Load processing job
    // -----------------------------------------------------------------

    let mut job = load_processing_job(job_path()).expect("load processing job");

    assert_eq!(job.tasks.len(), 3);

    // -----------------------------------------------------------------
    // Load authoritative global resource configuration
    // -----------------------------------------------------------------

    let mut config = load_processing_config(config_path(), project_root())
        .expect("load global processing configuration");

    // Override resources in memory for this test only.
    //
    // The actual global JSON file remains unchanged.

    config.resources.parquet.row_group_target_bytes = 16 * 1024;
    config.resources.parquet.file_target_bytes = 64 * 1024;

    // -----------------------------------------------------------------
    // Verify that legacy manifest resources are ignored
    // -----------------------------------------------------------------

    // Deliberately assign invalid values.
    //
    // If the executor accidentally uses manifest resources,
    // validation or Parquet initialization should fail.

    job.resources.workers = 0;
    job.resources.memory_budget_bytes = 0;
    job.resources.scratch_budget_bytes = 0;

    job.resources.parquet.row_group_target_bytes = 0;
    job.resources.parquet.file_target_bytes = 0;

    // -----------------------------------------------------------------
    // Configure isolated test output
    // -----------------------------------------------------------------

    let directory = test_directory();

    let staging = directory.join("staging");
    let dataset = directory.join("dataset");

    job.output.staging_path = staging.clone();
    job.output.dataset_path = dataset.clone();

    // -----------------------------------------------------------------
    // Execute processing job
    // -----------------------------------------------------------------

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — MULTI-TASK EXECUTOR TEST");
    println!("{}", "=".repeat(90));

    println!("Job file     : {}", job_path().display());
    println!("Config file  : {}", config_path().display());
    println!("Tasks        : {}", job.tasks.len());
    println!("Staging      : {}", staging.display());
    println!("Final output : {}", dataset.display());

    println!(
        "Row-group target : {} bytes",
        config.resources.parquet.row_group_target_bytes
    );

    println!(
        "File target      : {} bytes",
        config.resources.parquet.file_target_bytes
    );

    let result = execute_processing_job(&job, &config).expect("execute processing job");

    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "executor returned failure: {:?}",
        result.failure
    );

    // Stage 1 must write into staging only.
    assert!(
        !staging.exists(),
        "staging should disappear after successful commit"
    );

    assert!(
        dataset.is_dir(),
        "committed dataset directory does not exist"
    );

    assert!(
        dataset.join("manifest.json").is_file(),
        "committed dataset manifest does not exist"
    );

    // -----------------------------------------------------------------
    // Discover generated Parquet files
    // -----------------------------------------------------------------

    let trades_dir = dataset.join("trades");

    assert!(trades_dir.is_dir());

    let mut parquet_files: Vec<PathBuf> = fs::read_dir(&trades_dir)
        .expect("read Parquet directory")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect();

    parquet_files.sort();

    assert!(
        !parquet_files.is_empty(),
        "executor produced no Parquet files"
    );

    // -----------------------------------------------------------------
    // Read Parquet output and verify statistics
    // -----------------------------------------------------------------

    let mut total_rows = 0u64;

    let mut minimum_timestamp = None::<i64>;
    let mut maximum_timestamp = None::<i64>;

    for path in &parquet_files {
        let file = File::open(path).expect("open Parquet file");

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("read Parquet metadata")
            .build()
            .expect("build Parquet reader");

        for batch in reader {
            let batch = batch.expect("read RecordBatch");

            total_rows += batch.num_rows() as u64;

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .expect("timestamp column")
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("Int64 timestamp array");

            for timestamp in timestamps.values() {
                minimum_timestamp =
                    Some(minimum_timestamp.map_or(*timestamp, |value| value.min(*timestamp)));

                maximum_timestamp =
                    Some(maximum_timestamp.map_or(*timestamp, |value| value.max(*timestamp)));
            }
        }
    }

    // -----------------------------------------------------------------
    // Verify executor metrics
    // -----------------------------------------------------------------

    println!("\nRESULTS");

    println!("  Parquet files     : {}", parquet_files.len());

    println!("  Parquet rows      : {total_rows}");

    println!("  Events reported   : {}", result.events_written);

    println!("  Files reported    : {}", result.files_written);

    println!("  Minimum timestamp : {minimum_timestamp:?}");

    println!("  Maximum timestamp : {maximum_timestamp:?}");

    assert_eq!(
        total_rows, result.events_written,
        "Parquet row count differs from executor metrics"
    );

    assert_eq!(
        parquet_files.len() as u64,
        result.files_written,
        "Parquet file count differs from executor metrics"
    );

    assert_eq!(
        minimum_timestamp, result.start_timestamp_ns,
        "minimum timestamp differs"
    );

    assert_eq!(
        maximum_timestamp, result.end_timestamp_ns,
        "maximum timestamp differs"
    );

    // -----------------------------------------------------------------
    // Verify committed dataset manifest
    // -----------------------------------------------------------------

    let manifest_path = dataset.join("manifest.json");

    let manifest_contents = fs::read_to_string(&manifest_path).expect("read committed manifest");

    let manifest: DatasetManifest =
        serde_json::from_str(&manifest_contents).expect("deserialize committed manifest");

    assert_eq!(manifest.job_id, job.job_id.0,);

    assert_eq!(manifest.dataset_id, job.dataset_id.0,);

    assert_eq!(manifest.events_written, result.events_written,);

    assert_eq!(manifest.files_written, result.files_written,);

    assert_eq!(manifest.start_timestamp_ns, result.start_timestamp_ns,);

    assert_eq!(manifest.end_timestamp_ns, result.end_timestamp_ns,);

    assert_eq!(manifest.source_tasks.len(), job.tasks.len(),);

    assert_eq!(
        result.manifest_path.as_deref(),
        Some(manifest_path.to_string_lossy().as_ref()),
    );

    // The verified OKX linear futures archives contain:
    //
    // September 1: 116 matching trades
    // September 2:   0 matching trades
    // September 3:  46 matching trades
    //
    // Total: 162 trades.

    assert_eq!(total_rows, 162, "unexpected OKX futures trade count");

    println!("\nRESULT: PASS");

    // -----------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------

    fs::remove_dir_all(directory).expect("remove test directory");
}
