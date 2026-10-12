#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_processing_job,
    process::{load_processing_config, session::execute_session_tasks},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILES: [&str; 2] = [
    "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json",
    "okx-future-inverse-BTC-USD-261225-trade-20260901-20260904-d122.json",
];

// -----------------------------------------------------------------------------
// Test environment
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn test_directory() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory = std::env::temp_dir().join(format!(
        "marketforge-session-test-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).expect("create test directory");

    directory
}

// -----------------------------------------------------------------------------
// Global session integration test
// -----------------------------------------------------------------------------

#[test]
fn executes_tasks_across_multiple_jobs() {
    let root = project_root();
    let directory = test_directory();

    // -------------------------------------------------------------------------
    // Load processing jobs
    // -------------------------------------------------------------------------

    let mut jobs = Vec::new();

    for (index, filename) in JOB_FILES.iter().enumerate() {
        let mut job = load_processing_job(root.join("data/.jobs/process").join(filename))
            .expect("load processing job");

        // Each job must have isolated staging and dataset paths.
        job.output.staging_path = directory.join(format!("job-{index}/staging"));

        job.output.dataset_path = directory.join(format!("job-{index}/dataset"));

        fs::create_dir_all(&job.output.staging_path).expect("create job staging directory");

        jobs.push(job);
    }

    // -------------------------------------------------------------------------
    // Load global processing configuration
    // -------------------------------------------------------------------------

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    // Four workers shared globally across both jobs.
    config.resources.workers = 4;

    // Small Parquet targets for fast integration testing.
    config.resources.parquet.row_group_target_bytes = 16 * 1024;
    config.resources.parquet.file_target_bytes = 64 * 1024;

    // -------------------------------------------------------------------------
    // Execute global session
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — GLOBAL PROCESSING SESSION");
    println!("{}", "=".repeat(80));

    println!("Jobs       : {}", jobs.len());
    println!(
        "Total tasks: {}",
        jobs.iter().map(|job| job.tasks.len()).sum::<usize>()
    );
    println!("Workers    : {}", config.resources.workers);

    let results = execute_session_tasks(&jobs, &config).expect("execute global processing session");

    // -------------------------------------------------------------------------
    // Verify session-level results
    // -------------------------------------------------------------------------

    assert_eq!(results.jobs.len(), 2, "unexpected number of job results");

    assert_eq!(results.total_tasks, 6, "unexpected total task count");

    // -------------------------------------------------------------------------
    // Verify task execution and output isolation
    // -------------------------------------------------------------------------

    let mut total_records_read = 0u64;
    let mut total_events_written = 0u64;

    for (job, result) in jobs.iter().zip(&results.jobs) {
        assert_eq!(
            result.executions.len(),
            job.tasks.len(),
            "job task count mismatch"
        );

        for (task, execution) in job.tasks.iter().zip(&result.executions) {
            // Verify deterministic task ordering.
            assert_eq!(
                execution.task_id, task.task_id.0,
                "task execution order mismatch"
            );

            // Verify successful task processing.
            assert!(
                execution.is_success(),
                "task {} failed: {:?}",
                execution.task_id,
                execution.error,
            );

            // Verify task completion metrics.
            assert_eq!(
                execution.metrics.counters.tasks_completed, 1,
                "task {} was not marked complete",
                execution.task_id,
            );

            assert_eq!(
                execution.metrics.counters.tasks_failed, 0,
                "task {} was incorrectly marked failed",
                execution.task_id,
            );

            // Verify generic output paths.
            assert!(
                !execution.output_paths.is_empty(),
                "task {} produced no output paths",
                execution.task_id,
            );

            for path in &execution.output_paths {
                assert!(
                    path.is_dir(),
                    "task output directory does not exist: {}",
                    path.display(),
                );

                // Every task output must remain inside its job's staging.
                assert!(
                    path.starts_with(&job.output.staging_path),
                    "task output escaped its job staging directory: {}",
                    path.display(),
                );
            }

            total_records_read += execution.metrics.counters.records_read;

            total_events_written += execution.metrics.counters.events_written;
        }
    }

    // -------------------------------------------------------------------------
    // Verify known OKX fixture results
    // -------------------------------------------------------------------------

    // Linear futures:
    // 22,673 source records, 162 matching trades.
    //
    // Inverse futures:
    // 78,047 source records, 16,615 matching trades.

    assert_eq!(
        total_records_read,
        22_673 + 78_047,
        "unexpected source record count"
    );

    assert_eq!(
        total_events_written,
        162 + 16_615,
        "unexpected canonical trade count"
    );

    // -------------------------------------------------------------------------
    // Display results
    // -------------------------------------------------------------------------

    println!("\nRESULTS");

    println!("  Jobs completed  : {}", results.jobs.len());
    println!("  Tasks completed : {}", results.total_tasks);
    println!("  Records read    : {total_records_read}");
    println!("  Trades written  : {total_events_written}");

    for (job, result) in jobs.iter().zip(&results.jobs) {
        let job_records: u64 = result
            .executions
            .iter()
            .map(|execution| execution.metrics.counters.records_read)
            .sum();

        let job_trades: u64 = result
            .executions
            .iter()
            .map(|execution| execution.metrics.counters.events_written)
            .sum();

        println!(
            "  Job {} | tasks={} | records={} | trades={}",
            job.job_id.0,
            result.executions.len(),
            job_records,
            job_trades,
        );
    }

    println!("\nRESULT: PASS");

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------

    fs::remove_dir_all(directory).expect("remove test directory");
}
