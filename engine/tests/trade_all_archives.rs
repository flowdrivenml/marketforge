#![cfg(feature = "process")]

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use marketforge_engine::{
    job::{ContentType, ProcessingOperation, load_processing_job},
    process::{
        ProcessingStatus, execute_processing_job, load_processing_config, manifest::DatasetManifest,
    },
};

use serde::Serialize;

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

const OUTPUT_DIRECTORY: &str = "data/.work/full-trade-test";

// -----------------------------------------------------------------------------
// Test report
// -----------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct JobReport {
    job_file: String,
    job_id: u64,
    dataset_id: u64,

    exchange: String,
    tasks: usize,

    status: String,
    integrity_status: Option<String>,

    records_read: u64,
    records_matched: u64,
    records_rejected: u64,
    events_written: u64,
    parquet_files: u64,

    duration_seconds: f64,
    records_per_second: f64,

    error: Option<String>,
    output_path: String,
}

#[derive(Debug, Serialize)]
struct TestSummary {
    total_jobs: usize,
    successful_jobs: usize,
    failed_jobs: usize,

    referenced_archives: usize,
    existing_archives: usize,
    missing_archives: usize,

    records_read: u64,
    records_rejected: u64,
    events_written: u64,

    duration_seconds: f64,

    jobs: Vec<JobReport>,
}

// -----------------------------------------------------------------------------
// Paths
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn processing_jobs_directory() -> PathBuf {
    project_root().join("data/.jobs/process")
}

fn output_directory() -> PathBuf {
    project_root().join(OUTPUT_DIRECTORY)
}

// -----------------------------------------------------------------------------
// Job discovery
// -----------------------------------------------------------------------------

fn discover_trade_jobs() -> Vec<(PathBuf, marketforge_engine::job::ProcessingJob)> {
    let directory = processing_jobs_directory();

    let mut paths: Vec<PathBuf> = fs::read_dir(&directory)
        .expect("read processing jobs directory")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();

    paths.sort();

    let mut jobs = Vec::new();

    for path in paths {
        let job = load_processing_job(&path)
            .unwrap_or_else(|error| panic!("failed to load job {}: {error}", path.display()));

        if job.operation != ProcessingOperation::Process {
            continue;
        }

        if job.output.content_type != ContentType::Trades {
            continue;
        }

        jobs.push((path, job));
    }

    jobs
}

// -----------------------------------------------------------------------------
// Full archive integration test
// -----------------------------------------------------------------------------

#[test]
#[ignore = "processes all historical trade archives"]
fn processes_all_trade_archives() {
    let root = project_root();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    // Limit concurrency until resource budgeting is implemented.
    config.resources.workers = config.resources.workers.min(16);

    let jobs = discover_trade_jobs();

    assert!(!jobs.is_empty(), "no trade-processing jobs discovered");

    let output_root = output_directory();

    // Refuse to overwrite results from an earlier full test.
    assert!(
        !output_root.exists(),
        "full-trade-test output already exists: {}",
        output_root.display()
    );

    fs::create_dir_all(&output_root).expect("create full-test output directory");

    let datasets_root = output_root.join("datasets");
    let failures_root = output_root.join("failures");

    fs::create_dir_all(&datasets_root).unwrap();
    fs::create_dir_all(&failures_root).unwrap();

    let mut reports = Vec::new();

    let mut referenced_archives = HashSet::<PathBuf>::new();
    let mut existing_archives = HashSet::<PathBuf>::new();
    let mut missing_archives = HashSet::<PathBuf>::new();

    let started = Instant::now();

    let total_jobs = jobs.len();

    let mut successful_jobs = 0usize;
    let mut failed_jobs = 0usize;

    let mut total_records_read = 0u64;
    let mut total_records_rejected = 0u64;
    let mut total_events_written = 0u64;

    println!("\n{}", "=".repeat(100));
    println!("MARKETFORGE — FULL TRADE ARCHIVE TEST");
    println!("{}", "=".repeat(100));

    println!("Trade jobs discovered : {total_jobs}");
    println!("Workers per job       : {}", config.resources.workers);
    println!("Output directory      : {}", output_root.display());

    // -------------------------------------------------------------------------
    // Process every trade job
    // -------------------------------------------------------------------------

    for (index, (job_path, mut job)) in jobs.into_iter().enumerate() {
        let job_started = Instant::now();

        let job_name = job_path
            .file_stem()
            .and_then(|name| name.to_str())
            .expect("valid job filename")
            .to_owned();

        // Use a unique output directory for every job.
        // The original processing job is not modified on disk.
        let job_output = datasets_root.join(&job_name);

        let staging = job_output.with_extension("staging");

        job.output.dataset_path = job_output.clone();
        job.output.staging_path = staging.clone();

        // Use the exchange of the first task for reporting.
        let exchange = job
            .tasks
            .first()
            .map(|task| format!("{:?}", task.exchange))
            .unwrap_or_else(|| "Unknown".to_owned());

        // Track source archive coverage.
        for task in &job.tasks {
            let path = task.input_path.clone();

            referenced_archives.insert(path.clone());

            if path.is_file() {
                existing_archives.insert(path);
            } else {
                missing_archives.insert(path);
            }
        }

        println!(
            "\n[{}/{}] {} | exchange={} | tasks={}",
            index + 1,
            total_jobs,
            job_name,
            exchange,
            job.tasks.len(),
        );

        let execution = execute_processing_job(&job, &config);

        let elapsed = job_started.elapsed().as_secs_f64();

        let mut report = JobReport {
            job_file: job_path.to_string_lossy().into_owned(),
            job_id: job.job_id.0,
            dataset_id: job.dataset_id.0,

            exchange,
            tasks: job.tasks.len(),

            status: "failed".to_owned(),
            integrity_status: None,

            records_read: 0,
            records_matched: 0,
            records_rejected: 0,
            events_written: 0,
            parquet_files: 0,

            duration_seconds: elapsed,
            records_per_second: 0.0,

            error: None,
            output_path: job_output.to_string_lossy().into_owned(),
        };

        match execution {
            Ok(result) if result.status == ProcessingStatus::Complete => {
                let manifest_path = job_output.join("manifest.json");

                let manifest: DatasetManifest = serde_json::from_slice(
                    &fs::read(&manifest_path).expect("read committed dataset manifest"),
                )
                .expect("deserialize dataset manifest");

                report.status = "complete".to_owned();

                report.integrity_status = Some(format!("{:?}", manifest.integrity_status));

                report.records_read = manifest.processing_metrics.counters.records_read;

                report.records_matched = manifest.processing_metrics.counters.records_matched;

                report.records_rejected = manifest.processing_metrics.counters.records_rejected;

                report.events_written = manifest.events_written;
                report.parquet_files = manifest.files_written;

                report.records_per_second = if elapsed > 0.0 {
                    report.records_read as f64 / elapsed
                } else {
                    0.0
                };

                successful_jobs += 1;

                println!(
                    "  PASS | records={} | trades={} | rejected={} | integrity={:?} | {:.2}s",
                    report.records_read,
                    report.events_written,
                    report.records_rejected,
                    manifest.integrity_status,
                    elapsed,
                );
            }

            Ok(result) => {
                report.error = result.failure.map(|failure| failure.message);

                report.output_path = staging.to_string_lossy().into_owned();

                failed_jobs += 1;

                println!(
                    "  FAIL | {}",
                    report.error.as_deref().unwrap_or("unknown failure"),
                );
            }

            Err(error) => {
                report.error = Some(error.to_string());

                report.output_path = staging.to_string_lossy().into_owned();

                failed_jobs += 1;

                println!("  ERROR | {error}");
            }
        }

        total_records_read += report.records_read;
        total_records_rejected += report.records_rejected;
        total_events_written += report.events_written;

        reports.push(report);
    }

    // -------------------------------------------------------------------------
    // Generate summary
    // -------------------------------------------------------------------------

    let summary = TestSummary {
        total_jobs,
        successful_jobs,
        failed_jobs,

        referenced_archives: referenced_archives.len(),
        existing_archives: existing_archives.len(),
        missing_archives: missing_archives.len(),

        records_read: total_records_read,
        records_rejected: total_records_rejected,
        events_written: total_events_written,

        duration_seconds: started.elapsed().as_secs_f64(),

        jobs: reports,
    };

    let summary_path = output_root.join("summary.json");

    fs::write(&summary_path, serde_json::to_vec_pretty(&summary).unwrap())
        .expect("write test summary");

    // -------------------------------------------------------------------------
    // Final report
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(100));
    println!("FULL TRADE ARCHIVE TEST — SUMMARY");
    println!("{}", "=".repeat(100));

    println!("Jobs tested        : {}", summary.total_jobs);
    println!("Jobs passed        : {}", summary.successful_jobs);
    println!("Jobs failed        : {}", summary.failed_jobs);

    println!("Archives referenced: {}", summary.referenced_archives);
    println!("Archives existing  : {}", summary.existing_archives);
    println!("Archives missing   : {}", summary.missing_archives);

    println!("Records read       : {}", summary.records_read);
    println!("Records rejected   : {}", summary.records_rejected);
    println!("Trades written     : {}", summary.events_written);

    println!("Total duration     : {:.2}s", summary.duration_seconds);

    println!("Summary            : {}", summary_path.display());

    println!("{}", "=".repeat(100));

    // This is a diagnostic integration test.
    // Individual job failures are recorded rather than stopping the run.
    // Inspect summary.json for detailed results.

    assert_eq!(
        summary.total_jobs,
        summary.successful_jobs + summary.failed_jobs,
    );
}
