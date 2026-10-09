#![cfg(feature = "process")]

use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use marketforge_engine::{
    job::{ContentType, ProcessingJob, ProcessingOperation, load_processing_job},
    process::{
        ProcessingStatus, execute_processing_job, load_processing_config,
        manifest::DatasetManifest, session::execute_processing_session,
    },
};

use serde::Serialize;

// -----------------------------------------------------------------------------
// Scheduler mode
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchedulerMode {
    PerJob,
    Global,
}

impl SchedulerMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "per-job" => Ok(Self::PerJob),
            "global" => Ok(Self::Global),
            _ => Err(format!(
                "invalid scheduler mode: {value}; expected per-job or global"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::PerJob => "per-job",
            Self::Global => "global",
        }
    }
}

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

#[derive(Debug)]
struct BenchmarkConfig {
    dataset: String,
    scheduler: SchedulerMode,
    workers: Vec<usize>,
    repetitions: usize,
    output: PathBuf,
}

fn parse_args() -> Result<BenchmarkConfig, String> {
    let mut dataset = None;
    let mut workers = None;
    let mut repetitions = None;
    let mut output = None;
    let mut scheduler = SchedulerMode::PerJob;

    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;

        match argument.as_str() {
            "--dataset" => dataset = Some(value),

            "--scheduler" => {
                scheduler = SchedulerMode::parse(&value)?;
            }

            "--workers" => {
                let parsed = value
                    .split(',')
                    .map(|worker| {
                        worker
                            .parse::<usize>()
                            .map_err(|_| format!("invalid worker count: {worker}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if parsed.is_empty() || parsed.contains(&0) {
                    return Err("worker counts must be positive".to_owned());
                }

                workers = Some(parsed);
            }

            "--repetitions" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| "invalid repetition count".to_owned())?;

                if parsed == 0 {
                    return Err("repetitions must be positive".to_owned());
                }

                repetitions = Some(parsed);
            }

            "--output" => output = Some(PathBuf::from(value)),

            _ => return Err(format!("unknown argument: {argument}")),
        }
    }

    let dataset = dataset.unwrap_or_else(|| "trades".to_owned());

    if dataset != "trades" {
        return Err("only trades are currently supported".to_owned());
    }

    Ok(BenchmarkConfig {
        dataset,
        scheduler,
        workers: workers.unwrap_or_else(|| vec![1, 4, 8, 16]),
        repetitions: repetitions.unwrap_or(3),
        output: output.unwrap_or_else(|| PathBuf::from("benchmarks/results/trade-processing")),
    })
}

// -----------------------------------------------------------------------------
// Benchmark reports
// -----------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct JobBenchmark {
    job_file: String,
    exchange: String,

    workers: usize,
    repetition: usize,

    tasks: usize,

    records_read: u64,
    records_rejected: u64,
    events_written: u64,
    parquet_files: u64,

    // None for globally scheduled jobs because the current
    // session API does not expose individual job durations.
    elapsed_seconds: Option<f64>,
    throughput_records_per_second: Option<f64>,

    integrity_status: Option<String>,

    success: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionBenchmark {
    scheduler: String,
    workers: usize,
    repetition: usize,

    jobs: usize,
    tasks: usize,

    successful_jobs: usize,
    failed_jobs: usize,

    elapsed_seconds: f64,

    records_read: u64,
    events_written: u64,

    throughput_records_per_second: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    dataset: String,
    scheduler: String,

    workers: Vec<usize>,
    repetitions: usize,

    total_jobs: usize,
    total_executions: usize,

    results: Vec<JobBenchmark>,
    sessions: Vec<SessionBenchmark>,
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

fn discover_jobs(root: &Path) -> Result<Vec<(PathBuf, ProcessingJob)>, String> {
    let directory = root.join("data/.jobs/process");

    let mut paths = fs::read_dir(&directory)
        .map_err(|error| error.to_string())?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;

    paths.sort();

    let mut jobs = Vec::new();

    for path in paths {
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let job =
            load_processing_job(&path).map_err(|error| format!("{}: {error}", path.display()))?;

        if job.operation != ProcessingOperation::Process {
            continue;
        }

        if job.output.content_type != ContentType::Trades {
            continue;
        }

        // Exclude Gate.io monthly archives from parallel scaling benchmarks.
        if job
            .tasks
            .first()
            .is_some_and(|task| format!("{:?}", task.exchange) == "GateIo")
        {
            continue;
        }

        jobs.push((path, job));
    }

    Ok(jobs)
}

// -----------------------------------------------------------------------------
// Job helpers
// -----------------------------------------------------------------------------

fn job_name(path: &Path) -> Result<String, String> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| format!("invalid job filename: {}", path.display()))
}

fn job_exchange(job: &ProcessingJob) -> String {
    job.tasks
        .first()
        .map(|task| format!("{:?}", task.exchange))
        .unwrap_or_else(|| "Unknown".to_owned())
}

fn prepare_benchmark_job(
    original: &ProcessingJob,
    job_path: &Path,
    scratch: &Path,
    repetition: usize,
    workers: usize,
) -> Result<ProcessingJob, String> {
    let mut job = original.clone();

    let name = job_name(job_path)?;

    let run_directory = scratch.join(format!("rep-{repetition}/workers-{workers}/{name}"));

    fs::create_dir_all(&run_directory).map_err(|error| error.to_string())?;

    job.output.staging_path = run_directory.join("staging");
    job.output.dataset_path = run_directory.join("dataset");

    Ok(job)
}

fn empty_job_result(
    job_path: &Path,
    job: &ProcessingJob,
    workers: usize,
    repetition: usize,
) -> Result<JobBenchmark, String> {
    Ok(JobBenchmark {
        job_file: job_name(job_path)?,
        exchange: job_exchange(job),

        workers,
        repetition,

        tasks: job.tasks.len(),

        records_read: 0,
        records_rejected: 0,
        events_written: 0,
        parquet_files: 0,

        elapsed_seconds: None,
        throughput_records_per_second: None,

        integrity_status: None,

        success: false,
        error: None,
    })
}

// -----------------------------------------------------------------------------
// Manifest verification
// -----------------------------------------------------------------------------

fn read_verified_manifest(job: &ProcessingJob) -> Result<DatasetManifest, String> {
    let manifest_path = job.output.dataset_path.join("manifest.json");

    let contents = fs::read(&manifest_path)
        .map_err(|error| format!("{}: {error}", manifest_path.display()))?;

    let manifest: DatasetManifest =
        serde_json::from_slice(&contents).map_err(|error| format!("invalid manifest: {error}"))?;

    if manifest.job_id != job.job_id.0 {
        return Err(format!(
            "manifest job ID mismatch: expected={}, actual={}",
            job.job_id.0, manifest.job_id
        ));
    }

    if manifest.dataset_id != job.dataset_id.0 {
        return Err(format!(
            "manifest dataset ID mismatch: expected={}, actual={}",
            job.dataset_id.0, manifest.dataset_id
        ));
    }

    if manifest.source_tasks.len() != job.tasks.len() {
        return Err(format!(
            "manifest task count mismatch: expected={}, actual={}",
            job.tasks.len(),
            manifest.source_tasks.len()
        ));
    }

    if manifest.files_written != manifest.files.len() as u64 {
        return Err("manifest Parquet file count mismatch".to_owned());
    }

    if manifest.events_written != manifest.processing_metrics.counters.events_written {
        return Err("manifest event count mismatch".to_owned());
    }

    let mut total_rows = 0u64;

    for file in &manifest.files {
        let relative = Path::new(&file.path);

        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(format!(
                "invalid manifest-relative Parquet path: {}",
                file.path
            ));
        }

        let path = job.output.dataset_path.join(relative);

        let metadata =
            fs::metadata(&path).map_err(|error| format!("{}: {error}", path.display()))?;

        if !metadata.is_file() {
            return Err(format!(
                "manifest references non-file output: {}",
                path.display()
            ));
        }

        if metadata.len() != file.size_bytes {
            return Err(format!("Parquet size mismatch for {}", path.display()));
        }

        total_rows = total_rows
            .checked_add(file.rows)
            .ok_or_else(|| "Parquet row count overflow".to_owned())?;
    }

    if total_rows != manifest.events_written {
        return Err(format!(
            "Parquet row count mismatch: expected={}, actual={}",
            manifest.events_written, total_rows
        ));
    }

    Ok(manifest)
}

fn populate_success(result: &mut JobBenchmark, manifest: &DatasetManifest) {
    result.records_read = manifest.processing_metrics.counters.records_read;

    result.records_rejected = manifest.processing_metrics.counters.records_rejected;

    result.events_written = manifest.events_written;
    result.parquet_files = manifest.files_written;

    result.integrity_status = Some(format!("{:?}", manifest.integrity_status));

    result.success = true;
}

// -----------------------------------------------------------------------------
// Cleanup
// -----------------------------------------------------------------------------

fn cleanup_successful_job(job: &ProcessingJob) -> Result<(), String> {
    let run_directory = job
        .output
        .dataset_path
        .parent()
        .ok_or_else(|| "benchmark dataset has no parent directory".to_owned())?;

    fs::remove_dir_all(run_directory).map_err(|error| {
        format!(
            "failed to clean successful benchmark {}: {error}",
            run_directory.display()
        )
    })
}

// -----------------------------------------------------------------------------
// Per-job benchmark
// -----------------------------------------------------------------------------

fn benchmark_per_job(
    jobs: &[(PathBuf, ProcessingJob)],
    runtime_config: &marketforge_engine::process::ProcessingConfig,
    scratch: &Path,
    repetition: usize,
    workers: usize,
) -> Result<(Vec<JobBenchmark>, SessionBenchmark), String> {
    let mut results = Vec::with_capacity(jobs.len());

    let total_tasks: usize = jobs.iter().map(|(_, job)| job.tasks.len()).sum();

    let started = Instant::now();

    for (index, (job_path, original_job)) in jobs.iter().enumerate() {
        let job = prepare_benchmark_job(original_job, job_path, scratch, repetition, workers)?;

        let mut result = empty_job_result(job_path, &job, workers, repetition)?;

        let job_started = Instant::now();

        let execution = execute_processing_job(&job, runtime_config);

        let elapsed = job_started.elapsed().as_secs_f64();

        result.elapsed_seconds = Some(elapsed);

        match execution {
            Ok(execution) if execution.status == ProcessingStatus::Complete => {
                match read_verified_manifest(&job) {
                    Ok(manifest) => {
                        populate_success(&mut result, &manifest);

                        result.throughput_records_per_second =
                            Some(result.records_read as f64 / elapsed.max(f64::EPSILON));
                    }

                    Err(error) => {
                        result.error = Some(error);
                    }
                }
            }

            Ok(execution) => {
                result.error = execution
                    .failure
                    .map(|failure| failure.message)
                    .or_else(|| Some("processing returned failed status".to_owned()));
            }

            Err(error) => {
                result.error = Some(error.to_string());
            }
        }

        println!(
            "[{}/{}] {:<55} {:>8.3}s {}",
            index + 1,
            jobs.len(),
            result.job_file,
            elapsed,
            if result.success { "PASS" } else { "FAIL" },
        );

        if result.success {
            cleanup_successful_job(&job)?;
        }

        results.push(result);
    }

    let elapsed = started.elapsed().as_secs_f64();

    let records_read: u64 = results.iter().map(|result| result.records_read).sum();

    let events_written: u64 = results.iter().map(|result| result.events_written).sum();

    let successful_jobs = results.iter().filter(|result| result.success).count();

    let failed_jobs = results.len() - successful_jobs;

    let session = SessionBenchmark {
        scheduler: SchedulerMode::PerJob.as_str().to_owned(),
        workers,
        repetition,

        jobs: jobs.len(),
        tasks: total_tasks,

        successful_jobs,
        failed_jobs,

        elapsed_seconds: elapsed,

        records_read,
        events_written,

        throughput_records_per_second: records_read as f64 / elapsed.max(f64::EPSILON),
    };

    Ok((results, session))
}

// -----------------------------------------------------------------------------
// Global benchmark
// -----------------------------------------------------------------------------

fn benchmark_global(
    jobs: &[(PathBuf, ProcessingJob)],
    runtime_config: &marketforge_engine::process::ProcessingConfig,
    scratch: &Path,
    repetition: usize,
    workers: usize,
) -> Result<(Vec<JobBenchmark>, SessionBenchmark), String> {
    // -------------------------------------------------------------------------
    // Prepare all jobs before starting the timer
    // -------------------------------------------------------------------------

    let mut session_jobs = Vec::with_capacity(jobs.len());

    for (job_path, original_job) in jobs {
        session_jobs.push(prepare_benchmark_job(
            original_job,
            job_path,
            scratch,
            repetition,
            workers,
        )?);
    }

    // -------------------------------------------------------------------------
    // Execute all jobs through one global worker pool
    // -------------------------------------------------------------------------

    let started = Instant::now();

    let session = execute_processing_session(&session_jobs, runtime_config)
        .map_err(|error| error.to_string())?;

    let elapsed = started.elapsed().as_secs_f64();

    if session.jobs.len() != session_jobs.len() {
        return Err(format!(
            "global session result count mismatch: expected={}, actual={}",
            session_jobs.len(),
            session.jobs.len(),
        ));
    }

    // -------------------------------------------------------------------------
    // Verify each committed dataset
    // -------------------------------------------------------------------------

    let mut results = Vec::with_capacity(jobs.len());

    for (((job_path, _), job), execution) in jobs.iter().zip(&session_jobs).zip(session.jobs.iter())
    {
        let mut result = empty_job_result(job_path, job, workers, repetition)?;

        if execution.status == ProcessingStatus::Complete {
            match read_verified_manifest(job) {
                Ok(manifest) => {
                    populate_success(&mut result, &manifest);
                }

                Err(error) => {
                    result.error = Some(error);
                }
            }
        } else {
            result.error = execution
                .failure
                .as_ref()
                .map(|failure| failure.message.clone())
                .or_else(|| Some("processing returned failed status".to_owned()));
        }

        println!(
            "[GLOBAL] {:<55} {}",
            result.job_file,
            if result.success { "PASS" } else { "FAIL" },
        );

        results.push(result);
    }

    // -------------------------------------------------------------------------
    // Aggregate session statistics
    // -------------------------------------------------------------------------

    let records_read: u64 = results.iter().map(|result| result.records_read).sum();

    let events_written: u64 = results.iter().map(|result| result.events_written).sum();

    let successful_jobs = results.iter().filter(|result| result.success).count();

    let failed_jobs = results.len() - successful_jobs;

    let benchmark = SessionBenchmark {
        scheduler: SchedulerMode::Global.as_str().to_owned(),
        workers,
        repetition,

        jobs: session_jobs.len(),
        tasks: session.total_tasks,

        successful_jobs,
        failed_jobs,

        elapsed_seconds: elapsed,

        records_read,
        events_written,

        throughput_records_per_second: records_read as f64 / elapsed.max(f64::EPSILON),
    };

    println!(
        "\nGlobal session: workers={workers}, repetition={repetition}, \
         elapsed={elapsed:.3}s, throughput={:.0} records/s",
        benchmark.throughput_records_per_second,
    );

    // -------------------------------------------------------------------------
    // Remove verified successful datasets
    // -------------------------------------------------------------------------

    for (job, result) in session_jobs.iter().zip(&results) {
        if result.success {
            cleanup_successful_job(job)?;
        }
    }

    Ok((results, benchmark))
}

// -----------------------------------------------------------------------------
// Benchmark execution
// -----------------------------------------------------------------------------

fn run_benchmark(config: BenchmarkConfig) -> Result<(), String> {
    let root = project_root();

    let processing_config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .map_err(|error| error.to_string())?;

    let jobs = discover_jobs(&root)?;

    if jobs.is_empty() {
        return Err("no trade processing jobs discovered".to_owned());
    }

    let output = if config.output.is_absolute() {
        config.output.clone()
    } else {
        root.join(&config.output)
    };

    if output.exists() {
        return Err(format!(
            "benchmark output already exists: {}",
            output.display()
        ));
    }
    fs::create_dir_all(&output).map_err(|error| error.to_string())?;

    let scratch = output.join("scratch");

    fs::create_dir_all(&scratch).map_err(|error| error.to_string())?;

    let mut results = Vec::<JobBenchmark>::new();
    let mut sessions = Vec::<SessionBenchmark>::new();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — PROCESSING BENCHMARK");
    println!("{}", "=".repeat(90));

    println!("Dataset     : {}", config.dataset);
    println!("Scheduler   : {}", config.scheduler.as_str());
    println!("Jobs        : {}", jobs.len());
    println!("Workers     : {:?}", config.workers);
    println!("Repetitions : {}", config.repetitions);
    println!("Output      : {}", output.display());

    let started = Instant::now();

    // -------------------------------------------------------------------------
    // Execute benchmark configurations
    // -------------------------------------------------------------------------

    for repetition in 1..=config.repetitions {
        // Alternate worker order to reduce systematic cache bias.
        let mut worker_counts = config.workers.clone();

        if repetition % 2 == 0 {
            worker_counts.reverse();
        }

        for workers in worker_counts {
            let mut runtime_config = processing_config.clone();

            runtime_config.resources.workers = workers as _;

            println!("\n{}", "-".repeat(90));

            println!(
                "REPETITION {repetition}/{} — SCHEDULER {} — WORKERS {workers}",
                config.repetitions,
                config.scheduler.as_str(),
            );

            println!("{}", "-".repeat(90));

            let (run_results, session_benchmark) = match config.scheduler {
                SchedulerMode::PerJob => {
                    benchmark_per_job(&jobs, &runtime_config, &scratch, repetition, workers)?
                }

                SchedulerMode::Global => {
                    benchmark_global(&jobs, &runtime_config, &scratch, repetition, workers)?
                }
            };

            println!(
                "\nSession completed: {:.3}s | {:.0} records/s | passed={} | failed={}",
                session_benchmark.elapsed_seconds,
                session_benchmark.throughput_records_per_second,
                session_benchmark.successful_jobs,
                session_benchmark.failed_jobs,
            );

            results.extend(run_results);
            sessions.push(session_benchmark);
        }
    }

    // -------------------------------------------------------------------------
    // Validate benchmark accounting
    // -------------------------------------------------------------------------

    let expected_sessions = config.workers.len() * config.repetitions;

    let expected_executions = jobs.len() * expected_sessions;

    if sessions.len() != expected_sessions {
        return Err(format!(
            "benchmark session count mismatch: expected={}, actual={}",
            expected_sessions,
            sessions.len(),
        ));
    }

    if results.len() != expected_executions {
        return Err(format!(
            "benchmark execution count mismatch: expected={}, actual={}",
            expected_executions,
            results.len(),
        ));
    }

    // -------------------------------------------------------------------------
    // Construct benchmark report
    // -------------------------------------------------------------------------

    let report = BenchmarkReport {
        dataset: config.dataset,
        scheduler: config.scheduler.as_str().to_owned(),
        workers: config.workers,
        repetitions: config.repetitions,

        total_jobs: jobs.len(),
        total_executions: results.len(),

        results,
        sessions,
    };

    // -------------------------------------------------------------------------
    // Persist benchmark results
    // -------------------------------------------------------------------------

    let report_path = output.join("benchmark.json");

    let contents = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("failed to serialize benchmark report: {error}"))?;

    fs::write(&report_path, contents)
        .map_err(|error| format!("failed to write benchmark report: {error}"))?;

    // -------------------------------------------------------------------------
    // Display final benchmark summary
    // -------------------------------------------------------------------------

    let successful_executions = report
        .results
        .iter()
        .filter(|result| result.success)
        .count();

    let failed_executions = report.total_executions - successful_executions;

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — BENCHMARK SUMMARY");
    println!("{}", "=".repeat(90));

    println!("Scheduler          : {}", report.scheduler);
    println!("Jobs               : {}", report.total_jobs);
    println!("Sessions           : {}", report.sessions.len());
    println!("Total executions   : {}", report.total_executions);
    println!("Successful         : {successful_executions}");
    println!("Failed             : {failed_executions}");

    println!("\nWORKER SCALING");

    println!(
        "{:<10} {:>14} {:>20} {:>12}",
        "Workers", "Avg time (s)", "Records/sec", "Speedup",
    );

    let baseline = report
        .sessions
        .iter()
        .filter(|session| session.workers == 1)
        .map(|session| session.elapsed_seconds)
        .sum::<f64>()
        / report
            .sessions
            .iter()
            .filter(|session| session.workers == 1)
            .count()
            .max(1) as f64;

    for &workers in &report.workers {
        let measurements: Vec<&SessionBenchmark> = report
            .sessions
            .iter()
            .filter(|session| session.workers == workers)
            .collect();

        if measurements.is_empty() {
            continue;
        }

        let total_seconds: f64 = measurements
            .iter()
            .map(|session| session.elapsed_seconds)
            .sum();

        let total_records: u64 = measurements
            .iter()
            .map(|session| session.records_read)
            .sum();

        let average_seconds = total_seconds / measurements.len() as f64;

        let throughput = total_records as f64 / total_seconds.max(f64::EPSILON);

        let speedup = if baseline > 0.0 {
            baseline / average_seconds
        } else {
            0.0
        };

        println!(
            "{:<10} {:>14.3} {:>20.0} {:>11.2}x",
            workers, average_seconds, throughput, speedup,
        );
    }

    println!(
        "\nTotal benchmark time : {:.2}s",
        started.elapsed().as_secs_f64()
    );
    println!("Report               : {}", report_path.display());

    println!("{}", "=".repeat(90));

    // A benchmark with failed jobs must not be reported as successful.
    if failed_executions > 0 {
        return Err(format!(
            "benchmark completed with {failed_executions} failed job executions; \
             inspect {}",
            report_path.display(),
        ));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fn main() {
    if let Err(error) = parse_args().and_then(run_benchmark) {
        eprintln!("Benchmark failed: {error}");
        std::process::exit(1);
    }
}
