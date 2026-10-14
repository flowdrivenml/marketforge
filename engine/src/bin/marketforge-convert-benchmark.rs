use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::Instant,
};

use marketforge_engine::{job::load_convert_job, process::merge::PhysicalMergeManifest};

use serde::Serialize;

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkPhase {
    Materialize,
    Publish,
    Full,
}

impl BenchmarkPhase {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "materialize" => Ok(Self::Materialize),
            "publish" => Ok(Self::Publish),
            "full" => Ok(Self::Full),
            _ => Err(format!("invalid benchmark phase: {value}")),
        }
    }
}

struct BenchmarkConfig {
    job: PathBuf,
    workers: Vec<usize>,
    repetitions: usize,
    phase: BenchmarkPhase,
    output: PathBuf,
}

fn parse_args() -> Result<BenchmarkConfig, String> {
    let mut job = None;
    let mut workers = vec![1, 2, 4, 8, 16, 24, 32];
    let mut repetitions = 3;
    let mut phase = BenchmarkPhase::Full;
    let mut output = PathBuf::from("benchmarks/results/conversion");

    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;

        match argument.as_str() {
            "--job" => job = Some(PathBuf::from(value)),

            "--workers" => {
                workers = value
                    .split(',')
                    .map(|value| {
                        value
                            .parse::<usize>()
                            .map_err(|_| format!("invalid worker count: {value}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if workers.is_empty() || workers.contains(&0) {
                    return Err("worker counts must be positive".into());
                }

                if workers.iter().copied().collect::<HashSet<_>>().len() != workers.len() {
                    return Err("duplicate worker counts".into());
                }
            }

            "--repetitions" => {
                repetitions = value.parse().map_err(|_| "invalid repetition count")?;

                if repetitions == 0 {
                    return Err("repetitions must be positive".into());
                }
            }

            "--phase" => phase = BenchmarkPhase::parse(&value)?,

            "--output" => output = PathBuf::from(value),

            _ => return Err(format!("unknown argument: {argument}")),
        }
    }

    Ok(BenchmarkConfig {
        job: job.ok_or("missing --job")?,
        workers,
        repetitions,
        phase,
        output,
    })
}

// -----------------------------------------------------------------------------
// Results
// -----------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct BenchmarkRun {
    workers_requested: usize,
    workers_used: usize,
    repetition: usize,

    materialization_seconds: f64,
    publication_seconds: f64,
    total_seconds: f64,

    events_written: u64,
    depth_levels: u64,

    identity_events: u64,
    converted_events: u64,
    unavailable_events: u64,
    skipped_events: u64,

    depth_levels_converted: u64,
    depth_levels_unavailable: u64,
    depth_levels_skipped: u64,

    throughput_events_per_second: f64,
    throughput_depth_levels_per_second: f64,

    parquet_files: u64,
    output_bytes: u64,

    success: bool,
    error: Option<String>,
    prepass_seconds: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    source_job: String,
    phase: String,
    workers: Vec<usize>,
    repetitions: usize,
    results: Vec<BenchmarkRun>,
}

#[derive(Debug, serde::Deserialize)]
struct BenchmarkManifest {
    job_id: u64,
    statistics: BenchmarkStatistics,
    output: BenchmarkOutput,
}

#[derive(Debug, serde::Deserialize)]
struct BenchmarkStatistics {
    events_written: u64,
    identity: u64,
    converted: u64,
    unavailable: u64,

    #[serde(default)]
    skipped: u64,

    depth: Option<BenchmarkDepthStatistics>,
}

#[derive(Debug, serde::Deserialize)]
struct BenchmarkDepthStatistics {
    levels_read: u64,
    levels_converted: u64,
    levels_unavailable: u64,

    #[serde(default)]
    levels_skipped: u64,
}

#[derive(Debug, serde::Deserialize)]
struct BenchmarkOutput {
    files_written: u64,
    row_groups: u64,
}

#[derive(Debug, serde::Deserialize)]
struct BenchmarkReceipt {
    job_id: u64,

    events_written: u64,

    identity_events: u64,
    converted_events: u64,
    unavailable_events: u64,

    #[serde(default)]
    skipped_events: u64,

    files_written: u64,
    row_groups_written: u64,

    depth: Option<BenchmarkDepthStatistics>,
}

// -----------------------------------------------------------------------------
// Subprocess execution
// -----------------------------------------------------------------------------

fn execute(
    binary: &Path,
    root: &Path,
    job: &Path,
    workers: usize,
    mode: &str,
) -> Result<(Output, f64), String> {
    let started = Instant::now();

    let output = Command::new(binary)
        .arg("--project-root")
        .arg(root)
        .arg("--job")
        .arg(job)
        .arg(mode)
        .arg("--workers")
        .arg(workers.to_string())
        .output()
        .map_err(|error| error.to_string())?;

    Ok((output, started.elapsed().as_secs_f64()))
}

// -----------------------------------------------------------------------------
// Benchmark execution
// -----------------------------------------------------------------------------
fn read_benchmark_manifest(directory: &Path) -> Result<BenchmarkManifest, String> {
    let path = directory.join("manifest.json");

    serde_json::from_slice(
        &fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?,
    )
    .map_err(|error| format!("{}: {error}", path.display()))
}

fn run_benchmark(config: BenchmarkConfig) -> Result<(), String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("project root not found")?
        .canonicalize()
        .map_err(|error| error.to_string())?;

    let job_path = if config.job.is_absolute() {
        config.job.clone()
    } else {
        root.join(&config.job)
    };

    let original = load_convert_job(&job_path).map_err(|error| error.to_string())?;

    let input_merge = if original.input_merge.is_absolute() {
        original.input_merge.clone()
    } else {
        root.join(&original.input_merge)
    };

    let source_manifest_path = input_merge.join("manifest.json");

    let source_manifest: PhysicalMergeManifest = serde_json::from_slice(
        &fs::read(&source_manifest_path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    source_manifest
        .validate()
        .map_err(|error| error.to_string())?;

    let output_root = if config.output.is_absolute() {
        config.output.clone()
    } else {
        root.join(&config.output)
    };

    if output_root.exists() {
        return Err(format!(
            "benchmark output already exists: {}",
            output_root.display()
        ));
    }

    fs::create_dir_all(&output_root).map_err(|error| error.to_string())?;

    let scratch = output_root.join("scratch");

    fs::create_dir_all(&scratch).map_err(|error| error.to_string())?;

    let binary = root.join("engine/target/release/marketforge-convert");

    if !binary.is_file() {
        return Err(format!(
            "production conversion binary not found: {}",
            binary.display()
        ));
    }

    let mut results = Vec::new();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — CONVERSION BENCHMARK");
    println!("{}", "=".repeat(90));

    println!("Source      : {}", input_merge.display());
    println!("Events      : {}", source_manifest.events_written);
    println!("Workers     : {:?}", config.workers);
    println!("Repetitions : {}", config.repetitions);

    for repetition in 1..=config.repetitions {
        let mut worker_counts = config.workers.clone();

        if repetition % 2 == 0 {
            worker_counts.reverse();
        }

        for workers in worker_counts {
            let run_dir = scratch.join(format!("rep-{repetition}/workers-{workers}"));

            fs::create_dir_all(&run_dir).map_err(|error| error.to_string())?;

            let dataset_path = run_dir.join("dataset");
            let benchmark_job_path = run_dir.join("job.json");

            let mut job = original.clone();

            job.job_id = 9_400_000 + (repetition as u64) * 100 + workers as u64;

            job.input_merge = input_merge.clone();
            job.output.dataset_path = dataset_path.clone();

            job.resources.workers = workers as _;

            fs::write(
                &benchmark_job_path,
                serde_json::to_vec_pretty(&job).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;

            let mut run = BenchmarkRun {
                workers_requested: workers,
                workers_used: 0,
                repetition,

                materialization_seconds: 0.0,
                publication_seconds: 0.0,
                total_seconds: 0.0,

                events_written: 0,
                depth_levels: 0,

                identity_events: 0,
                converted_events: 0,
                unavailable_events: 0,
                skipped_events: 0,

                depth_levels_converted: 0,
                depth_levels_unavailable: 0,
                depth_levels_skipped: 0,

                throughput_events_per_second: 0.0,
                throughput_depth_levels_per_second: 0.0,

                parquet_files: 0,
                output_bytes: 0,

                success: false,
                error: None,
                prepass_seconds: 0.0,
            };

            let mut failure = None;

            if matches!(
                config.phase,
                BenchmarkPhase::Materialize | BenchmarkPhase::Publish | BenchmarkPhase::Full
            ) {
                let (output, elapsed) = execute(
                    &binary,
                    &root,
                    &benchmark_job_path,
                    workers,
                    "--materialize",
                )?;

                run.materialization_seconds = elapsed;
                run.prepass_seconds =
                    parse_metric::<f64>(&output, "Reference prepass").unwrap_or(0.0);

                run.workers_used = parse_metric::<usize>(&output, "Workers used").unwrap_or(0);
                if !output.status.success() {
                    failure = Some(String::from_utf8_lossy(&output.stderr).to_string());
                }
            }

            if failure.is_none()
                && matches!(config.phase, BenchmarkPhase::Publish | BenchmarkPhase::Full)
            {
                let (output, elapsed) =
                    execute(&binary, &root, &benchmark_job_path, workers, "--publish")?;

                run.publication_seconds = elapsed;

                if !output.status.success() {
                    failure = Some(String::from_utf8_lossy(&output.stderr).to_string());
                }
            }

            run.total_seconds = run.materialization_seconds + run.publication_seconds;

            let result_path = if config.phase == BenchmarkPhase::Materialize {
                dataset_path.with_extension("staging")
            } else {
                dataset_path.clone()
            };

            if failure.is_none() {
                let receipt = read_benchmark_receipt(&result_path)?;

                if receipt.job_id != job.job_id {
                    return Err("benchmark receipt job ID mismatch".into());
                }

                if receipt.events_written != source_manifest.events_written {
                    return Err("benchmark receipt event count mismatch".into());
                }

                let accounted = receipt
                    .identity_events
                    .checked_add(receipt.converted_events)
                    .and_then(|v| v.checked_add(receipt.unavailable_events))
                    .and_then(|v| v.checked_add(receipt.skipped_events))
                    .ok_or("benchmark status counter overflow")?;

                if accounted != receipt.events_written {
                    return Err("benchmark conversion status mismatch".into());
                }

                run.events_written = receipt.events_written;
                run.identity_events = receipt.identity_events;
                run.converted_events = receipt.converted_events;
                run.unavailable_events = receipt.unavailable_events;
                run.skipped_events = receipt.skipped_events;

                if let Some(depth) = &receipt.depth {
                    run.depth_levels = depth.levels_read;
                    run.depth_levels_converted = depth.levels_converted;
                    run.depth_levels_unavailable = depth.levels_unavailable;
                    run.depth_levels_skipped = depth.levels_skipped;

                    let accounted_levels = depth
                        .levels_converted
                        .checked_add(depth.levels_unavailable)
                        .and_then(|v| v.checked_add(depth.levels_skipped))
                        .ok_or("benchmark depth counter overflow")?;

                    if accounted_levels != depth.levels_read {
                        return Err("benchmark depth level accounting mismatch".into());
                    }
                }

                let parquet_files = collect_parquet_files(&result_path)?;

                run.parquet_files = parquet_files.len() as u64;

                run.output_bytes = parquet_files
                    .iter()
                    .map(|path| {
                        fs::metadata(path)
                            .map(|metadata| metadata.len())
                            .unwrap_or(0)
                    })
                    .sum();

                if run.parquet_files != receipt.files_written {
                    return Err("benchmark Parquet file count mismatch".into());
                }
                if receipt.row_groups_written == 0 {
                    return Err("benchmark receipt contains no row groups".into());
                }

                if config.phase != BenchmarkPhase::Materialize {
                    let manifest = read_benchmark_manifest(&result_path)?;

                    if manifest.job_id != job.job_id
                        || manifest.statistics.events_written != run.events_written
                        || manifest.output.files_written != run.parquet_files
                        || manifest.output.row_groups != receipt.row_groups_written
                    {
                        return Err("benchmark manifest mismatch".into());
                    }

                    if manifest.statistics.identity != run.identity_events
                        || manifest.statistics.converted != run.converted_events
                        || manifest.statistics.unavailable != run.unavailable_events
                        || manifest.statistics.skipped != run.skipped_events
                    {
                        return Err("benchmark manifest status mismatch".into());
                    }

                    if let Some(depth) = &manifest.statistics.depth {
                        if depth.levels_read != run.depth_levels
                            || depth.levels_converted != run.depth_levels_converted
                            || depth.levels_unavailable != run.depth_levels_unavailable
                            || depth.levels_skipped != run.depth_levels_skipped
                        {
                            return Err("benchmark manifest depth mismatch".into());
                        }
                    }
                }

                let measured_seconds = match config.phase {
                    BenchmarkPhase::Materialize => run.materialization_seconds,
                    BenchmarkPhase::Publish => run.publication_seconds,
                    BenchmarkPhase::Full => run.total_seconds,
                };

                run.throughput_events_per_second =
                    run.events_written as f64 / measured_seconds.max(f64::EPSILON);

                run.throughput_depth_levels_per_second =
                    run.depth_levels as f64 / measured_seconds.max(f64::EPSILON);

                run.success = true;

                fs::remove_dir_all(&run_dir).map_err(|error| error.to_string())?;
            } else {
                run.error = failure;
            }

            println!(
                "rep={} workers={:>2} events={:>10} elapsed={:.3}s throughput={:.0}/s {}",
                repetition,
                workers,
                run.events_written,
                run.total_seconds,
                run.throughput_events_per_second,
                if run.success { "PASS" } else { "FAIL" },
            );

            if let Some(error) = &run.error {
                eprintln!("ERROR: {error}");
            }

            results.push(run);

            if !results.last().is_some_and(|run| run.success) {
                return Err("benchmark failed; temporary output preserved".into());
            }
        }
    }

    let report = BenchmarkReport {
        source_job: job_path.display().to_string(),
        phase: match config.phase {
            BenchmarkPhase::Materialize => "materialize",
            BenchmarkPhase::Publish => "publish",
            BenchmarkPhase::Full => "full",
        }
        .to_owned(),
        workers: config.workers,
        repetitions: config.repetitions,
        results,
    };

    fs::write(
        output_root.join("benchmark.json"),
        serde_json::to_vec_pretty(&report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — CONVERSION BENCHMARK COMPLETE");
    println!("{}", "=".repeat(90));

    println!("Report: {}", output_root.join("benchmark.json").display());

    Ok(())
}

fn read_benchmark_receipt(directory: &Path) -> Result<BenchmarkReceipt, String> {
    let path = directory.join("execution.json");

    serde_json::from_slice(
        &fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?,
    )
    .map_err(|error| format!("{}: {error}", path.display()))
}

// -----------------------------------------------------------------------------
// Filesystem helpers
// -----------------------------------------------------------------------------

fn collect_parquet_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();

    if !root.exists() {
        return Err(format!(
            "benchmark dataset does not exist: {}",
            root.display()
        ));
    }

    let mut directories = vec![root.to_path_buf()];

    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let path = entry.path();

            if path.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }

    Ok(files)
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

fn parse_metric<T: std::str::FromStr>(output: &std::process::Output, label: &str) -> Option<T> {
    let stdout = String::from_utf8_lossy(&output.stdout);

    stdout.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;

        if key.trim() == label {
            value.trim().split_whitespace().next()?.parse().ok()
        } else {
            None
        }
    })
}
