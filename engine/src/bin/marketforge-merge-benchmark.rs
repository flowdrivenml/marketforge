use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use marketforge_engine::{
    job::{
        ContentType, MergeJob, MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning,
        MergeResources, MergeTimeRange,
    },
    process::merge::{PhysicalMergeManifest, execute_unified_merge_job},
};

use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum BenchmarkDataset {
    Trades,
    Depth,
    Combined,
}

impl BenchmarkDataset {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "trades" => Ok(Self::Trades),
            "depth" => Ok(Self::Depth),
            "combined" => Ok(Self::Combined),
            _ => Err(format!(
                "invalid dataset: {value}; expected trades, depth, combined"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Depth => "depth",
            Self::Combined => "combined",
        }
    }
}

#[derive(Debug)]
struct BenchmarkConfig {
    dataset: BenchmarkDataset,
    workers: Vec<usize>,
    repetitions: usize,
    output: PathBuf,
    duration_hours: u64,
    partitions: Option<usize>,
}

fn parse_args() -> Result<BenchmarkConfig, String> {
    let mut dataset = BenchmarkDataset::Trades;
    let mut workers = vec![4, 8, 16, 24, 32];
    let mut repetitions = 2;
    let mut output = None;
    let mut duration_hours = 1_u64;
    let mut partitions = None;

    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;

        match argument.as_str() {
            "--dataset" => {
                dataset = BenchmarkDataset::parse(&value)?;
            }

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
                    return Err("worker counts must be positive".to_owned());
                }

                if workers.iter().copied().collect::<HashSet<_>>().len() != workers.len() {
                    return Err("duplicate worker configurations".to_owned());
                }
            }

            "--repetitions" => {
                repetitions = value
                    .parse()
                    .map_err(|_| "invalid repetition count".to_owned())?;

                if repetitions == 0 {
                    return Err("repetitions must be positive".to_owned());
                }
            }

            "--hours" => {
                duration_hours = value
                    .parse()
                    .map_err(|_| "invalid benchmark duration".to_owned())?;

                if duration_hours == 0 || duration_hours > 72 {
                    return Err("benchmark duration must be between 1 and 72 hours".to_owned());
                }
            }

            "--output" => output = Some(PathBuf::from(value)),

            "--partitions" => {
                let count = value
                    .parse::<usize>()
                    .map_err(|_| "invalid partition count")?;

                if count == 0 {
                    return Err("partition count must be positive".to_owned());
                }

                partitions = Some(count);
            }

            _ => return Err(format!("unknown argument: {argument}")),
        }
    }

    let output = output
        .unwrap_or_else(|| PathBuf::from(format!("benchmarks/results/{}-merge", dataset.as_str())));

    Ok(BenchmarkConfig {
        dataset,
        workers,
        repetitions,
        output,
        duration_hours,
        partitions,
    })
}

// -----------------------------------------------------------------------------
// Dataset discovery
// -----------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct DatasetManifestInfo {
    dataset_id: u64,
    content_type: ContentType,
}

#[derive(Debug)]
struct DatasetInput {
    dataset_id: u64,
    content_type: ContentType,
    path: PathBuf,
}

fn discover_datasets(root: &Path) -> Result<Vec<DatasetInput>, String> {
    let locations = [
        root.join("data/processed"),
        root.join("data/.work/full-trade-test/datasets"),
    ];

    let mut datasets = Vec::new();
    let mut seen = HashSet::new();

    for location in locations {
        if !location.exists() {
            continue;
        }

        let entries = fs::read_dir(&location).map_err(|error| error.to_string())?;

        for entry in entries {
            let entry = entry.map_err(|error| error.to_string())?;
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            let manifest_path = path.join("manifest.json");

            if !manifest_path.is_file() {
                continue;
            }

            let manifest: DatasetManifestInfo = serde_json::from_slice(
                &fs::read(&manifest_path).map_err(|error| error.to_string())?,
            )
            .map_err(|error| format!("{}: {error}", manifest_path.display()))?;

            // Exclude the separate depth-processing test dataset.
            if manifest.dataset_id == 900001 {
                continue;
            }

            if !seen.insert(manifest.dataset_id) {
                return Err(format!(
                    "duplicate canonical dataset ID {}",
                    manifest.dataset_id
                ));
            }

            datasets.push(DatasetInput {
                dataset_id: manifest.dataset_id,
                content_type: manifest.content_type,
                path,
            });
        }
    }

    datasets.sort_by_key(|dataset| dataset.dataset_id);

    Ok(datasets)
}

fn select_datasets(datasets: &[DatasetInput], mode: BenchmarkDataset) -> Vec<&DatasetInput> {
    datasets
        .iter()
        .filter(|dataset| match mode {
            BenchmarkDataset::Trades => dataset.content_type == ContentType::Trades,
            BenchmarkDataset::Depth => dataset.content_type == ContentType::Depth,
            BenchmarkDataset::Combined => matches!(
                dataset.content_type,
                ContentType::Trades | ContentType::Depth
            ),
        })
        .collect()
}

// -----------------------------------------------------------------------------
// Benchmark results
// -----------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct BenchmarkRun {
    workers: usize,
    repetition: usize,
    partitions: usize,

    elapsed_seconds: f64,
    throughput_events_per_second: f64,

    events_written: u64,
    trade_events: Option<u64>,
    depth_events: Option<u64>,

    parquet_files: u64,
    output_bytes: u64,

    success: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    dataset: BenchmarkDataset,
    input_datasets: Vec<u64>,
    workers: Vec<usize>,
    repetitions: usize,
    results: Vec<BenchmarkRun>,
    partitions: Option<usize>,
}

// -----------------------------------------------------------------------------
// Job construction
// -----------------------------------------------------------------------------

fn benchmark_job(
    root: &Path,
    inputs: &[&DatasetInput],
    workers: usize,
    repetition: usize,
    output: &Path,
    duration_hours: u64,
    partitions: Option<usize>,
) -> Result<MergeJob, String> {
    let job_id = 9_200_000_u64
        .checked_add((repetition as u64) * 100)
        .and_then(|id| id.checked_add(workers as u64))
        .ok_or("benchmark job ID overflow")?;

    let input_paths = inputs
        .iter()
        .map(|input| (input.dataset_id, input.path.clone()))
        .collect::<HashMap<_, _>>();

    let start_ns = 1_788_220_800_000_000_000_i64;

    let duration_ns = i64::try_from(duration_hours)
        .map_err(|_| "benchmark duration overflow")?
        .checked_mul(3_600_000_000_000)
        .ok_or("benchmark duration overflow")?;

    let end_ns = start_ns
        .checked_add(duration_ns)
        .ok_or("benchmark interval overflow")?;
    let partitions_per_worker = match partitions {
        Some(total) => {
            if total < workers {
                return Err("fixed partition count cannot be smaller than worker count".to_owned());
            }

            if total % workers != 0 {
                return Err("fixed partition count must be divisible by worker count".to_owned());
            }

            total / workers
        }

        None => 4,
    };
    let job = MergeJob {
        job_id,
        operation: MergeOperation::Merge,

        inputs: inputs.iter().map(|input| input.dataset_id).collect(),
        input_paths,

        time_range: MergeTimeRange { start_ns, end_ns },

        resources: MergeResources {
            workers: workers as _,
            memory_limit_mb: 61_440,
            max_open_files: 1024,
        },

        partitioning: MergePartitioning {
            partitions_per_worker: partitions_per_worker as _,
        },

        parquet: MergeParquetConfig {
            row_group_target_mb: 128,
            file_target_mb: 512,
            compression_level: 3,
        },

        output: MergeOutput {
            dataset_path: output.to_path_buf(),
        },
    };

    job.validate().map_err(|error| error.to_string())?;

    let _ = root;

    Ok(job)
}

// -----------------------------------------------------------------------------
// Manifest verification
// -----------------------------------------------------------------------------

fn verify_manifest(
    dataset_path: &Path,
    expected_job_id: u64,
    expected_events: u64,
    expected_files: u64,
) -> Result<u64, String> {
    let path = dataset_path.join("manifest.json");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(&path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;

    manifest.validate().map_err(|error| error.to_string())?;

    if manifest.job_id != expected_job_id {
        return Err("manifest job identity mismatch".to_owned());
    }

    if manifest.events_written != expected_events {
        return Err("manifest event count mismatch".to_owned());
    }

    if manifest.files_written != expected_files {
        return Err("manifest file count mismatch".to_owned());
    }

    let files = &manifest.files;

    if files.len() as u64 != expected_files {
        return Err("manifest file inventory mismatch".to_owned());
    }

    let mut output_bytes = 0_u64;
    let mut total_rows = 0_u64;

    for file in files {
        let relative = Path::new(&file.path);

        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err("unsafe manifest file path".to_owned());
        }

        let path = dataset_path.join(relative);

        let metadata = fs::metadata(&path).map_err(|error| error.to_string())?;

        let expected_size = file.size_bytes;

        if metadata.len() != expected_size {
            return Err(format!("Parquet size mismatch: {}", path.display()));
        }

        output_bytes += metadata.len();

        total_rows += file.rows.ok_or("missing file row count")?;
    }

    if total_rows != expected_events {
        return Err(format!(
            "Parquet row count mismatch: expected={expected_events}, actual={total_rows}"
        ));
    }

    if total_rows != expected_events {
        return Err(format!(
            "Parquet row count mismatch: expected={expected_events}, actual={total_rows}"
        ));
    }

    Ok(output_bytes)
}

// -----------------------------------------------------------------------------
// Execution
// -----------------------------------------------------------------------------

fn run_benchmark(config: BenchmarkConfig) -> Result<(), String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("project root not found")?
        .to_path_buf();

    let datasets = discover_datasets(&root)?;
    let selected = select_datasets(&datasets, config.dataset);

    if selected.is_empty() {
        return Err("no canonical datasets selected".to_owned());
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

    let mut results = Vec::new();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — MERGE BENCHMARK");
    println!("{}", "=".repeat(90));

    println!("Mode        : {}", config.dataset.as_str());
    println!("Datasets    : {}", selected.len());
    println!("Workers     : {:?}", config.workers);
    println!("Repetitions : {}", config.repetitions);
    println!("Output      : {}", output.display());

    for repetition in 1..=config.repetitions {
        let mut worker_counts = config.workers.clone();

        // Alternate worker order to reduce cache bias.
        if repetition % 2 == 0 {
            worker_counts.reverse();
        }

        for workers in worker_counts {
            let run_directory = scratch.join(format!("rep-{repetition}/workers-{workers}"));

            fs::create_dir_all(&run_directory).map_err(|error| error.to_string())?;

            let dataset_path = run_directory.join("dataset");

            let job = benchmark_job(
                &root,
                &selected,
                workers,
                repetition,
                &dataset_path,
                config.duration_hours,
                config.partitions,
            )?;

            println!("\n{}", "-".repeat(90));
            println!(
                "REPETITION {repetition}/{} — WORKERS {workers}",
                config.repetitions
            );
            println!("{}", "-".repeat(90));

            let started = Instant::now();

            let execution = execute_unified_merge_job(&job, &root).map(|result| {
                (
                    result.events_written,
                    Some(result.trade_events),
                    Some(result.depth_events),
                    result.files_written,
                    result.partitions_completed,
                )
            });

            let elapsed = started.elapsed().as_secs_f64();

            let mut run = BenchmarkRun {
                workers,
                repetition,
                partitions: 0,

                elapsed_seconds: elapsed,
                throughput_events_per_second: 0.0,

                events_written: 0,
                trade_events: None,
                depth_events: None,

                parquet_files: 0,
                output_bytes: 0,

                success: false,
                error: None,
            };

            match execution {
                Ok((events, trade_events, depth_events, files, partitions)) => {
                    run.events_written = events;
                    run.trade_events = trade_events;
                    run.depth_events = depth_events;
                    run.parquet_files = files;
                    run.partitions = partitions;

                    match verify_manifest(&dataset_path, job.job_id, events, files) {
                        Ok(bytes) => {
                            run.output_bytes = bytes;
                            run.throughput_events_per_second =
                                events as f64 / elapsed.max(f64::EPSILON);
                            run.success = true;

                            fs::remove_dir_all(&run_directory)
                                .map_err(|error| error.to_string())?;
                        }

                        Err(error) => run.error = Some(error),
                    }
                }

                Err(error) => run.error = Some(error.to_string()),
            }

            println!("workers={workers:>2} | ");

            println!(
                "events={:>12} | elapsed={:.3}s | throughput={:.0}/s | {}",
                run.events_written,
                run.elapsed_seconds,
                run.throughput_events_per_second,
                if run.success { "PASS" } else { "FAIL" },
            );

            if let Some(error) = &run.error {
                eprintln!("ERROR: {error}");
            }

            results.push(run);

            if !results.last().is_some_and(|result| result.success) {
                return Err(
                    "benchmark execution failed; output preserved for inspection".to_owned(),
                );
            }
        }
    }

    let report = BenchmarkReport {
        dataset: config.dataset,
        input_datasets: selected.iter().map(|dataset| dataset.dataset_id).collect(),
        workers: config.workers,
        repetitions: config.repetitions,
        results,
        partitions: config.partitions,
    };

    let report_path = output.join("benchmark.json");

    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — BENCHMARK COMPLETE");
    println!("{}", "=".repeat(90));
    println!("Report: {}", report_path.display());
    println!("Partitions  : {:?}", config.partitions);

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
