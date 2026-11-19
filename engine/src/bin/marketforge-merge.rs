#[cfg(feature = "monitor-tui")]
use marketforge_engine::tui::merging::execute_merge_with_tui;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};
use {
    marketforge_engine::historical::job::load_merge_job,
    marketforge_engine::historical::merge::execute_unified_merge_job,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

// -----------------------------------------------------------------------------
// CLI arguments
// -----------------------------------------------------------------------------

struct CliArgs {
    job: PathBuf,
    validate: bool,
    result_file: Option<PathBuf>,
    project_root: PathBuf,
    config: PathBuf,

    #[cfg(feature = "monitor-tui")]
    tui: bool,
}

fn parse_args() -> Result<CliArgs, String> {
    let mut args = env::args().skip(1);

    let mut job = None;
    let mut validate = false;
    let mut result_file = None;
    let mut project_root = None;
    let mut config = PathBuf::from("data/.config/historical/general.toml");
    #[cfg(feature = "monitor-tui")]
    let mut tui = false;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--runtime-settings" => {
                use marketforge_engine::shared::historical::config::load_merge_runtime_settings;
                if job.is_some() || args.len() != 1 {
                    return Err(
                        "--runtime-settings requires exactly one runtime TOML path".to_owned()
                    );
                }
                let path = PathBuf::from(args.next().ok_or("missing runtime TOML path")?);
                let root = env::current_dir().map_err(|e| e.to_string())?;
                let config =
                    load_merge_runtime_settings(&path, &root).map_err(|e| e.to_string())?;
                println!(
                    "{}",
                    serde_json::to_string(&config).map_err(|e| e.to_string())?
                );
                std::process::exit(0);
            }
            "--validate-dataset" => {
                use marketforge_engine::historical::merge::validate_committed_merge;
                if job.is_some() || args.len() != 1 {
                    return Err("--validate-dataset requires exactly one directory".to_owned());
                }
                let root = PathBuf::from(args.next().ok_or("missing dataset directory")?);
                validate_committed_merge(&root).map_err(|error| error.to_string())?;
                println!("{{\"kind\":\"merge_dataset_validation\",\"status\":\"valid\"}}");
                std::process::exit(0);
            }
            "--job" | "--validate-job" => {
                if job.is_some() {
                    return Err("Choose exactly one of --job and --validate-job".to_owned());
                }
                validate = argument == "--validate-job";
                job = Some(PathBuf::from(args.next().ok_or("missing value for --job")?));
            }
            "--tui" => {
                #[cfg(feature = "monitor-tui")]
                {
                    tui = true;
                }

                #[cfg(not(feature = "monitor-tui"))]
                {
                    return Err("--tui requires the monitor-tui feature".to_owned());
                }
            }
            "--result-file" => {
                result_file = Some(PathBuf::from(
                    args.next().ok_or("missing value for --result-file")?,
                ));
            }
            "--config" => {
                config = PathBuf::from(args.next().ok_or("missing value for --config")?);
            }
            "--project-root" => {
                project_root = Some(PathBuf::from(
                    args.next().ok_or("missing value for --project-root")?,
                ));
            }

            "--help" | "-h" => {
                println!(
                    "Usage: marketforge-merge [--validate-job <PATH> | --validate-dataset <DIR>] or \
                     --job <PATH> \
                     [--project-root <PATH>] [--config <PATH>] [--result-file <PATH>] [--tui]"
                );

                std::process::exit(0);
            }

            unknown => {
                return Err(format!("unknown argument: {unknown}"));
            }
        }
    }

    if validate && result_file.is_some() {
        return Err("--result-file requires --job".to_owned());
    }
    let job = job.ok_or("missing required argument --job")?;

    let project_root = project_root.unwrap_or_else(|| PathBuf::from("."));

    Ok(CliArgs {
        job,
        validate,
        result_file,
        project_root,
        config,

        #[cfg(feature = "monitor-tui")]
        tui,
    })
}

// -----------------------------------------------------------------------------
// Output size
// -----------------------------------------------------------------------------

fn directory_size(path: &Path) -> Result<u64, String> {
    let mut total = 0_u64;

    for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;

        let metadata = entry.metadata().map_err(|error| error.to_string())?;

        if metadata.is_dir() {
            total = total
                .checked_add(directory_size(&entry.path())?)
                .ok_or("output size overflow")?;
        } else if metadata.is_file() {
            total = total
                .checked_add(metadata.len())
                .ok_or("output size overflow")?;
        }
    }

    Ok(total)
}

struct MergeCliResult {
    job_id: u64,
    mode: &'static str,

    dataset_path: PathBuf,
    manifest_path: PathBuf,

    partitions_completed: usize,

    events_written: u64,
    trade_events: Option<u64>,
    depth_events: Option<u64>,

    files_written: u64,
    skipped_initializations: u64,
}

// -----------------------------------------------------------------------------
// Execution
// -----------------------------------------------------------------------------

fn run(args: CliArgs) -> Result<serde_json::Value, String> {
    let project_root = args
        .project_root
        .canonicalize()
        .map_err(|error| format!("invalid project root: {error}"))?;

    let job_path = if args.job.is_absolute() {
        args.job
    } else {
        project_root.join(args.job)
    };

    use marketforge_engine::shared::historical::config::{
        load_merge_runtime_settings, resolve_path,
    };
    let document = load_merge_job(&job_path).map_err(|error| error.to_string())?;
    let runtime =
        load_merge_runtime_settings(&resolve_path(&project_root, &args.config), &project_root)
            .map_err(|error| error.to_string())?;
    let job = document
        .with_runtime(runtime)
        .map_err(|error| error.to_string())?;
    if args.validate {
        use marketforge_engine::historical::merge::{
            MergeCommitPaths, resolve_ranked_merge_streams,
        };
        let resolved = resolve_merge_inputs(&job, &project_root).map_err(|e| e.to_string())?;
        resolve_ranked_merge_streams(&resolved, &project_root, job.submission.as_ref())
            .map_err(|e| e.to_string())?;
        let output = MergeCommitPaths::new(&job.output.dataset_path, job.job_id)
            .map_err(|e| e.to_string())?;
        for path in [output.dataset_path, output.staging_path] {
            if path.exists() || path.is_symlink() {
                return Err(format!(
                    "merge destination already exists: {}",
                    path.display()
                ));
            }
        }
        println!("{{\"kind\":\"merge_job_validation\",\"status\":\"valid\"}}");
        return Ok(serde_json::json!({"status":"valid"}));
    }
    #[cfg(feature = "monitor-tui")]
    if args.tui {
        let result = match execute_merge_with_tui(job, project_root) {
            Ok(result) => result,
            Err(error) => {
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|cause| cause.kind() == std::io::ErrorKind::Interrupted)
                {
                    std::process::exit(130);
                }
                return Err(error.to_string());
            }
        };

        return Ok(success(
            result.job_id,
            &result.dataset_path,
            &result.manifest_path,
        ));
    }
    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — PARALLEL MERGE");
    println!("{}", "=".repeat(80));

    println!("Job ID          : {}", job.job_id);
    println!(
        "Selected files  : {}",
        job.submission.as_ref().map_or(0, |job| job.inputs.len())
    );
    println!("Publications    : {}", job.inputs.len());
    println!("Workers         : {}", job.resources.workers);

    println!(
        "Partitions      : {}",
        job.resources.workers * job.partitioning.partitions_per_worker
    );

    println!(
        "Time range      : [{} , {})",
        job.time_range.start_ns, job.time_range.end_ns
    );

    println!("Memory budget   : {} MiB", job.resources.memory_limit_mb);

    println!("Open-file budget: {}", job.resources.max_open_files);

    println!("Output          : {}", job.output.dataset_path.display());

    println!("{}", "-".repeat(80));

    // -------------------------------------------------------------------------
    // Execute merge
    // -------------------------------------------------------------------------

    let start = Instant::now();

    // -------------------------------------------------------------------------
    let merged =
        execute_unified_merge_job(&job, &project_root).map_err(|error| error.to_string())?;
    use marketforge_engine::historical::merge::UnifiedMergeMode;
    let result = MergeCliResult {
        job_id: merged.job_id,
        mode: match merged.mode {
            UnifiedMergeMode::Trades => "trades",
            UnifiedMergeMode::Depth => "depth",
            UnifiedMergeMode::Combined => "combined",
        },
        dataset_path: merged.dataset_path,
        manifest_path: merged.manifest_path,
        partitions_completed: merged.partitions_completed,
        events_written: merged.events_written,
        trade_events: Some(merged.trade_events),
        depth_events: Some(merged.depth_events),
        files_written: merged.files_written,
        skipped_initializations: merged.skipped_initializations,
    };

    let elapsed = start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Collect output metrics
    // -------------------------------------------------------------------------

    let output_bytes = directory_size(&result.dataset_path)?;

    let throughput = if elapsed > 0.0 {
        result.events_written as f64 / elapsed
    } else {
        0.0
    };

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — MERGE SUMMARY");
    println!("{}", "=".repeat(80));

    println!("Job ID          : {}", result.job_id);
    println!("Mode            : {}", result.mode);
    println!("Partitions      : {}", result.partitions_completed);

    println!("Merged events   : {}", result.events_written);
    if let Some(trades) = result.trade_events {
        println!("Trade events    : {trades}");
    }

    if let Some(depth) = result.depth_events {
        println!("Depth events    : {depth}");
    }

    println!("Parquet files   : {}", result.files_written);

    println!("Skipped snapshots: {}", result.skipped_initializations);

    println!("Elapsed time    : {:.3} s", elapsed);
    println!("Throughput      : {:.0} events/s", throughput);

    println!(
        "Output size     : {:.2} MiB",
        output_bytes as f64 / 1024.0 / 1024.0
    );

    println!("Manifest        : {}", result.manifest_path.display());

    println!("Status          : PASS");
    println!("{}", "=".repeat(80));

    Ok(success(
        result.job_id,
        &result.dataset_path,
        &result.manifest_path,
    ))
}

fn success(job_id: u64, dataset: &Path, manifest: &Path) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "kind": "merge_result",
        "status": "complete",
        "job_id": job_id,
        "dataset_path": dataset,
        "manifest_path": manifest,
    })
}

fn save_result(path: &Path, result: &serde_json::Value) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    writeln!(file, "{result}")?;
    file.sync_all()?;
    fs::File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("Merge argument error: {error}");
            return ExitCode::FAILURE;
        }
    };
    let path = args.result_file.clone();
    let result = run(args);
    if let Some(path) = path {
        let failure;
        let report = match &result {
            Ok(value) => value,
            Err(error) => {
                failure = serde_json::json!({
                    "schema_version": 1,
                    "kind": "merge_result",
                    "status": "failed",
                    "error": error,
                });
                &failure
            }
        };
        if let Err(error) = save_result(&path, report) {
            eprintln!("Cannot persist merge result: {error}");
            return ExitCode::FAILURE;
        }
    }
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Merge failed: {error}");
            ExitCode::FAILURE
        }
    }
}
