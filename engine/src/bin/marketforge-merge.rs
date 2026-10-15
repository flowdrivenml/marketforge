#[cfg(feature = "monitor-tui")]
use marketforge_engine::tui::merging::execute_merge_with_tui;
use marketforge_engine::{
    job::{ContentType, load_merge_job},
    process::merge::{execute_unified_merge_job, resolve_merge_inputs},
};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

// -----------------------------------------------------------------------------
// CLI arguments
// -----------------------------------------------------------------------------

struct CliArgs {
    job: PathBuf,
    project_root: PathBuf,

    #[cfg(feature = "monitor-tui")]
    tui: bool,
}

fn parse_args() -> Result<CliArgs, String> {
    let mut args = env::args().skip(1);

    let mut job = None;
    let mut project_root = None;
    #[cfg(feature = "monitor-tui")]
    let mut tui = false;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--job" => {
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
            "--project-root" => {
                project_root = Some(PathBuf::from(
                    args.next().ok_or("missing value for --project-root")?,
                ));
            }

            "--help" | "-h" => {
                println!(
                    "Usage: marketforge-merge \
                     --job <PATH> \
                     [--project-root <PATH>]"
                );

                std::process::exit(0);
            }

            unknown => {
                return Err(format!("unknown argument: {unknown}"));
            }
        }
    }

    let job = job.ok_or("missing required argument --job")?;

    let project_root = project_root.unwrap_or_else(|| PathBuf::from("."));

    Ok(CliArgs {
        job,
        project_root,

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

fn run() -> Result<(), String> {
    let args = parse_args()?;

    let project_root = args
        .project_root
        .canonicalize()
        .map_err(|error| format!("invalid project root: {error}"))?;

    let job_path = if args.job.is_absolute() {
        args.job
    } else {
        project_root.join(args.job)
    };

    let job = load_merge_job(&job_path).map_err(|error| error.to_string())?;
    #[cfg(feature = "monitor-tui")]
    if args.tui {
        let result =
            execute_merge_with_tui(job, project_root).map_err(|error| error.to_string())?;

        // The TUI already prints its permanent summary.
        // Do not execute the merge again.
        let _ = result;

        return Ok(());
    }
    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — PARALLEL MERGE");
    println!("{}", "=".repeat(80));

    println!("Job ID          : {}", job.job_id);
    println!("Input datasets  : {:?}", job.inputs);
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
    // Detect merge mode
    // -------------------------------------------------------------------------

    let inputs = resolve_merge_inputs(&job, &project_root).map_err(|error| error.to_string())?;

    let has_trades = inputs
        .iter()
        .any(|input| input.manifest.content_type == ContentType::Trades);

    let has_depth = inputs
        .iter()
        .any(|input| input.manifest.content_type == ContentType::Depth);

    if inputs
        .iter()
        .any(|input| input.manifest.content_type == ContentType::Combined)
    {
        return Err("combined canonical datasets are not supported as merge inputs".to_owned());
    }

    // -------------------------------------------------------------------------
    // Dispatch to the appropriate executor
    // -------------------------------------------------------------------------

    let result = match (has_trades, has_depth) {
        (false, false) => {
            return Err("merge contains no supported input datasets".to_owned());
        }

        _ => {
            let result = execute_unified_merge_job(&job, &project_root)
                .map_err(|error| error.to_string())?;

            MergeCliResult {
                job_id: result.job_id,

                mode: match (has_trades, has_depth) {
                    (true, false) => "trades",
                    (false, true) => "depth",
                    (true, true) => "combined",
                    (false, false) => unreachable!(),
                },

                dataset_path: result.dataset_path,
                manifest_path: result.manifest_path,

                partitions_completed: result.partitions_completed,

                events_written: result.events_written,
                trade_events: Some(result.trade_events),
                depth_events: Some(result.depth_events),

                files_written: result.files_written,
                skipped_initializations: result.skipped_initializations,
            }
        }
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

    Ok(())
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,

        Err(error) => {
            eprintln!("Merge failed: {error}");
            ExitCode::FAILURE
        }
    }
}
