use marketforge_engine::historical::convert::execution::execute_parallel_combined_conversion_with_progress;
use marketforge_engine::historical::convert::execution::execute_parallel_conversion_with_progress;
use marketforge_engine::historical::convert::execution::execute_parallel_depth_conversion_with_progress;
use marketforge_engine::historical::convert::output::{
    publish_combined_conversion_with_progress, publish_conversion_with_progress,
    publish_depth_conversion_with_progress,
};
use marketforge_engine::historical::convert::progress::{
    ConversionOutcome, ConversionPhase, ConversionProgressReporter,
};
use std::cell::Cell;
use std::io::IsTerminal;
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};
use {
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::execution::PhysicalConversionMetrics,
    marketforge_engine::historical::convert::execution::build_combined_reference_checkpoints_with_progress,
    marketforge_engine::historical::convert::execution::build_conversion_checkpoints_with_progress,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan_with_count,
    marketforge_engine::historical::convert::execution::build_depth_reference_checkpoints_with_progress,
    marketforge_engine::historical::convert::execution::materialize_depth_conversion_with_progress,
    marketforge_engine::historical::convert::execution::materialize_trade_conversion_with_progress,
    marketforge_engine::historical::convert::output::ConversionExecutionReceipt,
    marketforge_engine::historical::convert::output::ConversionPublication,
    marketforge_engine::historical::convert::{PreparedConversion, prepare_conversion_execution},
    marketforge_engine::historical::job::ConvertJob,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::historical::merge::UnifiedMergeMode,
};

thread_local! {
    static CONVERSION_TUI_MODE: Cell<bool> = const { Cell::new(false) };
}

#[cfg(feature = "monitor-tui")]
fn set_conversion_tui_mode(enabled: bool) {
    CONVERSION_TUI_MODE.with(|mode| mode.set(enabled));
}

fn conversion_tui_mode() -> bool {
    CONVERSION_TUI_MODE.with(Cell::get)
}
macro_rules! conversion_println {
    () => {
        if !conversion_tui_mode() {
            println!();
        }
    };

    ($($arg:tt)*) => {
        if !conversion_tui_mode() {
            println!($($arg)*);
        }
    };
}
// -----------------------------------------------------------------------------
// CLI modes
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    Validate,
    Materialize,
    Publish,
    Run,
}

struct Args {
    job: PathBuf,
    mode: ExecutionMode,
    workers: Option<usize>,
    tui: bool,
}

#[derive(Debug, serde::Serialize)]
struct ConversionRuntime {
    workers: usize,
    parquet: MergeParquetConfig,
}

fn resolve_runtime(args: &Args, job: &ConvertJob) -> Result<ConversionRuntime, String> {
    let workers = args.workers.unwrap_or(job.resources.workers);
    if workers == 0 || workers > job.resources.workers {
        return Err(format!(
            "requested workers {workers} exceed configured limit {}",
            job.resources.workers
        ));
    }
    let per_worker_files = if job.external_reference.is_some() {
        3
    } else {
        2
    };
    let files = workers
        .checked_mul(per_worker_files)
        .and_then(|value| value.checked_add(2));
    if files.is_none_or(|files| files > job.resources.max_open_files) {
        return Err("conversion workers exceed the prepared open-file budget".into());
    }
    let writer_memory = job.parquet.row_group_target_mb.checked_mul(workers as u64);
    if writer_memory.is_none_or(|memory| memory > job.resources.memory_limit_mb) {
        return Err("conversion writer row groups exceed the prepared memory budget".into());
    }
    Ok(ConversionRuntime {
        workers,
        parquet: job.parquet.clone(),
    })
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fn main() {
    if let Err(error) = run() {
        if error == "conversion interrupted" {
            eprintln!("Conversion interrupted");
            std::process::exit(130);
        }
        eprintln!("Conversion failed: {error}");
        std::process::exit(1);
    }
}
fn execute_conversion_with_progress(
    job: &ConvertJob,
    staging: &Path,
    runtime: &ConversionRuntime,
    mode: ExecutionMode,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    let result = (|| {
        reporter
            .check_interrupted()
            .map_err(|error| error.to_string())?;
        reporter.phase_started(ConversionPhase::SourceValidation);
        let prepared = prepare_conversion_execution(job).map_err(|error| error.to_string())?;
        prepared
            .verify_content(job, reporter)
            .map_err(|error| error.to_string())?;
        reporter.phase_completed(ConversionPhase::SourceValidation);
        reporter
            .check_interrupted()
            .map_err(|error| error.to_string())?;
        let source_manifest = &prepared.source;
        if mode == ExecutionMode::Validate {
            let plan = &prepared.plan;
            conversion_println!(
                "Validated job {} · {:?} · {} streams · {} references · {} physical rows",
                job.job_id,
                source_manifest.mode,
                plan.source_streams,
                plan.reference_streams,
                source_manifest.events_written
            );
            for stream in &plan.streams {
                let routes = stream
                    .paths
                    .iter()
                    .map(|path| {
                        path.currencies
                            .iter()
                            .map(|currency| currency.as_str())
                            .collect::<Vec<_>>()
                            .join(" → ")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                conversion_println!("  {}  {}", stream.source.stream_id, routes);
            }
            return Ok(());
        }

        let provenance = staging.join("conversion-runtime.json");
        let effective = serde_json::to_vec_pretty(&serde_json::json!({
            "runtime": runtime, "job": job,
        }))
        .map_err(|error| error.to_string())?;
        if mode != ExecutionMode::Publish {
            materialize_with_reporter(job, &prepared, staging, runtime, reporter)?;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&provenance)
                .map_err(|error| error.to_string())?;
            file.write_all(&effective)
                .and_then(|()| file.sync_all())
                .map_err(|error| error.to_string())?;
        }
        reporter
            .check_interrupted()
            .map_err(|error| error.to_string())?;
        if mode == ExecutionMode::Materialize {
            conversion_println!("Status           : MATERIALIZED (UNPUBLISHED)");
            conversion_println!(
                "Receipt          : {}",
                staging.join("execution.json").display()
            );
            conversion_println!("Runtime          : {}", provenance.display());
        } else {
            let pinned = fs::read(&provenance)
                .map_err(|error| format!("cannot read staged conversion runtime: {error}"))?;
            if pinned != effective {
                return Err("prepared conversion job or runtime differs from materialization; use the original job and worker count".to_owned());
            }
            publish_with_reporter(job, source_manifest, staging, runtime.workers, reporter)?;
            if mode == ExecutionMode::Publish
                && reporter.is_enabled()
                && let Ok(receipt) = ConversionExecutionReceipt::load(&job.output.dataset_path)
            {
                reporter.emit(marketforge_engine::historical::convert::progress::ConversionProgressEvent::PublishedReceipt(receipt));
            }
        }
        Ok(())
    })();
    let outcome = match &result {
        Ok(()) if mode == ExecutionMode::Validate => ConversionOutcome::Validated,
        Ok(()) if mode == ExecutionMode::Materialize => ConversionOutcome::Materialized,
        Ok(()) => ConversionOutcome::Published,
        Err(error) if error == "conversion interrupted" => ConversionOutcome::Interrupted,
        Err(error) => ConversionOutcome::Failed(error.clone()),
    };
    reporter.finished(outcome);
    result
}

fn run() -> Result<(), String> {
    let args = parse_args(env::args().skip(1))?;

    let job_path = &args.job;
    let job = load_convert_job(job_path).map_err(|error| error.to_string())?;
    let runtime = resolve_runtime(&args, &job)?;
    let source_manifest = load_source_manifest(&job.input_merge)?;
    let output = job.output.dataset_path.clone();
    let staging = output.with_extension("staging");
    if args.tui {
        if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
            return Err("dashboard error: --tui requires an interactive terminal".to_owned());
        }

        #[cfg(feature = "monitor-tui")]
        {
            return run_conversion_tui(job, source_manifest, staging, runtime, args.mode);
        }

        #[cfg(not(feature = "monitor-tui"))]
        {
            return Err("--tui requires the monitor-tui feature".to_owned());
        }
    }
    conversion_println!();
    conversion_println!("{}", "=".repeat(80));
    conversion_println!("MARKETFORGE — PHYSICAL CONVERSION");
    conversion_println!("{}", "=".repeat(80));

    conversion_println!("Job             : {}", job_path.display());
    conversion_println!("Mode            : {:?}", args.mode);
    conversion_println!("Input           : {}", job.input_merge.display());
    conversion_println!("Output          : {}", output.display());
    conversion_println!("Staging         : {}", staging.display());
    conversion_println!("Physical rows     : {}", source_manifest.events_written);
    conversion_println!(
        "Market events     : {}",
        source_manifest.events_written - source_manifest.reconstruction_events
    );
    conversion_println!(
        "Reconstruction   : {}",
        source_manifest.reconstruction_events
    );

    execute_conversion_with_progress(
        &job,
        &staging,
        &runtime,
        args.mode,
        &ConversionProgressReporter::disabled(),
    )
}
#[cfg(feature = "monitor-tui")]
fn run_conversion_tui(
    job: ConvertJob,
    source_manifest: PhysicalMergeManifest,
    staging: PathBuf,
    runtime: ConversionRuntime,
    mode: ExecutionMode,
) -> Result<(), String> {
    use std::sync::mpsc;

    use marketforge_engine::tui::conversion::ConversionDashboardContext;
    let (sender, receiver) = mpsc::channel();
    let reporter = ConversionProgressReporter::new(sender);
    let progress_state = reporter.state();
    let context = ConversionDashboardContext {
        operation: match mode {
            ExecutionMode::Validate => "Validate",
            ExecutionMode::Materialize => "Materialize",
            ExecutionMode::Publish => "Publish",
            ExecutionMode::Run => "Run",
        },
        mode: source_manifest.mode,
        target_quote: job.conversion.target_quote.clone(),
        instruments: job
            .source
            .streams
            .iter()
            .map(|stream| (&stream.instrument_namespace, stream.instrument_id))
            .collect::<std::collections::HashSet<_>>()
            .len(),
        streams: source_manifest.streams.len(),
        reconstruction_rows: source_manifest.reconstruction_events,
        trade_events: source_manifest.trade_events,
        depth_events: source_manifest.depth_events,
        requested_workers: runtime.workers,
        details: vec![
            format!(
                "Job {} · Source publication {}",
                job.job_id, source_manifest.job_id
            ),
            format!("Staging {}", staging.display()),
            format!(
                "Parquet {} MiB row groups · {} MiB files · Zstd {}",
                runtime.parquet.row_group_target_mb,
                runtime.parquet.file_target_mb,
                runtime.parquet.compression_level
            ),
            format!(
                "{} hops · {} ns staleness · {:?} path policy",
                job.conversion.max_hops,
                job.conversion.max_staleness_ns,
                job.conversion.path_policy
            ),
            format!(
                "External reference {}",
                job.external_reference
                    .as_ref()
                    .map_or("—".to_owned(), |source| source.merge_job_id.to_string())
            ),
            format!("Input {}", job.input_merge.display()),
            format!("Output {}", job.output.dataset_path.display()),
            format!(
                "{} market events · {} reconstruction rows",
                source_manifest.events_written - source_manifest.reconstruction_events,
                source_manifest.reconstruction_events
            ),
            format!(
                "Target {} · References {}",
                job.conversion.target_quote,
                job.conversion
                    .references
                    .iter()
                    .map(|reference| format!(
                        "{} {} {:?}",
                        reference.exchange, reference.symbol, reference.instrument_kind
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            format!(
                "{} requested workers · configured caps {} workers, {} MB, {} open files",
                runtime.workers,
                job.resources.workers,
                job.resources.memory_limit_mb,
                job.resources.max_open_files
            ),
        ],
        expected_rows: source_manifest.events_written,
    };
    let source_id = job.source.merge_job_id;
    let output = job.output.dataset_path.clone();
    let staged = staging.clone();
    let start = Instant::now();
    let (result, terminal) = join_dashboard_execution(
        &progress_state,
        move || {
            set_conversion_tui_mode(true);
            execute_conversion_with_progress(&job, &staging, &runtime, mode, &reporter)
        },
        |start| {
            marketforge_engine::tui::conversion::execute_conversion_with_tui(
                receiver,
                progress_state.clone(),
                context,
                start,
            )
        },
    );
    if result.is_ok() {
        println!("Source publication {source_id}");
        match mode {
            ExecutionMode::Validate => {
                println!("VALIDATED · {:.2}s", start.elapsed().as_secs_f64())
            }
            ExecutionMode::Materialize => print_dashboard_summary(
                ConversionOutcome::Materialized,
                &staged,
                start,
                &progress_state,
            ),
            ExecutionMode::Run | ExecutionMode::Publish => print_dashboard_summary(
                ConversionOutcome::Published,
                &output,
                start,
                &progress_state,
            ),
        }
    } else if staged.is_dir() {
        eprintln!("Conversion staging {}", staged.display());
    }
    if let Err(error) = terminal {
        if result
            .as_ref()
            .is_err_and(|error| error == "conversion interrupted")
        {
            return Err(format!(
                "dashboard error: {error}; conversion interrupted before completion"
            ));
        }
        eprintln!("Conversion dashboard error: {error}");
    }
    result
}

#[cfg(feature = "monitor-tui")]
fn print_dashboard_summary(
    outcome: ConversionOutcome,
    directory: &Path,
    start: Instant,
    progress: &marketforge_engine::historical::convert::progress::ConversionProgressState,
) {
    println!(
        "{} · {:.2}s",
        outcome.label(),
        start.elapsed().as_secs_f64()
    );
    if outcome == ConversionOutcome::Published {
        println!("Dataset {}", directory.display());
        println!("Manifest {}", directory.join("manifest.json").display());
    } else {
        println!("Staging {}", directory.display());
    }
    if let Ok(receipt) = ConversionExecutionReceipt::load(directory) {
        println!(
            "{} rows · {} identity · {} converted · {} unavailable · {} skipped · {} finalized files · {} row groups",
            receipt.events_written,
            receipt.identity_events,
            receipt.converted_events,
            receipt.unavailable_events,
            receipt.skipped_events,
            receipt.files_written,
            receipt.row_groups_written,
        );
    }
    let snapshots = progress.snapshots();
    if let Some(total) =
        marketforge_engine::historical::convert::progress::ConversionProgress::checked_sum(
            snapshots.values(),
        )
        && total.failures.iter().any(|&count| count > 0)
    {
        println!(
            "Unavailable rows · missing {} · stale {} · future {} · identity mismatch {} · arithmetic {} · no path {}",
            total.failures[0],
            total.failures[1],
            total.failures[2],
            total.failures[3],
            total.failures[4],
            total.failures[5]
        );
    }
    println!("Receipt {}", directory.join("execution.json").display());
    println!(
        "Runtime {}",
        directory.join("conversion-runtime.json").display()
    );
}

#[cfg(feature = "monitor-tui")]
fn join_dashboard_execution(
    progress: &marketforge_engine::historical::convert::progress::ConversionProgressState,
    work: impl FnOnce() -> Result<(), String> + Send,
    dashboard: impl FnOnce(std::sync::mpsc::Sender<()>) -> std::io::Result<()>,
) -> (Result<(), String>, std::io::Result<()>) {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    std::thread::scope(|scope| {
        let (start, ready) = std::sync::mpsc::channel();
        let execution = scope.spawn(move || {
            ready
                .recv()
                .map_err(|_| "conversion interrupted".to_owned())?;
            work()
        });
        let terminal = catch_unwind(AssertUnwindSafe(|| dashboard(start)))
            .unwrap_or_else(|_| Err(std::io::Error::other("conversion renderer panicked")));
        if terminal.is_err() {
            progress.interrupt();
        }
        let result = execution
            .join()
            .unwrap_or_else(|_| Err("conversion execution thread panicked".to_owned()));
        (result, terminal)
    })
}

// -----------------------------------------------------------------------------
// Materialization
// -----------------------------------------------------------------------------

fn materialize_with_reporter(
    job: &ConvertJob,
    prepared: &PreparedConversion,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    match prepared.source.mode {
        UnifiedMergeMode::Trades => {
            materialize_trades_with_reporter(job, prepared, staging, runtime, reporter)
        }
        UnifiedMergeMode::Depth => {
            materialize_depth_with_reporter(job, prepared, staging, runtime, reporter)
        }
        UnifiedMergeMode::Combined => {
            materialize_combined_with_reporter(job, prepared, staging, runtime, reporter)
        }
    }
}

fn materialize_trades_with_reporter(
    job: &ConvertJob,
    prepared: &PreparedConversion,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    let source_manifest = &prepared.source;
    let plan = &prepared.plan;
    if job.output.dataset_path.exists() {
        return Err(format!(
            "converted dataset already exists: {}",
            job.output.dataset_path.display()
        ));
    }

    if staging.exists() {
        return Err(format!(
            "conversion staging directory already exists: {}",
            staging.display()
        ));
    }

    let workers = runtime.workers;
    let parquet = &runtime.parquet;

    let start = Instant::now();

    let metrics: PhysicalConversionMetrics = if workers == 1 {
        // -------------------------------------------------------------
        // Sequential execution
        // -------------------------------------------------------------

        materialize_trade_conversion_with_progress(
            job,
            source_manifest,
            plan,
            staging,
            parquet,
            reporter,
        )
        .map_err(|error| error.to_string())?
    } else {
        // -------------------------------------------------------------
        // Parallel execution
        // -------------------------------------------------------------

        reporter.phase_started(ConversionPhase::PartitionPlanning);

        let partitions =
            build_conversion_partition_plan(&job.input_merge, source_manifest, workers)
                .map_err(|error| error.to_string())?;

        reporter.phase_completed(ConversionPhase::PartitionPlanning);

        conversion_println!("Requested workers : {}", workers);
        conversion_println!("Partitions        : {}", partitions.partitions.len());

        let prepass_start = Instant::now();

        reporter.phase_started(ConversionPhase::ReferencePrepass);

        let prepass = build_conversion_checkpoints_with_progress(
            job,
            &job.input_merge,
            source_manifest,
            plan,
            &partitions,
            reporter,
        )
        .map_err(|error| error.to_string())?;

        reporter.phase_completed(ConversionPhase::ReferencePrepass);

        conversion_println!(
            "Reference prepass : {:.3} s",
            prepass_start.elapsed().as_secs_f64()
        );

        let result = execute_parallel_conversion_with_progress(
            job,
            source_manifest,
            plan,
            &partitions,
            &prepass,
            staging,
            workers,
            parquet,
            reporter,
        )
        .map_err(|error| error.to_string())?;

        conversion_println!("Workers used      : {}", result.workers_used);

        result.metrics
    };
    reporter
        .check_interrupted()
        .map_err(|error| error.to_string())?;
    reporter.phase_started(ConversionPhase::ExecutionReceipt);
    // Persist execution metrics for a later publish-only invocation.
    let receipt =
        ConversionExecutionReceipt::new(job.job_id, &metrics).map_err(|error| error.to_string())?;

    receipt.write(staging).map_err(|error| error.to_string())?;
    reporter.phase_completed(ConversionPhase::ExecutionReceipt);
    let elapsed = start.elapsed().as_secs_f64();

    conversion_println!();
    conversion_println!("{}", "=".repeat(80));
    conversion_println!("CONVERSION MATERIALIZATION SUMMARY");
    conversion_println!("{}", "=".repeat(80));

    conversion_println!("Events read      : {}", metrics.events_read);
    conversion_println!("Events written   : {}", metrics.events_written);
    conversion_println!("Identity         : {}", metrics.identity_events);
    conversion_println!("Converted        : {}", metrics.converted_events);
    conversion_println!("Unavailable      : {}", metrics.unavailable_events);
    conversion_println!("Reference updates: {}", metrics.reference_updates);
    conversion_println!("Files            : {}", metrics.files_written);
    conversion_println!("Row groups       : {}", metrics.row_groups_written);
    conversion_println!("Elapsed          : {:.3} s", elapsed);

    if elapsed > 0.0 {
        conversion_println!(
            "Throughput       : {:.0} events/s",
            metrics.events_written as f64 / elapsed
        );
    }

    conversion_println!(
        "Receipt          : {}",
        staging.join("execution.json").display()
    );
    conversion_println!("Status           : MATERIALIZED — NOT COMMITTED");

    Ok(())
}

fn materialize_depth_with_reporter(
    job: &ConvertJob,
    prepared: &PreparedConversion,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    let source_manifest = &prepared.source;
    let plan = &prepared.plan;
    if job.output.dataset_path.exists() {
        return Err("converted depth dataset already exists".to_owned());
    }

    if staging.exists() {
        return Err("depth conversion staging directory already exists".to_owned());
    }

    let references = prepared
        .external
        .as_ref()
        .expect("validated depth reference");
    let reference_directory = job
        .conversion
        .reference_merge
        .as_deref()
        .expect("validated depth reference path");

    let workers = runtime.workers;
    let parquet = &runtime.parquet;

    let start = Instant::now();

    let metrics = if workers == 1 {
        materialize_depth_conversion_with_progress(
            job,
            source_manifest,
            references,
            plan,
            staging,
            parquet,
            None,
            reporter,
        )
        .map_err(|error| error.to_string())?
    } else {
        reporter.phase_started(ConversionPhase::PartitionPlanning);
        let partitions =
            build_conversion_partition_plan(&job.input_merge, source_manifest, workers)
                .map_err(|error| error.to_string())?;

        reporter.phase_completed(ConversionPhase::PartitionPlanning);
        conversion_println!("Requested workers : {}", workers);
        conversion_println!("Partitions        : {}", partitions.partitions.len());

        let prepass_start = Instant::now();

        reporter.phase_started(ConversionPhase::ReferencePrepass);

        let prepass = build_depth_reference_checkpoints_with_progress(
            &job.input_merge,
            source_manifest,
            reference_directory,
            references,
            plan,
            &partitions,
            reporter,
        )
        .map_err(|error| error.to_string())?;

        reporter.phase_completed(ConversionPhase::ReferencePrepass);

        conversion_println!(
            "Reference prepass : {:.3} s",
            prepass_start.elapsed().as_secs_f64()
        );

        let result = execute_parallel_depth_conversion_with_progress(
            job,
            source_manifest,
            references,
            plan,
            &partitions,
            &prepass,
            staging,
            workers,
            parquet,
            reporter,
        )
        .map_err(|error| error.to_string())?;

        conversion_println!("Workers used      : {}", result.workers_used);

        result.metrics
    };
    reporter
        .check_interrupted()
        .map_err(|error| error.to_string())?;
    reporter.phase_started(ConversionPhase::ExecutionReceipt);
    let receipt = ConversionExecutionReceipt::new_depth(job.job_id, &metrics)
        .map_err(|error| error.to_string())?;

    receipt.write(staging).map_err(|error| error.to_string())?;
    reporter.phase_completed(ConversionPhase::ExecutionReceipt);
    let elapsed = start.elapsed().as_secs_f64();

    conversion_println!();
    conversion_println!("{}", "=".repeat(80));
    conversion_println!("DEPTH CONVERSION MATERIALIZATION SUMMARY");
    conversion_println!("{}", "=".repeat(80));

    conversion_println!("Events read       : {}", metrics.events_read);
    conversion_println!("Events written    : {}", metrics.events_written);
    conversion_println!("Depth levels      : {}", metrics.levels_read);
    conversion_println!("Identity          : {}", metrics.identity_events);
    conversion_println!("Converted         : {}", metrics.converted_events);
    conversion_println!("Unavailable       : {}", metrics.unavailable_events);
    conversion_println!("Converted levels  : {}", metrics.levels_converted);
    conversion_println!("Unavailable levels: {}", metrics.levels_unavailable);
    conversion_println!("Reference updates : {}", metrics.reference_updates);
    conversion_println!("Files             : {}", metrics.files_written);
    conversion_println!("Row groups        : {}", metrics.row_groups_written);
    conversion_println!("Elapsed           : {:.3} s", elapsed);

    if elapsed > 0.0 {
        conversion_println!(
            "Throughput        : {:.0} events/s",
            metrics.events_written as f64 / elapsed
        );
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Publication
// -----------------------------------------------------------------------------

fn publish_with_reporter(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging: &Path,
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    if !staging.is_dir() {
        return Err(format!(
            "conversion staging directory does not exist: {}",
            staging.display()
        ));
    }

    if job.output.dataset_path.exists() {
        return Err(format!(
            "converted dataset already exists: {}",
            job.output.dataset_path.display()
        ));
    }

    let start = Instant::now();

    let result = match source_manifest.mode {
        UnifiedMergeMode::Trades => {
            publish_conversion_with_progress(job, source_manifest, staging, reporter)
        }

        UnifiedMergeMode::Depth => {
            publish_depth_conversion_with_progress(job, source_manifest, staging, workers, reporter)
        }

        UnifiedMergeMode::Combined => publish_combined_conversion_with_progress(
            job,
            source_manifest,
            staging,
            workers,
            reporter,
        ),
    }
    .map_err(|error| error.to_string())?;

    print_publication(&result, start.elapsed().as_secs_f64());

    Ok(())
}

fn print_publication(result: &ConversionPublication, elapsed: f64) {
    conversion_println!();
    conversion_println!("{}", "=".repeat(80));
    conversion_println!("CONVERSION PUBLICATION SUMMARY");
    conversion_println!("{}", "=".repeat(80));

    conversion_println!("Dataset          : {}", result.dataset_path.display());
    conversion_println!("Manifest         : {}", result.manifest_path.display());
    conversion_println!(
        "Receipt          : {}",
        result.dataset_path.join("execution.json").display()
    );
    conversion_println!(
        "Runtime          : {}",
        result
            .dataset_path
            .join("conversion-runtime.json")
            .display()
    );
    conversion_println!("Events           : {}", result.events_written);
    conversion_println!("Files            : {}", result.files_written);
    conversion_println!("Validation time  : {:.3} s", elapsed);
    conversion_println!("Status           : PUBLISHED");
}

// -----------------------------------------------------------------------------
// Source manifest
// -----------------------------------------------------------------------------

fn load_source_manifest(directory: &Path) -> Result<PhysicalMergeManifest, String> {
    let path = directory.join("manifest.json");

    let manifest: PhysicalMergeManifest = serde_json::from_slice(
        &fs::read(&path).map_err(|error| format!("cannot read source manifest: {error}"))?,
    )
    .map_err(|error| format!("invalid source manifest: {error}"))?;

    manifest.validate().map_err(|error| error.to_string())?;

    Ok(manifest)
}

// -----------------------------------------------------------------------------
// Arguments
// -----------------------------------------------------------------------------

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut job = None;
    let mut mode = None;
    let mut workers = None;
    let mut tui = false;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--project-root" | "--config" | "--catalog" | "--normalization-root" => {
                return Err(format!(
                    "{argument} is obsolete for prepared conversion jobs; re-prepare with Forge"
                ));
            }
            "--validate" => set_mode(&mut mode, ExecutionMode::Validate)?,
            "--job" => {
                job = Some(PathBuf::from(args.next().ok_or("missing --job value")?));
            }
            "--tui" => {
                tui = true;
            }
            "--materialize" => {
                set_mode(&mut mode, ExecutionMode::Materialize)?;
            }

            "--publish" => {
                set_mode(&mut mode, ExecutionMode::Publish)?;
            }

            "--run" => {
                set_mode(&mut mode, ExecutionMode::Run)?;
            }

            "--help" | "-h" => {
                conversion_println!(
                    "Usage: marketforge-convert --job PATH \
                     [--validate | --materialize | --publish | --run] \
                     [--workers N] [--tui]"
                );

                std::process::exit(0);
            }
            "--workers" => {
                if workers.is_some() {
                    return Err("duplicate --workers argument".to_owned());
                }

                let value = args.next().ok_or("missing --workers value")?;

                let count = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --workers value")?;

                if count == 0 {
                    return Err("--workers must be positive".to_owned());
                }

                workers = Some(count);
            }

            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }

    Ok(Args {
        job: job.ok_or("missing required --job argument")?,
        mode: mode.unwrap_or(ExecutionMode::Run),
        workers,
        tui,
    })
}

fn set_mode(current: &mut Option<ExecutionMode>, requested: ExecutionMode) -> Result<(), String> {
    if current.is_some() {
        return Err("conversion execution modes are mutually exclusive".to_owned());
    }

    *current = Some(requested);

    Ok(())
}

fn materialize_combined_with_reporter(
    job: &ConvertJob,
    prepared: &PreparedConversion,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    let source_manifest = &prepared.source;
    let plan = &prepared.plan;
    if job.output.dataset_path.exists() {
        return Err("converted combined dataset already exists".to_owned());
    }

    if staging.exists() {
        return Err("combined conversion staging already exists".to_owned());
    }

    if source_manifest.mode != UnifiedMergeMode::Combined {
        return Err("combined materializer requires combined source".to_owned());
    }

    // ---------------------------------------------------------------------
    // Use the validated physical publication and its reference bindings.
    let reference_directory = job.conversion.reference_merge.as_deref();
    let reference_manifest = prepared.external.as_ref();
    let execution_job = job;

    // ---------------------------------------------------------------------
    // Select internal and external reference streams
    // ---------------------------------------------------------------------

    let selection = CombinedReferenceSelection::resolve(plan, source_manifest, reference_manifest)
        .map_err(|error| error.to_string())?;

    // ---------------------------------------------------------------------
    // Build physical partitions
    // ---------------------------------------------------------------------

    let workers = runtime.workers;
    let requested_partitions = workers
        .checked_mul(4)
        .ok_or("conversion partition count overflow")?;

    reporter.phase_started(ConversionPhase::PartitionPlanning);
    let partitions = build_conversion_partition_plan_with_count(
        &execution_job.input_merge,
        source_manifest,
        requested_partitions,
    )
    .map_err(|error| error.to_string())?;

    reporter.phase_completed(ConversionPhase::PartitionPlanning);
    conversion_println!("Requested workers : {}", workers);
    conversion_println!("Partitions        : {}", partitions.partitions.len());

    // ---------------------------------------------------------------------
    // Build historical reference checkpoints
    // ---------------------------------------------------------------------

    let start = Instant::now();
    let prepass_start = Instant::now();

    reporter.phase_started(ConversionPhase::ReferencePrepass);

    let prepass = build_combined_reference_checkpoints_with_progress(
        &execution_job.input_merge,
        source_manifest,
        reference_directory,
        reference_manifest,
        &selection,
        &partitions,
        reporter,
    )
    .map_err(|error| error.to_string())?;

    reporter.phase_completed(ConversionPhase::ReferencePrepass);

    conversion_println!(
        "Reference prepass : {:.3} s",
        prepass_start.elapsed().as_secs_f64()
    );

    // ---------------------------------------------------------------------
    // Parallel combined materialization
    // ---------------------------------------------------------------------

    let parquet = &runtime.parquet;

    let result = execute_parallel_combined_conversion_with_progress(
        execution_job,
        source_manifest,
        reference_manifest,
        plan,
        &partitions,
        &prepass,
        &selection,
        staging,
        workers,
        parquet,
        reporter,
    )
    .map_err(|error| error.to_string())?;

    // ---------------------------------------------------------------------
    // Persist execution receipt
    // ---------------------------------------------------------------------

    reporter
        .check_interrupted()
        .map_err(|error| error.to_string())?;
    reporter.phase_started(ConversionPhase::ExecutionReceipt);

    let receipt = ConversionExecutionReceipt::new_combined(execution_job.job_id, &result.metrics)
        .map_err(|error| error.to_string())?;

    receipt.write(staging).map_err(|error| error.to_string())?;

    reporter.phase_completed(ConversionPhase::ExecutionReceipt);

    // ---------------------------------------------------------------------
    // Summary
    // ---------------------------------------------------------------------

    let metrics = &result.metrics;
    let elapsed = start.elapsed().as_secs_f64();

    conversion_println!();
    conversion_println!("{}", "=".repeat(80));
    conversion_println!("COMBINED CONVERSION MATERIALIZATION SUMMARY");
    conversion_println!("{}", "=".repeat(80));

    conversion_println!("Events read       : {}", metrics.events_read);
    conversion_println!("Events written    : {}", metrics.events_written);

    conversion_println!("Trades            : {}", metrics.trade_events);
    conversion_println!("Depth             : {}", metrics.depth_events);
    conversion_println!("Depth levels      : {}", metrics.depth_levels_read);

    conversion_println!("Identity          : {}", metrics.identity_events);
    conversion_println!("Converted         : {}", metrics.converted_events);
    conversion_println!("Unavailable       : {}", metrics.unavailable_events);

    conversion_println!("Converted levels  : {}", metrics.depth_levels_converted);
    conversion_println!("Unavailable levels: {}", metrics.depth_levels_unavailable);

    conversion_println!("Internal refs     : {}", metrics.internal_reference_updates);
    conversion_println!("External refs     : {}", metrics.external_reference_updates);

    conversion_println!("Workers used      : {}", result.workers_used);
    conversion_println!("Files             : {}", metrics.files_written);
    conversion_println!("Row groups        : {}", metrics.row_groups_written);
    conversion_println!("Skipped           : {}", metrics.skipped_events);
    conversion_println!("Skipped levels    : {}", metrics.depth_levels_skipped);
    conversion_println!("Elapsed           : {:.3} s", elapsed);

    if elapsed > 0.0 {
        conversion_println!(
            "Throughput        : {:.0} events/s",
            metrics.events_written as f64 / elapsed
        );
    }

    Ok(())
}

#[cfg(test)]
#[path = "../../tests/historical/support/conversion_fixture.rs"]
mod conversion_fixture;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/historical/convert/support/depth_fixture.rs"]
mod depth_fixture;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/historical/convert/support/market.rs"]
mod market;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_uses_prepared_runtime_and_caps_worker_overrides() {
        let mut job = conversion_fixture::canonical_job();
        job.resources.workers = 2;
        let parse = |args: &[&str]| parse_args(args.iter().map(|value| value.to_string()));
        let args = parse(&["--job", "job.json", "--workers", "1"]).unwrap();
        let runtime = resolve_runtime(&args, &job).unwrap();
        assert_eq!(runtime.workers, 1);
        assert_eq!(runtime.parquet, job.parquet);
        let args = parse(&["--job", "job.json", "--workers", "3"]).unwrap();
        assert!(
            resolve_runtime(&args, &job)
                .unwrap_err()
                .contains("exceed configured limit 2")
        );
        assert!(parse(&["--job", "job.json", "--catalog", "missing.json"]).is_err());
    }

    #[test]
    fn cli_reports_materialization_once_for_each_mode_and_worker_count() {
        use marketforge_engine::historical::convert::progress::ConversionProgressEvent as Event;
        use std::sync::mpsc;

        for mode in [
            UnifiedMergeMode::Trades,
            UnifiedMergeMode::Depth,
            UnifiedMergeMode::Combined,
        ] {
            for workers in [1, 2] {
                let temp = tempfile::tempdir().unwrap();
                let root = temp.path();
                let mut job = conversion_fixture::canonical_job();
                job.input_merge = root.join("source");
                job.output.dataset_path = root.join("output");
                job.parquet = market::parquet();
                let source = market::create(&job.input_merge, mode, true);
                job.source = market::prepared_binding(&job.input_merge, &source);
                if mode == UnifiedMergeMode::Depth {
                    let path = root.join("reference");
                    let external = market::create(&path, UnifiedMergeMode::Trades, false);
                    job.external_reference = Some(market::prepared_binding(&path, &external));
                    job.conversion.reference_merge = Some(path);
                }
                let staging = root.join("staging");
                let runtime = ConversionRuntime {
                    workers,
                    parquet: market::parquet(),
                };
                let launch = |mode| {
                    let (send, receive) = mpsc::channel();
                    execute_conversion_with_progress(
                        &job,
                        &staging,
                        &runtime,
                        mode,
                        &ConversionProgressReporter::new(send),
                    )
                    .unwrap();
                    receive.try_iter().collect::<Vec<_>>()
                };
                let valid = launch(ExecutionMode::Validate);
                assert!(matches!(
                    valid.as_slice(),
                    [
                        Event::PhaseStarted {
                            phase: ConversionPhase::SourceValidation
                        },
                        Event::PhaseCompleted {
                            phase: ConversionPhase::SourceValidation
                        },
                        Event::Finished(ConversionOutcome::Validated),
                    ]
                ));
                assert!(!staging.exists());
                let mut events = launch(if workers == 1 {
                    ExecutionMode::Materialize
                } else {
                    ExecutionMode::Run
                });
                if workers == 1 {
                    assert!(matches!(
                        events.pop(),
                        Some(Event::Finished(ConversionOutcome::Materialized))
                    ));
                    assert!(staging.is_dir() && !job.output.dataset_path.exists());
                }
                let mut conversion = None;
                let mut completed = None;
                let mut totals = false;
                let mut starts = 0;
                let mut ends = 0;
                let mut prepass_finished = false;
                for (index, event) in events.iter().enumerate() {
                    match event {
                        Event::PhaseCompleted {
                            phase:
                                ConversionPhase::ReferencePrepass | ConversionPhase::TimestampScan,
                        } => prepass_finished = true,
                        Event::PhaseStarted {
                            phase: ConversionPhase::PhysicalConversion,
                        } => {
                            assert!(prepass_finished && conversion.replace(index).is_none());
                        }
                        Event::PartitionStarted { .. } => {
                            assert!(conversion.is_some() && completed.is_none());
                            starts += 1;
                        }
                        Event::PartitionCompleted { .. } => {
                            assert!(conversion.is_some() && completed.is_none());
                            ends += 1;
                        }
                        Event::MaterializationTotals(metrics) => {
                            assert!(!totals && ends > 0 && starts == ends);
                            assert_eq!(metrics.events_written, source.events_written);
                            totals = true;
                        }
                        Event::PhaseCompleted {
                            phase: ConversionPhase::PhysicalConversion,
                        } => {
                            assert!(totals && completed.replace(index).is_none());
                        }
                        Event::PhaseStarted {
                            phase: ConversionPhase::PhysicalValidation,
                        } => {
                            assert!(completed.is_some())
                        }
                        _ => {}
                    }
                }
                assert!(completed > conversion && starts > 0);
                if workers == 1 {
                    let publish = launch(ExecutionMode::Publish);
                    assert!(matches!(
                        publish.last(),
                        Some(Event::Finished(ConversionOutcome::Published))
                    ));
                    assert!(
                        publish
                            .iter()
                            .any(|event| matches!(event, Event::PublishedReceipt(_)))
                    );
                    assert!(!publish.iter().any(|event| matches!(
                        event,
                        Event::PhaseStarted {
                            phase: ConversionPhase::PhysicalConversion
                        }
                    )));
                } else {
                    assert!(matches!(
                        events.last(),
                        Some(Event::Finished(ConversionOutcome::Published))
                    ));
                }
            }
        }
    }

    #[cfg(feature = "monitor-tui")]
    #[test]
    fn renderer_failure_cancels_and_joins_the_worker() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        };
        let progress = ConversionProgressReporter::disabled().state();
        let (entered, wait_entered) = mpsc::channel();
        let joined = AtomicBool::new(false);
        let (result, renderer) = join_dashboard_execution(
            &progress,
            || {
                entered.send(()).unwrap();
                while !progress.is_interrupted() {
                    std::thread::yield_now();
                }
                joined.store(true, Ordering::Release);
                Err("conversion interrupted".into())
            },
            |start| {
                start.send(()).unwrap();
                wait_entered.recv().unwrap();
                Err(std::io::Error::other("renderer error"))
            },
        );
        assert!(joined.load(Ordering::Acquire));
        assert_eq!(result.unwrap_err(), "conversion interrupted");
        assert_eq!(renderer.unwrap_err().to_string(), "renderer error");
    }

    #[cfg(feature = "monitor-tui")]
    #[test]
    fn channel_disconnect_propagates_worker_panic() {
        let (sender, receiver) = std::sync::mpsc::channel::<()>();
        let progress = ConversionProgressReporter::disabled().state();
        let (result, renderer) = join_dashboard_execution(
            &progress,
            move || {
                let _owned = sender;
                panic!("test worker panic");
            },
            |start| {
                start.send(()).unwrap();
                assert!(receiver.recv().is_err());
                Err(std::io::Error::other("channel disconnected"))
            },
        );
        assert!(progress.is_interrupted());
        assert_eq!(result.unwrap_err(), "conversion execution thread panicked");
        assert_eq!(renderer.unwrap_err().to_string(), "channel disconnected");
    }
}
