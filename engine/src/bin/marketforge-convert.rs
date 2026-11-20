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
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog_with_normalization_root,
    marketforge_engine::historical::job::ConvertJob,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::historical::merge::UnifiedMergeMode,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
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
    project_root: PathBuf,
    job: PathBuf,
    config: PathBuf,
    catalog: PathBuf,
    normalization_root: Option<PathBuf>,
    mode: ExecutionMode,
    workers: Option<usize>,
    tui: bool,
}

#[derive(Debug, serde::Serialize)]
struct ConversionRuntime {
    workers: usize,
    parquet: MergeParquetConfig,
    configuration_sha256: String,
    #[cfg(feature = "monitor-tui")]
    #[serde(skip)]
    config_path: PathBuf,
    #[serde(skip)]
    catalog_path: PathBuf,
    #[serde(skip)]
    normalization_root: Option<PathBuf>,
}

fn resolve_runtime(
    args: &Args,
    root: &Path,
    job: &mut ConvertJob,
) -> Result<ConversionRuntime, String> {
    use marketforge_engine::historical::normalize::manifest::file_sha256;
    use marketforge_engine::shared::historical::config::load_conversion_runtime_settings;
    let config = resolve_path(root, &args.config);
    let checksum = || {
        file_sha256(&config)
            .map_err(|error| format!("historical configuration {}: {error}", config.display()))
    };
    let configuration_sha256 = checksum()?;
    let (limits, parquet) =
        load_conversion_runtime_settings(&config, root).map_err(|error| error.to_string())?;
    if checksum()? != configuration_sha256 {
        return Err("historical configuration changed during conversion preparation".into());
    }
    job.resources.workers = job.resources.workers.min(limits.workers);
    job.resources.memory_limit_mb = job.resources.memory_limit_mb.min(limits.memory_limit_mb);
    job.resources.max_open_files = job.resources.max_open_files.min(limits.max_open_files);
    let workers = args.workers.unwrap_or(job.resources.workers);
    if workers == 0 || workers > job.resources.workers {
        return Err(format!(
            "requested workers {workers} exceed configured limit {}",
            job.resources.workers
        ));
    }
    Ok(ConversionRuntime {
        workers,
        parquet,
        configuration_sha256,
        #[cfg(feature = "monitor-tui")]
        config_path: config,
        catalog_path: resolve_path(root, &args.catalog),
        normalization_root: args
            .normalization_root
            .as_ref()
            .map(|path| resolve_path(root, path)),
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
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    runtime: &ConversionRuntime,
    mode: ExecutionMode,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    let result = (|| {
        reporter
            .check_interrupted()
            .map_err(|error| error.to_string())?;
        if mode == ExecutionMode::Validate {
            reporter.phase_started(ConversionPhase::SourceValidation);
            verify_input_inventory(&job.input_merge, source_manifest)?;
            reporter.phase_completed(ConversionPhase::SourceValidation);
            let plan = resolve_plan(job, source_manifest, root, runtime, reporter)?;
            let external = job
                .conversion
                .reference_merge
                .as_ref()
                .map(|directory| {
                    let manifest = load_source_manifest(directory)?;
                    verify_input_inventory(directory, &manifest)?;
                    Ok::<_, String>(manifest)
                })
                .transpose()?;
            if source_manifest.mode == UnifiedMergeMode::Combined {
                CombinedReferenceSelection::resolve(&plan, source_manifest, external.as_ref())
                    .map_err(|error| error.to_string())?;
            }
            if source_manifest.mode == UnifiedMergeMode::Depth && external.is_none() {
                return Err("depth conversion requires an external reference merge".into());
            }
            if external
                .as_ref()
                .is_some_and(|manifest| manifest.mode != UnifiedMergeMode::Trades)
            {
                return Err(
                    "external last-trade references require a trade-only physical merge".into(),
                );
            }
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
            "runtime": runtime, "resources": job.resources,
        }))
        .map_err(|error| error.to_string())?;
        if mode != ExecutionMode::Publish {
            reporter.phase_started(ConversionPhase::SourceValidation);
            verify_input_inventory(&job.input_merge, source_manifest)?;
            reporter.phase_completed(ConversionPhase::SourceValidation);
            materialize_with_reporter(job, source_manifest, root, staging, runtime, reporter)?;
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
                return Err("conversion runtime differs from materialization; use the original configuration and worker count".to_owned());
            }
            publish_with_reporter(job, source_manifest, staging, runtime.workers, reporter)?;
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

    let root = args
        .project_root
        .canonicalize()
        .map_err(|error| format!("invalid project root: {error}"))?;

    let job_path = resolve_path(&root, &args.job);

    let mut job = load_convert_job(&job_path).map_err(|error| error.to_string())?;

    job.input_merge = resolve_path(&root, &job.input_merge);
    job.output.dataset_path = resolve_path(&root, &job.output.dataset_path);

    job.validate().map_err(|error| error.to_string())?;
    let runtime = resolve_runtime(&args, &root, &mut job)?;
    let source_manifest = load_source_manifest(&job.input_merge)?;
    let output = job.output.dataset_path.clone();
    let staging = output.with_extension("staging");
    if args.tui {
        #[cfg(feature = "monitor-tui")]
        {
            return run_conversion_tui(job, source_manifest, root, staging, runtime, args.mode);
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
        &source_manifest,
        &root,
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
    root: PathBuf,
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
        title: format!(
            "Job {} · {:?} · {:?}",
            job.job_id, mode, source_manifest.mode
        ),
        details: vec![
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
                "Runtime {} ({})",
                runtime.config_path.display(),
                runtime.configuration_sha256
            ),
            format!(
                "{} effective workers · configured caps {} workers, {} MB, {} open files",
                runtime.workers,
                job.resources.workers,
                job.resources.memory_limit_mb,
                job.resources.max_open_files
            ),
        ],
        expected_rows: source_manifest.events_written,
    };
    let output = job.output.dataset_path.clone();
    let staged = staging.clone();
    let start = Instant::now();
    let (result, terminal) = join_dashboard_execution(
        &progress_state,
        move || {
            set_conversion_tui_mode(true);
            execute_conversion_with_progress(
                &job,
                &source_manifest,
                &root,
                &staging,
                &runtime,
                mode,
                &reporter,
            )
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
        match mode {
            ExecutionMode::Validate => {
                println!("VALIDATED · {:.2}s", start.elapsed().as_secs_f64())
            }
            ExecutionMode::Materialize => {
                print_dashboard_summary(ConversionOutcome::Materialized, &staged, start)
            }
            ExecutionMode::Run | ExecutionMode::Publish => {
                print_dashboard_summary(ConversionOutcome::Published, &output, start)
            }
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
fn print_dashboard_summary(outcome: ConversionOutcome, directory: &Path, start: Instant) {
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
            "{} rows · {} identity · {} converted · {} unavailable · {} skipped · {} files",
            receipt.events_written,
            receipt.identity_events,
            receipt.converted_events,
            receipt.unavailable_events,
            receipt.skipped_events,
            receipt.files_written
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
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    match source_manifest.mode {
        UnifiedMergeMode::Trades => {
            materialize_trades_with_reporter(job, source_manifest, root, staging, runtime, reporter)
        }

        UnifiedMergeMode::Depth => {
            materialize_depth_with_reporter(job, source_manifest, root, staging, runtime, reporter)
        }

        UnifiedMergeMode::Combined => materialize_combined_with_reporter(
            job,
            source_manifest,
            root,
            staging,
            runtime,
            reporter,
        ),
    }
}
fn materialize_trades_with_reporter(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
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

    let plan = resolve_plan(job, source_manifest, root, runtime, reporter)?;

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
            &plan,
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
            &plan,
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
            &plan,
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
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    if job.output.dataset_path.exists() {
        return Err("converted depth dataset already exists".to_owned());
    }

    if staging.exists() {
        return Err("depth conversion staging directory already exists".to_owned());
    }

    let plan = resolve_plan(job, source_manifest, root, runtime, reporter)?;

    let reference_directory = job
        .conversion
        .reference_merge
        .as_ref()
        .ok_or("depth conversion requires an external reference merge")?;

    let reference_directory = resolve_path(root, reference_directory);

    let references = load_source_manifest(&reference_directory)?;
    verify_input_inventory(&reference_directory, &references)?;

    let workers = runtime.workers;
    let parquet = &runtime.parquet;

    let start = Instant::now();

    let metrics = if workers == 1 {
        materialize_depth_conversion_with_progress(
            job,
            source_manifest,
            &references,
            &plan,
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
            &reference_directory,
            &references,
            &plan,
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
            &references,
            &plan,
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

fn resolve_plan(
    job: &ConvertJob,
    source: &PhysicalMergeManifest,
    root: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<marketforge_engine::historical::convert::ConversionPlan, String> {
    reporter.phase_started(ConversionPhase::CatalogResolution);
    let catalog = InstrumentCatalog::load(&runtime.catalog_path)
        .map_err(|error| format!("catalog {}: {error}", runtime.catalog_path.display()))?;
    let resolved = resolve_conversion_catalog_with_normalization_root(
        job,
        source,
        &catalog,
        root,
        runtime.normalization_root.as_deref(),
    )
    .map_err(|error| error.to_string())?;
    reporter.phase_completed(ConversionPhase::CatalogResolution);
    reporter
        .check_interrupted()
        .map_err(|error| error.to_string())?;
    reporter.phase_started(ConversionPhase::ConversionPlanning);
    let plan = plan_conversion(job, &resolved).map_err(|error| error.to_string())?;
    if source.mode == UnifiedMergeMode::Trades && plan.unresolved_streams > 0 {
        return Err(format!(
            "{} conversion streams have no available path",
            plan.unresolved_streams
        ));
    }
    reporter.phase_completed(ConversionPhase::ConversionPlanning);
    Ok(plan)
}

fn verify_input_inventory(
    directory: &Path,
    manifest: &PhysicalMergeManifest,
) -> Result<(), String> {
    marketforge_engine::historical::convert::execution::validate_conversion_input(
        directory, manifest,
    )
    .map_err(|error| error.to_string())
}

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
    let mut project_root = env::current_dir().map_err(|error| error.to_string())?;

    let mut job = None;
    let mut mode = None;
    let mut workers = None;
    let mut tui = false;
    let mut config = PathBuf::from("data/.config/historical/general.toml");
    let mut catalog = PathBuf::from("data/catalog/instruments.json");
    let mut normalization_root = None;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--project-root" => {
                project_root = PathBuf::from(args.next().ok_or("missing --project-root value")?);
            }

            "--config" => {
                config = PathBuf::from(args.next().ok_or("missing --config value")?);
            }

            "--catalog" => {
                catalog = PathBuf::from(args.next().ok_or("missing --catalog value")?);
            }
            "--normalization-root" => {
                normalization_root = Some(PathBuf::from(
                    args.next().ok_or("missing --normalization-root value")?,
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
                     [--project-root PATH] [--config PATH] \
                     [--validate | --materialize | --publish | --run] \
                     [--workers N] [--catalog PATH] [--normalization-root PATH] [--tui]"
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
        project_root,
        config,
        catalog,
        normalization_root,
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

fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn materialize_combined_with_reporter(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    runtime: &ConversionRuntime,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
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
    // Resolve instrument catalog and conversion plan
    // ---------------------------------------------------------------------

    let plan = resolve_plan(job, source_manifest, root, runtime, reporter)?;

    // ---------------------------------------------------------------------
    // Load external references when configured
    // ---------------------------------------------------------------------

    let reference_directory = job
        .conversion
        .reference_merge
        .as_ref()
        .map(|path| resolve_path(root, path));

    let reference_manifest = match &reference_directory {
        Some(directory) => {
            let manifest = load_source_manifest(directory)?;
            verify_input_inventory(directory, &manifest)?;
            Some(manifest)
        }
        None => None,
    };

    // The reference cursor uses job.conversion.reference_merge.
    // Ensure that the worker receives the resolved absolute path.
    let mut execution_job = job.clone();

    execution_job.conversion.reference_merge = reference_directory.clone();

    // ---------------------------------------------------------------------
    // Select internal and external reference streams
    // ---------------------------------------------------------------------

    let selection =
        CombinedReferenceSelection::resolve(&plan, source_manifest, reference_manifest.as_ref())
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
        reference_directory.as_deref(),
        reference_manifest.as_ref(),
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
        &execution_job,
        source_manifest,
        reference_manifest.as_ref(),
        &plan,
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
mod tests {
    use super::*;

    #[test]
    fn cli_resolves_shared_runtime_and_caps_worker_overrides() {
        let root = tempfile::tempdir().unwrap();
        let mut job = conversion_fixture::canonical_job();
        job.input_merge = root.path().join("input");
        job.output.dataset_path = root.path().join("output");
        job.resources.workers = 4;
        job.resources.memory_limit_mb = 128;
        job.resources.max_open_files = 32;
        let defaults = parse_args(["--job", "job.json"].map(String::from).into_iter()).unwrap();
        assert_eq!(
            defaults.config,
            Path::new("data/.config/historical/general.toml")
        );
        let config = include_str!("../shared/historical/config/fixtures/runtime.toml")
            .replace("workers = 1", "workers = 2")
            .replace(
                "row_group_target_bytes = 67108864",
                "row_group_target_bytes = 1048576",
            );
        fs::write(root.path().join("runtime.toml"), config).unwrap();
        let mut explicit = parse_args(
            [
                "--job",
                "job.json",
                "--config",
                "runtime.toml",
                "--workers",
                "1",
            ]
            .map(String::from)
            .into_iter(),
        )
        .unwrap();
        let runtime = resolve_runtime(&explicit, root.path(), &mut job).unwrap();
        assert_eq!(runtime.workers, 1);
        assert_eq!(runtime.parquet.row_group_target_mb, 1);
        assert_eq!(runtime.configuration_sha256.len(), 64);
        assert_eq!(job.resources.workers, 2);
        assert_eq!(job.resources.memory_limit_mb, 128);
        explicit.workers = Some(3);
        assert!(
            resolve_runtime(&explicit, root.path(), &mut job)
                .unwrap_err()
                .contains("exceed configured limit 2")
        );
        explicit.config = "missing.toml".into();
        assert!(
            resolve_runtime(&explicit, root.path(), &mut job)
                .unwrap_err()
                .contains("missing.toml")
        );
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
