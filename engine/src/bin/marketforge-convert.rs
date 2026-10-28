use marketforge_engine::historical::convert::execution::execute_parallel_combined_conversion_with_progress;
use marketforge_engine::historical::convert::execution::execute_parallel_conversion_with_progress;
use marketforge_engine::historical::convert::execution::execute_parallel_depth_conversion_with_progress;
use marketforge_engine::historical::convert::output::{
    publish_combined_conversion_with_progress, publish_conversion_with_progress,
    publish_depth_conversion_with_progress,
};
use marketforge_engine::historical::convert::progress::{
    ConversionPhase, ConversionProgressReporter,
};
use std::cell::Cell;
use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};
use {
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::execution::PhysicalConversionMetrics,
    marketforge_engine::historical::convert::execution::build_combined_reference_checkpoints_with_progress,
    marketforge_engine::historical::convert::execution::build_conversion_checkpoints,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan_with_count,
    marketforge_engine::historical::convert::execution::build_depth_reference_checkpoints,
    marketforge_engine::historical::convert::execution::materialize_depth_conversion,
    marketforge_engine::historical::convert::execution::materialize_trade_conversion,
    marketforge_engine::historical::convert::output::ConversionExecutionReceipt,
    marketforge_engine::historical::convert::output::ConversionPublication,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
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
    Materialize,
    Publish,
    Run,
}

struct Args {
    project_root: PathBuf,
    job: PathBuf,
    mode: ExecutionMode,
    workers: Option<usize>,
    tui: bool,
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fn main() {
    if let Err(error) = run() {
        eprintln!("Conversion failed: {error}");
        std::process::exit(1);
    }
}
fn execute_conversion_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
    mode: ExecutionMode,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    let result = execute_conversion_with_progress_inner(
        job,
        source_manifest,
        root,
        staging,
        workers,
        mode,
        reporter,
    );

    reporter.finished(result.is_ok());

    result
}

fn execute_conversion_with_progress_inner(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
    mode: ExecutionMode,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    match mode {
        ExecutionMode::Materialize => {
            materialize_with_progress(job, source_manifest, root, staging, workers, reporter)
        }

        ExecutionMode::Publish => {
            publish_with_progress(job, source_manifest, staging, workers, reporter)
        }

        ExecutionMode::Run => {
            materialize_with_progress(job, source_manifest, root, staging, workers, reporter)?;

            publish_with_progress(job, source_manifest, staging, workers, reporter)
        }
    }
}
fn materialize_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    reporter.phase_started(ConversionPhase::SourceValidation);

    source_manifest
        .validate()
        .map_err(|error| error.to_string())?;

    reporter.phase_completed(ConversionPhase::SourceValidation);

    let result = materialize_with_reporter(job, source_manifest, root, staging, workers, reporter);

    result
}
fn run() -> Result<(), String> {
    let args = parse_args()?;

    let root = args
        .project_root
        .canonicalize()
        .map_err(|error| format!("invalid project root: {error}"))?;

    let job_path = resolve_path(&root, &args.job);

    let mut job = load_convert_job(&job_path).map_err(|error| error.to_string())?;

    job.input_merge = resolve_path(&root, &job.input_merge);
    job.output.dataset_path = resolve_path(&root, &job.output.dataset_path);

    job.validate().map_err(|error| error.to_string())?;
    let workers = args.workers.unwrap_or(job.resources.workers as usize);

    if workers == 0 {
        return Err("conversion worker count must be positive".to_owned());
    }

    if workers > job.resources.workers as usize {
        return Err(format!(
            "requested workers {} exceed configured limit {}",
            workers, job.resources.workers,
        ));
    }
    let source_manifest = load_source_manifest(&job.input_merge)?;
    let output = job.output.dataset_path.clone();
    let staging = output.with_extension("staging");
    if args.tui {
        #[cfg(feature = "monitor-tui")]
        {
            return run_conversion_tui(job, source_manifest, root, staging, workers, args.mode);
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
    conversion_println!("Events expected : {}", source_manifest.events_written);

    match args.mode {
        ExecutionMode::Materialize => {
            materialize(&job, &source_manifest, &root, &staging, workers)?;
        }

        ExecutionMode::Publish => {
            publish(&job, &source_manifest, &staging, workers)?;
        }

        ExecutionMode::Run => {
            materialize(&job, &source_manifest, &root, &staging, workers)?;
            publish(&job, &source_manifest, &staging, workers)?;
        }
    }

    Ok(())
}
#[cfg(feature = "monitor-tui")]
fn run_conversion_tui(
    job: ConvertJob,
    source_manifest: PhysicalMergeManifest,
    root: PathBuf,
    staging: PathBuf,
    workers: usize,
    mode: ExecutionMode,
) -> Result<(), String> {
    use std::sync::mpsc;

    let (sender, receiver) = mpsc::channel();

    let reporter = ConversionProgressReporter::new(sender);
    let progress_state = reporter.state();

    let execution = std::thread::spawn(move || {
        set_conversion_tui_mode(true);

        execute_conversion_with_progress(
            &job,
            &source_manifest,
            &root,
            &staging,
            workers,
            mode,
            &reporter,
        )
    });

    // Temporary event consumer.
    // The Ratatui dashboard will replace this in Part 2.
    marketforge_engine::tui::conversion::execute_conversion_with_tui(receiver, progress_state)
        .map_err(|error| error.to_string())?;

    execution
        .join()
        .map_err(|_| "conversion execution thread panicked".to_owned())?
}

// -----------------------------------------------------------------------------
// Materialization
// -----------------------------------------------------------------------------

fn materialize(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
) -> Result<(), String> {
    materialize_with_reporter(
        job,
        source_manifest,
        root,
        staging,
        workers,
        &ConversionProgressReporter::disabled(),
    )
}

fn materialize_with_reporter(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    match source_manifest.mode {
        UnifiedMergeMode::Trades => {
            materialize_trades_with_reporter(job, source_manifest, root, staging, workers, reporter)
        }

        UnifiedMergeMode::Depth => {
            materialize_depth_with_reporter(job, source_manifest, root, staging, workers, reporter)
        }

        UnifiedMergeMode::Combined => materialize_combined_with_reporter(
            job,
            source_manifest,
            root,
            staging,
            workers,
            reporter,
        ),
    }
}
fn materialize_trades_with_reporter(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
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

    reporter.phase_started(ConversionPhase::CatalogResolution);

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json"))
        .map_err(|error| error.to_string())?;

    let resolved = resolve_conversion_catalog(job, source_manifest, &catalog, root)
        .map_err(|error| error.to_string())?;

    reporter.phase_completed(ConversionPhase::CatalogResolution);

    reporter.phase_started(ConversionPhase::ConversionPlanning);

    let plan = plan_conversion(job, &resolved).map_err(|error| error.to_string())?;

    reporter.phase_completed(ConversionPhase::ConversionPlanning);

    if plan.unresolved_streams > 0 {
        return Err(format!(
            "{} conversion streams have no available path",
            plan.unresolved_streams
        ));
    }

    // Current materializer emits one selected path per event.
    // Reject genuinely ambiguous multi-path plans until the
    // configured `all` policy is implemented.
    if plan.streams.iter().any(|stream| stream.paths.len() > 1) {
        return Err(
            "multiple conversion paths are not yet supported by physical materialization"
                .to_owned(),
        );
    }

    let parquet = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 512,
        compression_level: 3,
    };

    let start = Instant::now();

    let metrics: PhysicalConversionMetrics = if workers == 1 {
        // -------------------------------------------------------------
        // Sequential execution
        // -------------------------------------------------------------

        materialize_trade_conversion(job, source_manifest, &plan, staging, &parquet)
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

        let prepass = build_conversion_checkpoints(
            job,
            &job.input_merge,
            source_manifest,
            &plan,
            &partitions,
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
            &parquet,
            reporter,
        )
        .map_err(|error| error.to_string())?;

        conversion_println!("Workers used      : {}", result.workers_used);

        result.metrics
    };
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
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    if job.output.dataset_path.exists() {
        return Err("converted depth dataset already exists".to_owned());
    }

    if staging.exists() {
        return Err("depth conversion staging directory already exists".to_owned());
    }

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json"))
        .map_err(|error| error.to_string())?;

    let resolved = resolve_conversion_catalog(job, source_manifest, &catalog, root)
        .map_err(|error| error.to_string())?;

    let plan = plan_conversion(job, &resolved).map_err(|error| error.to_string())?;

    if plan.streams.iter().any(|stream| stream.paths.len() > 1) {
        return Err("multiple conversion paths are not yet supported".to_owned());
    }

    let reference_directory = job
        .conversion
        .reference_merge
        .as_ref()
        .ok_or("depth conversion requires an external reference merge")?;

    let reference_directory = resolve_path(root, reference_directory);

    let references = load_source_manifest(&reference_directory)?;

    let parquet = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 512,
        compression_level: 3,
    };

    let start = Instant::now();

    let metrics = if workers == 1 {
        materialize_depth_conversion(
            job,
            source_manifest,
            &references,
            &plan,
            staging,
            &parquet,
            None,
        )
        .map_err(|error| error.to_string())?
    } else {
        let partitions =
            build_conversion_partition_plan(&job.input_merge, source_manifest, workers)
                .map_err(|error| error.to_string())?;

        conversion_println!("Requested workers : {}", workers);
        conversion_println!("Partitions        : {}", partitions.partitions.len());

        let prepass_start = Instant::now();

        reporter.phase_started(ConversionPhase::ReferencePrepass);

        let prepass = build_depth_reference_checkpoints(
            &job.input_merge,
            source_manifest,
            &reference_directory,
            &references,
            &plan,
            &partitions,
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
            &parquet,
            reporter,
        )
        .map_err(|error| error.to_string())?;

        conversion_println!("Workers used      : {}", result.workers_used);

        result.metrics
    };
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

    conversion_println!("Status            : MATERIALIZED — NOT COMMITTED");

    Ok(())
}

// -----------------------------------------------------------------------------
// Publication
// -----------------------------------------------------------------------------

fn publish(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging: &Path,
    workers: usize,
) -> Result<(), String> {
    publish_with_reporter(
        job,
        source_manifest,
        staging,
        workers,
        &ConversionProgressReporter::disabled(),
    )
}

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

fn parse_args() -> Result<Args, String> {
    let mut project_root = env::current_dir().map_err(|error| error.to_string())?;

    let mut job = None;
    let mut mode = None;
    let mut workers = None;
    let mut tui = false;
    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--project-root" => {
                project_root = PathBuf::from(args.next().ok_or("missing --project-root value")?);
            }

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
                     [--project-root PATH] \
                     [--materialize | --publish | --run] \
                     [--workers N]"
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
    workers: usize,
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

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json"))
        .map_err(|error| error.to_string())?;

    let resolved = resolve_conversion_catalog(job, source_manifest, &catalog, root)
        .map_err(|error| error.to_string())?;

    let plan = plan_conversion(job, &resolved).map_err(|error| error.to_string())?;

    if plan.streams.iter().any(|stream| stream.paths.len() > 1) {
        return Err(
            "multiple conversion paths are not supported by combined materialization".to_owned(),
        );
    }

    // ---------------------------------------------------------------------
    // Load external references when configured
    // ---------------------------------------------------------------------

    let reference_directory = job
        .conversion
        .reference_merge
        .as_ref()
        .map(|path| resolve_path(root, path));

    let reference_manifest = match &reference_directory {
        Some(directory) => Some(load_source_manifest(directory)?),
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

    let requested_partitions = workers
        .checked_mul(4)
        .ok_or("conversion partition count overflow")?;

    let partitions = build_conversion_partition_plan_with_count(
        &execution_job.input_merge,
        source_manifest,
        requested_partitions,
    )
    .map_err(|error| error.to_string())?;

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

    let parquet = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 512,
        compression_level: 3,
    };

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
        &parquet,
        reporter,
    )
    .map_err(|error| error.to_string())?;

    // ---------------------------------------------------------------------
    // Persist execution receipt
    // ---------------------------------------------------------------------

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

    conversion_println!("Status            : MATERIALIZED — NOT COMMITTED");

    Ok(())
}

fn publish_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging: &Path,
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<(), String> {
    publish_with_reporter(job, source_manifest, staging, workers, reporter)
}
