use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{ConvertJob, MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::{
                CombinedReferenceSelection, PhysicalConversionMetrics,
                build_combined_reference_checkpoints, build_conversion_checkpoints,
                build_conversion_partition_plan, build_depth_reference_checkpoints,
                execute_parallel_combined_conversion, execute_parallel_conversion,
                execute_parallel_depth_conversion, materialize_depth_conversion,
                materialize_trade_conversion,
            },
            output::{
                ConversionExecutionReceipt, ConversionPublication, publish_combined_conversion,
                publish_conversion, publish_depth_conversion,
            },
            plan_conversion, resolve_conversion_catalog,
        },
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};

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

    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — PHYSICAL CONVERSION");
    println!("{}", "=".repeat(80));

    println!("Job             : {}", job_path.display());
    println!("Mode            : {:?}", args.mode);
    println!("Input           : {}", job.input_merge.display());
    println!("Output          : {}", output.display());
    println!("Staging         : {}", staging.display());
    println!("Events expected : {}", source_manifest.events_written);

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
    match source_manifest.mode {
        UnifiedMergeMode::Trades => {
            materialize_trades(job, source_manifest, root, staging, workers)
        }

        UnifiedMergeMode::Depth => materialize_depth(job, source_manifest, root, staging, workers),

        UnifiedMergeMode::Combined => {
            materialize_combined(job, source_manifest, root, staging, workers)
        }
    }
}

fn materialize_trades(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
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

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json"))
        .map_err(|error| error.to_string())?;

    let resolved = resolve_conversion_catalog(job, source_manifest, &catalog, root)
        .map_err(|error| error.to_string())?;

    let plan = plan_conversion(job, &resolved).map_err(|error| error.to_string())?;

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

        let partitions =
            build_conversion_partition_plan(&job.input_merge, source_manifest, workers)
                .map_err(|error| error.to_string())?;

        println!("Requested workers : {}", workers);
        println!("Partitions        : {}", partitions.partitions.len());

        let prepass_start = Instant::now();

        let prepass = build_conversion_checkpoints(
            job,
            &job.input_merge,
            source_manifest,
            &plan,
            &partitions,
        )
        .map_err(|error| error.to_string())?;

        println!(
            "Reference prepass : {:.3} s",
            prepass_start.elapsed().as_secs_f64()
        );

        let result = execute_parallel_conversion(
            job,
            source_manifest,
            &plan,
            &partitions,
            &prepass,
            staging,
            workers,
            &parquet,
        )
        .map_err(|error| error.to_string())?;

        println!("Workers used      : {}", result.workers_used);

        result.metrics
    };

    // Persist execution metrics for a later publish-only invocation.
    let receipt =
        ConversionExecutionReceipt::new(job.job_id, &metrics).map_err(|error| error.to_string())?;

    receipt.write(staging).map_err(|error| error.to_string())?;

    let elapsed = start.elapsed().as_secs_f64();

    println!();
    println!("{}", "=".repeat(80));
    println!("CONVERSION MATERIALIZATION SUMMARY");
    println!("{}", "=".repeat(80));

    println!("Events read      : {}", metrics.events_read);
    println!("Events written   : {}", metrics.events_written);
    println!("Identity         : {}", metrics.identity_events);
    println!("Converted        : {}", metrics.converted_events);
    println!("Unavailable      : {}", metrics.unavailable_events);
    println!("Reference updates: {}", metrics.reference_updates);
    println!("Files            : {}", metrics.files_written);
    println!("Row groups       : {}", metrics.row_groups_written);
    println!("Elapsed          : {:.3} s", elapsed);

    if elapsed > 0.0 {
        println!(
            "Throughput       : {:.0} events/s",
            metrics.events_written as f64 / elapsed
        );
    }

    println!(
        "Receipt          : {}",
        staging.join("execution.json").display()
    );
    println!("Status           : MATERIALIZED — NOT COMMITTED");

    Ok(())
}

fn materialize_depth(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
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

        println!("Requested workers : {}", workers);
        println!("Partitions        : {}", partitions.partitions.len());

        let prepass_start = Instant::now();

        let prepass = build_depth_reference_checkpoints(
            &job.input_merge,
            source_manifest,
            &reference_directory,
            &references,
            &plan,
            &partitions,
        )
        .map_err(|error| error.to_string())?;

        println!(
            "Reference prepass : {:.3} s",
            prepass_start.elapsed().as_secs_f64()
        );

        let result = execute_parallel_depth_conversion(
            job,
            source_manifest,
            &references,
            &plan,
            &partitions,
            &prepass,
            staging,
            workers,
            &parquet,
        )
        .map_err(|error| error.to_string())?;

        println!("Workers used      : {}", result.workers_used);

        result.metrics
    };

    let receipt = ConversionExecutionReceipt::new_depth(job.job_id, &metrics)
        .map_err(|error| error.to_string())?;

    receipt.write(staging).map_err(|error| error.to_string())?;

    let elapsed = start.elapsed().as_secs_f64();

    println!();
    println!("{}", "=".repeat(80));
    println!("DEPTH CONVERSION MATERIALIZATION SUMMARY");
    println!("{}", "=".repeat(80));

    println!("Events read       : {}", metrics.events_read);
    println!("Events written    : {}", metrics.events_written);
    println!("Depth levels      : {}", metrics.levels_read);
    println!("Identity          : {}", metrics.identity_events);
    println!("Converted         : {}", metrics.converted_events);
    println!("Unavailable       : {}", metrics.unavailable_events);
    println!("Converted levels  : {}", metrics.levels_converted);
    println!("Unavailable levels: {}", metrics.levels_unavailable);
    println!("Reference updates : {}", metrics.reference_updates);
    println!("Files             : {}", metrics.files_written);
    println!("Row groups        : {}", metrics.row_groups_written);
    println!("Elapsed           : {:.3} s", elapsed);

    if elapsed > 0.0 {
        println!(
            "Throughput        : {:.0} events/s",
            metrics.events_written as f64 / elapsed
        );
    }

    println!("Status            : MATERIALIZED — NOT COMMITTED");

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
        UnifiedMergeMode::Trades => publish_conversion(job, source_manifest, staging),

        UnifiedMergeMode::Depth => publish_depth_conversion(job, source_manifest, staging, workers),

        UnifiedMergeMode::Combined => {
            publish_combined_conversion(job, source_manifest, staging, workers)
        }
    }
    .map_err(|error| error.to_string())?;

    print_publication(&result, start.elapsed().as_secs_f64());

    Ok(())
}

fn print_publication(result: &ConversionPublication, elapsed: f64) {
    println!();
    println!("{}", "=".repeat(80));
    println!("CONVERSION PUBLICATION SUMMARY");
    println!("{}", "=".repeat(80));

    println!("Dataset          : {}", result.dataset_path.display());
    println!("Manifest         : {}", result.manifest_path.display());
    println!("Events           : {}", result.events_written);
    println!("Files            : {}", result.files_written);
    println!("Validation time  : {:.3} s", elapsed);
    println!("Status           : PUBLISHED");
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

    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--project-root" => {
                project_root = PathBuf::from(args.next().ok_or("missing --project-root value")?);
            }

            "--job" => {
                job = Some(PathBuf::from(args.next().ok_or("missing --job value")?));
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
                println!(
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

fn materialize_combined(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    root: &Path,
    staging: &Path,
    workers: usize,
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

    let partitions =
        build_conversion_partition_plan(&execution_job.input_merge, source_manifest, workers)
            .map_err(|error| error.to_string())?;

    println!("Requested workers : {}", workers);
    println!("Partitions        : {}", partitions.partitions.len());

    // ---------------------------------------------------------------------
    // Build historical reference checkpoints
    // ---------------------------------------------------------------------

    let start = Instant::now();
    let prepass_start = Instant::now();

    let prepass = build_combined_reference_checkpoints(
        &execution_job.input_merge,
        source_manifest,
        reference_directory.as_deref(),
        reference_manifest.as_ref(),
        &selection,
        &partitions,
    )
    .map_err(|error| error.to_string())?;

    println!(
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

    let result = execute_parallel_combined_conversion(
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
    )
    .map_err(|error| error.to_string())?;

    // ---------------------------------------------------------------------
    // Persist execution receipt
    // ---------------------------------------------------------------------

    let receipt = ConversionExecutionReceipt::new_combined(execution_job.job_id, &result.metrics)
        .map_err(|error| error.to_string())?;

    receipt.write(staging).map_err(|error| error.to_string())?;

    // ---------------------------------------------------------------------
    // Summary
    // ---------------------------------------------------------------------

    let metrics = &result.metrics;
    let elapsed = start.elapsed().as_secs_f64();

    println!();
    println!("{}", "=".repeat(80));
    println!("COMBINED CONVERSION MATERIALIZATION SUMMARY");
    println!("{}", "=".repeat(80));

    println!("Events read       : {}", metrics.events_read);
    println!("Events written    : {}", metrics.events_written);

    println!("Trades            : {}", metrics.trade_events);
    println!("Depth             : {}", metrics.depth_events);
    println!("Depth levels      : {}", metrics.depth_levels_read);

    println!("Identity          : {}", metrics.identity_events);
    println!("Converted         : {}", metrics.converted_events);
    println!("Unavailable       : {}", metrics.unavailable_events);

    println!("Converted levels  : {}", metrics.depth_levels_converted);
    println!("Unavailable levels: {}", metrics.depth_levels_unavailable);

    println!("Internal refs     : {}", metrics.internal_reference_updates);
    println!("External refs     : {}", metrics.external_reference_updates);

    println!("Workers used      : {}", result.workers_used);
    println!("Files             : {}", metrics.files_written);
    println!("Row groups        : {}", metrics.row_groups_written);
    println!("Skipped           : {}", metrics.skipped_events);
    println!("Skipped levels    : {}", metrics.depth_levels_skipped);
    println!("Elapsed           : {:.3} s", elapsed);

    if elapsed > 0.0 {
        println!(
            "Throughput        : {:.0} events/s",
            metrics.events_written as f64 / elapsed
        );
    }

    println!("Status            : MATERIALIZED — NOT COMMITTED");

    Ok(())
}
