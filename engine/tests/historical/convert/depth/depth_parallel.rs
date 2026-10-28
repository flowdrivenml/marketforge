use std::{fs, path::PathBuf, time::Instant};

use {
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
    marketforge_engine::historical::convert::execution::build_depth_reference_checkpoints,
    marketforge_engine::historical::convert::execution::execute_parallel_depth_conversion,
    marketforge_engine::historical::convert::output::validate_converted_depth,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

const EXPECTED_EVENTS: u64 = 5_661_998;
const WORKERS: usize = 2;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn converts_real_depth_with_two_workers() {
    let root = root();
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("depth-parallel.staging");

    // -------------------------------------------------------------------------
    // Load conversion job and physical manifests
    // -------------------------------------------------------------------------

    let job =
        load_convert_job(root.join("data/.jobs/convert/convert-depth-parallel-test.json")).unwrap();

    let source_directory = root.join("data/merged/depth-conversion-test");

    let reference_directory = root.join("data/merged/final-trades");

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(source_directory.join("manifest.json")).unwrap()).unwrap();

    let references: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------------------
    // Build deterministic depth partitions
    // -------------------------------------------------------------------------

    let partitions = build_conversion_partition_plan(&source_directory, &source, WORKERS).unwrap();

    assert_eq!(partitions.partitions.len(), WORKERS);
    assert_eq!(partitions.total_rows, EXPECTED_EVENTS);

    // -------------------------------------------------------------------------
    // Build historical reference checkpoints
    // -------------------------------------------------------------------------

    let prepass_start = Instant::now();

    let prepass = build_depth_reference_checkpoints(
        &source_directory,
        &source,
        &reference_directory,
        &references,
        &plan,
        &partitions,
    )
    .unwrap();

    let prepass_elapsed = prepass_start.elapsed().as_secs_f64();

    assert_eq!(prepass.checkpoints.len(), WORKERS);

    // -------------------------------------------------------------------------
    // Execute parallel depth workers
    // -------------------------------------------------------------------------

    let parquet = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 1024,
        compression_level: 3,
    };

    let worker_start = Instant::now();

    let result = execute_parallel_depth_conversion(
        &job,
        &source,
        &references,
        &plan,
        &partitions,
        &prepass,
        &staging,
        WORKERS,
        &parquet,
    )
    .unwrap();

    let worker_elapsed = worker_start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Validate worker accounting
    // -------------------------------------------------------------------------

    assert_eq!(result.workers_used, WORKERS);
    assert_eq!(result.partitions.len(), WORKERS);

    assert_eq!(result.metrics.events_read, EXPECTED_EVENTS);
    assert_eq!(result.metrics.events_written, EXPECTED_EVENTS);

    assert_eq!(
        result.metrics.identity_events
            + result.metrics.converted_events
            + result.metrics.unavailable_events,
        EXPECTED_EVENTS,
    );

    assert_eq!(
        result.metrics.levels_converted + result.metrics.levels_unavailable,
        result.metrics.levels_read,
    );

    // -------------------------------------------------------------------------
    // Validate complete physical output
    // -------------------------------------------------------------------------

    let validation_start = Instant::now();

    let validation = validate_converted_depth(&staging, 5_000_000_000).unwrap();

    let validation_elapsed = validation_start.elapsed().as_secs_f64();

    assert_eq!(validation.events, EXPECTED_EVENTS);

    assert_eq!(validation.identity, result.metrics.identity_events,);

    assert_eq!(validation.converted, result.metrics.converted_events,);

    assert_eq!(validation.unavailable, result.metrics.unavailable_events,);

    assert_eq!(validation.levels, result.metrics.levels_read,);

    assert_eq!(validation.levels_converted, result.metrics.levels_converted,);

    assert_eq!(
        validation.levels_unavailable,
        result.metrics.levels_unavailable,
    );

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    let total_elapsed = prepass_elapsed + worker_elapsed + validation_elapsed;

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — TWO-WORKER DEPTH CONVERSION");
    println!("{}", "=".repeat(90));

    println!("Events             : {}", result.metrics.events_written);
    println!("Depth levels       : {}", result.metrics.levels_read);
    println!("Workers            : {}", result.workers_used);
    println!("Partitions         : {}", result.partitions.len());

    println!();
    println!("Identity           : {}", result.metrics.identity_events);
    println!("Converted          : {}", result.metrics.converted_events);
    println!("Unavailable        : {}", result.metrics.unavailable_events);

    println!();
    println!("Converted levels   : {}", result.metrics.levels_converted);
    println!("Unavailable levels : {}", result.metrics.levels_unavailable);

    println!();
    println!("Reference updates  : {}", result.metrics.reference_updates);
    println!(
        "Observations read  : {}",
        result.metrics.reference_observations_read,
    );

    println!();
    println!("Parquet files      : {}", result.metrics.files_written);
    println!("Row groups         : {}", result.metrics.row_groups_written);

    println!();
    println!("Reference prepass  : {:.3} s", prepass_elapsed);
    println!("Worker execution   : {:.3} s", worker_elapsed);
    println!("Output validation  : {:.3} s", validation_elapsed);
    println!("Total              : {:.3} s", total_elapsed);

    if worker_elapsed > 0.0 {
        println!(
            "Worker throughput  : {:.0} events/s",
            EXPECTED_EVENTS as f64 / worker_elapsed,
        );

        println!(
            "Level throughput   : {:.0} levels/s",
            result.metrics.levels_read as f64 / worker_elapsed,
        );
    }

    println!("STATUS             : PASS");
}
