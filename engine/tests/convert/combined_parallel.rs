use std::{
    fs,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::{
                CombinedReferenceSelection, build_combined_reference_checkpoints,
                build_conversion_partition_plan, execute_parallel_combined_conversion,
                materialize_combined_conversion,
            },
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const EVENTS: u64 = 20_000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn read_parquet(directory: &Path) -> Vec<RecordBatch> {
    fn collect(directory: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                collect(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    collect(directory, &mut files);
    files.sort();

    let mut batches = Vec::new();

    for path in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
            .unwrap()
            .with_batch_size(4096)
            .build()
            .unwrap();

        for batch in reader {
            batches.push(batch.unwrap());
        }
    }

    batches
}

#[test]
fn parallel_combined_matches_sequential() {
    let root = root();

    let mut job =
        load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");

    job.input_merge = fixture_path.clone();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    source.validate().unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let partitions = build_conversion_partition_plan(&fixture_path, &source, 2).unwrap();

    assert_eq!(partitions.partitions.len(), 2);

    // ---------------------------------------------------------------------
    // Production reference prepass
    // ---------------------------------------------------------------------

    let prepass = build_combined_reference_checkpoints(
        &fixture_path,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    // ---------------------------------------------------------------------
    // Sequential physical materialization
    // ---------------------------------------------------------------------

    let temporary = tempfile::tempdir().unwrap();

    let sequential_output = temporary.path().join("sequential");
    let parallel_output = temporary.path().join("parallel");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    let sequential_metrics = materialize_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &sequential_output,
        &parquet_config,
        None,
    )
    .unwrap();

    // ---------------------------------------------------------------------
    // Parallel physical materialization
    // ---------------------------------------------------------------------

    let start = std::time::Instant::now();

    let parallel = execute_parallel_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &partitions,
        &prepass,
        &selection,
        &parallel_output,
        2,
        &parquet_config,
    )
    .unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    // ---------------------------------------------------------------------
    // Validate metrics
    // ---------------------------------------------------------------------

    assert_eq!(parallel.workers_used, 2);

    assert_eq!(parallel.metrics.events_read, EVENTS);
    assert_eq!(parallel.metrics.events_written, EVENTS);

    assert_eq!(
        parallel.metrics.trade_events,
        sequential_metrics.trade_events,
    );

    assert_eq!(
        parallel.metrics.depth_events,
        sequential_metrics.depth_events,
    );

    assert_eq!(
        parallel.metrics.identity_events,
        sequential_metrics.identity_events,
    );

    assert_eq!(
        parallel.metrics.converted_events,
        sequential_metrics.converted_events,
    );

    assert_eq!(
        parallel.metrics.unavailable_events,
        sequential_metrics.unavailable_events,
    );

    assert_eq!(
        parallel.metrics.depth_levels_read,
        sequential_metrics.depth_levels_read,
    );

    assert_eq!(
        parallel.metrics.depth_levels_converted,
        sequential_metrics.depth_levels_converted,
    );

    assert_eq!(
        parallel.metrics.depth_levels_unavailable,
        sequential_metrics.depth_levels_unavailable,
    );

    assert_eq!(
        parallel.metrics.internal_reference_updates,
        sequential_metrics.internal_reference_updates,
    );

    // ---------------------------------------------------------------------
    // Exact physical Arrow comparison
    // ---------------------------------------------------------------------

    let sequential_batches = read_parquet(&sequential_output);
    let parallel_batches = read_parquet(&parallel_output);

    let schema = sequential_batches[0].schema();

    let sequential_table =
        arrow_select::concat::concat_batches(&schema, &sequential_batches).unwrap();

    let parallel_table = arrow_select::concat::concat_batches(&schema, &parallel_batches).unwrap();

    assert_eq!(
        sequential_table, parallel_table,
        "parallel combined conversion differs from sequential"
    );

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PARALLEL COMBINED EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Events          : {}", EVENTS);
    println!("Workers         : {}", parallel.workers_used);
    println!("Partitions      : {}", parallel.partitions.len());
    println!("Trade events    : {}", parallel.metrics.trade_events);
    println!("Depth events    : {}", parallel.metrics.depth_events);
    println!("Depth levels    : {}", parallel.metrics.depth_levels_read);

    println!("Identity        : {}", parallel.metrics.identity_events);
    println!("Converted       : {}", parallel.metrics.converted_events);
    println!("Unavailable     : {}", parallel.metrics.unavailable_events);

    println!(
        "Internal refs   : {}",
        parallel.metrics.internal_reference_updates
    );

    println!(
        "External refs   : {}",
        parallel.metrics.external_reference_updates
    );

    println!("Parquet files   : {}", parallel.metrics.files_written);
    println!("Elapsed         : {:.3} s", elapsed);
    println!("STATUS          : PASS");
}

#[test]
fn combined_conversion_is_worker_count_independent() {
    let root = root();

    let mut job =
        load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");
    job.input_merge = fixture_path.clone();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    source.validate().unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let temporary = tempfile::tempdir().unwrap();

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    // ---------------------------------------------------------------------
    // Construct independent sequential baseline
    // ---------------------------------------------------------------------

    let sequential_output = temporary.path().join("sequential");

    let sequential_metrics = materialize_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &sequential_output,
        &parquet_config,
        None,
    )
    .unwrap();

    assert_eq!(sequential_metrics.events_written, EVENTS);

    let sequential_batches = read_parquet(&sequential_output);

    let schema = sequential_batches[0].schema();

    let expected = arrow_select::concat::concat_batches(&schema, &sequential_batches).unwrap();

    // ---------------------------------------------------------------------
    // Test independent partition configurations
    // ---------------------------------------------------------------------

    for workers in [1_usize, 2, 4] {
        let partitions = build_conversion_partition_plan(&fixture_path, &source, workers).unwrap();

        assert_eq!(
            partitions.partitions.len(),
            workers,
            "unexpected partition count for {workers} workers"
        );

        // Each worker configuration requires its own checkpoints.
        let prepass = build_combined_reference_checkpoints(
            &fixture_path,
            &source,
            Some(reference_directory),
            Some(&external),
            &selection,
            &partitions,
        )
        .unwrap();

        assert_eq!(prepass.events_scanned, EVENTS);

        let output = temporary.path().join(format!("parallel-{workers}"));

        let start = std::time::Instant::now();

        let result = execute_parallel_combined_conversion(
            &job,
            &source,
            Some(&external),
            &plan,
            &partitions,
            &prepass,
            &selection,
            &output,
            workers,
            &parquet_config,
        )
        .unwrap();

        let elapsed = start.elapsed().as_secs_f64();

        // -----------------------------------------------------------------
        // Verify execution accounting
        // -----------------------------------------------------------------

        assert_eq!(result.workers_used, workers);

        assert_eq!(result.partitions.len(), workers);

        assert_eq!(result.metrics.events_read, EVENTS);
        assert_eq!(result.metrics.events_written, EVENTS);

        assert_eq!(result.metrics.trade_events, sequential_metrics.trade_events,);

        assert_eq!(result.metrics.depth_events, sequential_metrics.depth_events,);

        assert_eq!(
            result.metrics.identity_events,
            sequential_metrics.identity_events,
        );

        assert_eq!(
            result.metrics.converted_events,
            sequential_metrics.converted_events,
        );

        assert_eq!(
            result.metrics.unavailable_events,
            sequential_metrics.unavailable_events,
        );

        assert_eq!(
            result.metrics.depth_levels_read,
            sequential_metrics.depth_levels_read,
        );

        assert_eq!(
            result.metrics.depth_levels_converted,
            sequential_metrics.depth_levels_converted,
        );

        assert_eq!(
            result.metrics.depth_levels_unavailable,
            sequential_metrics.depth_levels_unavailable,
        );

        assert_eq!(
            result.metrics.internal_reference_updates,
            sequential_metrics.internal_reference_updates,
        );

        // -----------------------------------------------------------------
        // Compare complete physical Arrow output
        // -----------------------------------------------------------------

        let actual_batches = read_parquet(&output);

        let actual = arrow_select::concat::concat_batches(&schema, &actual_batches).unwrap();

        assert_eq!(
            expected, actual,
            "combined conversion differs with {workers} workers"
        );

        println!();
        println!("{}", "=".repeat(90));
        println!("MARKETFORGE — COMBINED WORKER DETERMINISM");
        println!("{}", "=".repeat(90));

        println!("Workers          : {workers}");
        println!("Partitions       : {}", result.partitions.len());
        println!("Events compared  : {}", actual.num_rows());
        println!("Trade events     : {}", result.metrics.trade_events);
        println!("Depth events     : {}", result.metrics.depth_events);
        println!("Depth levels     : {}", result.metrics.depth_levels_read);
        println!(
            "Internal refs    : {}",
            result.metrics.internal_reference_updates
        );
        println!("Parquet files    : {}", result.metrics.files_written);
        println!("Execution        : {:.3} s", elapsed);
        println!("STATUS           : PASS");
    }
}

#[test]
fn parallel_combined_validation_matches_sequential_across_files() {
    use marketforge_engine::process::convert::output::{
        validate_converted_combined, validate_converted_combined_parallel,
    };

    let root = root();

    let mut job =
        load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");
    job.input_merge = fixture_path.clone();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    // Four row groups -> four independent partitions.
    let partitions = build_conversion_partition_plan(&fixture_path, &source, 4).unwrap();

    let prepass = build_combined_reference_checkpoints(
        &fixture_path,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    let temporary = tempfile::tempdir().unwrap();
    let output = temporary.path().join("parallel");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    let materialized = execute_parallel_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &partitions,
        &prepass,
        &selection,
        &output,
        4,
        &parquet_config,
    )
    .unwrap();

    assert_eq!(materialized.workers_used, 4);
    assert_eq!(materialized.metrics.events_written, EVENTS);
    assert_eq!(materialized.metrics.files_written, 4);

    // Independent sequential validation.
    let sequential = validate_converted_combined(&output, job.conversion.max_staleness_ns).unwrap();

    // Four validation workers across four physical files.
    let parallel =
        validate_converted_combined_parallel(&output, job.conversion.max_staleness_ns, 4).unwrap();

    assert_eq!(parallel.events, sequential.events);
    assert_eq!(parallel.trade_events, sequential.trade_events);
    assert_eq!(parallel.depth_events, sequential.depth_events);

    assert_eq!(parallel.identity, sequential.identity);
    assert_eq!(parallel.converted, sequential.converted);
    assert_eq!(parallel.unavailable, sequential.unavailable);

    assert_eq!(parallel.depth_levels, sequential.depth_levels);

    assert_eq!(
        parallel.numerical_trade_values,
        sequential.numerical_trade_values,
    );

    assert_eq!(
        parallel.numerical_depth_levels,
        sequential.numerical_depth_levels,
    );

    assert_eq!(parallel.references_checked, sequential.references_checked,);

    assert_eq!(parallel.files, 4);
    assert_eq!(parallel.row_groups, sequential.row_groups);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — MULTI-FILE PARALLEL COMBINED VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Events           : {}", parallel.events);
    println!("Trades           : {}", parallel.trade_events);
    println!("Depth            : {}", parallel.depth_events);
    println!("Depth levels     : {}", parallel.depth_levels);
    println!("Files            : {}", parallel.files);
    println!("Row groups       : {}", parallel.row_groups);
    println!("Validation workers: 4");
    println!("Numerical trades : {}", parallel.numerical_trade_values);
    println!("Numerical levels : {}", parallel.numerical_depth_levels);
    println!("References       : {}", parallel.references_checked);
    println!("STATUS           : PASS");
}
