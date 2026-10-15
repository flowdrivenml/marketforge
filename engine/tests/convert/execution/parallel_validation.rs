use std::{fs, path::PathBuf, time::Instant};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::{
                build_conversion_checkpoints, build_conversion_partition_plan,
                execute_parallel_conversion,
            },
            output::{validate_converted_trades, validate_source_preservation},
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_IDENTITY: u64 = 11_435_942;
const EXPECTED_CONVERTED: u64 = 1_180_489;
const EXPECTED_UNAVAILABLE: u64 = 11_215;
const EXPECTED_REFERENCES: u64 = 2_308_230;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn validates_partitioned_parallel_conversion() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------
    // Partition planning
    // -------------------------------------------------------------

    let partitions = build_conversion_partition_plan(&job.input_merge, &source, 2).unwrap();

    // -------------------------------------------------------------
    // Reference checkpoints
    // -------------------------------------------------------------

    let prepass =
        build_conversion_checkpoints(&job, &job.input_merge, &source, &plan, &partitions).unwrap();

    // -------------------------------------------------------------
    // Concurrent conversion
    // -------------------------------------------------------------

    let temporary = tempfile::tempdir().unwrap();
    let staging = temporary.path().join("parallel.staging");

    let parquet = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 512,
        compression_level: 3,
    };

    let start = Instant::now();

    let result = execute_parallel_conversion(
        &job,
        &source,
        &plan,
        &partitions,
        &prepass,
        &staging,
        2,
        &parquet,
    )
    .unwrap();

    let conversion_seconds = start.elapsed().as_secs_f64();

    // -------------------------------------------------------------
    // Validate converted Parquet
    // -------------------------------------------------------------

    let validation_start = Instant::now();

    let validation = validate_converted_trades(&staging, job.conversion.max_staleness_ns).unwrap();

    let validation_seconds = validation_start.elapsed().as_secs_f64();

    // -------------------------------------------------------------
    // Validate source preservation
    // -------------------------------------------------------------

    let preservation_start = Instant::now();

    let preservation = validate_source_preservation(&job.input_merge, &source, &staging).unwrap();

    let preservation_seconds = preservation_start.elapsed().as_secs_f64();

    // -------------------------------------------------------------
    // Verify accounting
    // -------------------------------------------------------------

    assert_eq!(result.metrics.events_written, EXPECTED_EVENTS);

    assert_eq!(validation.events, EXPECTED_EVENTS);
    assert_eq!(validation.identity, EXPECTED_IDENTITY);
    assert_eq!(validation.converted, EXPECTED_CONVERTED);
    assert_eq!(validation.unavailable, EXPECTED_UNAVAILABLE);
    assert_eq!(validation.references_checked, EXPECTED_REFERENCES);

    assert_eq!(preservation.events_compared, EXPECTED_EVENTS);
    assert_eq!(preservation.columns_compared, 24);

    assert_eq!(validation.files, result.metrics.files_written);
    assert_eq!(validation.row_groups, result.metrics.row_groups_written);

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — PARALLEL OUTPUT VALIDATION");
    println!("{}", "=".repeat(85));

    println!("Workers             : {}", result.workers_used);
    println!("Partitions          : {}", result.partitions.len());
    println!("Events              : {}", validation.events);
    println!("Identity            : {}", validation.identity);
    println!("Converted           : {}", validation.converted);
    println!("Unavailable         : {}", validation.unavailable);
    println!("References checked  : {}", validation.references_checked);
    println!("Source rows compared: {}", preservation.events_compared);
    println!("Original columns    : {}", preservation.columns_compared);

    println!();
    println!("Conversion time     : {:.3} s", conversion_seconds);
    println!("Validation time     : {:.3} s", validation_seconds);
    println!("Preservation time   : {:.3} s", preservation_seconds);

    println!("STATUS              : PASS");
}
