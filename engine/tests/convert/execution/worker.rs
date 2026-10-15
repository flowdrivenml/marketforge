#[path = "../support/arrow_comparison.rs"]
mod arrow_comparison;
use arrow_comparison::compare_datasets;
use std::{fs, path::PathBuf, time::Instant};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::{
                build_conversion_checkpoints, build_conversion_partition_plan,
                execute_conversion_partition,
            },
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_IDENTITY: u64 = 11_435_942;
const EXPECTED_CONVERTED: u64 = 1_180_489;
const EXPECTED_UNAVAILABLE: u64 = 11_215;

// -----------------------------------------------------------------------------
// Project paths
// -----------------------------------------------------------------------------

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn sequential_dataset() -> PathBuf {
    root().join("data/converted/multihop-USDT")
}

// -----------------------------------------------------------------------------
// Complete two-partition conversion
// -----------------------------------------------------------------------------

#[test]
fn two_partition_conversion_matches_sequential() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------------------
    // Plan two partitions
    // -------------------------------------------------------------------------

    let partitions = build_conversion_partition_plan(&job.input_merge, &source, 2).unwrap();

    assert_eq!(partitions.partitions.len(), 2);

    // -------------------------------------------------------------------------
    // Build reference checkpoints
    // -------------------------------------------------------------------------

    let prepass =
        build_conversion_checkpoints(&job, &job.input_merge, &source, &plan, &partitions).unwrap();

    assert_eq!(prepass.checkpoints.len(), 2);
    assert_eq!(prepass.events_scanned, EXPECTED_EVENTS);

    // -------------------------------------------------------------------------
    // Execute partition workers
    // -------------------------------------------------------------------------

    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("parallel.staging");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 512,
        compression_level: 3,
    };

    let start = Instant::now();

    let mut results = Vec::new();

    for (partition, checkpoint) in partitions.partitions.iter().zip(&prepass.checkpoints) {
        let result = execute_conversion_partition(
            &job,
            &source,
            &plan,
            &partitions,
            partition,
            checkpoint,
            &staging,
            &parquet_config,
        )
        .unwrap();

        results.push(result);
    }

    let elapsed = start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Aggregate metrics
    // -------------------------------------------------------------------------

    let events = results
        .iter()
        .map(|result| result.metrics.events_written)
        .sum::<u64>();

    let identity = results
        .iter()
        .map(|result| result.metrics.identity_events)
        .sum::<u64>();

    let converted = results
        .iter()
        .map(|result| result.metrics.converted_events)
        .sum::<u64>();

    let unavailable = results
        .iter()
        .map(|result| result.metrics.unavailable_events)
        .sum::<u64>();

    let reference_updates = results
        .iter()
        .map(|result| result.metrics.reference_updates)
        .sum::<u64>();

    assert_eq!(events, EXPECTED_EVENTS);
    assert_eq!(identity, EXPECTED_IDENTITY);
    assert_eq!(converted, EXPECTED_CONVERTED);
    assert_eq!(unavailable, EXPECTED_UNAVAILABLE);
    assert_eq!(reference_updates, EXPECTED_EVENTS);

    // -------------------------------------------------------------------------
    // Compare every original and conversion column
    // -------------------------------------------------------------------------

    let comparison_start = Instant::now();

    let compared = compare_datasets(&sequential_dataset(), &staging);

    let comparison_elapsed = comparison_start.elapsed().as_secs_f64();

    assert_eq!(compared, EXPECTED_EVENTS);

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — TWO-PARTITION CONVERSION EQUIVALENCE");
    println!("{}", "=".repeat(85));

    println!("Partitions       : {}", results.len());
    println!("Events           : {events}");
    println!("Identity         : {identity}");
    println!("Converted        : {converted}");
    println!("Unavailable      : {unavailable}");
    println!("Reference updates: {reference_updates}");

    println!("Worker execution : {:.3} s", elapsed);
    println!("Arrow comparison : {:.3} s", comparison_elapsed);

    println!("STATUS           : PASS");
}
