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
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

#[path = "../support/arrow_comparison.rs"]
mod arrow_comparison;

use arrow_comparison::compare_datasets;

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_IDENTITY: u64 = 11_435_942;
const EXPECTED_CONVERTED: u64 = 1_180_489;
const EXPECTED_UNAVAILABLE: u64 = 11_215;

const SEQUENTIAL_BASELINE_SECONDS: f64 = 17.180;

// -----------------------------------------------------------------------------
// Project paths
// -----------------------------------------------------------------------------

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Concurrent conversion integration test
// -----------------------------------------------------------------------------

#[test]
fn two_worker_parallel_conversion_matches_sequential() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------------------
    // Partition planning
    // -------------------------------------------------------------------------

    let partitions = build_conversion_partition_plan(&job.input_merge, &source, 2).unwrap();

    assert_eq!(partitions.partitions.len(), 2);

    // -------------------------------------------------------------------------
    // Reference checkpoint prepass
    // -------------------------------------------------------------------------

    let prepass_start = Instant::now();

    let prepass =
        build_conversion_checkpoints(&job, &job.input_merge, &source, &plan, &partitions).unwrap();

    let prepass_seconds = prepass_start.elapsed().as_secs_f64();

    assert_eq!(prepass.events_scanned, EXPECTED_EVENTS);
    assert_eq!(prepass.reference_updates, EXPECTED_EVENTS);
    assert_eq!(prepass.checkpoints.len(), 2);

    // -------------------------------------------------------------------------
    // Execute workers concurrently
    // -------------------------------------------------------------------------

    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("parallel.staging");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 512,
        compression_level: 3,
    };

    let workers_start = Instant::now();

    let result = execute_parallel_conversion(
        &job,
        &source,
        &plan,
        &partitions,
        &prepass,
        &staging,
        2,
        &parquet_config,
    )
    .unwrap();

    let worker_seconds = workers_start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Validate aggregate metrics
    // -------------------------------------------------------------------------

    assert_eq!(result.workers_used, 2);
    assert_eq!(result.partitions.len(), 2);

    assert_eq!(result.metrics.events_read, EXPECTED_EVENTS);
    assert_eq!(result.metrics.events_written, EXPECTED_EVENTS);

    assert_eq!(result.metrics.identity_events, EXPECTED_IDENTITY);
    assert_eq!(result.metrics.converted_events, EXPECTED_CONVERTED);
    assert_eq!(result.metrics.unavailable_events, EXPECTED_UNAVAILABLE);

    assert_eq!(result.metrics.reference_updates, EXPECTED_EVENTS);

    // -------------------------------------------------------------------------
    // Exact 32-column Arrow comparison
    // -------------------------------------------------------------------------

    let comparison_start = Instant::now();

    let compared = compare_datasets(&root.join("data/converted/multihop-USDT"), &staging);

    let comparison_seconds = comparison_start.elapsed().as_secs_f64();

    assert_eq!(compared, EXPECTED_EVENTS);

    // Exclude regression comparison from production conversion timing.
    let total_seconds = prepass_seconds + worker_seconds;

    let worker_throughput = EXPECTED_EVENTS as f64 / worker_seconds;

    let total_throughput = EXPECTED_EVENTS as f64 / total_seconds;

    let speedup = SEQUENTIAL_BASELINE_SECONDS / total_seconds;

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — TWO-WORKER PARALLEL CONVERSION");
    println!("{}", "=".repeat(85));

    println!("Events              : {EXPECTED_EVENTS}");
    println!("Workers             : {}", result.workers_used);
    println!("Partitions          : {}", result.partitions.len());

    println!();
    println!("Reference prepass   : {:.3} s", prepass_seconds);
    println!("Worker execution    : {:.3} s", worker_seconds);
    println!("Total conversion    : {:.3} s", total_seconds);
    println!("Arrow comparison    : {:.3} s", comparison_seconds);

    println!();
    println!("Worker throughput   : {:.0} events/s", worker_throughput);
    println!("Total throughput    : {:.0} events/s", total_throughput);
    println!("Sequential baseline : {:.3} s", SEQUENTIAL_BASELINE_SECONDS);
    println!("End-to-end speedup  : {:.3}x", speedup);

    println!();
    println!("Identity            : {}", result.metrics.identity_events);
    println!("Converted           : {}", result.metrics.converted_events);
    println!(
        "Unavailable         : {}",
        result.metrics.unavailable_events
    );
    println!("Reference updates   : {}", result.metrics.reference_updates);

    println!("STATUS              : PASS");
}
