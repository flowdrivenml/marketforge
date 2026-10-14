use std::{fs, path::PathBuf, time::Instant};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{execute_conversion_sequential, plan_conversion, resolve_conversion_catalog},
        merge::PhysicalMergeManifest,
    },
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn executes_real_multihop_conversion() {
    let root = root();

    // -------------------------------------------------------------------------
    // Load conversion configuration
    // -------------------------------------------------------------------------

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    // -------------------------------------------------------------------------
    // Resolve and plan
    // -------------------------------------------------------------------------

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    assert_eq!(plan.source_streams, 3);
    assert_eq!(plan.reference_streams, 3);
    assert_eq!(plan.identity_streams, 1);
    assert_eq!(plan.convertible_streams, 2);
    assert_eq!(plan.unresolved_streams, 0);

    // -------------------------------------------------------------------------
    // Execute
    // -------------------------------------------------------------------------

    let start = Instant::now();

    let stats = execute_conversion_sequential(&job, &manifest, &plan).unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    let throughput = stats.events_read as f64 / elapsed;

    // -------------------------------------------------------------------------
    // Validate accounting
    // -------------------------------------------------------------------------

    assert_eq!(stats.events_read, 12_627_646);

    assert_eq!(
        stats.identity_events + stats.converted_events + stats.unavailable_events,
        stats.events_read
    );

    assert_eq!(stats.reference_updates, stats.events_read);

    assert_eq!(stats.streams.len(), 3);

    // -------------------------------------------------------------------------
    // Validate per-stream counts
    // -------------------------------------------------------------------------

    let btcusdt = stats.streams.get(&106).unwrap();
    let ethbtc = stats.streams.get(&129).unwrap();
    let btcusdc = stats.streams.get(&130).unwrap();

    assert_eq!(btcusdt.events, 11_435_942);
    assert_eq!(ethbtc.events, 52_748);
    assert_eq!(btcusdc.events, 1_138_956);

    assert_eq!(btcusdt.identity, btcusdt.events);
    assert_eq!(ethbtc.identity, 0);
    assert_eq!(btcusdc.identity, 0);

    // -------------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — REAL HISTORICAL CONVERSION");
    println!("{}", "=".repeat(85));

    println!("Events read       : {}", stats.events_read);
    println!("Identity events   : {}", stats.identity_events);
    println!("Converted events  : {}", stats.converted_events);
    println!("Unavailable events: {}", stats.unavailable_events);
    println!("Reference updates : {}", stats.reference_updates);

    println!("Elapsed           : {:.3} s", elapsed);
    println!("Throughput        : {:.0} events/s", throughput);

    println!();

    for (dataset_id, stream) in &stats.streams {
        println!("Dataset {dataset_id}");
        println!("  Events      : {}", stream.events);
        println!("  Identity    : {}", stream.identity);
        println!("  Converted   : {}", stream.converted);
        println!("  Unavailable : {}", stream.unavailable);
        println!("  Missing     : {}", stream.missing_reference);
        println!("  Stale       : {}", stream.stale_reference);
        println!("  Future      : {}", stream.future_reference);
        println!("  Arithmetic : {}", stream.arithmetic_failure);
        println!("  Mismatch    : {}", stream.identity_mismatch);
    }

    println!("{}", "=".repeat(85));
}
