use std::{fs, path::PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use {
    marketforge_engine::historical::convert::ConversionPlan,
    marketforge_engine::historical::convert::execution::CombinedConversionKernel,
    marketforge_engine::historical::convert::execution::CombinedConversionMetrics,
    marketforge_engine::historical::convert::execution::CombinedConversionRow,
    marketforge_engine::historical::convert::execution::CombinedReferenceAlignment,
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::reference::ExternalReferenceCursor,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::ConvertJob,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

const EVENTS: usize = 20_000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

struct Fixture {
    job: ConvertJob,
    plan: ConversionPlan,
    source: PhysicalMergeManifest,
    external: PhysicalMergeManifest,
}

fn fixture() -> Fixture {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    Fixture {
        job,
        plan,
        source,
        external,
    }
}

struct ExecutionResult {
    rows: Vec<CombinedConversionRow>,
    metrics: CombinedConversionMetrics,
    internal_updates: u64,
    external_updates: u64,
}

fn execute(fixture: &Fixture, batch_size: usize, use_batch_kernel: bool) -> ExecutionResult {
    let reference_directory = fixture.job.conversion.reference_merge.as_ref().unwrap();

    let selection = CombinedReferenceSelection::resolve(
        &fixture.plan,
        &fixture.source,
        Some(&fixture.external),
    )
    .unwrap();

    let cursor = ExternalReferenceCursor::new(
        reference_directory,
        &fixture.external,
        selection.external_streams.iter().cloned(),
    )
    .unwrap();

    let mut alignment = CombinedReferenceAlignment::new(selection, Some(cursor)).unwrap();

    let kernel = CombinedConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let mut metrics = CombinedConversionMetrics::default();
    let mut rows = Vec::with_capacity(EVENTS);

    let mut files = fixture.source.files.clone();

    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut remaining = EVENTS;

    for entry in files {
        if remaining == 0 {
            break;
        }

        let file = fs::File::open(fixture.job.input_merge.join(entry.path)).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(batch_size)
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let count = batch.num_rows().min(remaining);

            let input = batch.slice(0, count);

            if use_batch_kernel {
                rows.extend(
                    kernel
                        .convert_batch(&input, &mut alignment, &mut metrics)
                        .unwrap(),
                );
            } else {
                for row in 0..input.num_rows() {
                    rows.push(
                        kernel
                            .convert_event(&input, row, &mut alignment, &mut metrics)
                            .unwrap(),
                    );
                }
            }

            remaining -= count;

            if remaining == 0 {
                break;
            }
        }
    }

    assert_eq!(remaining, 0);

    ExecutionResult {
        rows,
        metrics,
        internal_updates: alignment.internal_updates(),
        external_updates: alignment.external_updates(),
    }
}

#[test]
fn combined_conversion_is_batch_boundary_independent() {
    let fixture = fixture();

    let large = execute(&fixture, 4096, true);
    let small = execute(&fixture, 257, true);

    assert_eq!(large.rows.len(), EVENTS);
    assert_eq!(small.rows.len(), EVENTS);

    for (index, (left, right)) in large.rows.iter().zip(&small.rows).enumerate() {
        assert_eq!(left, right, "combined conversion mismatch at event {index}");
    }

    assert_eq!(large.metrics.events_read, small.metrics.events_read,);

    assert_eq!(large.metrics.trade_events, small.metrics.trade_events,);

    assert_eq!(large.metrics.depth_events, small.metrics.depth_events,);

    assert_eq!(
        large.metrics.depth.levels_read,
        small.metrics.depth.levels_read,
    );

    assert_eq!(
        large.metrics.depth.levels_converted,
        small.metrics.depth.levels_converted,
    );

    assert_eq!(
        large.metrics.depth.levels_unavailable,
        small.metrics.depth.levels_unavailable,
    );

    assert_eq!(large.internal_updates, small.internal_updates,);

    assert_eq!(large.external_updates, small.external_updates,);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED BATCH EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Events compared : {}", EVENTS);
    println!("Trade events    : {}", large.metrics.trade_events);
    println!("Depth events    : {}", large.metrics.depth_events);
    println!("Depth levels    : {}", large.metrics.depth.levels_read);
    println!("Internal refs   : {}", large.internal_updates);
    println!("External refs   : {}", large.external_updates);
    println!("Batch sizes     : 4096 vs 257");
    println!("STATUS          : PASS");
}

#[test]
fn combined_batch_kernel_matches_row_kernel() {
    let fixture = fixture();

    let row = execute(&fixture, 4096, false);
    let batch = execute(&fixture, 4096, true);

    assert_eq!(row.rows, batch.rows);

    assert_eq!(row.metrics.events_read, batch.metrics.events_read,);

    assert_eq!(row.metrics.trade_events, batch.metrics.trade_events,);

    assert_eq!(row.metrics.depth_events, batch.metrics.depth_events,);

    assert_eq!(
        row.metrics.depth.levels_read,
        batch.metrics.depth.levels_read,
    );

    assert_eq!(row.internal_updates, batch.internal_updates,);

    assert_eq!(row.external_updates, batch.external_updates,);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED KERNEL EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Events compared : {}", row.rows.len());
    println!("Trade events    : {}", row.metrics.trade_events);
    println!("Depth events    : {}", row.metrics.depth_events);
    println!("Depth levels    : {}", row.metrics.depth.levels_read);
    println!("STATUS          : PASS");
}
