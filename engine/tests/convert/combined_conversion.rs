use std::{fs, path::PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{
            execution::{
                CombinedConversionKernel, CombinedConversionMetrics, CombinedConversionRow,
                CombinedReferenceAlignment, CombinedReferenceSelection,
            },
            plan_conversion,
            reference::ExternalReferenceCursor,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const MAX_EVENTS: usize = 20_000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn converts_real_combined_timeline() {
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

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let cursor = ExternalReferenceCursor::new(
        reference_directory,
        &external,
        selection.external_streams.iter().cloned(),
    )
    .unwrap();

    let mut alignment = CombinedReferenceAlignment::new(selection, Some(cursor)).unwrap();

    let kernel = CombinedConversionKernel::new(&job, &plan).unwrap();

    let mut metrics = CombinedConversionMetrics::default();

    let mut files = source.files.clone();

    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut remaining = MAX_EVENTS;

    for entry in files {
        if remaining == 0 {
            break;
        }

        let file = fs::File::open(job.input_merge.join(entry.path)).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(4096)
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            if remaining == 0 {
                break;
            }

            let count = batch.num_rows().min(remaining);

            for row in 0..count {
                let converted = kernel
                    .convert_event(&batch, row, &mut alignment, &mut metrics)
                    .unwrap();

                match converted {
                    CombinedConversionRow::Trade(_) => {}
                    CombinedConversionRow::Depth(_) => {}
                }
            }

            remaining -= count;
        }
    }

    assert_eq!(metrics.events_read, MAX_EVENTS as u64);

    assert_eq!(
        metrics.events_read,
        metrics.trade_events + metrics.depth_events,
    );

    assert_eq!(metrics.trade.events_read, metrics.trade_events,);

    assert_eq!(metrics.depth.events_read, metrics.depth_events,);

    assert_eq!(
        alignment.reference_updates(),
        alignment.internal_updates() + alignment.external_updates(),
    );

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — UNIFIED COMBINED CONVERSION");
    println!("{}", "=".repeat(90));

    println!("Events          : {}", metrics.events_read);
    println!("Trade events    : {}", metrics.trade_events);
    println!("Depth events    : {}", metrics.depth_events);
    println!("Depth levels    : {}", metrics.depth.levels_read);
    println!("Internal refs   : {}", alignment.internal_updates());
    println!("External refs   : {}", alignment.external_updates());
    println!("STATUS          : PASS");
}
