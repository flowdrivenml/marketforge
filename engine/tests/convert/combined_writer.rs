use std::{fs, path::PathBuf};

use arrow_array::{Array, ListArray, StructArray, UInt8Array};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{ConvertJob, load_convert_job},
    process::{
        convert::{
            ConversionPlan,
            execution::{
                CombinedConversionKernel, CombinedConversionMetrics, CombinedReferenceAlignment,
                CombinedReferenceSelection,
            },
            output::ConvertedCombinedBatchBuilder,
            plan_conversion,
            reference::ExternalReferenceCursor,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const EVENTS: usize = 20_000;
const BATCH_SIZE: usize = 4096;

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

    let reference_path = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_path.join("manifest.json")).unwrap()).unwrap();

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

#[test]
fn materializes_real_combined_arrow_and_parquet() {
    let fixture = fixture();

    let selection = CombinedReferenceSelection::resolve(
        &fixture.plan,
        &fixture.source,
        Some(&fixture.external),
    )
    .unwrap();

    let reference_path = fixture.job.conversion.reference_merge.as_ref().unwrap();

    let cursor = ExternalReferenceCursor::new(
        reference_path,
        &fixture.external,
        selection.external_streams.iter().cloned(),
    )
    .unwrap();

    let mut alignment = CombinedReferenceAlignment::new(selection, Some(cursor)).unwrap();

    let kernel = CombinedConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let mut metrics = CombinedConversionMetrics::default();

    let temporary = tempfile::tempdir().unwrap();
    let output_path = temporary.path().join("combined.parquet");

    let mut writer: Option<ArrowWriter<fs::File>> = None;
    let mut builder: Option<ConvertedCombinedBatchBuilder> = None;

    let mut files = fixture.source.files.clone();
    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut remaining = EVENTS;
    let mut source_batches = Vec::new();
    let mut converted_batches = Vec::new();

    for entry in files {
        if remaining == 0 {
            break;
        }

        let file = fs::File::open(fixture.job.input_merge.join(entry.path)).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(BATCH_SIZE)
            .build()
            .unwrap();

        for batch in reader {
            if remaining == 0 {
                break;
            }

            let batch = batch.unwrap();
            let count = batch.num_rows().min(remaining);
            let source = batch.slice(0, count);

            if builder.is_none() {
                let combined_builder = ConvertedCombinedBatchBuilder::new(source.schema()).unwrap();

                let properties = WriterProperties::builder().build();

                writer = Some(
                    ArrowWriter::try_new(
                        fs::File::create(&output_path).unwrap(),
                        combined_builder.output_schema(),
                        Some(properties),
                    )
                    .unwrap(),
                );

                builder = Some(combined_builder);
            }

            let rows = kernel
                .convert_batch(&source, &mut alignment, &mut metrics)
                .unwrap();

            let converted = builder.as_ref().unwrap().build(&source, &rows).unwrap();

            // Every original source column must remain unchanged.
            for index in 0..source.num_columns() {
                assert_eq!(
                    source.column(index),
                    converted.column(index),
                    "source column {index} changed"
                );
            }

            writer.as_mut().unwrap().write(&converted).unwrap();

            source_batches.push(source);
            converted_batches.push(converted);

            remaining -= count;
        }
    }

    writer.take().unwrap().close().unwrap();

    assert_eq!(remaining, 0);
    assert_eq!(metrics.events_read, EVENTS as u64);

    // ---------------------------------------------------------------------
    // Validate physical Parquet round-trip
    // ---------------------------------------------------------------------

    let file = fs::File::open(&output_path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(BATCH_SIZE)
        .build()
        .unwrap();

    let mut read_batches = Vec::new();

    for batch in reader {
        read_batches.push(batch.unwrap());
    }

    let physical =
        arrow_select::concat::concat_batches(&converted_batches[0].schema(), &read_batches)
            .unwrap();

    let expected =
        arrow_select::concat::concat_batches(&converted_batches[0].schema(), &converted_batches)
            .unwrap();

    assert_eq!(physical, expected);

    // ---------------------------------------------------------------------
    // Validate event-kind payload nullability
    // ---------------------------------------------------------------------

    let kinds = physical
        .column_by_name("event_kind")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    let trades = physical
        .column_by_name("trade_target")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let depths = physical
        .column_by_name("depth_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let original_depths = physical
        .column_by_name("depth")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let original_changes = original_depths
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let mut trade_events = 0_u64;
    let mut depth_events = 0_u64;
    let mut levels = 0_u64;

    for row in 0..physical.num_rows() {
        match kinds.value(row) {
            0 => {
                trade_events += 1;

                assert!(!trades.is_null(row));
                assert!(depths.is_null(row));
            }

            1 => {
                depth_events += 1;

                assert!(trades.is_null(row));
                assert!(!depths.is_null(row));

                let original_count = original_changes.value_length(row);

                let converted_count = depths.value_length(row);

                assert_eq!(
                    original_count, converted_count,
                    "depth level count mismatch at row {row}"
                );

                levels += original_count as u64;
            }

            kind => panic!("invalid event kind {kind}"),
        }
    }

    assert_eq!(trade_events, metrics.trade_events);
    assert_eq!(depth_events, metrics.depth_events);
    assert_eq!(levels, metrics.depth.levels_read);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PHYSICAL COMBINED MATERIALIZATION");
    println!("{}", "=".repeat(90));

    println!("Events          : {}", physical.num_rows());
    println!("Trade events    : {}", trade_events);
    println!("Depth events    : {}", depth_events);
    println!("Depth levels    : {}", levels);
    println!(
        "Parquet size    : {} bytes",
        fs::metadata(output_path).unwrap().len()
    );
    println!("Internal refs   : {}", alignment.internal_updates());
    println!("External refs   : {}", alignment.external_updates());
    println!("STATUS          : PASS");
}
