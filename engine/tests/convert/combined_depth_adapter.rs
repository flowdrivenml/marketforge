use std::{fs, path::PathBuf};

use arrow_array::{Array, RecordBatch, StructArray, UInt8Array};
use arrow_schema::{Field, Schema};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{
            execution::{DepthConversionKernel, DepthConversionMetrics},
            plan_conversion,
            reference::ReferenceState,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const MAX_EVENTS: usize = 256;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

struct Fixture {
    job: marketforge_engine::job::ConvertJob,
    plan: marketforge_engine::process::convert::ConversionPlan,
    batches: Vec<RecordBatch>,
}

fn fixture() -> Fixture {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let mut files = manifest.files.clone();

    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut batches = Vec::new();
    let mut events = 0;

    for entry in files {
        if events >= MAX_EVENTS {
            break;
        }

        let file = fs::File::open(job.input_merge.join(&entry.path)).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(4096)
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let kinds = batch
                .column_by_name("event_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            if kinds.values().iter().any(|kind| *kind == 1) {
                batches.push(batch);
                events += 1;
            }

            if events >= MAX_EVENTS {
                break;
            }
        }
    }

    assert!(!batches.is_empty());

    Fixture { job, plan, batches }
}

fn first_depth_row(batch: &RecordBatch) -> Option<usize> {
    let kinds = batch
        .column_by_name("event_kind")?
        .as_any()
        .downcast_ref::<UInt8Array>()?;

    (0..kinds.len()).find(|&row| kinds.value(row) == 1)
}

fn adapt_depth_batch(batch: &RecordBatch) -> RecordBatch {
    let depths = batch
        .column_by_name("depth")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let changes = depths.column_by_name("changes").unwrap();

    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();

    let mut columns = batch.columns().to_vec();

    fields.push(Field::new("changes", changes.data_type().clone(), true));

    columns.push(changes.clone());

    RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn combined_depth_adapter_matches_original_kernel() {
    let fixture = fixture();

    let kernel = DepthConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let state = ReferenceState::new();

    let mut compared = 0_u64;
    let mut adapted_metrics = DepthConversionMetrics::default();
    let mut original_metrics = DepthConversionMetrics::default();

    for batch in &fixture.batches {
        let adapted = adapt_depth_batch(batch);

        let kinds = batch
            .column_by_name("event_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            if kinds.value(row) != 1 {
                continue;
            }

            let combined = kernel
                .convert_combined_event(batch, row, &state, &mut adapted_metrics)
                .unwrap();

            let original = kernel
                .convert_event(&adapted, row, &state, &mut original_metrics)
                .unwrap();

            assert_eq!(combined.metadata.status, original.metadata.status,);

            assert_eq!(combined.metadata.rate, original.metadata.rate,);

            assert_eq!(combined.metadata.references, original.metadata.references,);

            assert_eq!(combined.levels, original.levels,);

            compared += 1;
        }
    }

    assert!(compared > 0);

    assert_eq!(adapted_metrics.events_read, original_metrics.events_read,);

    assert_eq!(adapted_metrics.levels_read, original_metrics.levels_read,);

    assert_eq!(
        adapted_metrics.levels_converted,
        original_metrics.levels_converted,
    );

    assert_eq!(
        adapted_metrics.levels_unavailable,
        original_metrics.levels_unavailable,
    );

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED DEPTH ADAPTER");
    println!("{}", "=".repeat(90));

    println!("Events compared : {}", compared);
    println!("Levels compared : {}", adapted_metrics.levels_read);
    println!("STATUS          : PASS");
}

#[test]
fn rejects_trade_event_in_depth_adapter() {
    let fixture = fixture();

    let kernel = DepthConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let state = ReferenceState::new();

    for batch in &fixture.batches {
        let kinds = batch
            .column_by_name("event_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            if kinds.value(row) != 0 {
                continue;
            }

            let result = kernel.convert_combined_event(
                batch,
                row,
                &state,
                &mut DepthConversionMetrics::default(),
            );

            assert!(result.is_err());
            return;
        }
    }

    panic!("fixture contains no trade event");
}

#[test]
fn rejects_out_of_bounds_combined_depth_row() {
    let fixture = fixture();

    let kernel = DepthConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let batch = &fixture.batches[0];

    let result = kernel.convert_combined_event(
        batch,
        batch.num_rows(),
        &ReferenceState::new(),
        &mut DepthConversionMetrics::default(),
    );

    assert!(result.is_err());
}

#[test]
fn combined_depth_adapter_preserves_source_columns() {
    let fixture = fixture();

    let kernel = DepthConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let batch = &fixture.batches[0];
    let row = first_depth_row(batch).unwrap();

    let original = batch.clone();

    kernel
        .convert_combined_event(
            batch,
            row,
            &ReferenceState::new(),
            &mut DepthConversionMetrics::default(),
        )
        .unwrap();

    assert_eq!(batch, &original);
}
