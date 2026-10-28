use std::{fs, path::PathBuf};

use arrow_array::{Array, Int64Array, RecordBatch};
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
    marketforge_engine::historical::convert::reference::ExternalReferencePosition,
    marketforge_engine::historical::convert::reference::ReferenceState,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::ConvertJob,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

const EVENTS: usize = 20_000;
const BOUNDARY: usize = 10_000;
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

fn selection(fixture: &Fixture) -> CombinedReferenceSelection {
    CombinedReferenceSelection::resolve(&fixture.plan, &fixture.source, Some(&fixture.external))
        .unwrap()
}

fn cursor(fixture: &Fixture, selection: &CombinedReferenceSelection) -> ExternalReferenceCursor {
    ExternalReferenceCursor::new(
        fixture.job.conversion.reference_merge.as_ref().unwrap(),
        &fixture.external,
        selection.external_streams.iter().cloned(),
    )
    .unwrap()
}

fn read_prefix(fixture: &Fixture) -> Vec<RecordBatch> {
    let mut files = fixture.source.files.clone();

    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut remaining = EVENTS;
    let mut batches = Vec::new();

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

            batches.push(batch.slice(0, count));

            remaining -= count;
        }
    }

    assert_eq!(remaining, 0);

    batches
}

fn convert_range(
    fixture: &Fixture,
    batches: &[RecordBatch],
    start: usize,
    end: usize,
    alignment: &mut CombinedReferenceAlignment,
) -> Vec<CombinedConversionRow> {
    let kernel = CombinedConversionKernel::new(&fixture.job, &fixture.plan).unwrap();

    let mut metrics = CombinedConversionMetrics::default();
    let mut converted = Vec::with_capacity(end - start);

    let mut global_row = 0;

    for batch in batches {
        let batch_start = global_row;
        let batch_end = batch_start + batch.num_rows();

        let overlap_start = start.max(batch_start);
        let overlap_end = end.min(batch_end);

        if overlap_start < overlap_end {
            let local_start = overlap_start - batch_start;
            let count = overlap_end - overlap_start;

            let slice = batch.slice(local_start, count);

            converted.extend(
                kernel
                    .convert_batch(&slice, alignment, &mut metrics)
                    .unwrap(),
            );
        }

        global_row = batch_end;
    }

    assert_eq!(converted.len(), end - start);

    converted
}

fn timestamp_at(batches: &[RecordBatch], target: usize) -> i64 {
    let mut offset = 0;

    for batch in batches {
        if target < offset + batch.num_rows() {
            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            return timestamps.value(target - offset);
        }

        offset += batch.num_rows();
    }

    panic!("timestamp index out of bounds");
}

#[test]
fn restored_combined_checkpoint_matches_sequential() {
    let fixture = fixture();
    let batches = read_prefix(&fixture);

    let selected = selection(&fixture);

    // ---------------------------------------------------------------------
    // Uninterrupted sequential conversion
    // ---------------------------------------------------------------------

    let mut sequential =
        CombinedReferenceAlignment::new(selected.clone(), Some(cursor(&fixture, &selected)))
            .unwrap();

    let expected = convert_range(&fixture, &batches, 0, EVENTS, &mut sequential);

    // ---------------------------------------------------------------------
    // Construct checkpoint by processing the prefix
    // ---------------------------------------------------------------------

    let mut prefix =
        CombinedReferenceAlignment::new(selected.clone(), Some(cursor(&fixture, &selected)))
            .unwrap();

    convert_range(&fixture, &batches, 0, BOUNDARY, &mut prefix);

    let checkpoint_state: ReferenceState = prefix.snapshot();

    let checkpoint_position: ExternalReferencePosition = prefix.external_position().unwrap();

    // ---------------------------------------------------------------------
    // Resume external cursor from checkpoint
    // ---------------------------------------------------------------------

    let resumed_cursor = ExternalReferenceCursor::resume(
        fixture.job.conversion.reference_merge.as_ref().unwrap(),
        &fixture.external,
        selected.external_streams.iter().cloned(),
        checkpoint_position,
    )
    .unwrap();

    let mut restored = CombinedReferenceAlignment::from_checkpoint(
        selected,
        Some(resumed_cursor),
        checkpoint_state,
    )
    .unwrap();

    // ---------------------------------------------------------------------
    // Convert remaining events
    // ---------------------------------------------------------------------

    let actual = convert_range(&fixture, &batches, BOUNDARY, EVENTS, &mut restored);

    assert_eq!(actual.len(), EVENTS - BOUNDARY);

    for (index, (left, right)) in expected[BOUNDARY..].iter().zip(&actual).enumerate() {
        assert_eq!(
            left,
            right,
            "restored conversion mismatch at event {}",
            BOUNDARY + index,
        );
    }

    // ---------------------------------------------------------------------
    // Verify historical reference ordering
    // ---------------------------------------------------------------------

    let boundary_timestamp = timestamp_at(&batches, BOUNDARY);

    for observation in restored.state().observations() {
        assert!(observation.key.timestamp_ns <= timestamp_at(&batches, EVENTS - 1));
    }

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED CHECKPOINT RESTORATION");
    println!("{}", "=".repeat(90));

    println!("Events compared    : {}", actual.len());
    println!("Boundary row       : {}", BOUNDARY);
    println!("Boundary timestamp : {}", boundary_timestamp);
    println!(
        "Reference position : {}",
        checkpoint_position.observations_consumed
    );
    println!("Reference streams  : {}", restored.state().len());
    println!("STATUS             : PASS");
}
