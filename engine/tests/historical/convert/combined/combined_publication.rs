use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use std::sync::Arc;
use std::{fs, path::PathBuf};
use {
    marketforge_engine::historical::convert::output::ConversionExecutionReceipt,
    marketforge_engine::historical::convert::output::ConversionManifest,
    marketforge_engine::historical::convert::output::SourcePreservationMode,
    marketforge_engine::historical::convert::output::publish_combined_conversion,
    marketforge_engine::historical::convert::output::validate_source_preservation_with_mode,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
};

const EVENTS: u64 = 12;

struct Fixture {
    _temporary: tempfile::TempDir,
    job: marketforge_engine::historical::job::ConvertJob,
    source: PhysicalMergeManifest,
    staging: PathBuf,
}

fn materialized_fixture() -> Fixture {
    use marketforge_engine::historical::{
        convert::execution::materialize_combined_conversion, merge::UnifiedMergeMode,
    };

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let mut job = crate::conversion_fixture::canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("combined");
    let source = crate::market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let catalog = crate::market::catalog(root);
    let resolved = resolve_conversion_catalog(&job, &source, &catalog, root).unwrap();
    let plan = plan_conversion(&job, &resolved).unwrap();
    let staging = root.join("combined.staging");
    let metrics = materialize_combined_conversion(
        &job,
        &source,
        None,
        &plan,
        &staging,
        &crate::market::parquet(),
        None,
    )
    .unwrap();
    assert_eq!(metrics.events_written, EVENTS);
    ConversionExecutionReceipt::new_combined(job.job_id, &metrics)
        .unwrap()
        .write(&staging)
        .unwrap();
    Fixture {
        _temporary: temporary,
        job,
        source,
        staging,
    }
}

#[test]
fn publishes_valid_combined_conversion() {
    let fixture = materialized_fixture();

    let publication =
        publish_combined_conversion(&fixture.job, &fixture.source, &fixture.staging, 2).unwrap();

    assert_eq!(publication.events_written, EVENTS);

    assert!(publication.dataset_path.is_dir());
    assert!(publication.manifest_path.is_file());

    assert!(!fixture.staging.exists());

    let manifest: ConversionManifest =
        serde_json::from_slice(&fs::read(&publication.manifest_path).unwrap()).unwrap();

    manifest.validate().unwrap();

    assert_eq!(manifest.output.content_type, "combined");
    assert_eq!(manifest.statistics.events_written, EVENTS);

    assert_eq!(
        manifest.statistics.identity
            + manifest.statistics.converted
            + manifest.statistics.unavailable,
        EVENTS,
    );

    let depth = manifest.statistics.depth.as_ref().unwrap();

    assert_eq!(depth.levels_read, 5);

    assert_eq!(
        depth.levels_converted + depth.levels_unavailable,
        depth.levels_read,
    );

    assert!(manifest.integrity.conversion_validation);
    assert!(manifest.integrity.source_preservation);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — ATOMIC COMBINED PUBLICATION");
    println!("{}", "=".repeat(90));

    println!("Events           : {}", publication.events_written);
    println!("Files            : {}", publication.files_written);
    println!("Depth levels     : {}", depth.levels_read);
    println!(
        "Source preserved : {}",
        manifest.integrity.source_events_compared
    );
    println!("Manifest         : {}", publication.manifest_path.display());
    println!("STATUS           : PASS");
}

#[test]
fn rejects_missing_combined_receipt() {
    let fixture = materialized_fixture();

    fs::remove_file(fixture.staging.join("execution.json")).unwrap();

    assert!(
        publish_combined_conversion(&fixture.job, &fixture.source, &fixture.staging, 2,).is_err()
    );

    assert!(!fixture.job.output.dataset_path.exists());
    assert!(fixture.staging.exists());
}

#[test]
fn rejects_incorrect_combined_receipt() {
    let fixture = materialized_fixture();

    let path = fixture.staging.join("execution.json");

    let mut receipt: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

    receipt["converted_events"] = serde_json::json!(0);

    fs::write(&path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();

    assert!(
        publish_combined_conversion(&fixture.job, &fixture.source, &fixture.staging, 2,).is_err()
    );

    assert!(!fixture.job.output.dataset_path.exists());
    assert!(fixture.staging.exists());
}

#[test]
fn rejects_existing_combined_destination() {
    let fixture = materialized_fixture();

    fs::create_dir(&fixture.job.output.dataset_path).unwrap();

    assert!(
        publish_combined_conversion(&fixture.job, &fixture.source, &fixture.staging, 2,).is_err()
    );

    assert!(fixture.staging.exists());
}

#[test]
fn rejects_corrupted_original_source_column() {
    let fixture = materialized_fixture();

    let files = marketforge_engine::historical::convert::output::discover_conversion_files(
        &fixture.staging,
    )
    .unwrap();
    let path = &files[0];

    // Read the original converted batches.
    let file = fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

    assert!(!batches.is_empty());

    // Modify one original source column.
    let first = &batches[0];

    let column_index = first.schema().index_of("stream_rank").unwrap();

    let original = first
        .column(column_index)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    let mut values = original.values().to_vec();

    values[0] = values[0].wrapping_add(1);

    let corrupted = Arc::new(UInt32Array::from(values)) as ArrayRef;

    let mut columns = first.columns().to_vec();
    columns[column_index] = corrupted;

    batches[0] = RecordBatch::try_new(first.schema(), columns).unwrap();

    // Rewrite only the temporary converted file.
    let output = fs::File::create(path).unwrap();

    let properties = WriterProperties::builder().build();

    let mut writer = ArrowWriter::try_new(output, batches[0].schema(), Some(properties)).unwrap();

    for batch in &batches {
        writer.write(batch).unwrap();
    }

    writer.close().unwrap();

    // Source preservation must reject the modified source value.
    let result = validate_source_preservation_with_mode(
        &fixture.job.input_merge,
        &fixture.source,
        &fixture.staging,
        SourcePreservationMode::Full,
        8,
    );

    assert!(
        result.is_err(),
        "source preservation accepted corrupted original market data"
    );

    println!("Corrupted source column detected successfully");
    println!("STATUS: PASS");
}
