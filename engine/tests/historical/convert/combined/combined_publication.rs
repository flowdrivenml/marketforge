use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use std::sync::Arc;
use std::{fs, path::PathBuf};
use {
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::execution::build_combined_reference_checkpoints,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
    marketforge_engine::historical::convert::execution::execute_parallel_combined_conversion,
    marketforge_engine::historical::convert::output::ConversionExecutionReceipt,
    marketforge_engine::historical::convert::output::ConversionManifest,
    marketforge_engine::historical::convert::output::SourcePreservationMode,
    marketforge_engine::historical::convert::output::publish_combined_conversion,
    marketforge_engine::historical::convert::output::validate_source_preservation_with_mode,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

const EVENTS: u64 = 20_000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

struct Fixture {
    _temporary: tempfile::TempDir,
    job: marketforge_engine::historical::job::ConvertJob,
    source: PhysicalMergeManifest,
    staging: PathBuf,
}

fn materialized_fixture() -> Fixture {
    let root = root();

    let mut job =
        load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");

    job.input_merge = fixture_path.clone();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    source.validate().unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let partitions = build_conversion_partition_plan(&fixture_path, &source, 2).unwrap();

    let prepass = build_combined_reference_checkpoints(
        &fixture_path,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("combined.staging");

    job.output.dataset_path = temporary.path().join("combined");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    let materialized = execute_parallel_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &partitions,
        &prepass,
        &selection,
        &staging,
        2,
        &parquet_config,
    )
    .unwrap();

    assert_eq!(materialized.metrics.events_written, EVENTS);

    let receipt =
        ConversionExecutionReceipt::new_combined(job.job_id, &materialized.metrics).unwrap();

    receipt.write(&staging).unwrap();

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
    assert_eq!(publication.files_written, 2);

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

    assert_eq!(depth.levels_read, 741_562);

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

    // Locate one converted Parquet file.
    let mut files = Vec::new();

    fn collect_parquet(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                collect_parquet(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }

    collect_parquet(&fixture.staging, &mut files);
    files.sort();

    let path = files.first().expect("missing converted Parquet file");

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
