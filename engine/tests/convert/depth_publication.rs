use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use arrow_array::{Int64Array, RecordBatch};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::materialize_depth_conversion,
            output::{ConversionExecutionReceipt, ConversionManifest, publish_depth_conversion},
            plan_conversion, resolve_conversion_catalog,
        },
        merge::{MergeTimelineFile, PhysicalMergeManifest, UnifiedMergeMode},
    },
};

const EVENTS: usize = 12_194;
const WORKERS: usize = 16;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn timestamp(batch: &RecordBatch, row: usize) -> i64 {
    batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}

fn create_depth_source(
    directory: &Path,
    source_directory: &Path,
    original: &PhysicalMergeManifest,
) -> PhysicalMergeManifest {
    assert_eq!(original.mode, UnifiedMergeMode::Depth);

    let mut files = original.files.clone();

    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut remaining = EVENTS;
    let mut batches = Vec::new();

    for entry in files {
        if remaining == 0 {
            break;
        }

        let file = File::open(source_directory.join(entry.path)).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(remaining.min(4096))
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

    let output = directory.join("timeline/partition-000000");
    fs::create_dir_all(&output).unwrap();

    let path = output.join("part-000000.parquet");

    let file = File::create(&path).unwrap();

    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None).unwrap();

    for batch in &batches {
        writer.write(batch).unwrap();
    }

    writer.close().unwrap();

    let first = batches.first().unwrap();
    let last = batches.last().unwrap();

    let first_timestamp = timestamp(first, 0);
    let last_timestamp = timestamp(last, last.num_rows() - 1);

    let mut manifest = original.clone();

    manifest.events_written = EVENTS as u64;
    manifest.trade_events = 0;
    manifest.depth_events = EVENTS as u64;

    manifest.canonical_rows = batches
        .iter()
        .map(|batch| {
            let counts = batch
                .column_by_name("canonical_row_count")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();

            (0..counts.len()).map(|row| counts.value(row)).sum::<u64>()
        })
        .sum();

    manifest.files_written = 1;
    manifest.row_groups = 1;

    manifest.start_timestamp_ns = Some(first_timestamp);
    manifest.end_timestamp_ns = Some(last_timestamp);

    manifest.files = vec![MergeTimelineFile {
        path: "timeline/partition-000000/part-000000.parquet".into(),
        size_bytes: fs::metadata(&path).unwrap().len(),
        partition_id: Some(0),
        rows: Some(EVENTS as u64),
        start_timestamp_ns: Some(first_timestamp),
        end_timestamp_ns: Some(last_timestamp),
    }];

    // The extracted fixture does not preserve original
    // physical partition checkpoint coordinates.
    // The extracted fixture has no original physical checkpoint coordinates.
    manifest.checkpoints.clear();
    manifest.skipped_initializations.clear();
    manifest.boundary_links.clear();

    // Reconstruction metadata belongs to the original physical merge.
    // It must not reference checkpoints removed from this fixture.
    use marketforge_engine::process::merge::{
        PhysicalInitialReconstruction, PhysicalReconstructionAvailability,
    };

    manifest.initial_reconstruction = manifest
        .streams
        .iter()
        .map(|stream| PhysicalInitialReconstruction {
            dataset_id: stream.dataset_id,
            availability: PhysicalReconstructionAvailability::Unavailable,
            first_physical_checkpoint_ns: None,
            prior_canonical_checkpoint_ns: None,
        })
        .collect();

    manifest.validate().unwrap();

    manifest
}

struct Fixture {
    _temporary: tempfile::TempDir,
    job: marketforge_engine::job::ConvertJob,
    source: PhysicalMergeManifest,
    staging: PathBuf,
    metrics: marketforge_engine::process::convert::execution::PhysicalDepthConversionMetrics,
}

fn fixture() -> Fixture {
    let temporary = tempfile::tempdir().unwrap();

    let root = root();

    let original_job =
        load_convert_job(root.join("data/.jobs/convert/convert-depth-test.json")).unwrap();

    let original_source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(original_job.input_merge.join("manifest.json")).unwrap())
            .unwrap();

    let input = temporary.path().join("input");
    let staging = temporary.path().join("depth.staging");
    let final_path = temporary.path().join("depth");

    fs::create_dir_all(&input).unwrap();

    let source = create_depth_source(&input, &original_job.input_merge, &original_source);

    let mut job = original_job;

    job.input_merge = input;
    job.output.dataset_path = final_path;

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let references: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let parquet = MergeParquetConfig {
        row_group_target_mb: 16,
        file_target_mb: 64,
        compression_level: 3,
    };

    let metrics =
        materialize_depth_conversion(&job, &source, &references, &plan, &staging, &parquet, None)
            .unwrap();

    assert_eq!(metrics.events_written, EVENTS as u64);

    ConversionExecutionReceipt::new_depth(job.job_id, &metrics)
        .unwrap()
        .write(&staging)
        .unwrap();

    Fixture {
        _temporary: temporary,
        job,
        source,
        staging,
        metrics,
    }
}

#[test]
fn publishes_complete_depth_conversion() {
    let fixture = fixture();

    let publication =
        publish_depth_conversion(&fixture.job, &fixture.source, &fixture.staging, WORKERS).unwrap();

    assert!(!fixture.staging.exists());
    assert!(publication.dataset_path.is_dir());
    assert!(publication.manifest_path.is_file());

    let manifest: ConversionManifest =
        serde_json::from_slice(&fs::read(&publication.manifest_path).unwrap()).unwrap();

    manifest.validate().unwrap();

    assert_eq!(manifest.output.content_type, "depth");
    assert_eq!(manifest.statistics.events_written, EVENTS as u64);

    let depth = manifest.statistics.depth.as_ref().unwrap();

    assert_eq!(depth.levels_read, fixture.metrics.levels_read);
    assert_eq!(depth.levels_converted, fixture.metrics.levels_converted,);
    assert_eq!(depth.levels_unavailable, fixture.metrics.levels_unavailable,);

    assert_eq!(manifest.integrity.source_events_compared, EVENTS as u64,);

    assert_eq!(
        manifest.statistics.reference_updates,
        fixture.metrics.reference_updates,
    );

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — ATOMIC DEPTH PUBLICATION");
    println!("{}", "=".repeat(90));

    println!("Events          : {}", publication.events_written);
    println!("Depth levels    : {}", depth.levels_read);
    println!("Files           : {}", publication.files_written);
    println!("Workers         : {}", WORKERS);
    println!(
        "Source preserved: {}",
        manifest.integrity.source_events_compared
    );
    println!("STATUS          : PASS");
}

#[test]
fn rejects_existing_depth_destination() {
    let fixture = fixture();

    let final_path = &fixture.job.output.dataset_path;

    fs::create_dir_all(final_path).unwrap();
    fs::write(final_path.join("marker.txt"), "existing").unwrap();

    assert!(
        publish_depth_conversion(&fixture.job, &fixture.source, &fixture.staging, WORKERS,)
            .is_err()
    );

    assert!(fixture.staging.exists());

    assert_eq!(
        fs::read_to_string(final_path.join("marker.txt")).unwrap(),
        "existing",
    );
}

#[test]
fn rejects_missing_depth_receipt() {
    let fixture = fixture();

    fs::remove_file(fixture.staging.join("execution.json")).unwrap();

    assert!(
        publish_depth_conversion(&fixture.job, &fixture.source, &fixture.staging, WORKERS,)
            .is_err()
    );

    assert!(fixture.staging.exists());
    assert!(!fixture.job.output.dataset_path.exists());
}

#[test]
fn rejects_incorrect_depth_receipt() {
    let fixture = fixture();

    let path = fixture.staging.join("execution.json");

    let mut receipt: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

    receipt["depth"]["levels_converted"] = serde_json::json!(1);

    fs::write(&path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();

    assert!(
        publish_depth_conversion(&fixture.job, &fixture.source, &fixture.staging, WORKERS,)
            .is_err()
    );

    assert!(fixture.staging.exists());
    assert!(!fixture.job.output.dataset_path.exists());
}
