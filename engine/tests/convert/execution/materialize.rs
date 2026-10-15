use std::{
    fs::{self, File},
    path::PathBuf,
};

use arrow_array::{Array, Decimal256Array, RecordBatch, UInt8Array};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::materialize_trade_conversion,
            output::{
                CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY,
                CONVERSION_STATUS_UNAVAILABLE,
            },
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

// -----------------------------------------------------------------------------
// Project fixtures
// -----------------------------------------------------------------------------

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn source_dataset() -> PathBuf {
    root().join("data/merged/conversion-multihop")
}

fn load_manifest() -> PhysicalMergeManifest {
    serde_json::from_slice(&fs::read(source_dataset().join("manifest.json")).unwrap()).unwrap()
}

fn parquet_config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 3,
    }
}

// -----------------------------------------------------------------------------
// Create a small physical fixture
// -----------------------------------------------------------------------------

fn create_fixture(directory: &std::path::Path, rows: usize) -> PhysicalMergeManifest {
    let source = source_dataset();
    let mut manifest = load_manifest();

    let mut files = manifest.files.clone();
    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut remaining = rows;
    let mut batches = Vec::<RecordBatch>::new();

    for entry in files {
        if remaining == 0 {
            break;
        }

        let file = File::open(source.join(entry.path)).unwrap();

        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(remaining.min(65_536))
            .build()
            .unwrap();

        while remaining > 0 {
            let Some(batch) = reader.next() else {
                break;
            };

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

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();

    for batch in &batches {
        writer.write(batch).unwrap();
    }

    writer.close().unwrap();

    let first = batches.first().unwrap();
    let last = batches.last().unwrap();

    let timestamp = |batch: &RecordBatch, row: usize| -> i64 {
        batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(row)
    };

    let first_timestamp = timestamp(first, 0);
    let last_timestamp = timestamp(last, last.num_rows() - 1);

    manifest.events_written = rows as u64;
    manifest.trade_events = rows as u64;
    manifest.depth_events = 0;
    manifest.canonical_rows = rows as u64;
    manifest.files_written = 1;
    manifest.row_groups = 1;

    manifest.start_timestamp_ns = Some(first_timestamp);
    manifest.end_timestamp_ns = Some(last_timestamp);

    manifest.files = vec![marketforge_engine::historical::merge::MergeTimelineFile {
        path: "timeline/partition-000000/part-000000.parquet".to_owned(),
        size_bytes: fs::metadata(&path).unwrap().len(),
        partition_id: Some(0),
        rows: Some(rows as u64),
        start_timestamp_ns: Some(first_timestamp),
        end_timestamp_ns: Some(last_timestamp),
    }];

    manifest
}

// -----------------------------------------------------------------------------
// Physical conversion integration test
// -----------------------------------------------------------------------------

#[test]
fn materializes_real_trade_conversion_fixture() {
    let temporary = tempfile::tempdir().unwrap();

    let input = temporary.path().join("input");
    let staging = temporary.path().join("staging");

    fs::create_dir_all(&input).unwrap();

    let manifest = create_fixture(&input, 20_000);

    let mut job =
        load_convert_job(root().join("data/.jobs/convert/convert-multihop.json")).unwrap();

    job.input_merge = input.clone();

    let catalog = InstrumentCatalog::load(&root().join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root()).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------------------
    // Execute physical materialization
    // -------------------------------------------------------------------------

    let metrics =
        materialize_trade_conversion(&job, &manifest, &plan, &staging, &parquet_config()).unwrap();

    assert_eq!(metrics.events_read, 20_000);
    assert_eq!(metrics.events_written, 20_000);

    assert_eq!(
        metrics.identity_events + metrics.converted_events + metrics.unavailable_events,
        20_000
    );

    assert!(metrics.files_written >= 1);

    // -------------------------------------------------------------------------
    // Validate physical output
    // -------------------------------------------------------------------------

    let mut parquet_files = fs::read_dir(&staging)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect::<Vec<_>>();

    parquet_files.sort();

    assert_eq!(parquet_files.len() as u64, metrics.files_written);

    let mut total = 0_u64;
    let mut identity = 0_u64;
    let mut converted = 0_u64;
    let mut unavailable = 0_u64;

    for path in parquet_files {
        let file = File::open(path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            total += batch.num_rows() as u64;

            let statuses = batch
                .column_by_name("conversion_status")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            let prices = batch
                .column_by_name("price_target")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            for row in 0..batch.num_rows() {
                match statuses.value(row) {
                    CONVERSION_STATUS_IDENTITY => {
                        identity += 1;
                        assert!(!prices.is_null(row));
                    }

                    CONVERSION_STATUS_CONVERTED => {
                        converted += 1;
                        assert!(!prices.is_null(row));
                    }

                    CONVERSION_STATUS_UNAVAILABLE => {
                        unavailable += 1;
                        assert!(prices.is_null(row));
                    }

                    other => panic!("unexpected conversion status {other}"),
                }
            }
        }
    }

    assert_eq!(total, 20_000);
    assert_eq!(identity, metrics.identity_events);
    assert_eq!(converted, metrics.converted_events);
    assert_eq!(unavailable, metrics.unavailable_events);
}
