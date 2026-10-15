use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::PathBuf,
};

use arrow_array::{Array, Decimal256Array, Int64Array, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::historical::normalize::{
    convert::execution::PhysicalConversionReader, merge::PhysicalMergeManifest,
};

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn fixture() -> (PathBuf, PhysicalMergeManifest) {
    let dataset = root().join("data/merged/conversion-multihop");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&std::fs::read(dataset.join("manifest.json")).unwrap()).unwrap();

    (dataset, manifest)
}

fn reader(batch_size: usize) -> PhysicalConversionReader {
    let (dataset, manifest) = fixture();

    PhysicalConversionReader::new(&dataset, &manifest, batch_size).unwrap()
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

#[test]
fn opens_physical_dataset() {
    let reader = reader(1024);

    assert_eq!(reader.records_read(), 0);
    assert_eq!(reader.files_opened(), 0);
}

#[test]
fn rejects_zero_batch_size() {
    let (dataset, manifest) = fixture();

    assert!(PhysicalConversionReader::new(&dataset, &manifest, 0).is_err());
}

// -----------------------------------------------------------------------------
// Arrow schema
// -----------------------------------------------------------------------------

#[test]
fn preserves_physical_trade_schema() {
    let mut reader = reader(1024);

    let batch = reader.next_batch().unwrap().unwrap();

    assert_eq!(batch.num_columns(), 24);

    assert!(batch.column_by_name("event_timestamp_ns").is_some());
    assert!(batch.column_by_name("price").is_some());
    assert!(batch.column_by_name("quantity_quote").is_some());
    assert!(batch.column_by_name("stream_rank").is_some());

    let prices = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(prices.precision(), 76);
    assert_eq!(prices.scale(), 28);
}

#[test]
fn preserves_original_arrow_values() {
    let (dataset, manifest) = fixture();

    let first_file = dataset.join(
        &manifest
            .files
            .iter()
            .min_by_key(|file| (file.partition_id, &file.path))
            .unwrap()
            .path,
    );

    let file = File::open(first_file).unwrap();

    let mut original_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(1024)
        .build()
        .unwrap();

    let original = original_reader.next().unwrap().unwrap();

    let mut conversion_reader = reader(1024);

    let converted = conversion_reader.next_batch().unwrap().unwrap();

    assert_eq!(original.schema(), converted.schema());
    assert_eq!(original.num_rows(), converted.num_rows());

    for index in 0..original.num_columns() {
        assert_eq!(
            original.column(index).to_data(),
            converted.column(index).to_data()
        );
    }
}

// -----------------------------------------------------------------------------
// Batch boundaries
// -----------------------------------------------------------------------------

#[test]
fn respects_batch_size() {
    let mut reader = reader(128);

    for _ in 0..10 {
        let batch = reader.next_batch().unwrap().unwrap();

        assert!(batch.num_rows() > 0);
        assert!(batch.num_rows() <= 128);
    }
}

#[test]
fn tracks_records_read() {
    let mut reader = reader(100);

    let first = reader.next_batch().unwrap().unwrap();
    let second = reader.next_batch().unwrap().unwrap();

    assert_eq!(
        reader.records_read(),
        (first.num_rows() + second.num_rows()) as u64
    );
}

// -----------------------------------------------------------------------------
// Ordering
// -----------------------------------------------------------------------------

#[test]
fn first_thousand_events_are_ordered() {
    let mut reader = reader(1000);

    let batch = reader.next_batch().unwrap().unwrap();

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let ranks = batch
        .column_by_name("stream_rank")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    let tasks = batch
        .column_by_name("task_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let ordinals = batch
        .column_by_name("event_ordinal")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let mut previous = None;

    for row in 0..batch.num_rows() {
        let key = (
            timestamps.value(row),
            ranks.value(row),
            tasks.value(row),
            ordinals.value(row),
        );

        if let Some(last) = previous {
            assert!(last <= key);
        }

        previous = Some(key);
    }
}

// -----------------------------------------------------------------------------
// Stream identity
// -----------------------------------------------------------------------------

#[test]
fn validates_reference_stream_identity() {
    let (_, manifest) = fixture();

    let expected = manifest
        .streams
        .iter()
        .map(|stream| (stream.stream_rank, stream.dataset_id))
        .collect::<HashMap<_, _>>();

    let mut reader = reader(65_536);

    let mut seen = HashSet::new();

    for _ in 0..20 {
        let Some(batch) = reader.next_batch().unwrap() else {
            break;
        };

        let ranks = batch
            .column_by_name("stream_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();

        let datasets = batch
            .column_by_name("dataset_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let rank = ranks.value(row);
            let dataset = datasets.value(row);

            assert_eq!(expected.get(&rank), Some(&dataset));

            seen.insert(dataset);
        }

        if seen.len() == expected.len() {
            break;
        }
    }

    assert_eq!(seen.len(), 3);
}

// -----------------------------------------------------------------------------
// Full dataset validation
// -----------------------------------------------------------------------------

#[test]
fn streams_entire_physical_dataset() {
    let (dataset, manifest) = fixture();

    let mut reader = PhysicalConversionReader::new(&dataset, &manifest, 65_536).unwrap();

    let expected = manifest
        .streams
        .iter()
        .map(|stream| (stream.stream_rank, stream.dataset_id))
        .collect::<HashMap<_, _>>();

    let mut counts = HashMap::<u64, u64>::new();
    let mut total = 0_u64;

    while let Some(batch) = reader.next_batch().unwrap() {
        let ranks = batch
            .column_by_name("stream_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();

        let datasets = batch
            .column_by_name("dataset_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let rank = ranks.value(row);
            let dataset = datasets.value(row);

            assert_eq!(expected.get(&rank), Some(&dataset));

            *counts.entry(dataset).or_default() += 1;
        }

        total += batch.num_rows() as u64;
    }

    assert_eq!(total, manifest.events_written);
    assert_eq!(reader.records_read(), manifest.events_written);
    assert_eq!(reader.files_opened(), manifest.files.len() as u64);

    assert_eq!(counts.get(&106), Some(&11_435_942));
    assert_eq!(counts.get(&129), Some(&52_748));
    assert_eq!(counts.get(&130), Some(&1_138_956));
}

// -----------------------------------------------------------------------------
// Invalid inputs
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_parquet_file() {
    let (dataset, mut manifest) = fixture();

    manifest.files[0].path = "timeline/partition-999999/missing.parquet".to_owned();

    let mut reader = PhysicalConversionReader::new(&dataset, &manifest, 1024).unwrap();

    assert!(reader.next_batch().is_err());
}
