use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use arrow_array::{Array, UInt32Array, UInt64Array};

use {
    marketforge_engine::historical::convert::execution::PhysicalConversionReader,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
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
