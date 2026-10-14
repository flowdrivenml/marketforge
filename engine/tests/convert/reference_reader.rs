use std::{collections::HashMap, path::PathBuf};

use marketforge_engine::process::{
    convert::{
        ConversionDecimal,
        reference::{PhysicalReferenceReader, REFERENCE_READ_BATCH_SIZE},
    },
    merge::PhysicalMergeManifest,
};

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

fn reader(batch_size: usize) -> PhysicalReferenceReader {
    let (dataset, manifest) = fixture();

    PhysicalReferenceReader::new(&dataset, &manifest, batch_size).unwrap()
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

#[test]
fn opens_real_physical_dataset() {
    let reader = reader(1024);

    assert_eq!(reader.records_read(), 0);
}

#[test]
fn rejects_zero_batch_size() {
    let (dataset, manifest) = fixture();

    assert!(PhysicalReferenceReader::new(&dataset, &manifest, 0).is_err());
}

// -----------------------------------------------------------------------------
// Decoding
// -----------------------------------------------------------------------------

#[test]
fn decodes_first_reference_observation() {
    let mut reader = reader(1024);

    let observation = reader.next_observation().unwrap().unwrap();

    assert_eq!(observation.dataset_id, 106);
    assert_eq!(observation.key.stream_rank, 0);
    assert_eq!(observation.stream_id, "binance:spot:spot:BTCUSDT:trade");

    assert!(observation.price.is_positive());
    assert_eq!(reader.records_read(), 1);
}

#[test]
fn preserves_decimal256_price() {
    let mut reader = reader(1024);

    let observation = reader.next_observation().unwrap().unwrap();

    let reconstructed = ConversionDecimal::new(observation.price.mantissa()).unwrap();

    assert_eq!(observation.price, reconstructed);
}

// -----------------------------------------------------------------------------
// Chronological ordering
// -----------------------------------------------------------------------------

#[test]
fn first_thousand_events_are_ordered() {
    let mut reader = reader(1024);

    let mut previous = None;

    for _ in 0..1000 {
        let observation = reader.next_observation().unwrap().unwrap();

        if let Some(key) = previous {
            assert!(key <= observation.key);
        }

        previous = Some(observation.key);
    }
}

// -----------------------------------------------------------------------------
// Reference identities
// -----------------------------------------------------------------------------

#[test]
fn all_reference_instruments_appear() {
    let mut reader = reader(65_536);

    let mut found = HashMap::new();

    while let Some(observation) = reader.next_observation().unwrap() {
        found
            .entry(observation.dataset_id)
            .or_insert(observation.stream_id);

        if found.len() == 3 {
            break;
        }
    }

    assert_eq!(found.len(), 3);

    assert_eq!(found.get(&106).unwrap(), "binance:spot:spot:BTCUSDT:trade");

    assert_eq!(found.get(&129).unwrap(), "binance:spot:spot:ETHBTC:trade");

    assert_eq!(found.get(&130).unwrap(), "binance:spot:spot:BTCUSDC:trade");
}

// -----------------------------------------------------------------------------
// Full dataset streaming
// -----------------------------------------------------------------------------

#[test]
fn streams_entire_dataset_without_order_regression() {
    let (dataset, manifest) = fixture();

    let mut reader =
        PhysicalReferenceReader::new(&dataset, &manifest, REFERENCE_READ_BATCH_SIZE).unwrap();

    let mut counts = HashMap::<u64, u64>::new();
    let mut previous = None;

    while let Some(observation) = reader.next_observation().unwrap() {
        if let Some(key) = previous {
            assert!(key <= observation.key);
        }

        previous = Some(observation.key);

        *counts.entry(observation.dataset_id).or_default() += 1;
    }

    assert_eq!(reader.records_read(), manifest.events_written);

    assert_eq!(counts.values().sum::<u64>(), 12_627_646);

    assert_eq!(counts.get(&106), Some(&11_435_942));
    assert_eq!(counts.get(&129), Some(&52_748));
    assert_eq!(counts.get(&130), Some(&1_138_956));
}
