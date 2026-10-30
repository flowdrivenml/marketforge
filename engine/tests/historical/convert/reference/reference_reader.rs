use std::{collections::HashMap, path::PathBuf};

use {
    marketforge_engine::historical::convert::reference::PhysicalReferenceReader,
    marketforge_engine::historical::convert::reference::REFERENCE_READ_BATCH_SIZE,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
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
