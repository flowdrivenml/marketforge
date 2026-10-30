use std::path::PathBuf;

use {
    marketforge_engine::historical::convert::ConversionDecimal,
    marketforge_engine::historical::convert::reference::PhysicalReferenceReader,
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
