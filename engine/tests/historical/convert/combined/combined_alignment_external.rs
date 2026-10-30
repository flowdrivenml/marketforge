use std::{collections::HashSet, fs, path::PathBuf};

use arrow_buffer::i256;

use {
    marketforge_engine::historical::convert::ConversionDecimal,
    marketforge_engine::historical::convert::execution::CombinedReferenceAlignment,
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::reference::ExternalReferenceCursor,
    marketforge_engine::historical::convert::reference::ReferenceKey,
    marketforge_engine::historical::convert::reference::ReferenceObservation,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
};

const ETHBTC: &str = "binance:spot:spot:ETHBTC:trade";
const BTCUSDC: &str = "binance:spot:spot:BTCUSDC:trade";
const BTCUSDT: &str = "binance:spot:spot:BTCUSDT:trade";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn observation(
    dataset_id: u64,
    stream_id: &str,
    timestamp_ns: i64,
    rank: u32,
    ordinal: u64,
    price: i128,
) -> ReferenceObservation {
    ReferenceObservation {
        dataset_id,
        stream_id: stream_id.to_owned(),
        key: ReferenceKey::new(timestamp_ns, rank, 0, ordinal),
        price: ConversionDecimal::new(i256::from_i128(price)).unwrap(),
    }
}

fn external_fixture() -> CombinedReferenceAlignment {
    let directory = root().join("data/merged/final-trades");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json")).unwrap()).unwrap();

    let selection = CombinedReferenceSelection {
        internal_streams: HashSet::from([ETHBTC.to_owned(), BTCUSDC.to_owned()]),
        external_streams: HashSet::from([BTCUSDT.to_owned()]),
    };

    let cursor = ExternalReferenceCursor::new(
        &directory,
        &manifest,
        selection.external_streams.iter().cloned(),
    )
    .unwrap();

    CombinedReferenceAlignment::new(selection, Some(cursor)).unwrap()
}

#[test]
fn advances_real_external_references() {
    let mut alignment = external_fixture();

    let timestamp_ns = 1_788_220_810_000_000_000_i64;

    alignment.advance_before(timestamp_ns).unwrap();

    assert!(alignment.external_updates() > 0);

    assert_eq!(alignment.internal_updates(), 0);

    let reference = alignment
        .state()
        .observations()
        .find(|reference| reference.stream_id == BTCUSDT)
        .unwrap();

    assert!(reference.key.timestamp_ns < timestamp_ns);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED REFERENCE ALIGNMENT");
    println!("{}", "=".repeat(90));

    println!("External updates : {}", alignment.external_updates());
    println!("Observations read: {}", alignment.observations_read());
    println!("Reference streams: {}", alignment.state().len());
    println!("STATUS           : PASS");
}

#[test]
fn internal_and_external_references_share_state() {
    let mut alignment = external_fixture();

    let timestamp_ns = 1_788_220_810_000_000_000_i64;

    alignment.advance_before(timestamp_ns).unwrap();

    alignment
        .observe_internal(observation(129, ETHBTC, timestamp_ns, 0, 1, 5))
        .unwrap();

    alignment
        .observe_internal(observation(130, BTCUSDC, timestamp_ns, 1, 1, 60_000))
        .unwrap();

    assert_eq!(alignment.state().len(), 3);
    assert_eq!(alignment.internal_updates(), 2);
    assert!(alignment.external_updates() > 0);

    assert_eq!(
        alignment.reference_updates(),
        alignment.internal_updates() + alignment.external_updates()
    );
}
