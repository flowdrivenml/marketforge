use std::collections::HashSet;

use arrow_buffer::i256;

use {
    marketforge_engine::historical::convert::ConversionDecimal,
    marketforge_engine::historical::convert::execution::CombinedReferenceAlignment,
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::reference::ReferenceKey,
    marketforge_engine::historical::convert::reference::ReferenceObservation,
    marketforge_engine::historical::convert::reference::ReferenceUnavailable,
};

const ETHBTC: &str = "binance:spot:spot:ETHBTC:trade";
const BTCUSDC: &str = "binance:spot:spot:BTCUSDC:trade";
const BTCUSDT: &str = "binance:spot:spot:BTCUSDT:trade";

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

fn internal_selection() -> CombinedReferenceSelection {
    CombinedReferenceSelection {
        internal_streams: HashSet::from([ETHBTC.to_owned(), BTCUSDC.to_owned()]),
        external_streams: HashSet::new(),
    }
}

#[test]
fn accepts_internal_reference_after_event() {
    let mut alignment = CombinedReferenceAlignment::new(internal_selection(), None).unwrap();

    alignment
        .observe_internal(observation(129, ETHBTC, 100, 0, 1, 5))
        .unwrap();

    assert_eq!(alignment.internal_updates(), 1);
    assert_eq!(alignment.external_updates(), 0);
    assert_eq!(alignment.reference_updates(), 1);

    let current = ReferenceKey::new(101, 0, 0, 2);

    let reference = alignment.state().evaluate(ETHBTC, current, 10).unwrap();

    assert_eq!(reference.dataset_id, 129);
    assert_eq!(reference.key.timestamp_ns, 100);
}

#[test]
fn rejects_duplicate_internal_reference() {
    let mut alignment = CombinedReferenceAlignment::new(internal_selection(), None).unwrap();

    let reference = observation(129, ETHBTC, 100, 0, 1, 5);

    alignment.observe_internal(reference.clone()).unwrap();

    assert!(alignment.observe_internal(reference).is_err());

    assert_eq!(alignment.internal_updates(), 1);
}

#[test]
fn ignores_unselected_internal_streams() {
    let mut alignment = CombinedReferenceAlignment::new(internal_selection(), None).unwrap();

    alignment
        .observe_internal(observation(999, "unknown:trade", 100, 0, 1, 5))
        .unwrap();

    assert_eq!(alignment.internal_updates(), 0);
    assert!(alignment.state().is_empty());
}

#[test]
fn rejects_overlapping_reference_sources() {
    let selection = CombinedReferenceSelection {
        internal_streams: HashSet::from([ETHBTC.to_owned()]),
        external_streams: HashSet::from([ETHBTC.to_owned()]),
    };

    assert!(CombinedReferenceAlignment::new(selection, None).is_err());
}

#[test]
fn rejects_missing_external_cursor() {
    let selection = CombinedReferenceSelection {
        internal_streams: HashSet::new(),
        external_streams: HashSet::from([BTCUSDT.to_owned()]),
    };

    assert!(CombinedReferenceAlignment::new(selection, None).is_err());
}

#[test]
fn preserves_internal_reference_ordering() {
    let mut alignment = CombinedReferenceAlignment::new(internal_selection(), None).unwrap();

    alignment
        .observe_internal(observation(129, ETHBTC, 100, 0, 1, 5))
        .unwrap();

    let future = ReferenceKey::new(99, 0, 0, 1);

    assert_eq!(
        alignment.state().evaluate(ETHBTC, future, 100).unwrap_err(),
        ReferenceUnavailable::Future,
    );

    let current = ReferenceKey::new(100, 0, 0, 1);

    assert_eq!(
        alignment
            .state()
            .evaluate(ETHBTC, current, 100)
            .unwrap_err(),
        ReferenceUnavailable::Future,
    );

    let later = ReferenceKey::new(100, 1, 0, 1);

    assert!(alignment.state().evaluate(ETHBTC, later, 100).is_ok());
}

#[test]
fn reference_snapshot_preserves_state() {
    let mut alignment = CombinedReferenceAlignment::new(internal_selection(), None).unwrap();

    alignment
        .observe_internal(observation(129, ETHBTC, 100, 0, 1, 5))
        .unwrap();

    let snapshot = alignment.snapshot();

    assert_eq!(snapshot.len(), 1);

    alignment
        .observe_internal(observation(130, BTCUSDC, 110, 1, 1, 60_000))
        .unwrap();

    assert_eq!(snapshot.len(), 1);
    assert_eq!(alignment.state().len(), 2);
}
