use arrow_buffer::i256;
use marketforge_engine::process::convert::ConversionDecimal;

use marketforge_engine::process::convert::reference::{
    ReferenceKey, ReferenceObservation, ReferenceState, ReferenceUnavailable,
};

fn key(timestamp: i64, rank: u32, task: u64, ordinal: u64) -> ReferenceKey {
    ReferenceKey::new(timestamp, rank, task, ordinal)
}
fn decimal(value: i64) -> ConversionDecimal {
    let mantissa = i256::from_i128(i128::from(value) * 10_i128.pow(28));

    ConversionDecimal::new(mantissa).unwrap()
}
fn observation(
    stream: &str,
    timestamp: i64,
    rank: u32,
    task: u64,
    ordinal: u64,
    price: i64,
) -> ReferenceObservation {
    ReferenceObservation {
        dataset_id: 106,
        stream_id: stream.to_owned(),
        key: key(timestamp, rank, task, ordinal),
        price: decimal(price),
    }
}

#[test]
fn stores_latest_reference() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    state
        .update(observation("BTCUSDT", 200, 0, 0, 2, 61_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(300, 1, 0, 0), 1_000);

    assert_eq!(result.unwrap().price, decimal(61_000));
}

#[test]
fn rejects_timestamp_regression() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 200, 0, 0, 2, 60_000))
        .unwrap();

    assert!(
        state
            .update(observation("BTCUSDT", 100, 0, 0, 3, 61_000))
            .is_err()
    );
}

#[test]
fn rejects_duplicate_observation_key() {
    let mut state = ReferenceState::new();

    let observation = observation("BTCUSDT", 100, 0, 0, 1, 60_000);

    state.update(observation.clone()).unwrap();

    assert!(state.update(observation).is_err());
}

#[test]
fn accepts_same_timestamp_with_later_ordinal() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 2, 61_000))
        .unwrap();

    assert_eq!(state.len(), 1);
}

#[test]
fn accepts_equal_timestamp_earlier_stream_rank() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(100, 1, 0, 1), 1_000);

    assert!(result.is_ok());
}

#[test]
fn rejects_equal_timestamp_future_stream_rank() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 2, 0, 1, 60_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(100, 1, 0, 1), 1_000);

    assert_eq!(result.unwrap_err(), ReferenceUnavailable::Future);
}

#[test]
fn detects_stale_reference() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(201, 1, 0, 1), 100);

    assert_eq!(result.unwrap_err(), ReferenceUnavailable::Stale);
}

#[test]
fn accepts_reference_at_staleness_boundary() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(200, 1, 0, 1), 100);

    assert!(result.is_ok());
}

#[test]
fn reports_missing_reference() {
    let state = ReferenceState::new();

    let result = state.evaluate("UNKNOWN", key(100, 0, 0, 1), 100);

    assert_eq!(result.unwrap_err(), ReferenceUnavailable::Missing);
}

#[test]
fn rejects_zero_reference_price() {
    let mut state = ReferenceState::new();

    assert!(
        state
            .update(observation("BTCUSDT", 100, 0, 0, 1, 0))
            .is_err()
    );
}

#[test]
fn rejects_negative_reference_price() {
    let mut state = ReferenceState::new();

    assert!(
        state
            .update(observation("BTCUSDT", 100, 0, 0, 1, -1))
            .is_err()
    );
}

#[test]
fn preserves_reference_provenance() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 7, 42, 60_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(200, 1, 0, 0), 1_000).unwrap();

    assert_eq!(result.dataset_id, 106);
    assert_eq!(result.stream_id, "BTCUSDT");
    assert_eq!(result.key.task_rank, 7);
    assert_eq!(result.key.event_ordinal, 42);
}

#[test]
fn maintains_independent_reference_streams() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    state
        .update(observation("ETHBTC", 150, 1, 0, 1, 1))
        .unwrap();

    assert_eq!(state.len(), 2);

    let btc = state.evaluate("BTCUSDT", key(200, 2, 0, 0), 1_000).unwrap();

    let eth = state.evaluate("ETHBTC", key(200, 2, 0, 0), 1_000).unwrap();

    assert_eq!(btc.price, decimal(60_000));
    assert_eq!(eth.price, decimal(1));
}

#[test]
fn newer_observation_refreshes_staleness() {
    let mut state = ReferenceState::new();

    state
        .update(observation("BTCUSDT", 100, 0, 0, 1, 60_000))
        .unwrap();

    state
        .update(observation("BTCUSDT", 190, 0, 0, 2, 60_000))
        .unwrap();

    let result = state.evaluate("BTCUSDT", key(200, 1, 0, 0), 20);

    assert!(result.is_ok());
}
