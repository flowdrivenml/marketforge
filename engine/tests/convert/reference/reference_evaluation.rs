use arrow_buffer::i256;

use marketforge_engine::{
    instrument::Currency,
    process::convert::{
        ConversionDecimal, ConversionDirection, ConversionEdge, ConversionPath,
        reference::{
            ConversionUnavailable, ReferenceKey, ReferenceObservation, ReferenceState,
            evaluate_conversion_path,
        },
    },
};

fn currency(value: &str) -> Currency {
    Currency::new(value).unwrap()
}

fn decimal(value: &str) -> ConversionDecimal {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches('-');

    let (integer, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));

    assert!(fractional.len() <= 28);

    let mantissa = format!(
        "{}{}{:0<28}",
        if negative { "-" } else { "" },
        integer,
        fractional
    );

    ConversionDecimal::new(i256::from_string(&mantissa).unwrap()).unwrap()
}

fn key(timestamp: i64, rank: u32, ordinal: u64) -> ReferenceKey {
    ReferenceKey::new(timestamp, rank, 0, ordinal)
}

fn edge(
    from: &str,
    to: &str,
    dataset_id: u64,
    rank: u32,
    stream: &str,
    direction: ConversionDirection,
) -> ConversionEdge {
    ConversionEdge {
        from: currency(from),
        to: currency(to),
        reference_dataset_id: dataset_id,
        reference_stream_rank: rank,
        reference_stream_id: stream.to_owned(),
        direction,
    }
}

fn path(currencies: &[&str], edges: Vec<ConversionEdge>) -> ConversionPath {
    ConversionPath {
        currencies: currencies.iter().map(|value| currency(value)).collect(),
        edges,
    }
}

fn update(
    state: &mut ReferenceState,
    dataset_id: u64,
    stream: &str,
    timestamp: i64,
    rank: u32,
    ordinal: u64,
    price: &str,
) {
    state
        .update(ReferenceObservation {
            dataset_id,
            stream_id: stream.to_owned(),
            key: key(timestamp, rank, ordinal),
            price: decimal(price),
        })
        .unwrap();
}

fn btc_usdt() -> ConversionEdge {
    edge(
        "BTC",
        "USDT",
        106,
        0,
        "BTCUSDT",
        ConversionDirection::Direct,
    )
}

fn eth_btc() -> ConversionEdge {
    edge("ETH", "BTC", 129, 1, "ETHBTC", ConversionDirection::Direct)
}

fn usdc_btc() -> ConversionEdge {
    edge(
        "USDC",
        "BTC",
        130,
        2,
        "BTCUSDC",
        ConversionDirection::Inverse,
    )
}

#[test]
fn evaluates_direct_conversion() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "60000");

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(200, 3, 1),
        1_000,
    )
    .unwrap();

    assert_eq!(result.rate, decimal("60000"));
    assert_eq!(result.references.len(), 1);
}

#[test]
fn evaluates_inverse_conversion() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "2");

    let inverse = edge(
        "USDT",
        "BTC",
        106,
        0,
        "BTCUSDT",
        ConversionDirection::Inverse,
    );

    let result = evaluate_conversion_path(
        &path(&["USDT", "BTC"], vec![inverse]),
        &state,
        key(200, 3, 1),
        1_000,
    )
    .unwrap();

    assert_eq!(result.rate, decimal("0.5"));
}

#[test]
fn evaluates_eth_to_usdt_multihop() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "60000");
    update(&mut state, 129, "ETHBTC", 110, 1, 1, "0.05");

    let result = evaluate_conversion_path(
        &path(&["ETH", "BTC", "USDT"], vec![eth_btc(), btc_usdt()]),
        &state,
        key(200, 3, 1),
        1_000,
    )
    .unwrap();

    assert_eq!(result.rate, decimal("3000"));
    assert_eq!(result.references.len(), 2);
}

#[test]
fn evaluates_usdc_to_usdt_multihop() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "60000");
    update(&mut state, 130, "BTCUSDC", 110, 2, 1, "59950");

    let result = evaluate_conversion_path(
        &path(&["USDC", "BTC", "USDT"], vec![usdc_btc(), btc_usdt()]),
        &state,
        key(200, 3, 1),
        1_000,
    )
    .unwrap();

    assert_eq!(result.rate, decimal("1.0008340283569641367806505421"));
}

#[test]
fn rejects_missing_reference() {
    let state = ReferenceState::new();

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(200, 3, 1),
        1_000,
    );

    assert_eq!(
        result.unwrap_err(),
        ConversionUnavailable::MissingReference("BTCUSDT".to_owned())
    );
}

#[test]
fn rejects_stale_reference() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "60000");

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(201, 3, 1),
        100,
    );

    assert_eq!(
        result.unwrap_err(),
        ConversionUnavailable::StaleReference("BTCUSDT".to_owned())
    );
}

#[test]
fn rejects_future_reference() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 200, 0, 1, "60000");

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(100, 3, 1),
        1_000,
    );

    assert_eq!(
        result.unwrap_err(),
        ConversionUnavailable::FutureReference("BTCUSDT".to_owned())
    );
}

#[test]
fn accepts_equal_timestamp_earlier_rank() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "60000");

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(100, 1, 1),
        0,
    );

    assert!(result.is_ok());
}

#[test]
fn rejects_equal_timestamp_later_rank() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 2, 1, "60000");

    let later_rank_edge = edge(
        "BTC",
        "USDT",
        106,
        2,
        "BTCUSDT",
        ConversionDirection::Direct,
    );

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![later_rank_edge]),
        &state,
        key(100, 1, 1),
        0,
    );

    assert_eq!(
        result.unwrap_err(),
        ConversionUnavailable::FutureReference("BTCUSDT".to_owned())
    );
}

#[test]
fn rejects_reference_dataset_mismatch() {
    let mut state = ReferenceState::new();

    update(&mut state, 999, "BTCUSDT", 100, 0, 1, "60000");

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(200, 3, 1),
        1_000,
    );

    assert!(matches!(
        result,
        Err(ConversionUnavailable::ReferenceIdentityMismatch(_))
    ));
}

#[test]
fn preserves_reference_provenance() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 42, "60000");

    let result = evaluate_conversion_path(
        &path(&["BTC", "USDT"], vec![btc_usdt()]),
        &state,
        key(200, 3, 1),
        1_000,
    )
    .unwrap();

    assert_eq!(result.references[0].dataset_id, 106);
    assert_eq!(result.references[0].key.event_ordinal, 42);
    assert_eq!(result.references[0].price, decimal("60000"));
}

#[test]
fn evaluates_identity_conversion() {
    let state = ReferenceState::new();

    let result =
        evaluate_conversion_path(&path(&["USDT"], vec![]), &state, key(200, 0, 1), 1_000).unwrap();

    assert_eq!(result.rate, ConversionDecimal::one());
    assert!(result.references.is_empty());
}

#[test]
fn evaluation_is_deterministic() {
    let mut state = ReferenceState::new();

    update(&mut state, 106, "BTCUSDT", 100, 0, 1, "60000");

    let conversion_path = path(&["BTC", "USDT"], vec![btc_usdt()]);

    let first = evaluate_conversion_path(&conversion_path, &state, key(200, 3, 1), 1_000).unwrap();

    let second = evaluate_conversion_path(&conversion_path, &state, key(200, 3, 1), 1_000).unwrap();

    assert_eq!(first.rate, second.rate);
    assert_eq!(first.references.len(), second.references.len());
}
