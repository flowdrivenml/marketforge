use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    infrastructure::rest::{HistoricalTradeCursor, HistoricalTradePage},
    recovery::{
        RecoveryAcquisitionKey, RecoveryAttemptId, RecoveryOperation, RecoveryRequest,
        RecoveryRequestError, RecoveryRequester, RecoverySourceEpoch,
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

// ============================================================
// FIXTURES
// ============================================================

fn source(channel: SourceChannel) -> CreateSourceStreamRequest {
    CreateSourceStreamRequest {
        instrument_id: 42,
        exchange: ExchangeId::Binance,
        symbol: "BTCUSDT".into(),
        market_category: MarketType::Spot,
        instrument_type: "spot".into(),

        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),
        settlement_asset: None,

        quantity_type: QuantityType::Base,
        contract_value: None,
        contract_value_asset: None,

        channel,
    }
}

fn requester(stream_id: &str, generation: u64, attempt: u64) -> RecoveryRequester {
    RecoveryRequester {
        stream_id: stream_id.into(),

        epoch: RecoverySourceEpoch {
            stream_generation: generation,
            connection_generation: generation + 10,
            subscription_generation: generation + 20,
        },

        attempt: RecoveryAttemptId(attempt),
    }
}

pub(super) fn depth_request(
    stream_id: &str,
    generation: u64,
    attempt: u64,
    depth: Option<u64>,
) -> RecoveryRequest {
    RecoveryRequest::new(
        requester(stream_id, generation, attempt),
        source(SourceChannel::Depth),
        1,
        RecoveryOperation::DepthSnapshot { depth },
    )
    .unwrap()
}

// ============================================================
// TESTS
// ============================================================

#[test]
fn identical_acquisitions_share_the_same_key() {
    let a = depth_request("stream-a", 10, 1, Some(1000));
    let b = depth_request("stream-b", 17, 3, Some(1000));

    assert_eq!(
        RecoveryAcquisitionKey::from_request(&a),
        RecoveryAcquisitionKey::from_request(&b),
    );

    assert_ne!(a.requester, b.requester);
}

#[test]
fn different_depths_have_different_keys() {
    let a = depth_request("stream-a", 10, 1, Some(100));
    let b = depth_request("stream-b", 10, 1, Some(1000));

    assert_ne!(
        RecoveryAcquisitionKey::from_request(&a),
        RecoveryAcquisitionKey::from_request(&b),
    );
}

#[test]
fn default_and_explicit_depth_are_distinct() {
    let a = depth_request("stream-a", 10, 1, None);
    let b = depth_request("stream-b", 10, 1, Some(1000));

    assert_ne!(
        RecoveryAcquisitionKey::from_request(&a),
        RecoveryAcquisitionKey::from_request(&b),
    );
}

#[test]
fn native_instrument_binding_affects_coalescing() {
    let a = depth_request("stream-a", 10, 1, Some(1000));

    let mut b = depth_request("stream-b", 10, 1, Some(1000));

    b.source.symbol = "ETHUSDT".into();

    assert_ne!(
        RecoveryAcquisitionKey::from_request(&a),
        RecoveryAcquisitionKey::from_request(&b),
    );
}

#[test]
fn configuration_generations_do_not_mix() {
    let a = depth_request("stream-a", 10, 1, Some(1000));

    let mut b = depth_request("stream-b", 10, 1, Some(1000));

    b.configuration_generation = 2;

    assert_ne!(
        RecoveryAcquisitionKey::from_request(&a),
        RecoveryAcquisitionKey::from_request(&b),
    );
}

#[test]
fn historical_cursors_affect_coalescing() {
    let make = |from_id| {
        RecoveryRequest::new(
            requester("trade-stream", 10, 1),
            source(SourceChannel::Trades),
            1,
            RecoveryOperation::HistoricalTrades {
                page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::TradeId { from_id })
                    .with_limit(100),
            },
        )
        .unwrap()
    };

    let a = make(1000);
    let b = make(2000);

    assert_ne!(
        RecoveryAcquisitionKey::from_request(&a),
        RecoveryAcquisitionKey::from_request(&b),
    );
}

#[test]
fn different_historical_page_limits_have_different_keys() {
    let make = |limit| {
        RecoveryRequest::new(
            requester("trade-stream", 10, 1),
            source(SourceChannel::Trades),
            1,
            RecoveryOperation::HistoricalTrades {
                page: HistoricalTradePage::latest().with_limit(limit),
            },
        )
        .unwrap()
    };

    assert_ne!(
        RecoveryAcquisitionKey::from_request(&make(100)),
        RecoveryAcquisitionKey::from_request(&make(500)),
    );
}

#[test]
fn rejects_channel_mismatch() {
    let result = RecoveryRequest::new(
        requester("stream-a", 10, 1),
        source(SourceChannel::Trades),
        1,
        RecoveryOperation::DepthSnapshot { depth: Some(1000) },
    );

    assert!(matches!(result, Err(RecoveryRequestError::ChannelMismatch),));
}

#[test]
fn rejects_zero_limits() {
    let result = RecoveryRequest::new(
        requester("stream-a", 10, 1),
        source(SourceChannel::Depth),
        1,
        RecoveryOperation::DepthSnapshot { depth: Some(0) },
    );

    assert!(matches!(result, Err(RecoveryRequestError::ZeroLimit),));
}

#[test]
fn different_requesters_share_keys_without_sharing_authority() {
    let a = depth_request("stream-a", 10, 1, Some(1000));
    let b = depth_request("stream-b", 11, 2, Some(1000));

    let key_a = RecoveryAcquisitionKey::from_request(&a);
    let key_b = RecoveryAcquisitionKey::from_request(&b);

    assert_eq!(key_a, key_b);

    assert_ne!(a.requester.epoch, b.requester.epoch,);

    assert_ne!(a.requester.attempt, b.requester.attempt,);
}
