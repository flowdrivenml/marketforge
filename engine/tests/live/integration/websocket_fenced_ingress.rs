use rust_decimal::Decimal;
use std::path::PathBuf;

use marketforge_engine::{
    canonical::{BookSide, L2LevelUpdate, SequenceMetadata},
    live::{
        config::format::DepthFormatConfig,
        infrastructure::normalization::{DepthMessageKind, NormalizedDepth},
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler, DepthControllerOutcome,
            DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
        },
    },
};
use marketforge_engine::{
    canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    live::{
        infrastructure::{
            normalization::{NormalizationSource, NormalizedTrades, TradeRecordSemantics},
            websocket::{ConnectionFence, FencedIngress, FencedIngressError},
        },
        integrity::{
            BoundTradeIngress, BoundTradeIngressOutcome, CompiledTradeIntegrityPlan,
            TradeControllerOutcome, TradeCoverageAction, TradeIdentityKind, TradeIdentityLimits,
            TradeSequenceKind, TradeSourceBinding, TradeSourceEpoch,
        },
    },
};

// ============================================================
// TEST IDENTITY
// ============================================================

const STREAM: &str = "integration:bybit:spot:BTCUSDT:trades";

fn epoch(generation: u64) -> TradeSourceEpoch {
    TradeSourceEpoch {
        stream_generation: generation,
        connection_generation: generation,
        subscription_generation: generation,
    }
}

fn binding(generation: u64) -> TradeSourceBinding {
    TradeSourceBinding::new(
        STREAM,
        Exchange::Bybit,
        "spot",
        42,
        "BTCUSDT",
        epoch(generation),
    )
    .unwrap()
}

// ============================================================
// EXISTING TRADE INTEGRITY CONTROLLER
// ============================================================

fn ingress() -> BoundTradeIngress {
    let plan = CompiledTradeIntegrityPlan {
        format_id: "bybit_spot".into(),
        identity_kind: TradeIdentityKind::Opaque,
        sequence_kind: TradeSequenceKind::None,
        detect_duplicates: true,
        detect_id_regression: false,
        detect_id_gaps: false,
        detect_cross_sequence_regression: false,
        allow_equal_cross_sequence: false,
        coverage_action: TradeCoverageAction::ReportUnverifiedCoverage,
        supports_individual_trades: true,
    };

    BoundTradeIngress::new(
        binding(1),
        TradeIdentityLimits {
            max_ids: 100,
            max_id_bytes: 128,
            max_batch_records: 100,
        },
        plan,
    )
    .unwrap()
}

// ============================================================
// CANONICAL TRADE FIXTURE
// ============================================================

fn message(id: &str) -> NormalizedTrades {
    let trade = Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 42,
            symbol: "BTCUSDT".into(),
            stream_id: STREAM.into(),
        },

        trade_id: Some(id.into()),
        sequence: None,

        side: TradeSide::Buy,
        price: Decimal::from(80_000),

        quantity_base: Some(Decimal::ONE),
        quantity_quote: None,
        quantity_contracts: None,

        is_rpi: None,
        trade_iv: None,
        mark_iv: None,

        index_price: None,
        mark_price: None,
    };

    NormalizedTrades::new(
        STREAM,
        NormalizationSource::WebSocket,
        TradeRecordSemantics::Individual,
        vec![trade],
    )
    .unwrap()
}

// ============================================================
// VALID GENERATION
// ============================================================

#[test]
fn w33_fenced_trade_ingress_accepts_ready_generation() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();
    fence.ready(&stamp).unwrap();

    let mut controller = ingress();

    let result = FencedIngress::trade(
        &fence,
        &stamp,
        epoch(1),
        &mut controller,
        &message("TRADE-1"),
    )
    .unwrap();

    assert!(matches!(
        result,
        BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(_)),
    ));

    assert!(controller.controller().identities().contains("TRADE-1"));
}

// ============================================================
// GENERATION MISMATCH
// ============================================================

#[test]
fn w33_fenced_trade_ingress_rejects_generation_mismatch() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();
    fence.ready(&stamp).unwrap();

    let mut controller = ingress();

    let result = FencedIngress::trade(
        &fence,
        &stamp,
        epoch(999),
        &mut controller,
        &message("STALE"),
    );

    assert!(matches!(
        result,
        Err(FencedIngressError::GenerationMismatch {
            source_generation: 999,
            connection_generation: 1,
        }),
    ));

    // The stale trade must not enter deduplication state.
    assert!(!controller.controller().identities().contains("STALE"));
}

// ============================================================
// DISCONNECTED GENERATION
// ============================================================

#[test]
fn w33_fenced_trade_ingress_rejects_revoked_connection() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();
    fence.ready(&stamp).unwrap();

    fence.failed(&stamp).unwrap();

    let mut controller = ingress();

    let result = FencedIngress::trade(
        &fence,
        &stamp,
        epoch(1),
        &mut controller,
        &message("AFTER-DISCONNECT"),
    );

    assert!(matches!(
        result,
        Err(FencedIngressError::ConnectionNotReady),
    ));

    assert!(
        !controller
            .controller()
            .identities()
            .contains("AFTER-DISCONNECT")
    );
}

// ============================================================
// HANDSHAKE IS NOT READY
// ============================================================

#[test]
fn w33_fenced_trade_ingress_rejects_connected_but_unready() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    let mut controller = ingress();

    assert!(matches!(
        FencedIngress::trade(
            &fence,
            &stamp,
            epoch(1),
            &mut controller,
            &message("TOO-EARLY"),
        ),
        Err(FencedIngressError::ConnectionNotReady),
    ));

    assert!(!controller.controller().identities().contains("TOO-EARLY"));
}

// ============================================================
// OLD DATA AFTER RECONNECT
// ============================================================

#[test]
fn w33_fenced_trade_ingress_rejects_old_epoch_after_reconnect() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let first = fence.begin_connect().unwrap();

    fence.connected(&first).unwrap();
    fence.ready(&first).unwrap();

    fence.failed(&first).unwrap();

    let second = fence.begin_connect().unwrap();

    fence.connected(&second).unwrap();
    fence.ready(&second).unwrap();

    let mut controller = ingress();

    assert!(matches!(
        FencedIngress::trade(&fence, &first, epoch(1), &mut controller, &message("OLD"),),
        Err(FencedIngressError::ConnectionNotReady),
    ));

    // The replacement connection has valid transport
    // authority, but the controller is still bound to
    // source epoch 1.
    //
    // Its existing source-binding logic must reject
    // epoch 2 until the stream is correctly rebound.
    let result =
        FencedIngress::trade(&fence, &second, epoch(2), &mut controller, &message("NEW")).unwrap();

    assert!(!matches!(
        result,
        BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(_)),
    ));

    assert!(!controller.controller().identities().contains("NEW"));
}

fn depth_epoch(generation: u64) -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: generation,
        connection_generation: generation,
        subscription_generation: generation,
    }
}

fn depth_ingress() -> BoundDepthIngress {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/formats/depth/bybit_spot.toml");

    let format = DepthFormatConfig::load(path).unwrap();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    let binding = DepthSourceBinding::new(
        "integration:bybit:spot:BTCUSDT:depth",
        Exchange::Bybit,
        "spot",
        42,
        "BTCUSDT",
        Some(200),
        depth_epoch(1),
    )
    .unwrap();

    BoundDepthIngress::new(
        binding,
        plan,
        DepthBufferLimits {
            max_messages: 16,
            max_bytes: 1_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap()
}

fn depth_message() -> NormalizedDepth {
    let stream = "integration:bybit:spot:BTCUSDT:depth";

    let level = L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 42,
            symbol: "BTCUSDT".into(),
            stream_id: stream.into(),
        },

        side: BookSide::Bid,
        price: Decimal::from(80_000),

        quantity_base: Some(Decimal::ONE),
        quantity_quote: None,
        quantity_contracts: None,

        order_count: None,
    };

    NormalizedDepth::new(
        stream,
        NormalizationSource::WebSocket,
        DepthMessageKind::Snapshot,
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(100),
            sequence_previous: None,
            cross_sequence: Some(100),
        },
        vec![level],
        Some(1_000_000_000),
        Some(200),
    )
    .unwrap()
}

#[test]
fn w33_fenced_depth_ingress_rejects_stale_generation() {
    let fence = ConnectionFence::new("runtime-1", "depth-slot").unwrap();

    let first = fence.begin_connect().unwrap();

    fence.connected(&first).unwrap();
    fence.ready(&first).unwrap();

    // Disconnect before processing the queued snapshot.
    fence.failed(&first).unwrap();

    let second = fence.begin_connect().unwrap();

    fence.connected(&second).unwrap();
    fence.ready(&second).unwrap();

    let mut controller = depth_ingress();

    let result = FencedIngress::depth(
        &fence,
        &first,
        depth_epoch(1),
        &mut controller,
        depth_message(),
        1024,
    );

    assert!(matches!(
        result,
        Err(FencedIngressError::ConnectionNotReady),
    ));

    // No stale snapshot may establish synchronization.
    assert!(!controller.is_synchronized());

    // Even though the new physical connection is Ready,
    // its epoch is not automatically authorized by the
    // existing depth controller.
    let result = FencedIngress::depth(
        &fence,
        &second,
        depth_epoch(2),
        &mut controller,
        depth_message(),
        1024,
    )
    .unwrap();

    assert!(!matches!(result, DepthControllerOutcome::Accepted { .. }));

    assert!(!controller.is_synchronized());
}
