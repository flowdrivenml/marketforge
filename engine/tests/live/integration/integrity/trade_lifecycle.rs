use marketforge_engine::{
    canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    live::{
        infrastructure::normalization::{
            NormalizationSource, NormalizedTrades, TradeRecordSemantics,
        },
        integrity::{
            BoundTradeIngress, BoundTradeIngressOutcome, CompiledTradeIntegrityPlan,
            TradeControllerOutcome, TradeCoverageAction, TradeIdentityKind, TradeIdentityLimits,
            TradeSequenceKind, TradeSourceBinding, TradeSourceBindingError, TradeSourceEpoch,
        },
    },
};

use rust_decimal::Decimal;

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

fn plan() -> CompiledTradeIntegrityPlan {
    CompiledTradeIntegrityPlan {
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
    }
}

fn ingress() -> BoundTradeIngress {
    BoundTradeIngress::new(
        binding(10),
        TradeIdentityLimits {
            max_ids: 100,
            max_id_bytes: 128,
            max_batch_records: 100,
        },
        plan(),
    )
    .unwrap()
}

fn trade(id: &str, exchange: Exchange, instrument_id: i64) -> Trade {
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange,
            instrument_id,
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
    }
}

fn message(trades: Vec<Trade>) -> NormalizedTrades {
    NormalizedTrades::new(
        STREAM,
        NormalizationSource::WebSocket,
        TradeRecordSemantics::Individual,
        trades,
    )
    .unwrap()
}

fn valid(id: &str) -> NormalizedTrades {
    message(vec![trade(id, Exchange::Bybit, 42)])
}

#[test]
fn trade_ingress_deduplicates_across_batches() {
    let mut ingress = ingress();

    let first = ingress.on_websocket(
        epoch(10),
        &message(vec![
            trade("A", Exchange::Bybit, 42),
            trade("B", Exchange::Bybit, 42),
        ]),
    );

    let BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)) = first else {
        panic!("expected accepted first trade batch");
    };

    assert_eq!(batch.trades.len(), 2);
    assert_eq!(batch.duplicate_records, 0);

    let second = ingress.on_websocket(
        epoch(10),
        &message(vec![
            trade("B", Exchange::Bybit, 42),
            trade("C", Exchange::Bybit, 42),
        ]),
    );

    let BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)) = second
    else {
        panic!("expected accepted second trade batch");
    };

    assert_eq!(batch.received_records, 2);
    assert_eq!(batch.duplicate_records, 1);
    assert_eq!(batch.trades.len(), 1);

    assert_eq!(batch.trades[0].trade_id.as_deref(), Some("C"),);

    assert!(ingress.controller().identities().contains("A"));
    assert!(ingress.controller().identities().contains("B"));
    assert!(ingress.controller().identities().contains("C"));
}

#[test]
fn mixed_instrument_batch_quarantines_without_partial_commit() {
    let mut ingress = ingress();

    let mixed = message(vec![
        trade("A", Exchange::Bybit, 42),
        trade("B", Exchange::Bybit, 999),
    ]);

    let outcome = ingress.on_websocket(epoch(10), &mixed);

    assert!(matches!(
        outcome,
        BoundTradeIngressOutcome::SourceQuarantined {
            reason: TradeSourceBindingError::WrongInstrument
        }
    ));

    assert!(ingress.is_quarantined());

    // The valid first record was not partially committed.
    assert_eq!(ingress.controller().identities().retained_count(), 0,);

    let later = ingress.on_websocket(epoch(10), &valid("C"));

    assert!(matches!(
        later,
        BoundTradeIngressOutcome::AdmissionSuspended { generation: 10 }
    ));

    assert!(!ingress.controller().identities().contains("C"));
}

#[test]
fn obsolete_trade_epoch_cannot_quarantine_current_stream() {
    let mut ingress = ingress();

    let wrong_source = message(vec![trade("BAD", Exchange::Binance, 999)]);

    let outcome = ingress.on_websocket(epoch(9), &wrong_source);

    assert!(matches!(outcome, BoundTradeIngressOutcome::ObsoleteEpoch));

    assert!(!ingress.is_quarantined());

    assert_eq!(ingress.controller().identities().retained_count(), 0,);

    let current = ingress.on_websocket(epoch(10), &valid("A"));

    assert!(matches!(
        current,
        BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(_))
    ));
}

#[test]
fn generation_change_clears_quarantine_but_marks_history_unverified() {
    let mut ingress = ingress();

    ingress.on_websocket(
        epoch(10),
        &message(vec![trade("BAD", Exchange::Binance, 42)]),
    );

    assert!(ingress.is_quarantined());

    ingress.begin_generation(binding(11)).unwrap();

    assert!(!ingress.is_quarantined());

    let result = ingress.on_websocket(epoch(11), &valid("NEW"));

    let BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)) = result
    else {
        panic!("expected accepted new-generation trades");
    };

    assert_eq!(batch.trades.len(), 1);

    // A new generation does not prove that old history
    // was recovered.
    assert!(batch.coverage_history_unverified);
}
