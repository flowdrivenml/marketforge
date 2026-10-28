use marketforge_engine::live::integrity::{IntegrityAdmissionError, IntegrityAdmissionState};
use marketforge_engine::{
    live::{
        infrastructure::normalization::{
            NormalizationSource, NormalizedTrades, TradeRecordSemantics,
        },
        integrity::{
            BoundTradeIngressOutcome, CompiledTradeIntegrityPlan, CreateTradeIntegrity,
            IntegrityDecision, IntegrityEffect, IntegrityFacadeFactory, IntegrityInput,
            IntegrityLifecycleEvent, IntegrityLifecycleOutcome, IntegrityOutcome,
            StreamIntegrityHandle, TradeControllerOutcome, TradeCoverageAction, TradeIdentityKind,
            TradeIdentityLimits, TradeSequenceKind, TradeSourceBinding, TradeSourceEpoch,
        },
    },
    shared::domain::canonical::{EventEnvelope, Exchange, Trade, TradeSide},
};

use rust_decimal::Decimal;

const STREAM: &str = "bybit:spot:BTCUSDT:trades";

// ============================================================
// FIXTURES
// ============================================================

fn epoch(stream: u64, connection: u64, subscription: u64) -> TradeSourceEpoch {
    TradeSourceEpoch {
        stream_generation: stream,
        connection_generation: connection,
        subscription_generation: subscription,
    }
}

fn binding(current: TradeSourceEpoch) -> TradeSourceBinding {
    TradeSourceBinding::new(STREAM, Exchange::Bybit, "spot", 42, "BTCUSDT", current).unwrap()
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

fn integrity() -> StreamIntegrityHandle {
    IntegrityFacadeFactory::trades(CreateTradeIntegrity {
        binding: binding(epoch(10, 20, 30)),
        plan: plan(),

        identity_limits: TradeIdentityLimits {
            max_ids: 100,
            max_id_bytes: 128,
            max_batch_records: 100,
        },
    })
    .unwrap()
}

fn trade(id: &str, exchange: Exchange) -> Trade {
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange,
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
    }
}

fn message(id: &str) -> NormalizedTrades {
    NormalizedTrades::new(
        STREAM,
        NormalizationSource::WebSocket,
        TradeRecordSemantics::Individual,
        vec![trade(id, Exchange::Bybit)],
    )
    .unwrap()
}

fn input<'a>(current: TradeSourceEpoch, message: &'a NormalizedTrades) -> IntegrityInput<'a> {
    IntegrityInput::Trades {
        epoch: current,
        message,
    }
}

fn accepted_ids(outcome: IntegrityOutcome) -> Vec<String> {
    let IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
        TradeControllerOutcome::Accepted(batch),
    )) = outcome
    else {
        panic!("expected accepted trade batch");
    };

    batch
        .trades
        .into_iter()
        .map(|trade| trade.trade_id.unwrap())
        .collect()
}

// ============================================================
// TESTS
// ============================================================

#[test]
fn factory_creates_source_bound_trade_handle() {
    let integrity = integrity();

    assert_eq!(integrity.stream_id(), STREAM);
    assert_eq!(integrity.stream_generation(), 10);
    assert!(integrity.book().is_none());

    let status = integrity.status();

    assert_eq!(status.depth_synchronized, None);
    assert_eq!(status.trades_admissible, Some(true));
    assert_eq!(status.trades_quarantined, Some(false));
}

#[test]
fn single_and_batch_processing_are_equivalent() {
    let mut single = integrity();
    let mut batched = integrity();

    let current = epoch(10, 20, 30);

    let messages = [message("A"), message("B"), message("B"), message("C")];

    let single_results: Vec<Vec<String>> = messages
        .iter()
        .map(|message| accepted_ids(single.process(input(current, message))))
        .collect();

    let batch = batched.evaluate_batch(messages.iter().map(|message| input(current, message)));

    assert_eq!(batch.len(), 4);

    let batch_results: Vec<Vec<String>> = batch.outcomes.into_iter().map(accepted_ids).collect();

    assert_eq!(single_results, batch_results);

    assert_eq!(
        batch_results,
        vec![
            vec!["A".to_string()],
            vec!["B".to_string()],
            Vec::<String>::new(),
            vec!["C".to_string()],
        ],
    );
}

#[test]
fn batch_callback_observes_each_committed_transition() {
    let mut integrity = integrity();

    let current = epoch(10, 20, 30);

    let messages = [message("A"), message("B"), message("B"), message("C")];

    let mut observed = Vec::new();

    integrity
        .process_batch_with(
            messages.iter().map(|message| input(current, message)),
            |handle, outcome| {
                assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

                let mut effects = Vec::new();

                outcome.for_each_effect(|effect| {
                    effects.push(effect);
                });

                assert_eq!(effects, vec![IntegrityEffect::AcceptedData],);

                observed.push((accepted_ids(outcome), handle.status().trades_admissible));

                Ok::<(), ()>(())
            },
        )
        .unwrap();

    assert_eq!(observed.len(), 4);

    assert_eq!(observed[0].0, vec!["A"]);
    assert_eq!(observed[1].0, vec!["B"]);
    assert!(observed[2].0.is_empty());
    assert_eq!(observed[3].0, vec!["C"]);

    assert!(
        observed
            .iter()
            .all(|(_, admissible)| *admissible == Some(true))
    );
}

#[test]
fn obsolete_epoch_is_ignored_without_quarantine() {
    let mut integrity = integrity();

    let invalid_epoch = epoch(10, 19, 30);

    let message = message("A");

    let outcome = integrity.process(input(invalid_epoch, &message));

    assert_eq!(outcome.decision(), IntegrityDecision::Ignored,);

    assert!(matches!(
        outcome,
        IntegrityOutcome::Trades(BoundTradeIngressOutcome::ObsoleteEpoch),
    ));

    assert_eq!(integrity.status().trades_quarantined, Some(false),);
}

#[test]
fn source_quarantine_blocks_subsequent_admission() {
    let mut integrity = integrity();

    let current = epoch(10, 20, 30);

    let invalid = NormalizedTrades::new(
        STREAM,
        NormalizationSource::WebSocket,
        TradeRecordSemantics::Individual,
        vec![trade("BAD", Exchange::Binance)],
    )
    .unwrap();

    let outcome = integrity.process(input(current, &invalid));

    assert_eq!(outcome.decision(), IntegrityDecision::Rejected,);

    assert!(matches!(
        outcome,
        IntegrityOutcome::Trades(BoundTradeIngressOutcome::SourceQuarantined { .. }),
    ));

    assert_eq!(integrity.status().trades_quarantined, Some(true),);

    let valid = message("A");

    let subsequent = integrity.process(input(current, &valid));

    assert!(matches!(
        subsequent,
        IntegrityOutcome::Trades(BoundTradeIngressOutcome::AdmissionSuspended { .. }),
    ));
}

#[test]
fn trusted_generation_transition_clears_quarantine() {
    let mut integrity = integrity();

    let old = epoch(10, 20, 30);
    let next = epoch(11, 21, 31);

    let invalid = NormalizedTrades::new(
        STREAM,
        NormalizationSource::WebSocket,
        TradeRecordSemantics::Individual,
        vec![trade("BAD", Exchange::Binance)],
    )
    .unwrap();

    integrity.process(input(old, &invalid));

    assert_eq!(integrity.status().trades_quarantined, Some(true),);

    let transition = integrity
        .on_lifecycle(IntegrityLifecycleEvent::TradeGeneration(binding(next)))
        .unwrap();

    assert_eq!(transition, IntegrityLifecycleOutcome::GenerationAdvanced,);

    assert_eq!(integrity.status().trades_quarantined, Some(false),);

    assert_eq!(integrity.stream_generation(), 11);

    let valid = message("A");

    let outcome = integrity.process(input(next, &valid));

    let IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
        TradeControllerOutcome::Accepted(batch),
    )) = outcome
    else {
        panic!("expected accepted new-generation trade");
    };

    assert!(batch.coverage_history_unverified);
    assert_eq!(batch.trades.len(), 1);
}

#[test]
fn interruption_does_not_claim_trade_recovery() {
    let mut integrity = integrity();

    let outcome = integrity
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert_eq!(outcome, IntegrityLifecycleOutcome::TradeCoverageInterrupted,);

    // This notification reports unresolved coverage.
    // It does not prove that missing historical records
    // have been recovered.
}

#[test]
fn interruption_blocks_trade_processing() {
    let mut handle = integrity();
    let current = epoch(10, 20, 30);

    let first = message("A");

    assert_eq!(
        handle.process(input(current, &first)).decision(),
        IntegrityDecision::Accepted,
    );

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );

    let second = message("B");

    let outcome = handle.process(input(current, &second));

    assert!(matches!(
        outcome,
        IntegrityOutcome::AdmissionBlocked(IntegrityAdmissionError::Interrupted),
    ));

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );
}

#[test]
fn new_generation_reopens_trade_admission() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let next = epoch(11, 21, 31);

    handle
        .on_lifecycle(IntegrityLifecycleEvent::TradeGeneration(binding(next)))
        .unwrap();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Active,);

    let current_message = message("A");

    let outcome = handle.process(input(next, &current_message));

    assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

    // The old acquisition generation must remain fenced.
    let obsolete_message = message("B");

    let obsolete = handle.process(input(epoch(10, 20, 30), &obsolete_message));

    assert_eq!(obsolete.decision(), IntegrityDecision::Ignored,);
}

#[test]
fn stopped_stream_rejects_all_subsequent_messages() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
        .unwrap();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);

    let current = epoch(10, 20, 30);
    let trade = message("A");

    assert!(matches!(
        handle.process(input(current, &trade)),
        IntegrityOutcome::AdmissionBlocked(IntegrityAdmissionError::Stopped),
    ));

    // An interruption notification must not reopen
    // a previously stopped stream.
    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);
}

#[test]
fn batch_processing_respects_interruption_gate() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let current = epoch(10, 20, 30);

    let messages = [message("A"), message("B"), message("C")];

    let results = handle.evaluate_batch(messages.iter().map(|message| input(current, message)));

    assert_eq!(results.len(), 3);

    assert!(results.outcomes.iter().all(|outcome| {
        matches!(
            outcome,
            IntegrityOutcome::AdmissionBlocked(IntegrityAdmissionError::Interrupted)
        )
    }));
}

#[test]
fn rejected_generation_transition_keeps_gate_closed() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    // Reusing the current generation is not a valid
    // successor transition.
    let invalid_next = binding(epoch(10, 20, 30));

    let result = handle.on_lifecycle(IntegrityLifecycleEvent::TradeGeneration(invalid_next));

    assert!(result.is_err());

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );
}

#[test]
fn stopped_trade_handle_cannot_be_restarted() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
        .unwrap();

    let next = epoch(11, 21, 31);

    // The high-level lifecycle API cannot reopen it.
    let lifecycle_result =
        handle.on_lifecycle(IntegrityLifecycleEvent::TradeGeneration(binding(next)));

    assert!(lifecycle_result.is_err());

    // The direct generation API cannot bypass the guard.
    let direct_result = handle.begin_trade_generation(binding(next));

    assert!(direct_result.is_err());

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);

    assert_eq!(handle.stream_generation(), 10);

    let trade = message("A");

    assert!(matches!(
        handle.process(input(next, &trade)),
        IntegrityOutcome::AdmissionBlocked(IntegrityAdmissionError::Stopped),
    ));
}

#[test]
fn evaluate_batch_preserves_single_message_admission_semantics() {
    let mut sequential = integrity();
    let mut evaluated = integrity();

    let current = epoch(10, 20, 30);

    let messages = [message("A"), message("B"), message("B"), message("C")];

    let sequential_results: Vec<_> = messages
        .iter()
        .map(|message| sequential.process(input(current, message)).decision())
        .collect();

    let evaluation =
        evaluated.evaluate_batch(messages.iter().map(|message| input(current, message)));

    let evaluated_results: Vec<_> = evaluation
        .outcomes
        .iter()
        .map(IntegrityOutcome::decision)
        .collect();

    assert_eq!(sequential_results, evaluated_results);

    assert_eq!(
        sequential.status().trades_admissible,
        evaluated.status().trades_admissible,
    );
}
