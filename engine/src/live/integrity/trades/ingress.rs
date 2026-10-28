use crate::live::infrastructure::normalization::{NormalizationSource, NormalizedTrades};

use crate::live::integrity::source_plan_compatibility::SourcePlanCompatibility;
use crate::live::integrity::trades::binding::{
    TradeSourceBinding, TradeSourceBindingError, TradeSourceEpoch,
};
use crate::live::integrity::trades::compiler::CompiledTradeIntegrityPlan;
use crate::live::integrity::trades::controller::{LiveTradeController, TradeControllerOutcome};
use crate::live::integrity::trades::identity::TradeIdentityLimits;

// ============================================================
// INGRESS OUTCOMES
// ============================================================

/// Trade ingress result.
///
/// The wrapper distinguishes:
/// - Controller integrity processing.
/// - Obsolete acquisition work.
/// - Source-provenance quarantine.
/// - Subsequent admission denied by quarantine.
///
/// A processed controller result may independently contain
/// CoverageReviewRequired or CoverageReviewPending.
#[derive(Debug)]
pub enum BoundTradeIngressOutcome {
    Processed(TradeControllerOutcome),

    ObsoleteEpoch,

    /// This message first established that source routing
    /// could not be trusted.
    ///
    /// The ingress has entered persistent quarantine.
    SourceQuarantined {
        reason: TradeSourceBindingError,
    },

    /// Source routing previously failed.
    ///
    /// No message processing occurred.
    AdmissionSuspended {
        generation: u64,
    },
}

// ============================================================
// SOURCE-BOUND TRADE INGRESS
// ============================================================

/// Single-owner source-bound trade integrity entrypoint.
///
/// Owns:
/// - An immutable source binding for the current epoch.
/// - The per-stream LiveTradeController.
/// - Persistent source-provenance quarantine state.
///
/// Does not own:
/// - Connections.
/// - Subscription registration.
/// - Normalization.
/// - REST reconciliation.
/// - Recovery scheduling.
/// - Publication.
/// - Persistence.
/// - GapTracker.
///
/// The source runtime must serialize all calls.
///
/// A quarantine does not prove executions were lost.
/// It means the current acquisition routing cannot safely
/// authorize further trade acceptance.
#[derive(Debug)]
pub struct BoundTradeIngress {
    binding: TradeSourceBinding,

    controller: LiveTradeController,

    quarantined: bool,
}

impl BoundTradeIngress {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        binding: TradeSourceBinding,
        limits: TradeIdentityLimits,
        plan: CompiledTradeIntegrityPlan,
    ) -> Result<Self, String> {
        SourcePlanCompatibility::validate(binding.exchange(), binding.market(), &plan.format_id)
            .map_err(|error| {
                format!(
                    "trade source and compiled plan are incompatible: \
         {error:?}"
                )
            })?;
        let controller = LiveTradeController::new(
            binding.stream_id(),
            binding.epoch().stream_generation,
            limits,
            plan,
        )?;

        Ok(Self {
            binding,
            controller,
            quarantined: false,
        })
    }

    // ========================================================
    // READ-ONLY INSPECTION
    // ========================================================

    pub fn binding(&self) -> &TradeSourceBinding {
        &self.binding
    }

    pub fn controller(&self) -> &LiveTradeController {
        &self.controller
    }

    pub fn is_quarantined(&self) -> bool {
        self.quarantined
    }

    /// Whether provenance quarantine and controller-level
    /// coverage review both permit local admission.
    ///
    /// True is not a historical-completeness guarantee.
    pub fn can_accept_trades(&self) -> bool {
        !self.quarantined && self.controller.can_accept_trades()
    }

    // ========================================================
    // WEBSOCKET TRADE ADMISSION
    // ========================================================

    /// Process a normalized WebSocket trade message.
    ///
    /// The epoch must come from the trusted connection
    /// and subscription owner, not the native payload.
    ///
    /// Obsolete work is ignored before quarantine checks,
    /// preventing old messages from damaging current state.
    ///
    /// A current-epoch identity failure quarantines
    /// this ingress without advancing trade identities
    /// or accepted sequence baselines.
    pub fn on_websocket(
        &mut self,
        epoch: TradeSourceEpoch,
        message: &NormalizedTrades,
    ) -> BoundTradeIngressOutcome {
        // ----------------------------------------------------
        // EPOCH FENCING
        // ----------------------------------------------------

        if self.binding.validate_epoch(epoch).is_err() {
            return BoundTradeIngressOutcome::ObsoleteEpoch;
        }

        // ----------------------------------------------------
        // PERSISTENT QUARANTINE
        // ----------------------------------------------------

        if self.quarantined {
            return BoundTradeIngressOutcome::AdmissionSuspended {
                generation: self.binding.epoch().stream_generation,
            };
        }

        // ----------------------------------------------------
        // SOURCE IDENTITY VALIDATION
        // ----------------------------------------------------

        if let Err(reason) =
            self.binding
                .validate_message(epoch, message, NormalizationSource::WebSocket)
        {
            // An obsolete epoch was checked above.
            //
            // This branch therefore indicates invalid
            // current-epoch source routing or provenance.
            self.quarantined = true;

            return BoundTradeIngressOutcome::SourceQuarantined { reason };
        }

        // ----------------------------------------------------
        // CANONICAL TRADE INTEGRITY
        // ----------------------------------------------------

        let outcome = self.controller.process(epoch.stream_generation, message);

        BoundTradeIngressOutcome::Processed(outcome)
    }

    // ========================================================
    // AUTHORITATIVE GENERATION TRANSITION
    // ========================================================

    /// Establish a newer acquisition epoch for the same
    /// logical trade stream.
    ///
    /// The source registry must supply the new binding.
    ///
    /// The controller resets generation-scoped identity
    /// and ordering state and retires any old coverage hold.
    ///
    /// Local acceptance may resume, but this operation
    /// does not establish that historical coverage has
    /// been reconciled.
    ///
    /// The runtime must preserve unresolved incidents
    /// before retiring the previous generation.
    pub fn begin_generation(&mut self, next: TradeSourceBinding) -> Result<(), String> {
        self.binding.validate_successor(&next).map_err(|error| {
            format!(
                "invalid trade source generation: \
                     {error:?}"
            )
        })?;

        // Advance the controller first.
        //
        // If transition validation fails, the binding
        // and quarantine state remain unchanged.
        self.controller
            .begin_generation(next.epoch().stream_generation)?;

        self.binding = next;
        self.quarantined = false;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::{
        live::{
            infrastructure::normalization::TradeRecordSemantics,
            integrity::{
                TradeControllerOutcome, TradeCoverageAction, TradeIdentityKind, TradeSequenceKind,
            },
        },
        shared::domain::canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    };

    const STREAM: &str = "bybit:spot:BTCUSDT:trades";

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

    fn limits() -> TradeIdentityLimits {
        TradeIdentityLimits {
            max_ids: 100,
            max_id_bytes: 128,
            max_batch_records: 100,
        }
    }

    fn ingress() -> BoundTradeIngress {
        BoundTradeIngress::new(binding(epoch(10, 20, 30)), limits(), plan()).unwrap()
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

    fn valid_message(id: &str) -> NormalizedTrades {
        message(vec![trade(id, Exchange::Bybit, 42)])
    }

    #[test]
    fn correct_source_reaches_trade_controller() {
        let mut ingress = ingress();

        let outcome = ingress.on_websocket(epoch(10, 20, 30), &valid_message("A"));

        assert!(matches!(
            outcome,
            BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(_))
        ));

        assert!(ingress.controller().identities().contains("A"));
        assert!(ingress.can_accept_trades());
    }

    #[test]
    fn bad_instrument_quarantines_without_partial_commit() {
        let mut ingress = ingress();

        let input = message(vec![
            trade("A", Exchange::Bybit, 42),
            trade("B", Exchange::Bybit, 999),
        ]);

        let outcome = ingress.on_websocket(epoch(10, 20, 30), &input);

        assert!(matches!(
            outcome,
            BoundTradeIngressOutcome::SourceQuarantined {
                reason: TradeSourceBindingError::WrongInstrument
            }
        ));

        assert!(ingress.is_quarantined());
        assert!(!ingress.can_accept_trades());

        // No trade from this batch was committed.
        assert_eq!(ingress.controller().identities().retained_count(), 0,);
    }

    #[test]
    fn quarantine_blocks_later_valid_batches() {
        let mut ingress = ingress();

        ingress.on_websocket(
            epoch(10, 20, 30),
            &message(vec![trade("A", Exchange::Binance, 42)]),
        );

        let outcome = ingress.on_websocket(epoch(10, 20, 30), &valid_message("B"));

        assert!(matches!(
            outcome,
            BoundTradeIngressOutcome::AdmissionSuspended { generation: 10 }
        ));

        assert!(!ingress.controller().identities().contains("B"));
    }

    #[test]
    fn obsolete_epoch_does_not_quarantine() {
        let mut ingress = ingress();

        let outcome = ingress.on_websocket(
            epoch(10, 19, 30),
            &message(vec![trade("A", Exchange::Binance, 999)]),
        );

        assert!(matches!(outcome, BoundTradeIngressOutcome::ObsoleteEpoch));

        assert!(!ingress.is_quarantined());
        assert_eq!(ingress.controller().identities().retained_count(), 0,);
    }

    #[test]
    fn obsolete_subscription_does_not_change_state() {
        let mut ingress = ingress();

        ingress.on_websocket(epoch(10, 20, 30), &valid_message("A"));

        let outcome = ingress.on_websocket(epoch(10, 20, 29), &valid_message("B"));

        assert!(matches!(outcome, BoundTradeIngressOutcome::ObsoleteEpoch));

        assert!(ingress.controller().identities().contains("A"));
        assert!(!ingress.controller().identities().contains("B"));
        assert!(!ingress.is_quarantined());
    }

    #[test]
    fn new_generation_clears_source_quarantine() {
        let mut ingress = ingress();

        ingress.on_websocket(
            epoch(10, 20, 30),
            &message(vec![trade("A", Exchange::Binance, 42)]),
        );

        assert!(ingress.is_quarantined());

        ingress
            .begin_generation(binding(epoch(11, 21, 31)))
            .unwrap();

        assert!(!ingress.is_quarantined());

        let outcome = ingress.on_websocket(epoch(11, 21, 31), &valid_message("B"));

        let BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)) = outcome
        else {
            panic!("expected accepted new-generation trade");
        };

        assert_eq!(batch.trades.len(), 1);

        // Local trade identity history has restarted.
        assert!(batch.coverage_history_unverified);
    }

    #[test]
    fn invalid_rebinding_preserves_current_epoch() {
        let mut ingress = ingress();

        let invalid = TradeSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            999,
            "BTCUSDT",
            epoch(11, 21, 31),
        )
        .unwrap();

        assert!(ingress.begin_generation(invalid).is_err());

        assert_eq!(ingress.binding().epoch(), epoch(10, 20, 30),);

        assert_eq!(ingress.controller().generation(), 10);
    }

    #[test]
    fn obsolete_generation_remains_ignored_after_rebinding() {
        let mut ingress = ingress();

        ingress
            .begin_generation(binding(epoch(11, 21, 31)))
            .unwrap();

        let outcome = ingress.on_websocket(epoch(10, 20, 30), &valid_message("OLD"));

        assert!(matches!(outcome, BoundTradeIngressOutcome::ObsoleteEpoch));

        assert!(!ingress.controller().identities().contains("OLD"));
    }
    #[test]
    fn rejects_trade_plan_for_another_exchange() {
        let mut incompatible = plan();
        incompatible.format_id = "binance_spot".into();

        let result = BoundTradeIngress::new(binding(epoch(10, 20, 30)), limits(), incompatible);

        assert!(result.is_err());
    }

    #[test]
    fn rejects_trade_plan_for_another_market() {
        let mut incompatible = plan();
        incompatible.format_id = "bybit_inverse".into();

        let result = BoundTradeIngress::new(binding(epoch(10, 20, 30)), limits(), incompatible);

        assert!(result.is_err());
    }
    #[test]
    fn ingress_exposes_controller_state_without_mutable_access() {
        let mut ingress = ingress();

        let outcome = ingress.on_websocket(epoch(10, 20, 30), &valid_message("A"));

        assert!(matches!(
            outcome,
            BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(_))
        ));

        let controller = ingress.controller();

        assert_eq!(controller.generation(), 10);
        assert_eq!(controller.stream_id(), STREAM);

        assert!(controller.identities().contains("A"));

        assert!(controller.can_accept_trades());
        assert!(!controller.is_coverage_review_pending());
    }
}
