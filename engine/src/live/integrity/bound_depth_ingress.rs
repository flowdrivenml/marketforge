use super::CompiledRestDepthEndpoint;
use super::{
    CompiledDepthContinuityPlan, DepthBookLimits, DepthBufferLimits, DepthControllerIgnoreReason,
    DepthControllerOutcome, DepthControllerRecoveryReason, DepthResourceLimits,
    DepthSnapshotSource, DepthSourceBinding, DepthSourceBindingError, DepthSourceEpoch,
    DepthSyncToken, LiveDepthController, RestDepthRequestContext, RestDepthSyncCoordinator,
    SourcePlanCompatibility,
};
use crate::live::config::{exchange::ExchangeConfig, format::DepthFormatConfig};
use crate::live::infrastructure::normalization::{NormalizationSource, NormalizedDepth};

// ============================================================
// INTERNAL DEPTH MODE
// ============================================================

#[derive(Debug)]
enum BoundDepthMode {
    WebSocket(LiveDepthController),
    Rest(RestDepthSyncCoordinator),
}

// ============================================================
// SOURCE-BOUND INGRESS
// ============================================================

/// Single-owner depth integrity entrypoint.
///
/// Every accepted message must pass:
/// 1. Runtime epoch validation.
/// 2. Canonical source identity validation.
/// 3. Exchange-specific depth integrity processing.
///
/// The underlying controller remains inaccessible for
/// mutable operations.
///
/// Connection/subscription generations are trusted
/// runtime assertions, not claims made by the exchange.
#[derive(Debug)]
pub struct BoundDepthIngress {
    binding: DepthSourceBinding,
    mode: BoundDepthMode,
    active_rest_request: Option<RestDepthRequestContext>,
    rest_endpoint_policy: Option<CompiledRestDepthEndpoint>,
    /// Requested depth of the REST snapshot that established
    /// the current synchronized book.
    accepted_rest_requested_limit: Option<u64>,
}

impl BoundDepthIngress {
    pub fn new(
        binding: DepthSourceBinding,
        plan: CompiledDepthContinuityPlan,
        buffer_limits: DepthBufferLimits,
        resources: DepthResourceLimits,
    ) -> Result<Self, String> {
        let generation = binding.epoch().stream_generation;
        SourcePlanCompatibility::validate(binding.exchange(), binding.market(), &plan.format_id)
            .map_err(|error| {
                format!(
                    "depth source and compiled plan are incompatible: \
         {error:?}"
                )
            })?;
        let mode = match plan.snapshot_source {
            DepthSnapshotSource::WebSocket => {
                let mut controller = match binding.subscribed_depth() {
                    Some(depth) => LiveDepthController::new_with_subscription_depth(
                        binding.stream_id(),
                        plan,
                        generation,
                        depth,
                    )?,

                    None => LiveDepthController::new(binding.stream_id(), plan, generation)?,
                };

                controller.set_resource_limits(resources)?;

                BoundDepthMode::WebSocket(controller)
            }

            DepthSnapshotSource::Rest => {
                let coordinator = RestDepthSyncCoordinator::new_with_resources(
                    binding.stream_id(),
                    plan,
                    generation,
                    buffer_limits,
                    resources,
                )?;

                BoundDepthMode::Rest(coordinator)
            }
        };
        Ok(Self {
            binding,
            mode,
            active_rest_request: None,
            rest_endpoint_policy: None,
            accepted_rest_requested_limit: None,
        })
    }
    /// Bind the configured REST snapshot endpoint.
    ///
    /// Must be called before beginning the first REST
    /// synchronization attempt.
    ///
    /// Configuration is immutable for this ingress instance.
    /// Reconfiguration requires constructing a new ingress.
    pub fn configure_rest_endpoint(
        &mut self,
        exchange: &ExchangeConfig,
        format: &DepthFormatConfig,
    ) -> Result<(), String> {
        if !matches!(&self.mode, BoundDepthMode::Rest(_)) {
            return Err("this source does not use REST synchronization".into());
        }

        if self.rest_endpoint_policy.is_some() {
            return Err("REST endpoint policy is already configured".into());
        }

        if self.active_rest_request.is_some() {
            return Err("cannot configure REST endpoint during an attempt".into());
        }

        let policy = CompiledRestDepthEndpoint::compile(&self.binding, exchange, format)
            .map_err(|error| format!("invalid REST endpoint policy: {error:?}"))?;

        self.rest_endpoint_policy = Some(policy);

        Ok(())
    }
    // ========================================================
    // READ-ONLY INSPECTION
    // ========================================================

    pub fn binding(&self) -> &DepthSourceBinding {
        &self.binding
    }

    pub fn controller(&self) -> &LiveDepthController {
        match &self.mode {
            BoundDepthMode::WebSocket(controller) => controller,
            BoundDepthMode::Rest(coordinator) => coordinator.controller(),
        }
    }

    pub fn is_synchronized(&self) -> bool {
        match &self.mode {
            BoundDepthMode::WebSocket(controller) => controller.is_synchronized(),

            BoundDepthMode::Rest(coordinator) => coordinator.is_synchronized(),
        }
    }
    /// REST request limit associated with the current synchronized book.
    ///
    /// This is trusted request metadata, not a guarantee that
    /// the exchange returned this many levels.
    pub fn accepted_rest_requested_limit(&self) -> Option<u64> {
        if self.is_synchronized() {
            self.accepted_rest_requested_limit
        } else {
            None
        }
    }
    // ========================================================
    // RETAINED BOOK CONFIGURATION
    // ========================================================

    /// Configure the authoritative order book's retained-state
    /// budget.
    ///
    /// The limits apply to both bid and ask levels combined.
    ///
    /// Configuration is rejected while the underlying controller
    /// is synchronized, holds a staged snapshot, or has an active
    /// REST synchronization attempt.
    ///
    /// After explicit invalidation, the policy may be changed
    /// before synchronization restarts.
    pub fn set_book_limits(&mut self, limits: DepthBookLimits) -> Result<(), String> {
        match &mut self.mode {
            BoundDepthMode::WebSocket(controller) => controller.set_book_limits(limits),

            BoundDepthMode::Rest(coordinator) => coordinator.set_book_limits(limits),
        }
    }

    /// Inspect the effective retained-book limits.
    pub fn book_limits(&self) -> DepthBookLimits {
        self.controller().book_limits()
    }
    // ========================================================
    // WEBSOCKET MESSAGE ADMISSION
    // ========================================================

    pub fn on_websocket(
        &mut self,
        epoch: DepthSourceEpoch,
        message: NormalizedDepth,
        accounted_bytes: usize,
    ) -> DepthControllerOutcome {
        let validation =
            self.binding
                .validate_message(epoch, &message, NormalizationSource::WebSocket);

        if let Err(error) = validation {
            return self.reject_invalid_source(error);
        }

        let outcome = match &mut self.mode {
            BoundDepthMode::WebSocket(controller) => {
                controller.process(epoch.stream_generation, &message)
            }

            BoundDepthMode::Rest(coordinator) => {
                coordinator.on_websocket(epoch.stream_generation, message, accounted_bytes)
            }
        };

        if matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
                | DepthControllerOutcome::RecoveryRequired { .. }
        ) {
            self.active_rest_request = None;
        }

        outcome
    }

    // ========================================================
    // REST ATTEMPT OWNERSHIP
    // ========================================================

    pub fn begin_rest_attempt(&mut self, request: RestDepthRequestContext) -> Result<(), String> {
        request
            .validate(self.binding.epoch())
            .map_err(|error| format!("invalid REST request context: {error:?}"))?;
        let policy = self
            .rest_endpoint_policy
            .as_ref()
            .ok_or_else(|| "REST snapshot endpoint has not been configured".to_string())?;

        policy.validate_request(&request).map_err(|error| {
            format!(
                "REST snapshot request violates endpoint policy: \
         {error:?}"
            )
        })?;
        if request.source_epoch != self.binding.epoch() {
            return Err("REST request belongs to another source epoch".into());
        }

        let token = request.sync_token;

        match &mut self.mode {
            BoundDepthMode::Rest(coordinator) => {
                // The coordinator first validates monotonic
                // attempt identity and establishes the new
                // synchronization buffer.
                //
                // If it fails, retain the previous request
                // context unchanged.
                coordinator.begin_attempt(token)?;

                // A new synchronization attempt supersedes previous
                // REST coverage metadata.
                self.accepted_rest_requested_limit = None;

                self.active_rest_request = Some(request);

                Ok(())
            }

            BoundDepthMode::WebSocket(_) => Err("WebSocket snapshot profile does not use \
             REST synchronization"
                .into()),
        }
    }

    pub fn on_rest_snapshot(
        &mut self,
        epoch: DepthSourceEpoch,
        token: DepthSyncToken,
        snapshot: &NormalizedDepth,
    ) -> DepthControllerOutcome {
        // ----------------------------------------------------
        // AUTHORITATIVE SOURCE EPOCH
        // ----------------------------------------------------

        if self.binding.validate_epoch(epoch).is_err() {
            return Self::obsolete();
        }

        if token.stream_generation != epoch.stream_generation {
            return Self::obsolete();
        }

        // ----------------------------------------------------
        // AUTHORITATIVE REST REQUEST
        // ----------------------------------------------------

        let Some(request) = self.active_rest_request.as_ref() else {
            return Self::obsolete();
        };

        if request.source_epoch != epoch || request.sync_token != token {
            return Self::obsolete();
        }

        // The underlying coordinator must still recognize
        // this as its current attempt.
        match &self.mode {
            BoundDepthMode::Rest(coordinator) => {
                if coordinator.active_attempt() != Some(token) {
                    return Self::obsolete();
                }
            }

            BoundDepthMode::WebSocket(_) => {
                return DepthControllerOutcome::RecoveryRequired {
                    reason: DepthControllerRecoveryReason::InvalidSource,
                };
            }
        }

        // ----------------------------------------------------
        // CANONICAL SNAPSHOT IDENTITY
        // ----------------------------------------------------

        if let Err(error) =
            self.binding
                .validate_message(epoch, snapshot, NormalizationSource::Rest)
        {
            return self.reject_invalid_source(error);
        }

        // REST coverage is tied to the validated request,
        // not compared against subscribed WebSocket depth.
        //
        // Endpoint-specific coverage rules remain the
        // responsibility of the request/normalization
        // contract and will be compiled separately.
        let policy = match self.rest_endpoint_policy.as_ref() {
            Some(policy) => policy,

            None => {
                self.invalidate();

                return DepthControllerOutcome::RecoveryRequired {
                    reason: DepthControllerRecoveryReason::InvalidSnapshot(
                        "REST endpoint policy is missing".into(),
                    ),
                };
            }
        };

        let request = self
            .active_rest_request
            .as_ref()
            .expect("active request was verified above");

        if let Err(error) = policy.validate_response(request, snapshot) {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "REST response violates endpoint policy: \
                     {error:?}"
                )),
            };
        }
        // ----------------------------------------------------
        // COMPLETE AUTHORIZED ATTEMPT
        // ----------------------------------------------------

        let outcome = match &mut self.mode {
            BoundDepthMode::Rest(coordinator) => coordinator.on_rest_snapshot(token, snapshot),

            BoundDepthMode::WebSocket(_) => unreachable!(),
        };
        // Preserve the requested depth only when this REST
        // attempt successfully establishes an authoritative book.
        if matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ) {
            self.accepted_rest_requested_limit = Some(request.requested_limit);
        } else if matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }) {
            self.accepted_rest_requested_limit = None;
        }
        // The REST snapshot callback has completed.
        //
        // Both successful and failed attempts retire their
        // bound request. A retry requires a newer token.
        //
        // Obsolete callbacks were returned above without
        // affecting the current attempt.
        if matches!(
            outcome,
            DepthControllerOutcome::Accepted { .. }
                | DepthControllerOutcome::RecoveryRequired { .. }
        ) {
            self.active_rest_request = None;
        }

        outcome
    }
    // ========================================================
    // GENERATION TRANSITION
    // ========================================================

    /// Establish an explicitly newer stream generation.
    ///
    /// The new binding must be supplied by the trusted
    /// source registry after connection/subscription
    /// reconciliation.
    ///
    /// Rebinding retains the same logical stream,
    /// exchange, market and instrument.
    ///
    /// Changes to those identifiers require constructing
    /// a new source stream, not silently rebinding one.
    pub fn begin_generation(&mut self, next: DepthSourceBinding) -> Result<(), String> {
        if next.stream_id() != self.binding.stream_id()
            || next.exchange() != self.binding.exchange()
            || next.market() != self.binding.market()
            || next.instrument_id() != self.binding.instrument_id()
            || next.symbol() != self.binding.symbol()
            || next.subscribed_depth() != self.binding.subscribed_depth()
        {
            return Err("source identity cannot change during \
                 stream generation transition"
                .into());
        }

        if next.epoch().stream_generation <= self.binding.epoch().stream_generation {
            return Err("source stream generation must increase".into());
        }

        match &mut self.mode {
            BoundDepthMode::WebSocket(controller) => {
                controller.begin_generation(next.epoch().stream_generation)?;
            }

            BoundDepthMode::Rest(coordinator) => {
                coordinator.begin_generation(next.epoch().stream_generation)?;
            }
        }
        self.active_rest_request = None;
        self.binding = next;
        self.accepted_rest_requested_limit = None;

        Ok(())
    }

    // ========================================================
    // EXPLICIT LIFECYCLE
    // ========================================================

    pub fn invalidate(&mut self) {
        match &mut self.mode {
            BoundDepthMode::WebSocket(controller) => {
                controller.invalidate();
            }

            BoundDepthMode::Rest(coordinator) => {
                coordinator.invalidate();
            }
        }
        self.active_rest_request = None;
        self.accepted_rest_requested_limit = None;
    }

    pub fn expire_pending_snapshot(&mut self) -> Option<DepthControllerOutcome> {
        match &mut self.mode {
            BoundDepthMode::WebSocket(controller) => controller.expire_pending_snapshot(),

            BoundDepthMode::Rest(_) => None,
        }
    }

    // ========================================================
    // REJECTION
    // ========================================================

    fn reject_invalid_source(&mut self, error: DepthSourceBindingError) -> DepthControllerOutcome {
        // Delayed messages from an obsolete epoch must
        // not damage the current synchronized book.
        if error == DepthSourceBindingError::ObsoleteEpoch {
            return Self::obsolete();
        }

        // A misrouted message associated with the CURRENT
        // authoritative epoch means that source routing
        // cannot be trusted.
        //
        // Fail closed rather than skipping it and
        // silently claiming uninterrupted L2 continuity.
        self.invalidate();

        let reason = match error {
            DepthSourceBindingError::WrongStream => DepthControllerRecoveryReason::WrongStream,

            DepthSourceBindingError::ObsoleteEpoch => {
                unreachable!()
            }

            _ => DepthControllerRecoveryReason::InvalidSource,
        };

        DepthControllerOutcome::RecoveryRequired { reason }
    }

    fn obsolete() -> DepthControllerOutcome {
        DepthControllerOutcome::Ignored {
            reason: DepthControllerIgnoreReason::ObsoleteGeneration,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use rust_decimal::Decimal;

    use crate::{
        canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
        live::{
            config::format::DepthFormatConfig, infrastructure::normalization::DepthMessageKind,
            integrity::DepthContinuityCompiler,
        },
    };

    const STREAM: &str = "stream-42";

    fn epoch(stream: u64, connection: u64, subscription: u64) -> DepthSourceEpoch {
        DepthSourceEpoch {
            stream_generation: stream,
            connection_generation: connection,
            subscription_generation: subscription,
        }
    }

    fn binding(current: DepthSourceEpoch) -> DepthSourceBinding {
        DepthSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            Some(200),
            current,
        )
        .unwrap()
    }

    fn plan(name: &str) -> CompiledDepthContinuityPlan {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth")
            .join(format!("{name}.toml"));

        let config = DepthFormatConfig::load(path).unwrap();

        DepthContinuityCompiler::compile(&config).unwrap()
    }

    fn ingress() -> BoundDepthIngress {
        BoundDepthIngress::new(
            binding(epoch(10, 20, 30)),
            plan("bybit_spot"),
            DepthBufferLimits {
                max_messages: 10,
                max_bytes: 1_000_000,
            },
            DepthResourceLimits::default(),
        )
        .unwrap()
    }

    fn snapshot(exchange: Exchange, instrument_id: i64, symbol: &str) -> NormalizedDepth {
        let level = L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange,
                instrument_id,
                symbol: symbol.into(),
                stream_id: STREAM.into(),
            },

            side: BookSide::Bid,
            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::from(3)),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        };

        NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            DepthMessageKind::Snapshot,
            SequenceMetadata {
                sequence_first: None,
                sequence_last: Some(100),
                sequence_previous: None,
                cross_sequence: Some(200),
            },
            vec![level],
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap()
    }

    #[test]
    fn correct_binding_accepts_snapshot() {
        let mut ingress = ingress();

        let result = ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "BTCUSDT"),
            1024,
        );

        assert!(matches!(
            result,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert!(ingress.is_synchronized());
    }

    #[test]
    fn matching_stream_id_does_not_hide_wrong_exchange() {
        let mut ingress = ingress();

        let result = ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Binance, 42, "BTCUSDT"),
            1024,
        );

        assert!(matches!(
            result,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource
            }
        ));

        assert!(!ingress.is_synchronized());
    }

    #[test]
    fn matching_stream_id_does_not_hide_wrong_instrument() {
        let mut ingress = ingress();

        let result = ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 999, "BTCUSDT"),
            1024,
        );

        assert!(matches!(
            result,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource
            }
        ));
    }

    #[test]
    fn matching_instrument_id_does_not_hide_wrong_symbol() {
        let mut ingress = ingress();

        let result = ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "ETHUSDT"),
            1024,
        );

        assert!(matches!(
            result,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource
            }
        ));
    }

    #[test]
    fn old_connection_cannot_invalidate_current_book() {
        let mut ingress = ingress();

        ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "BTCUSDT"),
            1024,
        );

        assert!(ingress.is_synchronized());

        let result = ingress.on_websocket(
            epoch(10, 19, 30),
            snapshot(Exchange::Binance, 999, "ETHUSDT"),
            1024,
        );

        assert!(matches!(
            result,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        // The obsolete message cannot damage the
        // already synchronized authoritative book.
        assert!(ingress.is_synchronized());
    }

    #[test]
    fn old_subscription_cannot_invalidate_current_book() {
        let mut ingress = ingress();

        ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "BTCUSDT"),
            1024,
        );

        let result = ingress.on_websocket(
            epoch(10, 20, 29),
            snapshot(Exchange::Binance, 999, "ETHUSDT"),
            1024,
        );

        assert!(matches!(
            result,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert!(ingress.is_synchronized());
    }

    #[test]
    fn new_generation_rejects_old_connection_epoch() {
        let mut ingress = ingress();

        ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "BTCUSDT"),
            1024,
        );

        ingress
            .begin_generation(binding(epoch(11, 21, 31)))
            .unwrap();

        assert!(!ingress.is_synchronized());

        let result = ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "BTCUSDT"),
            1024,
        );

        assert!(matches!(result, DepthControllerOutcome::Ignored { .. }));

        assert_eq!(ingress.binding().epoch(), epoch(11, 21, 31),);
    }

    #[test]
    fn generation_transition_cannot_change_instrument() {
        let mut ingress = ingress();

        let invalid = DepthSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            999,
            "BTCUSDT",
            Some(200),
            epoch(11, 21, 31),
        )
        .unwrap();

        assert!(ingress.begin_generation(invalid).is_err());

        assert_eq!(ingress.binding().epoch(), epoch(10, 20, 30),);
    }

    #[test]
    fn empty_message_requires_trusted_epoch() {
        let source = binding(epoch(10, 20, 30));

        let empty = NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            SequenceMetadata::default(),
            vec![],
            None,
            None,
        )
        .unwrap();

        assert!(
            source
                .validate_message(epoch(10, 20, 30), &empty, NormalizationSource::WebSocket,)
                .is_ok()
        );

        assert_eq!(
            source.validate_message(epoch(10, 19, 30), &empty, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::ObsoleteEpoch),
        );

        // This validates provenance only.
        // Exchange-specific sequence validation still
        // determines whether the empty delta is valid.
    }
    #[test]
    fn rejects_depth_plan_for_another_exchange() {
        let result = BoundDepthIngress::new(
            binding(epoch(10, 20, 30)),
            plan("binance_spot"),
            DepthBufferLimits {
                max_messages: 10,
                max_bytes: 1_000_000,
            },
            DepthResourceLimits::default(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_depth_plan_for_another_market() {
        let result = BoundDepthIngress::new(
            binding(epoch(10, 20, 30)),
            plan("bybit_linear"),
            DepthBufferLimits {
                max_messages: 10,
                max_bytes: 1_000_000,
            },
            DepthResourceLimits::default(),
        );

        assert!(result.is_err());
    }
    #[test]
    fn ingress_exposes_authoritative_book_read_only() {
        let mut ingress = ingress();

        let outcome = ingress.on_websocket(
            epoch(10, 20, 30),
            snapshot(Exchange::Bybit, 42, "BTCUSDT"),
            1024,
        );

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        let controller = ingress.controller();

        assert!(controller.is_synchronized());
        assert!(controller.book().is_initialized());

        assert_eq!(controller.stream_generation(), 10);
        assert_eq!(controller.book_generation(), 1);
    }
}
