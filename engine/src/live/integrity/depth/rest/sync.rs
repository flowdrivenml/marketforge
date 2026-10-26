use crate::live::infrastructure::normalization::{
    DepthMessageKind, NormalizationSource, NormalizedDepth,
};

use crate::live::integrity::depth::book_budget::DepthBookLimits;
use crate::live::integrity::depth::buffer::{
    DepthBufferLimits, DepthSyncToken, PreSnapshotDepthBuffer,
};
use crate::live::integrity::depth::continuity_compiler::{
    CompiledDepthContinuityPlan, DepthSnapshotSource,
};
use crate::live::integrity::depth::controller::{
    DepthControllerIgnoreReason, DepthControllerOutcome, DepthControllerRecoveryReason,
    LiveDepthController,
};
use crate::live::integrity::depth::resources::{DepthResourceAccounting, DepthResourceLimits};

// ============================================================
// SYNCHRONIZATION PHASE
// ============================================================

/// Current REST synchronization state for one L2 stream.
///
/// ApplyingSnapshot is intentionally not exposed because
/// snapshot reconciliation is synchronous and executed
/// under exclusive mutable access to the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestDepthSyncPhase {
    AwaitingAttempt,
    Buffering,
    Synchronized,
}

// ============================================================
// REST DEPTH SYNCHRONIZATION COORDINATOR
// ============================================================

/// Serializes REST snapshot synchronization and subsequent
/// WebSocket depth processing for one source stream.
///
/// This component owns:
///
/// - The LiveDepthController.
/// - The temporary pre-snapshot buffer.
/// - The current REST synchronization phase.
///
/// It does not own:
///
/// - WebSocket connections or subscriptions.
/// - REST HTTP requests.
/// - Recovery scheduling.
/// - Publication or recording.
/// - Gap incident history.
///
/// All ordered depth messages for a stream must enter through
/// this coordinator. The underlying controller is deliberately
/// exposed only through an immutable reference.
///
/// Concurrent producers must use an external single-owner
/// actor or equivalent ordered serialization mechanism.
#[derive(Debug)]
pub struct RestDepthSyncCoordinator {
    controller: LiveDepthController,

    limits: DepthBufferLimits,

    buffer: Option<PreSnapshotDepthBuffer<NormalizedDepth>>,

    phase: RestDepthSyncPhase,

    resources: DepthResourceLimits,
}

impl RestDepthSyncCoordinator {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub(in crate::live::integrity) fn new(
        stream_id: impl Into<String>,
        plan: CompiledDepthContinuityPlan,
        stream_generation: u64,
        limits: DepthBufferLimits,
    ) -> Result<Self, String> {
        limits
            .validate()
            .map_err(|error| format!("invalid depth buffer limits: {error:?}"))?;

        if plan.snapshot_source != DepthSnapshotSource::Rest || !plan.requires_update_buffer {
            return Err("REST depth coordinator requires a REST snapshot \
                 profile with pre-snapshot buffering"
                .into());
        }

        let controller = LiveDepthController::new(stream_id, plan, stream_generation)?;

        Ok(Self {
            controller,
            limits,
            buffer: None,
            phase: RestDepthSyncPhase::AwaitingAttempt,
            resources: DepthResourceLimits::default(),
        })
    }
    /// Configure retained order-book limits.
    ///
    /// The authoritative controller validates both the limits
    /// and whether its synchronization state permits a change.
    ///
    /// This does not expose mutable controller access.
    pub(in crate::live::integrity) fn set_book_limits(
        &mut self,
        limits: DepthBookLimits,
    ) -> Result<(), String> {
        self.controller.set_book_limits(limits)
    }
    // ========================================================
    // READ-ONLY INSPECTION
    // ========================================================

    pub fn controller(&self) -> &LiveDepthController {
        &self.controller
    }

    pub fn phase(&self) -> RestDepthSyncPhase {
        self.phase
    }

    pub fn is_synchronized(&self) -> bool {
        self.phase == RestDepthSyncPhase::Synchronized && self.controller.is_synchronized()
    }

    pub fn active_attempt(&self) -> Option<DepthSyncToken> {
        self.controller.active_rest_attempt()
    }

    pub fn buffered_messages(&self) -> usize {
        self.buffer.as_ref().map_or(0, PreSnapshotDepthBuffer::len)
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer
            .as_ref()
            .map_or(0, PreSnapshotDepthBuffer::accounted_bytes)
    }

    // ========================================================
    // STREAM GENERATION
    // ========================================================

    /// Begin a newer authoritative source-stream generation.
    ///
    /// Cancels any pending snapshot attempt and discards
    /// buffered updates.
    ///
    /// The surrounding runtime must ensure generation
    /// changes correspond to authoritative lifecycle events.
    pub(in crate::live::integrity) fn begin_generation(
        &mut self,
        generation: u64,
    ) -> Result<(), String> {
        self.controller.begin_generation(generation)?;

        self.buffer = None;
        self.phase = RestDepthSyncPhase::AwaitingAttempt;

        Ok(())
    }

    // ========================================================
    // BEGIN REST SYNCHRONIZATION
    // ========================================================

    /// Begin one explicit REST synchronization attempt.
    ///
    /// Call this before issuing the REST snapshot request.
    ///
    /// WebSocket message routing must already be established
    /// so incoming updates can be admitted to this buffer.
    ///
    /// Starting a newer attempt invalidates old book state.
    ///
    /// The token must be unique and monotonically increasing
    /// within the current stream generation.
    pub(in crate::live::integrity) fn begin_attempt(
        &mut self,
        token: DepthSyncToken,
    ) -> Result<(), String> {
        // Allocate before mutating authoritative state.
        //
        // If buffer construction fails, an existing valid
        // synchronization attempt remains untouched.
        let buffer = PreSnapshotDepthBuffer::new(token, self.limits)
            .map_err(|error| format!("cannot initialize depth buffer: {error:?}"))?;

        // Controller validates the stream generation and
        // monotonic attempt identifier.
        self.controller.begin_rest_attempt(token)?;

        self.buffer = Some(buffer);
        self.phase = RestDepthSyncPhase::Buffering;

        Ok(())
    }

    // ========================================================
    // WEBSOCKET ADMISSION
    // ========================================================

    /// Accept one ordered WebSocket depth message.
    ///
    /// While buffering:
    ///     Retain deltas without modifying the book.
    ///
    /// Once synchronized:
    ///     Forward deltas to the depth controller.
    ///
    /// Gate.io authoritative WebSocket snapshots are
    /// handled directly through the depth controller
    /// and supersede an in-flight REST attempt.
    ///
    /// `accounted_bytes` must conservatively represent
    /// the retained message's memory footprint.
    ///
    /// Returns an explicit outcome for every message.
    pub(in crate::live::integrity) fn on_websocket(
        &mut self,
        generation: u64,
        message: NormalizedDepth,
        accounted_bytes: usize,
    ) -> DepthControllerOutcome {
        // ----------------------------------------------------
        // GENERATION FENCING
        // ----------------------------------------------------

        if generation != self.controller.stream_generation() {
            return Self::obsolete();
        }

        // ----------------------------------------------------
        // MESSAGE IDENTITY
        // ----------------------------------------------------

        if message.stream_id() != self.controller.stream_id() {
            return self.fail(DepthControllerRecoveryReason::WrongStream);
        }

        if message.source() != NormalizationSource::WebSocket {
            return self.fail(DepthControllerRecoveryReason::InvalidSource);
        }

        // ----------------------------------------------------
        // AUTHORITATIVE WEBSOCKET SNAPSHOTS
        // ----------------------------------------------------

        if message.kind() == DepthMessageKind::Snapshot {
            // Only supported profiles, such as Gate.io,
            // may establish state with a WebSocket snapshot.
            //
            // The underlying controller validates this.
            let outcome = self.controller.process(generation, &message);

            match outcome {
                DepthControllerOutcome::Accepted { .. } => {
                    // WebSocket snapshot supersedes the
                    // pending REST attempt.
                    self.buffer = None;
                    self.phase = RestDepthSyncPhase::Synchronized;
                }

                DepthControllerOutcome::RecoveryRequired { .. } => {
                    self.cancel_current_attempt();
                }

                DepthControllerOutcome::Ignored { .. }
                | DepthControllerOutcome::SnapshotStaged { .. } => {
                    // Do not alter existing phase.
                }
            }

            return outcome;
        }

        // ----------------------------------------------------
        // PHASE-SPECIFIC DELTA HANDLING
        // ----------------------------------------------------

        match self.phase {
            RestDepthSyncPhase::AwaitingAttempt => DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotRequired,
            },

            RestDepthSyncPhase::Buffering => self.buffer_delta(message, accounted_bytes),

            RestDepthSyncPhase::Synchronized => {
                if let Err(error) =
                    DepthResourceAccounting::validate_message(&message, self.resources)
                {
                    return self.fail(DepthControllerRecoveryReason::InvalidBook(format!(
                        "synchronized depth message exceeds \
                     limits: {error:?}"
                    )));
                }

                let outcome = self.controller.process(generation, &message);

                if matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }) {
                    self.cancel_current_attempt();
                }

                outcome
            }
        }
    }

    // ========================================================
    // BUFFER ADMISSION
    // ========================================================

    fn buffer_delta(
        &mut self,
        message: NormalizedDepth,
        accounted_bytes: usize,
    ) -> DepthControllerOutcome {
        let Some(token) = self.controller.active_rest_attempt() else {
            return self.fail(DepthControllerRecoveryReason::SnapshotRequired);
        };

        // --------------------------------------------------------
        // MESSAGE LEVEL LIMIT
        // --------------------------------------------------------

        if let Err(error) = DepthResourceAccounting::validate_message(&message, self.resources) {
            return self.fail(DepthControllerRecoveryReason::InvalidBook(format!(
                "depth message exceeds resource limits: \
                     {error:?}"
            )));
        }

        // --------------------------------------------------------
        // RETAINED MEMORY ACCOUNTING
        // --------------------------------------------------------

        let estimated_bytes = match DepthResourceAccounting::estimate_retained_bytes(&message) {
            Ok(bytes) => bytes,

            Err(error) => {
                return self.fail(DepthControllerRecoveryReason::InvalidBook(format!(
                    "cannot account for buffered \
                             depth memory: {error:?}"
                )));
            }
        };

        // The source runtime may supply a larger charge if
        // it knows the actual allocation footprint.
        //
        // It cannot reduce our internal minimum estimate.
        //
        // This also prevents accounted_bytes=0 from bypassing
        // the buffer's positive-size requirement.
        let effective_bytes = accounted_bytes.max(estimated_bytes);

        // --------------------------------------------------------
        // BOUNDED BUFFER ADMISSION
        // --------------------------------------------------------

        let result = match self.buffer.as_mut() {
            Some(buffer) => buffer.push(token, message, effective_bytes),

            None => {
                return self.fail(DepthControllerRecoveryReason::SnapshotRequired);
            }
        };

        match result {
            Ok(()) => DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::AwaitingSnapshot,
            },

            Err(error) => self.fail(DepthControllerRecoveryReason::InvalidSnapshot(format!(
                "pre-snapshot buffer admission \
                         failed: {error:?}"
            ))),
        }
    }

    // ========================================================
    // REST SNAPSHOT COMPLETION
    // ========================================================

    /// Complete one REST snapshot synchronization attempt.
    ///
    /// This is one exclusive state transition:
    ///
    /// Buffering
    ///    -> validate snapshot
    ///    -> replay buffered deltas
    ///    -> commit book
    ///    -> Synchronized
    ///
    /// No WebSocket message can enter this coordinator
    /// between those steps through safe Rust borrowing.
    ///
    /// The caller must still guarantee that messages
    /// are submitted in source-receipt order.
    pub(in crate::live::integrity) fn on_rest_snapshot(
        &mut self,
        token: DepthSyncToken,
        snapshot: &NormalizedDepth,
    ) -> DepthControllerOutcome {
        // An old response must not affect a newer attempt.
        if self.controller.active_rest_attempt() != Some(token)
            || token.stream_generation != self.controller.stream_generation()
        {
            return Self::obsolete();
        }

        if self.phase != RestDepthSyncPhase::Buffering {
            return Self::obsolete();
        }

        let Some(buffer) = self.buffer.as_mut() else {
            return self.fail(DepthControllerRecoveryReason::SnapshotRequired);
        };

        let outcome = self
            .controller
            .complete_rest_snapshot(token, buffer, snapshot);

        match outcome {
            DepthControllerOutcome::Accepted { .. } => {
                // Snapshot reconciliation completed.
                //
                // The buffer has been drained and cannot
                // receive further updates through this API.
                self.buffer = None;
                self.phase = RestDepthSyncPhase::Synchronized;
            }

            DepthControllerOutcome::RecoveryRequired { .. } => {
                self.cancel_current_attempt();
            }

            DepthControllerOutcome::Ignored { .. }
            | DepthControllerOutcome::SnapshotStaged { .. } => {
                // Obsolete results must not mutate state.
            }
        }

        outcome
    }
    pub(in crate::live::integrity) fn new_with_resources(
        stream_id: impl Into<String>,
        plan: CompiledDepthContinuityPlan,
        stream_generation: u64,
        buffer_limits: DepthBufferLimits,
        resource_limits: DepthResourceLimits,
    ) -> Result<Self, String> {
        let resource_limits = resource_limits
            .validate()
            .map_err(|error| format!("invalid depth resources: {error:?}"))?;

        let mut coordinator = Self::new(stream_id, plan, stream_generation, buffer_limits)?;

        coordinator.resources = resource_limits;
        coordinator
            .controller
            .set_resource_limits(resource_limits)?;
        Ok(coordinator)
    }

    pub fn resource_limits(&self) -> DepthResourceLimits {
        self.resources
    }
    // ========================================================
    // INVALIDATION
    // ========================================================

    /// Explicitly invalidate current book synchronization.
    ///
    /// The Recovery Coordinator may subsequently request
    /// a newer REST synchronization attempt.
    pub(in crate::live::integrity) fn invalidate(&mut self) {
        self.cancel_current_attempt();
    }

    fn cancel_current_attempt(&mut self) {
        self.controller.invalidate();
        self.buffer = None;
        self.phase = RestDepthSyncPhase::AwaitingAttempt;
    }

    fn fail(&mut self, reason: DepthControllerRecoveryReason) -> DepthControllerOutcome {
        self.cancel_current_attempt();

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
        live::{config::format::DepthFormatConfig, integrity::DepthContinuityCompiler},
    };

    fn plan(profile: &str) -> CompiledDepthContinuityPlan {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth")
            .join(format!("{profile}.toml"));

        let config = DepthFormatConfig::load(path).unwrap();

        DepthContinuityCompiler::compile(&config).unwrap()
    }

    fn coordinator(profile: &str) -> RestDepthSyncCoordinator {
        RestDepthSyncCoordinator::new(
            "stream-42",
            plan(profile),
            10,
            DepthBufferLimits {
                max_messages: 4,
                max_bytes: 4_096,
            },
        )
        .unwrap()
    }

    fn token(attempt: u64) -> DepthSyncToken {
        DepthSyncToken {
            stream_generation: 10,
            attempt,
        }
    }

    fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Binance,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: "stream-42".into(),
            },

            side,
            price: Decimal::from(price),

            quantity_base: Some(Decimal::from(quantity)),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }
    }

    fn sequence(first: Option<u64>, last: Option<u64>) -> SequenceMetadata {
        SequenceMetadata {
            sequence_first: first,
            sequence_last: last,
            sequence_previous: None,
            cross_sequence: None,
        }
    }

    fn delta(first: u64, last: u64) -> NormalizedDepth {
        NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            sequence(Some(first), Some(last)),
            vec![level(BookSide::Bid, 80_000, last as i64)],
            Some(1_000_000_000),
            None,
        )
        .unwrap()
    }

    fn rest_snapshot(last: u64) -> NormalizedDepth {
        NormalizedDepth::new(
            "stream-42",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            sequence(None, Some(last)),
            vec![
                level(BookSide::Bid, 80_000, 3),
                level(BookSide::Ask, 80_010, 2),
            ],
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap()
    }

    // ========================================================
    // BUFFERING
    // ========================================================

    #[test]
    fn deltas_are_buffered_before_snapshot() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        let outcome = sync.on_websocket(10, delta(101, 105), 1024);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::AwaitingSnapshot
            }
        ));

        assert_eq!(sync.phase(), RestDepthSyncPhase::Buffering);
        assert_eq!(sync.buffered_messages(), 1);

        assert!(!sync.is_synchronized());

        assert!(!sync.controller().book().is_initialized());
    }

    // ========================================================
    // SYNCHRONIZATION HANDOFF
    // ========================================================

    #[test]
    fn snapshot_handoff_preserves_message_order() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(101, 105), 1024);
        sync.on_websocket(10, delta(106, 110), 1024);

        let outcome = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert_eq!(sync.phase(), RestDepthSyncPhase::Synchronized);

        assert!(sync.is_synchronized());

        assert_eq!(sync.controller().baseline().unwrap().last_sequence, 110,);

        assert_eq!(sync.buffered_messages(), 0);
    }

    // ========================================================
    // DELTAS AFTER HANDOFF
    // ========================================================

    #[test]
    fn subsequent_deltas_use_synchronized_controller() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(101, 105), 1024);

        sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        let outcome = sync.on_websocket(10, delta(106, 110), 1024);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: false,
                ..
            }
        ));

        assert_eq!(sync.controller().baseline().unwrap().last_sequence, 110,);

        assert!(sync.is_synchronized());
    }

    // ========================================================
    // BUFFER OVERFLOW
    // ========================================================

    #[test]
    fn buffer_overflow_invalidates_sync_attempt() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        for i in 0..4 {
            let outcome = sync.on_websocket(10, delta(101 + i, 101 + i), 1024);

            assert!(matches!(
                outcome,
                DepthControllerOutcome::Ignored {
                    reason: DepthControllerIgnoreReason::AwaitingSnapshot
                }
            ));
        }

        let overflow = sync.on_websocket(10, delta(105, 105), 1024);

        assert!(matches!(
            overflow,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        assert_eq!(sync.phase(), RestDepthSyncPhase::AwaitingAttempt);

        assert!(!sync.is_synchronized());
        assert_eq!(sync.buffered_messages(), 0);
    }

    // ========================================================
    // OBSOLETE RESPONSES
    // ========================================================

    #[test]
    fn old_snapshot_cannot_complete_new_attempt() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(101, 105), 1024);

        sync.begin_attempt(token(2)).unwrap();

        sync.on_websocket(10, delta(201, 205), 1024);

        let obsolete = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(
            obsolete,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert_eq!(sync.buffered_messages(), 1);
        assert_eq!(sync.active_attempt(), Some(token(2)));

        let current = sync.on_rest_snapshot(token(2), &rest_snapshot(200));

        assert!(matches!(current, DepthControllerOutcome::Accepted { .. }));

        assert_eq!(sync.controller().baseline().unwrap().last_sequence, 205,);
    }

    // ========================================================
    // FAILED HANDOFF
    // ========================================================

    #[test]
    fn failed_bridge_never_publishes_partial_book() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(105, 110), 1024);

        let outcome = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        assert_eq!(sync.phase(), RestDepthSyncPhase::AwaitingAttempt);

        assert!(!sync.controller().book().is_initialized());
        assert!(!sync.is_synchronized());
    }

    // ========================================================
    // RECONNECTION
    // ========================================================

    #[test]
    fn generation_change_cancels_buffering() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(101, 105), 1024);

        sync.begin_generation(11).unwrap();

        assert_eq!(sync.phase(), RestDepthSyncPhase::AwaitingAttempt);

        assert_eq!(sync.buffered_messages(), 0);
        assert!(!sync.is_synchronized());

        let obsolete = sync.on_websocket(10, delta(106, 110), 1024);

        assert!(matches!(
            obsolete,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));
    }

    // ========================================================
    // DUPLICATE SNAPSHOT COMPLETION
    // ========================================================

    #[test]
    fn snapshot_attempt_cannot_complete_twice() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(101, 105), 1024);

        let first = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(first, DepthControllerOutcome::Accepted { .. }));

        let second = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(
            second,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert!(sync.is_synchronized());

        assert_eq!(sync.controller().book_generation(), 1);
    }
    // ============================================================
    // N5N.7 — REST SYNCHRONIZATION ACCEPTANCE TESTS
    // ============================================================

    #[test]
    fn failed_snapshot_requires_a_new_attempt() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        // This range cannot bridge snapshot sequence 100.
        sync.on_websocket(10, delta(110, 115), 1024);

        let outcome = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        assert_eq!(sync.phase(), RestDepthSyncPhase::AwaitingAttempt);

        assert_eq!(sync.active_attempt(), None);
        assert!(!sync.is_synchronized());
        assert_eq!(sync.buffered_messages(), 0);

        // More data cannot silently resume the failed attempt.
        let outcome = sync.on_websocket(10, delta(116, 120), 1024);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotRequired
            }
        ));

        assert!(!sync.is_synchronized());

        // The old attempt identifier cannot be reused.
        assert!(sync.begin_attempt(token(1)).is_err());

        // A newer attempt can be started explicitly.
        sync.begin_attempt(token(2)).unwrap();

        assert_eq!(sync.phase(), RestDepthSyncPhase::Buffering);
    }

    #[test]
    fn invalid_snapshot_does_not_expose_book_state() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        sync.on_websocket(10, delta(101, 105), 1024);

        // A REST snapshot without a sequence baseline
        // cannot establish a synchronized book.
        let invalid_snapshot = NormalizedDepth::new(
            "stream-42",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            SequenceMetadata {
                sequence_first: None,
                sequence_last: None,
                sequence_previous: None,
                cross_sequence: None,
            },
            vec![level(BookSide::Bid, 80_000, 3)],
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap();

        let outcome = sync.on_rest_snapshot(token(1), &invalid_snapshot);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(_)
            }
        ));

        assert!(!sync.is_synchronized());
        assert!(!sync.controller().book().is_initialized());

        assert!(sync.controller().book().bids().is_empty());
        assert!(sync.controller().book().asks().is_empty());

        assert_eq!(sync.controller().baseline(), None);
        assert_eq!(sync.buffered_messages(), 0);
    }

    #[test]
    fn overflow_prevents_late_snapshot_acceptance() {
        let mut sync = coordinator("binance_spot");

        sync.begin_attempt(token(1)).unwrap();

        // The test coordinator permits four messages.
        for i in 0..4 {
            sync.on_websocket(10, delta(101 + i, 101 + i), 1024);
        }

        let overflow = sync.on_websocket(10, delta(105, 105), 1024);

        assert!(matches!(
            overflow,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        // The REST response was already in flight.
        // It arrives after buffer overflow.
        let late_snapshot = sync.on_rest_snapshot(token(1), &rest_snapshot(100));

        assert!(matches!(
            late_snapshot,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert!(!sync.is_synchronized());
        assert!(!sync.controller().book().is_initialized());
        assert_eq!(sync.active_attempt(), None);
    }
    #[test]
    fn caller_cannot_underreport_buffered_memory() {
        let mut sync = RestDepthSyncCoordinator::new_with_resources(
            "stream-42",
            plan("binance_spot"),
            10,
            DepthBufferLimits {
                max_messages: 10,
                max_bytes: 1,
            },
            DepthResourceLimits::default(),
        )
        .unwrap();

        sync.begin_attempt(token(1)).unwrap();

        // The caller claims that this complete normalized
        // message occupies only one byte.
        let outcome = sync.on_websocket(10, delta(101, 105), 1);

        // The internal estimator overrides that claim.
        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        assert_eq!(sync.phase(), RestDepthSyncPhase::AwaitingAttempt);

        assert_eq!(sync.buffered_messages(), 0);
        assert!(!sync.is_synchronized());
    }

    #[test]
    fn oversized_buffered_delta_invalidates_attempt() {
        let sync = RestDepthSyncCoordinator::new_with_resources(
            "stream-42",
            plan("binance_spot"),
            10,
            DepthBufferLimits {
                max_messages: 10,
                max_bytes: 1_000_000,
            },
            DepthResourceLimits {
                max_levels_per_message: 0,
                ..DepthResourceLimits::default()
            },
        );

        // Invalid resource policy must fail at construction.
        assert!(sync.is_err());
    }
}
