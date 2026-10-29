use crate::live::infrastructure::normalization::NormalizedDepth;
use crate::live::{
    infrastructure::normalization::{
        DepthMessageKind, NormalizedMessage, Normalizer, NormalizerError,
    },
    integrity::{
        DepthControllerIgnoreReason, DepthControllerOutcome, DepthPublicationEvent,
        DepthPublicationPhase, DepthSourceEpoch, DepthSyncToken, IntegrityDecision, IntegrityInput,
        IntegrityLifecycleError, IntegrityLifecycleEvent, IntegrityOutcome,
        IntegrityPublicationAdapter, IntegrityPublicationError, IntegrityRecoveryError,
        IntegrityRecoveryResult, PreparedDepthPublication, RestDepthRequestContext,
        RestDepthSyncPhase, StreamIntegrityHandle, TradeSourceEpoch,
    },
};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceProcessorError {
    #[error(transparent)]
    Normalization(#[from] NormalizerError),

    #[error("integrity lifecycle error: {0:?}")]
    Lifecycle(IntegrityLifecycleError),

    #[error("integrity channel does not match normalized input")]
    WrongChannel,

    #[error("normalized message belongs to another stream")]
    WrongStream,

    #[error("source generation is not active")]
    InactiveGeneration,

    #[error("depth publication failed: {0:?}")]
    DepthPublication(IntegrityPublicationError),

    #[error("downstream handoff failed: {0}")]
    Handoff(String),

    #[error("depth publication requires recovery before ordinary processing")]
    PublicationResetRequired,

    #[error("integrity REST recovery error: {0:?}")]
    Recovery(IntegrityRecoveryError),

    #[error("REST recovery snapshot cannot replace an uncertain publication")]
    RecoveryPublicationResetRequired,

    #[error("REST depth synchronization buffer requires a fresh recovery attempt")]
    DepthBufferRecoveryRequired,
}

impl From<IntegrityLifecycleError> for SourceProcessorError {
    fn from(error: IntegrityLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Trusted acquisition epoch supplied by the source runtime.
///
/// This must never be constructed from exchange-controlled JSON.
#[derive(Debug, Clone, Copy)]
pub enum SourceProcessingEpoch {
    Depth(DepthSourceEpoch),
    Trades(TradeSourceEpoch),
}

/// Single-owner source processing boundary.
///
/// The caller must serialize calls for each logical stream.
///
/// It does not own:
/// - WebSocket connections;
/// - REST workers;
/// - recovery scheduling;
/// - the canonical event router;
/// - StreamRuntimeRegistry lifecycle authority.
pub struct SourceProcessor {
    integrity: StreamIntegrityHandle,
    publication: Option<IntegrityPublicationAdapter>,
}

impl SourceProcessor {
    pub fn new(integrity: StreamIntegrityHandle) -> Result<Self, SourceProcessorError> {
        let publication = match integrity.channel() {
            crate::live::integrity::IntegrityChannel::Depth => Some(
                IntegrityPublicationAdapter::new(&integrity)
                    .map_err(SourceProcessorError::DepthPublication)?,
            ),

            crate::live::integrity::IntegrityChannel::Trades => None,
        };

        Ok(Self {
            integrity,
            publication,
        })
    }

    pub fn integrity(&self) -> &StreamIntegrityHandle {
        &self.integrity
    }
    pub(super) fn integrity_mut(&mut self) -> &mut StreamIntegrityHandle {
        &mut self.integrity
    }
    pub fn publication(&self) -> Option<&IntegrityPublicationAdapter> {
        self.publication.as_ref()
    }

    /// Normalize and process one WebSocket data payload.
    ///
    /// The caller must first establish a trusted acquisition
    /// generation through the Integrity lifecycle facade.
    ///
    /// Both callbacks must complete their handoff before
    /// this method returns.
    pub fn process_websocket(
        &mut self,
        normalizer: &Normalizer,
        payload: &Value,
        epoch: SourceProcessingEpoch,
        accounted_bytes: usize,
        publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
        publish_trades: impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        if normalizer.stream_id() != self.integrity.stream_id() {
            return Err(SourceProcessorError::WrongStream);
        }

        // Reject interrupted/stopped processing before even
        // attempting normalization.
        if !self.integrity.admission_state().allows_processing() {
            return Err(SourceProcessorError::InactiveGeneration);
        }

        let normalized = normalizer.normalize_ws(payload)?;

        self.process_normalized(
            normalized,
            epoch,
            accounted_bytes,
            publish_depth,
            publish_trades,
        )
    }
    /// Admit a WebSocket delta into REST synchronization while
    /// downstream publication is being reset.
    ///
    /// This path never publishes canonical depth events.
    ///
    /// It is valid only after downstream invalidation was
    /// acknowledged and while integrity is unsynchronized.
    pub fn buffer_depth_during_reset(
        &mut self,
        message: NormalizedDepth,
        epoch: DepthSourceEpoch,
        accounted_bytes: usize,
    ) -> Result<(), SourceProcessorError> {
        use crate::live::integrity::DepthControllerOutcome;

        let publication = self
            .publication
            .as_ref()
            .ok_or(SourceProcessorError::WrongChannel)?;

        if !publication.reset_invalidation_complete() {
            return Err(SourceProcessorError::PublicationResetRequired);
        }

        if message.stream_id() != self.integrity.stream_id() {
            return Err(SourceProcessorError::WrongStream);
        }

        if !self.integrity.admission_state().allows_processing() {
            return Err(SourceProcessorError::InactiveGeneration);
        }

        // An initialized book must not be incrementally modified
        // while the consumer has no authoritative baseline.
        if self.integrity.status().depth_synchronized != Some(false) {
            return Err(SourceProcessorError::PublicationResetRequired);
        }
        // Only the existing REST synchronization coordinator may
        // retain deltas during publication reset.
        //
        // Being unsynchronized alone is insufficient: there must
        // be an active REST attempt with an initialized buffer.
        if self.integrity.rest_depth_sync_phase() != Some(RestDepthSyncPhase::Buffering) {
            return Err(SourceProcessorError::PublicationResetRequired);
        }
        // This entrypoint is exclusively for the REST
        // pre-snapshot delta buffer.
        if message.kind() != DepthMessageKind::Delta {
            return Err(SourceProcessorError::PublicationResetRequired);
        }

        let outcome = self.integrity.process(IntegrityInput::Depth {
            epoch,
            message,
            accounted_bytes,
        });

        match outcome {
            IntegrityOutcome::Depth(DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::AwaitingSnapshot,
            }) => {
                // The REST coordinator confirmed that this delta
                // was retained for snapshot reconciliation.
                //
                // No publication has occurred.
                Ok(())
            }

            IntegrityOutcome::Depth(DepthControllerOutcome::RecoveryRequired { .. }) => {
                Err(SourceProcessorError::DepthBufferRecoveryRequired)
            }

            _ => {
                // Stale, ignored, accepted or unexpected outcomes
                // are not evidence of successful REST buffering.
                Err(SourceProcessorError::PublicationResetRequired)
            }
        }
    }
    /// Process one already-normalized message.
    ///
    /// This entrypoint is also useful for deterministic
    /// integration tests using existing normalized fixtures.
    pub fn process_normalized(
        &mut self,
        normalized: NormalizedMessage,
        epoch: SourceProcessingEpoch,
        accounted_bytes: usize,
        mut publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
        mut publish_trades: impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        if normalized.stream_id() != self.integrity.stream_id() {
            return Err(SourceProcessorError::WrongStream);
        }

        if !self.integrity.admission_state().allows_processing() {
            return Err(SourceProcessorError::InactiveGeneration);
        }
        // Do not mutate the authoritative depth book when the
        // publication fence cannot accept ordinary updates.
        //
        // Recovery uses a separate, explicitly authorized path.
        if self
            .publication
            .as_ref()
            .is_some_and(|publication| publication.phase() == DepthPublicationPhase::ResetRequired)
        {
            return Err(SourceProcessorError::PublicationResetRequired);
        }
        let outcome = match (normalized, epoch) {
            (NormalizedMessage::Depth(message), SourceProcessingEpoch::Depth(epoch)) => {
                self.integrity.process(IntegrityInput::Depth {
                    epoch,
                    message,
                    accounted_bytes,
                })
            }

            (NormalizedMessage::Trades(message), SourceProcessingEpoch::Trades(epoch)) => {
                self.integrity.process(IntegrityInput::Trades {
                    epoch,
                    message: &message,
                })
            }

            _ => return Err(SourceProcessorError::WrongChannel),
        };

        if self.publication.is_some() {
            self.publish_depth_outcome(&outcome, &mut publish_depth)?;
        } else {
            Self::publish_trade_outcome(&outcome, &mut publish_trades)?;
        }

        Ok(())
    }

    /// Apply a trusted connection or stream lifecycle event.
    ///
    /// Depth lifecycle changes must be handed to consumers
    /// before any subsequent depth input is processed.
    pub fn on_lifecycle(
        &mut self,
        event: IntegrityLifecycleEvent,
        mut publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        self.integrity.on_lifecycle(event)?;

        if let Some(publication) = self.publication.as_mut() {
            let prepared = publication
                .prepare_state_change(&self.integrity)
                .map_err(SourceProcessorError::DepthPublication)?;

            Self::complete_handoff(publication, prepared, &mut publish_depth)?;
        }

        Ok(())
    }

    /// Handle an Integrity depth outcome without allowing
    /// another book mutation before publication completion.
    fn publish_depth_outcome(
        &mut self,
        outcome: &IntegrityOutcome,
        publish: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        let publication = self
            .publication
            .as_mut()
            .ok_or(SourceProcessorError::WrongChannel)?;

        let prepared = publication
            .prepare(&self.integrity, outcome)
            .map_err(SourceProcessorError::DepthPublication)?;

        Self::complete_handoff(publication, prepared, publish)
    }

    fn complete_handoff(
        publication: &mut IntegrityPublicationAdapter,
        prepared: PreparedDepthPublication<'_>,
        publish: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        // No downstream event is necessary for an empty
        // preparation, but the fence still needs resolution.
        if prepared.is_empty() {
            return publication
                .acknowledge(prepared)
                .map_err(SourceProcessorError::DepthPublication);
        }

        match publish(prepared.events()) {
            Ok(()) => publication
                .acknowledge(prepared)
                .map_err(SourceProcessorError::DepthPublication),

            Err(error) => {
                // A failed or uncertain handoff must not
                // advance the acknowledged publication state.
                publication
                    .handoff_failed(prepared)
                    .map_err(SourceProcessorError::DepthPublication)?;

                Err(SourceProcessorError::Handoff(error))
            }
        }
    }
    /// Acknowledge consumer invalidation after an uncertain
    /// depth handoff.
    ///
    /// This does not resume normal delta publication.
    /// A fresh authoritative snapshot is still required.
    pub fn reset_publication_invalidation(
        &mut self,
        mut publish: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        let publication = self
            .publication
            .as_mut()
            .ok_or(SourceProcessorError::WrongChannel)?;

        let prepared = publication
            .prepare_reset_invalidation()
            .map_err(SourceProcessorError::DepthPublication)?;

        Self::complete_handoff(publication, prepared, &mut publish)
    }

    /// Complete publication reset using a newly accepted
    /// authoritative depth snapshot.
    ///
    /// The caller must already have invalidated consumers.
    /// The outcome must originate from the current integrity
    /// controller; it cannot be a fabricated replacement.
    /// Process one depth message while restoring publication
    /// after an uncertain downstream handoff.
    ///
    /// The caller must first acknowledge downstream invalidation
    /// through reset_publication_invalidation().
    ///
    /// A REST-backed source must complete its REST reconciliation
    /// through the integrity recovery facade instead; this
    /// method only accepts normalized WebSocket depth messages.
    ///
    /// A Bitget staged snapshot is not published. Its bridging
    /// delta may establish a complete authoritative snapshot.
    ///
    /// No ordinary delta publication is permitted while the
    /// publication fence remains ResetRequired.
    pub fn process_reset_depth(
        &mut self,
        message: crate::live::infrastructure::normalization::NormalizedDepth,
        epoch: DepthSourceEpoch,
        accounted_bytes: usize,
        mut publish: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<bool, SourceProcessorError> {
        use crate::live::integrity::{DepthControllerOutcome, IntegrityOutcome};

        let publication = self
            .publication
            .as_ref()
            .ok_or(SourceProcessorError::WrongChannel)?;

        if publication.phase() != DepthPublicationPhase::ResetRequired {
            return Err(SourceProcessorError::PublicationResetRequired);
        }

        if message.stream_id() != self.integrity.stream_id() {
            return Err(SourceProcessorError::WrongStream);
        }

        if !self.integrity.admission_state().allows_processing() {
            return Err(SourceProcessorError::InactiveGeneration);
        }

        // Fail before integrity mutation if downstream invalidation
        // has not yet cleared the previous published-book identity.
        //
        // The publication fence will perform its additional
        // uncertain-book validation when preparing the snapshot.
        // Do not process a replacement snapshot until the
        // uncertain downstream book has been invalidated
        // and the invalidation has been acknowledged.
        if !publication.reset_invalidation_complete() {
            return Err(SourceProcessorError::PublicationResetRequired);
        }

        // A synchronized book must never accept another incremental
        // mutation while downstream publication is uncertain.
        //
        // A complete WebSocket snapshot can establish a replacement.
        //
        // An unsynchronized Bitget-style controller may require
        // a staged snapshot followed by a bridging delta.
        if self.integrity.status().depth_synchronized == Some(true)
            && message.kind() != DepthMessageKind::Snapshot
        {
            return Err(SourceProcessorError::PublicationResetRequired);
        }

        let outcome = self.integrity.process(IntegrityInput::Depth {
            epoch,
            message,
            accounted_bytes,
        });

        match &outcome {
            IntegrityOutcome::Depth(DepthControllerOutcome::Accepted {
                snapshot: true, ..
            }) => {
                let publication = self
                    .publication
                    .as_mut()
                    .ok_or(SourceProcessorError::WrongChannel)?;

                let prepared = publication
                    .prepare_reset_snapshot(&self.integrity, &outcome)
                    .map_err(SourceProcessorError::DepthPublication)?;

                Self::complete_handoff(publication, prepared, &mut publish)?;

                Ok(true)
            }

            IntegrityOutcome::Depth(DepthControllerOutcome::Accepted {
                snapshot: false, ..
            }) => {
                // An accepted incremental update is not a valid
                // replacement for a complete authoritative book.
                //
                // The source runner must establish a new snapshot
                // rather than resuming ordinary delta publication.
                Err(SourceProcessorError::PublicationResetRequired)
            }

            IntegrityOutcome::Depth(DepthControllerOutcome::SnapshotStaged { .. })
            | IntegrityOutcome::Depth(DepthControllerOutcome::Ignored { .. })
            | IntegrityOutcome::Depth(DepthControllerOutcome::RecoveryRequired { .. }) => {
                // No complete authoritative snapshot is available.
                // The fence remains ResetRequired.
                //
                // RecoveryRequired must also be communicated to
                // the runner's recovery coordinator.
                Ok(false)
            }

            _ => Err(SourceProcessorError::WrongChannel),
        }
    }
    /// Begin one trusted REST depth synchronization attempt.
    ///
    /// Must run on the same serialized source actor as WebSocket
    /// depth processing.
    ///
    /// Starting an attempt invalidates any previously synchronized
    /// book. Downstream invalidation must be acknowledged before
    /// the source processes another depth update.
    pub fn prepare_rest_depth_recovery(
        &mut self,
        request: RestDepthRequestContext,
        mut publish: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        if self.publication.is_none() {
            return Err(SourceProcessorError::WrongChannel);
        }

        // REST synchronization is allowed during publication reset
        // only after the affected downstream book was explicitly
        // invalidated and that handoff was acknowledged.
        if self.publication.as_ref().is_some_and(|publication| {
            publication.phase() == DepthPublicationPhase::ResetRequired
                && !publication.reset_invalidation_complete()
        }) {
            return Err(SourceProcessorError::PublicationResetRequired);
        }

        self.integrity
            .prepare_depth_recovery(request)
            .map_err(SourceProcessorError::Recovery)?;

        let publication = self
            .publication
            .as_mut()
            .ok_or(SourceProcessorError::WrongChannel)?;

        // In reset mode, the publication fence cannot perform
        // ordinary preparation. The acknowledged invalidation
        // is already authoritative.
        //
        // A replacement snapshot will complete the reset later.
        if publication.phase() == DepthPublicationPhase::ResetRequired {
            return Ok(());
        }

        let prepared = publication
            .prepare_state_change(&self.integrity)
            .map_err(SourceProcessorError::DepthPublication)?;

        Self::complete_handoff(publication, prepared, &mut publish)
    }

    /// Complete one previously authorized REST depth snapshot.
    ///
    /// Returns true only when an accepted synchronized snapshot
    /// has passed the downstream publication fence.
    ///
    /// A stale REST response is ignored by the integrity layer
    /// without replacing the current authoritative book.
    pub fn complete_rest_depth_recovery(
        &mut self,
        epoch: DepthSourceEpoch,
        token: DepthSyncToken,
        snapshot: &NormalizedDepth,
        mut publish: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<bool, SourceProcessorError> {
        if self.publication.is_none() {
            return Err(SourceProcessorError::WrongChannel);
        }

        if snapshot.stream_id() != self.integrity.stream_id() {
            return Err(SourceProcessorError::WrongStream);
        }

        if !self.integrity.admission_state().allows_processing() {
            return Err(SourceProcessorError::InactiveGeneration);
        }

        let resetting = self
            .publication
            .as_ref()
            .is_some_and(|publication| publication.phase() == DepthPublicationPhase::ResetRequired);

        if resetting
            && !self
                .publication
                .as_ref()
                .is_some_and(|publication| publication.reset_invalidation_complete())
        {
            return Err(SourceProcessorError::RecoveryPublicationResetRequired);
        }

        let outcome = self
            .integrity
            .on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
                epoch,
                token,
                snapshot,
            });

        let accepted_snapshot = matches!(
            &outcome,
            IntegrityOutcome::Depth(DepthControllerOutcome::Accepted { snapshot: true, .. })
        );

        if resetting {
            // A REST response that did not establish a complete
            // authoritative snapshot cannot restore publication.
            //
            // Do not call ordinary publication preparation while
            // the fence is ResetRequired.
            if !accepted_snapshot {
                return Ok(false);
            }

            let publication = self
                .publication
                .as_mut()
                .ok_or(SourceProcessorError::WrongChannel)?;

            let prepared = publication
                .prepare_reset_snapshot(&self.integrity, &outcome)
                .map_err(SourceProcessorError::DepthPublication)?;

            Self::complete_handoff(publication, prepared, &mut publish)?;
        } else {
            // Normal REST synchronization retains its existing
            // acknowledged publication behavior.
            self.publish_depth_outcome(&outcome, &mut publish)?;
        }

        Ok(accepted_snapshot)
    }
    fn publish_trade_outcome(
        outcome: &IntegrityOutcome,
        publish: &mut impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        if outcome.decision() != IntegrityDecision::Accepted {
            return Ok(());
        }

        publish(outcome).map_err(SourceProcessorError::Handoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use rust_decimal::Decimal;

    use crate::{
        live::{
            config::format::DepthFormatConfig,
            infrastructure::normalization::{
                DepthMessageKind, NormalizationSource, NormalizedDepth, NormalizedTrades,
                TradeRecordSemantics,
            },
            integrity::{
                BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler,
                DepthPublicationPhase, DepthResourceLimits, DepthSourceBinding,
            },
        },
        shared::domain::canonical::{
            BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata,
        },
    };

    const STREAM: &str = "bybit-btc-depth";

    fn epoch(stream: u64, connection: u64, subscription: u64) -> DepthSourceEpoch {
        DepthSourceEpoch {
            stream_generation: stream,
            connection_generation: connection,
            subscription_generation: subscription,
        }
    }

    fn current_epoch() -> DepthSourceEpoch {
        epoch(10, 20, 30)
    }

    fn binding(epoch: DepthSourceEpoch) -> DepthSourceBinding {
        DepthSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            Some(200),
            epoch,
        )
        .unwrap()
    }

    fn processor() -> SourceProcessor {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth/bybit_spot.toml");

        let format = DepthFormatConfig::load(path).unwrap();

        let plan = DepthContinuityCompiler::compile(&format).unwrap();

        let ingress = BoundDepthIngress::new(
            binding(current_epoch()),
            plan,
            DepthBufferLimits {
                max_messages: 32,
                max_bytes: 1_000_000,
            },
            DepthResourceLimits::default(),
        )
        .unwrap();

        let integrity = StreamIntegrityHandle::depth(ingress);

        SourceProcessor::new(integrity).unwrap()
    }

    fn level(price: i64, quantity: i64) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: STREAM.into(),
            },
            side: BookSide::Bid,
            price: Decimal::from(price),
            quantity_base: Some(Decimal::from(quantity)),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }
    }

    fn depth(
        kind: DepthMessageKind,
        update_id: u64,
        cross_sequence: u64,
        quantity: i64,
    ) -> NormalizedMessage {
        NormalizedMessage::Depth(
            NormalizedDepth::new(
                STREAM,
                NormalizationSource::WebSocket,
                kind,
                SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(update_id),
                    sequence_previous: None,
                    cross_sequence: Some(cross_sequence),
                },
                vec![level(80_000, quantity)],
                Some(1_000_000_000),
                Some(200),
            )
            .unwrap(),
        )
    }

    fn snapshot(update_id: u64, cross_sequence: u64) -> NormalizedMessage {
        depth(DepthMessageKind::Snapshot, update_id, cross_sequence, 3)
    }

    fn delta(update_id: u64, cross_sequence: u64) -> NormalizedMessage {
        depth(DepthMessageKind::Delta, update_id, cross_sequence, 5)
    }

    fn submit(
        processor: &mut SourceProcessor,
        message: NormalizedMessage,
        epoch: DepthSourceEpoch,
        mut on_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceProcessorError> {
        processor.process_normalized(
            message,
            SourceProcessingEpoch::Depth(epoch),
            1024,
            |events| on_depth(events),
            |_| panic!("depth input cannot publish trades"),
        )
    }

    #[test]
    fn accepted_snapshot_is_published_and_acknowledged() {
        let mut processor = processor();

        let mut snapshots = 0;

        submit(
            &mut processor,
            snapshot(100, 200),
            current_epoch(),
            |events| {
                assert_eq!(events.len(), 1);

                assert!(matches!(
                    events[0],
                    DepthPublicationEvent::Snapshot {
                        stream_generation: 10,
                        book_generation: 1,
                        ..
                    }
                ));

                snapshots += 1;

                Ok(())
            },
        )
        .unwrap();

        assert_eq!(snapshots, 1);

        let publication = processor.publication().unwrap();

        assert_eq!(publication.state().published_book_generation, Some(1));

        assert_eq!(publication.phase(), DepthPublicationPhase::Ready);
    }

    #[test]
    fn accepted_delta_requires_previous_snapshot() {
        let mut processor = processor();

        let mut publications = 0;

        // A Bybit delta before the initial snapshot
        // must not be published as valid L2 data.
        submit(&mut processor, delta(101, 201), current_epoch(), |events| {
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, DepthPublicationEvent::Delta { .. }))
            );

            publications += events.len();

            Ok(())
        })
        .unwrap();

        assert_eq!(publications, 0);

        assert_eq!(
            processor
                .publication()
                .unwrap()
                .state()
                .published_book_generation,
            None
        );

        // A snapshot establishes the initial book.
        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Ok(())
        })
        .unwrap();

        // Now a valid subsequent delta can publish.
        let mut deltas = 0;

        submit(&mut processor, delta(101, 201), current_epoch(), |events| {
            assert_eq!(events.len(), 1);

            assert!(matches!(
                events[0],
                DepthPublicationEvent::Delta {
                    book_generation: 1,
                    ..
                }
            ));

            deltas += 1;

            Ok(())
        })
        .unwrap();

        assert_eq!(deltas, 1);

        assert_eq!(
            processor
                .publication()
                .unwrap()
                .state()
                .publication_revision,
            2
        );
    }

    #[test]
    fn invalid_sequence_invalidates_published_book() {
        let mut processor = processor();

        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Ok(())
        })
        .unwrap();

        let mut invalidations = 0;

        // Cross-sequence regression violates
        // the Bybit continuity contract.
        submit(&mut processor, delta(101, 199), current_epoch(), |events| {
            for event in events {
                if matches!(
                    event,
                    DepthPublicationEvent::Invalidated {
                        book_generation: 1,
                        ..
                    }
                ) {
                    invalidations += 1;
                }
            }

            Ok(())
        })
        .unwrap();

        assert_eq!(invalidations, 1);

        assert_eq!(
            processor
                .publication()
                .unwrap()
                .state()
                .published_book_generation,
            None
        );

        assert_eq!(
            processor.integrity().status().depth_synchronized,
            Some(false)
        );
    }

    #[test]
    fn obsolete_connection_cannot_modify_current_book() {
        let mut processor = processor();

        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Ok(())
        })
        .unwrap();

        let original_revision = processor
            .publication()
            .unwrap()
            .state()
            .publication_revision;

        let mut published = 0;

        submit(
            &mut processor,
            delta(101, 201),
            epoch(10, 19, 30),
            |events| {
                published += events.len();
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(published, 0);

        assert_eq!(
            processor
                .publication()
                .unwrap()
                .state()
                .publication_revision,
            original_revision
        );

        assert_eq!(
            processor.integrity().status().depth_synchronized,
            Some(true)
        );
    }

    #[test]
    fn interrupted_source_invalidates_published_book() {
        let mut processor = processor();

        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Ok(())
        })
        .unwrap();

        let mut invalidated = false;

        processor
            .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted, |events| {
                invalidated = events.iter().any(|event| {
                    matches!(
                        event,
                        DepthPublicationEvent::Invalidated {
                            book_generation: 1,
                            ..
                        }
                    )
                });

                Ok(())
            })
            .unwrap();

        assert!(invalidated);

        assert_eq!(
            processor.integrity().status().depth_synchronized,
            Some(false)
        );

        // The interrupted source cannot process new
        // messages until the runtime establishes a new epoch.
        assert!(matches!(
            submit(&mut processor, delta(101, 201), current_epoch(), |_| Ok(())),
            Err(SourceProcessorError::InactiveGeneration)
        ));
    }

    #[test]
    fn failed_handoff_does_not_acknowledge_snapshot() {
        let mut processor = processor();

        let result = submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream queue unavailable".into())
        });

        assert!(matches!(result, Err(SourceProcessorError::Handoff(_))));

        let publication = processor.publication().unwrap();

        assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired);

        assert_eq!(publication.state().published_book_generation, None);

        // Ordinary publication cannot resume.
        let result = submit(&mut processor, delta(101, 201), current_epoch(), |_| Ok(()));

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));
    }

    #[test]
    fn wrong_channel_is_rejected() {
        let mut processor = processor();

        let trades = NormalizedTrades::new(
            STREAM,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            vec![],
        )
        .unwrap();

        let result = processor.process_normalized(
            NormalizedMessage::Trades(trades),
            SourceProcessingEpoch::Depth(current_epoch()),
            1024,
            |_| panic!("wrong channel cannot publish depth"),
            |_| panic!("wrong channel cannot publish trades"),
        );

        assert!(matches!(result, Err(SourceProcessorError::WrongChannel)));

        assert_eq!(
            processor.integrity().status().depth_synchronized,
            Some(false)
        );
    }
    #[test]
    fn failed_handoff_blocks_ordinary_depth_mutation() {
        let mut processor = processor();

        let result = submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("publication unavailable".into())
        });

        assert!(matches!(result, Err(SourceProcessorError::Handoff(_))));

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::ResetRequired
        );

        let baseline_before = processor
            .integrity()
            .book()
            .map(|_| processor.integrity().status());

        let result = submit(&mut processor, delta(101, 201), current_epoch(), |_| {
            panic!("ordinary publication must remain blocked")
        });

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));

        assert_eq!(
            processor
                .integrity()
                .book()
                .map(|_| processor.integrity().status()),
            baseline_before
        );
    }
    #[test]
    fn full_snapshot_restores_publication_after_handoff_failure() {
        let mut processor = processor();

        let failed = submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream failure".into())
        });

        assert!(matches!(failed, Err(SourceProcessorError::Handoff(_))));

        let mut invalidations = 0;

        processor
            .reset_publication_invalidation(|events| {
                invalidations += events
                    .iter()
                    .filter(|event| matches!(event, DepthPublicationEvent::Invalidated { .. }))
                    .count();

                Ok(())
            })
            .unwrap();

        assert_eq!(invalidations, 1);

        assert_eq!(
            processor
                .publication()
                .unwrap()
                .state()
                .published_book_generation,
            None
        );

        // A fresh source snapshot creates another book generation.
        let mut snapshots = 0;

        let restored = match snapshot(200, 300) {
            NormalizedMessage::Depth(message) => processor
                .process_reset_depth(message, current_epoch(), 1024, |events| {
                    snapshots += events
                        .iter()
                        .filter(|event| matches!(event, DepthPublicationEvent::Snapshot { .. }))
                        .count();

                    Ok(())
                })
                .unwrap(),

            _ => unreachable!(),
        };

        assert!(restored);
        assert_eq!(snapshots, 1);

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::Ready
        );

        assert_eq!(
            processor
                .publication()
                .unwrap()
                .state()
                .published_book_generation,
            Some(2)
        );

        // Ordinary delta publication is available again.
        let mut deltas = 0;

        submit(&mut processor, delta(201, 301), current_epoch(), |events| {
            deltas += events
                .iter()
                .filter(|event| matches!(event, DepthPublicationEvent::Delta { .. }))
                .count();

            Ok(())
        })
        .unwrap();

        assert_eq!(deltas, 1);
    }
    #[test]
    fn reset_does_not_accept_incremental_delta_as_snapshot() {
        let mut processor = processor();

        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream failed".into())
        })
        .unwrap_err();

        processor
            .reset_publication_invalidation(|_| Ok(()))
            .unwrap();

        let result = match delta(101, 201) {
            NormalizedMessage::Depth(message) => {
                processor.process_reset_depth(message, current_epoch(), 1024, |_| {
                    panic!("reset must not publish delta")
                })
            }

            _ => unreachable!(),
        };

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::ResetRequired
        );
    }
    #[test]
    fn reset_rejects_delta_before_mutating_synchronized_book() {
        let mut processor = processor();

        // Integrity accepts the initial snapshot, but downstream
        // publication fails.
        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream unavailable".into())
        })
        .unwrap_err();

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::ResetRequired
        );

        // Consumers acknowledge invalidation.
        processor
            .reset_publication_invalidation(|_| Ok(()))
            .unwrap();

        // The authoritative integrity book may still be
        // synchronized even though publication is uncertain.
        assert_eq!(
            processor.integrity().status().depth_synchronized,
            Some(true)
        );

        let before = processor.integrity().status();

        // An incremental update must be rejected BEFORE
        // it reaches the integrity controller.
        let result = match delta(101, 201) {
            NormalizedMessage::Depth(message) => {
                processor.process_reset_depth(message, current_epoch(), 1024, |_| {
                    panic!("delta must not be published")
                })
            }

            _ => unreachable!(),
        };

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));

        assert_eq!(processor.integrity().status(), before);

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::ResetRequired
        );
    }
    #[test]
    fn reset_requires_acknowledged_invalidation_before_snapshot_mutation() {
        let mut processor = processor();

        // Integrity accepts a snapshot, but publication fails.
        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream unavailable".into())
        })
        .unwrap_err();

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::ResetRequired
        );

        assert!(
            !processor
                .publication()
                .unwrap()
                .reset_invalidation_complete()
        );

        let before = processor.integrity().status();

        // A replacement snapshot must not reach integrity
        // until downstream invalidation is acknowledged.
        let result = match snapshot(200, 300) {
            NormalizedMessage::Depth(message) => {
                processor.process_reset_depth(message, current_epoch(), 1024, |_| {
                    panic!("unacknowledged reset cannot publish")
                })
            }

            _ => unreachable!(),
        };

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));

        assert_eq!(processor.integrity().status(), before);

        // Now explicitly invalidate the downstream book.
        processor
            .reset_publication_invalidation(|_| Ok(()))
            .unwrap();

        assert!(
            processor
                .publication()
                .unwrap()
                .reset_invalidation_complete()
        );

        // The replacement snapshot may now establish
        // an acknowledged book.
        let restored = match snapshot(200, 300) {
            NormalizedMessage::Depth(message) => processor
                .process_reset_depth(message, current_epoch(), 1024, |_| Ok(()))
                .unwrap(),

            _ => unreachable!(),
        };

        assert!(restored);

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::Ready
        );
    }
    #[test]
    fn reset_buffering_rejects_delta_when_book_is_still_synchronized() {
        let mut processor = processor();

        // Establish an authoritative book, but fail publication.
        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream unavailable".into())
        })
        .unwrap_err();

        processor
            .reset_publication_invalidation(|_| Ok(()))
            .unwrap();

        assert_eq!(
            processor.publication().unwrap().phase(),
            DepthPublicationPhase::ResetRequired
        );

        assert_eq!(
            processor.integrity().status().depth_synchronized,
            Some(true)
        );

        let before = processor.integrity().status();

        let result = match delta(101, 201) {
            NormalizedMessage::Depth(message) => {
                processor.buffer_depth_during_reset(message, current_epoch(), 1024)
            }

            _ => unreachable!(),
        };

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));

        assert_eq!(processor.integrity().status(), before);
    }
    #[test]
    fn reset_buffering_requires_active_rest_synchronization() {
        let mut processor = processor();

        // This fixture uses Bybit WebSocket snapshot
        // synchronization, not REST synchronization.
        assert_eq!(processor.integrity().rest_depth_sync_phase(), None);

        // Establish a synchronized book, but fail downstream
        // publication.
        submit(&mut processor, snapshot(100, 200), current_epoch(), |_| {
            Err("downstream unavailable".into())
        })
        .unwrap_err();

        // Downstream explicitly acknowledges invalidation.
        processor
            .reset_publication_invalidation(|_| Ok(()))
            .unwrap();

        // The WebSocket controller is not an active REST buffer.
        let result = match delta(101, 201) {
            NormalizedMessage::Depth(message) => {
                processor.buffer_depth_during_reset(message, current_epoch(), 1024)
            }

            _ => unreachable!(),
        };

        assert!(matches!(
            result,
            Err(SourceProcessorError::PublicationResetRequired)
        ));

        assert_eq!(processor.integrity().rest_depth_sync_phase(), None);
    }
    #[test]
    fn trade_coverage_hold_cannot_publish_canonical_data() {
        use crate::live::integrity::{BoundTradeIngressOutcome, TradeControllerOutcome};

        let outcome = IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
            TradeControllerOutcome::CoverageReviewPending { generation: 10 },
        ));

        let mut publications = 0;

        SourceProcessor::publish_trade_outcome(&outcome, &mut |_| {
            publications += 1;
            Ok(())
        })
        .unwrap();

        assert_eq!(publications, 0);

        assert_eq!(outcome.decision(), IntegrityDecision::Pending,);
    }

    #[test]
    fn obsolete_trade_epoch_cannot_publish() {
        use crate::live::integrity::BoundTradeIngressOutcome;

        let outcome = IntegrityOutcome::Trades(BoundTradeIngressOutcome::ObsoleteEpoch);

        SourceProcessor::publish_trade_outcome(&outcome, &mut |_| {
            panic!("obsolete trades must not be published");
        })
        .unwrap();

        assert_eq!(outcome.decision(), IntegrityDecision::Ignored,);
    }
}
