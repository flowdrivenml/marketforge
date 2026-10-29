use super::{
    SourceProcessingEpoch, SourceProcessor, SourceProcessorError, SourceReadiness,
    SourceStreamInstance, SourceTradeBufferError, SourceTradeBufferLimits, SourceTradeLiveBuffer,
    SourceTradeNumericCandidate, SourceTradePublication, SourceTradeReconcileError,
    SourceTradeRecoveryError, SourceTradeRecoveryReview,
};
use crate::live::infrastructure::normalization::{
    NormalizationSource, NormalizedDepth, NormalizedMessage, NormalizedTrades,
};
use crate::live::integrity::IntegrityTradeRecoveryCommitError;
use crate::live::integrity::{DepthPublicationEvent, IntegrityDecision, IntegrityLifecycleEvent};
use crate::live::integrity::{DepthSourceEpoch, DepthSyncToken, RestDepthRequestContext};
use crate::live::integrity::{IntegrityTradeRecoveryError, TradeRecoveryPreflight};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRunnerState {
    AwaitingConnection,
    Connected,
    Interrupted,
    Stopped,
}

#[derive(Debug, Error)]
pub enum SourceRunnerError {
    #[error("source runner is stopped")]
    Stopped,

    #[error("source has no active acquisition")]
    NotConnected,

    #[error("source already has an active acquisition")]
    AlreadyConnected,

    #[error("source acquisition generation is obsolete")]
    ObsoleteGeneration,

    #[error("lifecycle event does not match acquisition epoch")]
    EpochMismatch,

    #[error("source instance and integrity stream IDs differ")]
    WrongStream,

    #[error(transparent)]
    Processor(#[from] SourceProcessorError),

    #[error(transparent)]
    TradeBuffer(#[from] SourceTradeBufferError),

    #[error("trade recovery review is no longer authoritative")]
    ObsoleteTradeRecoveryReview,

    #[error("trade recovery requires a retained live buffer")]
    MissingTradeRecoveryBuffer,

    #[error(transparent)]
    TradeReconciliation(#[from] SourceTradeReconcileError),

    #[error(transparent)]
    TradeRecoveryPreflight(#[from] IntegrityTradeRecoveryError),

    #[error(transparent)]
    TradeRecoveryCommit(#[from] IntegrityTradeRecoveryCommitError),
}

/// Single-owner orchestration of one source stream.
///
/// The registry controls whether this runner exists.
///
/// The WebSocket manager owns the physical connection.
///
/// This runner owns the processing order for the
/// logical source stream.
pub struct SourceStreamRunner {
    instance: SourceStreamInstance,
    processor: SourceProcessor,

    state: SourceRunnerState,

    active_epoch: Option<SourceProcessingEpoch>,

    /// Highest stream-acquisition generation observed.
    ///
    /// This is separate from RuntimeGeneration.
    last_stream_generation: u64,
    accepted_trade_publication: bool,
    trade_buffer: Option<SourceTradeLiveBuffer>,
}

impl SourceStreamRunner {
    pub fn new(
        instance: SourceStreamInstance,
        processor: SourceProcessor,
    ) -> Result<Self, SourceRunnerError> {
        if instance.stream_id() != processor.integrity().stream_id() {
            return Err(SourceRunnerError::WrongStream);
        }

        Ok(Self {
            last_stream_generation: processor.integrity().stream_generation(),
            instance,
            processor,
            state: SourceRunnerState::AwaitingConnection,
            active_epoch: None,
            accepted_trade_publication: false,
            trade_buffer: None,
        })
    }
    /// Current trusted acquisition epoch.
    ///
    /// None means the runner cannot admit source data.
    pub fn active_epoch(&self) -> Option<SourceProcessingEpoch> {
        self.active_epoch
    }
    pub fn stream_id(&self) -> &str {
        self.instance.stream_id()
    }

    pub fn state(&self) -> SourceRunnerState {
        self.state
    }

    pub fn processor(&self) -> &SourceProcessor {
        &self.processor
    }

    pub fn instance(&self) -> &SourceStreamInstance {
        &self.instance
    }

    /// Establish a trusted acquisition.
    ///
    /// The first acquisition may use the generation
    /// already installed during integrity construction.
    ///
    /// Reconnection requires a strictly newer generation
    /// and a matching integrity lifecycle transition.
    pub fn connected(
        &mut self,
        epoch: SourceProcessingEpoch,
        lifecycle: Option<IntegrityLifecycleEvent>,
        mut publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state == SourceRunnerState::Connected {
            return Err(SourceRunnerError::AlreadyConnected);
        }

        let generation = epoch_generation(epoch);

        match lifecycle {
            None => {
                // Only the first acquisition may reuse the
                // integrity handle's initial generation.
                if self.state != SourceRunnerState::AwaitingConnection
                    || generation != self.last_stream_generation
                {
                    return Err(SourceRunnerError::ObsoleteGeneration);
                }
            }

            Some(lifecycle) => {
                if generation <= self.last_stream_generation {
                    return Err(SourceRunnerError::ObsoleteGeneration);
                }

                if !lifecycle_matches(&lifecycle, epoch) {
                    return Err(SourceRunnerError::EpochMismatch);
                }

                self.processor.on_lifecycle(lifecycle, &mut publish_depth)?;

                self.last_stream_generation = generation;
            }
        }
        // A new acquisition must not inherit an older generation's
        // unaccepted live-trade buffer.
        self.trade_buffer = None;
        self.active_epoch = Some(epoch);
        self.state = SourceRunnerState::Connected;

        Ok(())
    }
    /// Process one WebSocket depth delta while a REST-backed
    /// publication reset is waiting for its authoritative snapshot.
    ///
    /// No depth publication takes place here.
    pub fn data_during_depth_reset(
        &mut self,
        payload: &Value,
        accounted_bytes: usize,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        let Some(SourceProcessingEpoch::Depth(epoch)) = self.active_epoch else {
            return Err(SourceRunnerError::EpochMismatch);
        };

        let normalized = self
            .instance
            .normalizer()
            .normalize_ws(payload)
            .map_err(SourceProcessorError::Normalization)?;

        let NormalizedMessage::Depth(message) = normalized else {
            return Err(SourceRunnerError::Processor(
                SourceProcessorError::WrongChannel,
            ));
        };

        self.processor
            .buffer_depth_during_reset(message, epoch, accounted_bytes)?;

        Ok(())
    }
    /// Acknowledge downstream invalidation after an uncertain
    /// depth publication.
    ///
    /// The stream remains non-ready until a new authoritative
    /// snapshot is reconciled and acknowledged.
    pub fn reset_depth_publication(
        &mut self,
        publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        if !matches!(self.active_epoch, Some(SourceProcessingEpoch::Depth(_))) {
            return Err(SourceRunnerError::EpochMismatch);
        }

        self.processor
            .reset_publication_invalidation(publish_depth)?;

        Ok(())
    }
    /// Process one already-routed WebSocket JSON payload.
    ///
    /// Transport-level routing, subscription acknowledgment,
    /// and payload-size admission belong upstream.
    ///
    /// This method does not claim the source is ready merely
    /// because normalization succeeds.
    pub fn data(
        &mut self,
        payload: &Value,
        accounted_bytes: usize,
        publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
        mut publish_trades: impl for<'a> FnMut(SourceTradePublication<'a>) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        let epoch = self.active_epoch.ok_or(SourceRunnerError::NotConnected)?;

        // A coverage hold has already stopped ordinary trade admission.
        //
        // Retain subsequent WebSocket batches without submitting them
        // to the controller, which would return CoverageReviewPending
        // and otherwise discard their canonical records.
        if let SourceProcessingEpoch::Trades(trade_epoch) = epoch {
            if self.processor.integrity().trade_coverage_hold().is_some() {
                return self.buffer_held_trade_message(payload, accounted_bytes, trade_epoch);
            }
        }

        let mut accepted_and_published = false;

        self.processor.process_websocket(
            self.instance.normalizer(),
            payload,
            epoch,
            accounted_bytes,
            publish_depth,
            |outcome| {
                let accepted = outcome.decision() == IntegrityDecision::Accepted;

                publish_trades(SourceTradePublication::Integrity(outcome))?;

                if accepted {
                    accepted_and_published = true;
                }

                Ok(())
            },
        )?;
        // The just-processed batch may have created a coverage hold.
        //
        // Integrity already retains that triggering batch.
        // Initialize the separate buffer for subsequent live data.
        if let SourceProcessingEpoch::Trades(trade_epoch) = epoch {
            if self.processor.integrity().trade_coverage_hold().is_some() {
                self.ensure_trade_recovery_buffer(trade_epoch)?;
            }
        }
        if accepted_and_published {
            self.accepted_trade_publication = true;
        }

        Ok(())
    }

    /// Interrupt the current acquisition.
    ///
    /// A WebSocket disconnect must invalidate integrity,
    /// even if the logical stream remains enabled.
    pub fn disconnected(
        &mut self,
        mut publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        // Revoke data admission before performing
        // downstream invalidation.
        self.active_epoch = None;
        self.state = SourceRunnerState::Interrupted;
        // Retained recovery data from this acquisition must not
        // accidentally be replayed under the next acquisition epoch.
        //
        // The interruption itself remains an unverified coverage event.
        self.trade_buffer = None;
        self.processor.on_lifecycle(
            IntegrityLifecycleEvent::SourceInterrupted,
            &mut publish_depth,
        )?;

        Ok(())
    }

    /// Terminal stop for this runner.
    ///
    /// WebSocket detachment and task cancellation must
    /// already have been coordinated by the executor.
    pub fn stop(
        &mut self,
        mut publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Ok(());
        }
        self.trade_buffer = None;
        self.active_epoch = None;
        self.state = SourceRunnerState::Stopped;

        self.processor
            .on_lifecycle(IntegrityLifecycleEvent::SourceStopped, &mut publish_depth)?;

        Ok(())
    }

    pub fn readiness(&self) -> SourceReadiness {
        let integrity = self.processor.integrity().status();

        let publication = self
            .processor
            .publication()
            .map(|publication| (publication.phase(), publication.state()));

        SourceReadiness::evaluate(
            self.state,
            integrity,
            publication,
            self.accepted_trade_publication,
        )
    }
    /// Start a trusted REST attempt on the owning source actor.
    pub fn prepare_rest_depth_recovery(
        &mut self,
        request: RestDepthRequestContext,
        publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        let Some(SourceProcessingEpoch::Depth(active_epoch)) = self.active_epoch else {
            return Err(SourceRunnerError::EpochMismatch);
        };

        if request.source_epoch != active_epoch {
            return Err(SourceRunnerError::EpochMismatch);
        }

        self.processor
            .prepare_rest_depth_recovery(request, publish_depth)?;

        Ok(())
    }

    /// Complete a REST attempt without bypassing source
    /// acquisition fencing or depth publication acknowledgement.
    pub fn complete_rest_depth_recovery(
        &mut self,
        epoch: DepthSourceEpoch,
        token: DepthSyncToken,
        snapshot: &NormalizedDepth,
        publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<bool, SourceRunnerError> {
        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        let Some(SourceProcessingEpoch::Depth(active_epoch)) = self.active_epoch else {
            return Err(SourceRunnerError::EpochMismatch);
        };

        if epoch != active_epoch {
            // Reject before allowing the result to touch
            // the currently authoritative integrity generation.
            return Err(SourceRunnerError::EpochMismatch);
        }

        Ok(self
            .processor
            .complete_rest_depth_recovery(epoch, token, snapshot, publish_depth)?)
    }
    /// Capture a pending trade coverage review.
    ///
    /// This operation is read-only and does not resume
    /// trade publication.
    pub fn pending_trade_recovery(
        &self,
    ) -> Result<SourceTradeRecoveryReview, SourceTradeRecoveryError> {
        SourceTradeRecoveryReview::capture(self.processor.integrity())
    }
    const TRADE_BUFFER_LIMITS: SourceTradeBufferLimits = SourceTradeBufferLimits {
        max_batches: 1024,
        max_records: 8192,
        max_bytes: 16 * 1024 * 1024,
    };
    /// Reconcile REST trades with the unresolved coverage hold
    /// and subsequent buffered WebSocket trades.
    ///
    /// The supplied REST batch must already have been normalized
    /// through the trusted REST request/response binding.
    ///
    /// This is a READ-ONLY recovery preflight.
    ///
    /// It does not:
    /// - Commit identities.
    /// - Advance the accepted ordering baseline.
    /// - Clear the coverage hold.
    /// - Publish canonical trades.
    /// - Establish complete exchange historical coverage.
    pub fn preflight_numeric_trade_recovery(
        &self,
        expected_epoch: crate::live::integrity::TradeSourceEpoch,
        review: &SourceTradeRecoveryReview,
        rest: &NormalizedTrades,
        max_records: usize,
    ) -> Result<TradeRecoveryPreflight, SourceRunnerError> {
        // --------------------------------------------------------
        // SOURCE LIFECYCLE
        // --------------------------------------------------------

        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        let Some(SourceProcessingEpoch::Trades(active_epoch)) = self.active_epoch else {
            return Err(SourceRunnerError::EpochMismatch);
        };

        // Full acquisition identity is mandatory.
        //
        // A completion belonging to an older connection or
        // subscription may not inspect the current recovery state.
        if expected_epoch != active_epoch {
            return Err(SourceRunnerError::EpochMismatch);
        }

        // --------------------------------------------------------
        // AUTHORITATIVE COVERAGE REVIEW
        // --------------------------------------------------------

        let integrity = self.processor.integrity();

        if !review.is_current(integrity) {
            return Err(SourceRunnerError::ObsoleteTradeRecoveryReview);
        }

        // --------------------------------------------------------
        // RETAINED RECOVERY INPUTS
        // --------------------------------------------------------

        let triggering = integrity
            .trade_coverage_triggering_trades()
            .ok_or(SourceRunnerError::ObsoleteTradeRecoveryReview)?;

        let buffered = self
            .trade_buffer
            .as_ref()
            .ok_or(SourceRunnerError::MissingTradeRecoveryBuffer)?;

        if buffered.epoch() != expected_epoch {
            return Err(SourceRunnerError::EpochMismatch);
        }

        // --------------------------------------------------------
        // NUMERIC RECONCILIATION
        // --------------------------------------------------------

        let candidate = SourceTradeNumericCandidate::build(
            self.stream_id(),
            review.baseline.numeric_trade_id,
            rest,
            triggering,
            buffered,
            max_records,
        )?;

        // --------------------------------------------------------
        // AUTHORITATIVE INTEGRITY PREFLIGHT
        // --------------------------------------------------------

        integrity
            .preflight_trade_recovery(expected_epoch, review.baseline, &candidate.trades)
            .map_err(Into::into)
    }
    /// Complete an externally authorized numeric trade recovery.
    ///
    /// The caller must first validate:
    /// - The exact REST recovery request and completion.
    /// - The exchange-specific coverage contract.
    /// - The current recovery attempt identity.
    /// - The reconciled candidate through source preflight.
    ///
    /// This method does not independently establish REST
    /// coverage completeness.
    ///
    /// Publication and integrity mutation remain serialized
    /// by the source runner.
    pub(super) fn complete_numeric_trade_recovery(
        &mut self,
        expected_epoch: crate::live::integrity::TradeSourceEpoch,
        review: &SourceTradeRecoveryReview,
        prepared: TradeRecoveryPreflight,
        publish: impl FnOnce(&[crate::shared::domain::canonical::Trade]) -> Result<(), String>,
    ) -> Result<(), SourceRunnerError> {
        // --------------------------------------------------------
        // LIFECYCLE
        // --------------------------------------------------------

        if self.state == SourceRunnerState::Stopped {
            return Err(SourceRunnerError::Stopped);
        }

        if self.state != SourceRunnerState::Connected {
            return Err(SourceRunnerError::NotConnected);
        }

        let Some(SourceProcessingEpoch::Trades(active_epoch)) = self.active_epoch else {
            return Err(SourceRunnerError::EpochMismatch);
        };

        if expected_epoch != active_epoch {
            return Err(SourceRunnerError::EpochMismatch);
        }

        // --------------------------------------------------------
        // AUTHORITATIVE RECOVERY STATE
        // --------------------------------------------------------

        let integrity = self.processor.integrity();

        if !review.is_current(integrity) {
            return Err(SourceRunnerError::ObsoleteTradeRecoveryReview);
        }

        if prepared.generation() != expected_epoch.stream_generation
            || prepared.baseline() != review.baseline
        {
            return Err(SourceRunnerError::ObsoleteTradeRecoveryReview);
        }

        let buffer = self
            .trade_buffer
            .as_ref()
            .ok_or(SourceRunnerError::MissingTradeRecoveryBuffer)?;

        if buffer.epoch() != expected_epoch {
            return Err(SourceRunnerError::EpochMismatch);
        }

        // --------------------------------------------------------
        // PUBLICATION-SAFE INTEGRITY COMPLETION
        // --------------------------------------------------------

        // The integrity facade revalidates the current
        // acquisition and controller state before publication.
        //
        // On callback failure, the controller preserves
        // its coverage hold and accepted baseline.
        let accepted = self.processor.integrity_mut().complete_trade_recovery(
            expected_epoch,
            prepared,
            publish,
        )?;

        // --------------------------------------------------------
        // SUCCESSFUL COMPLETION
        // --------------------------------------------------------

        // The accepted batch has crossed the synchronous
        // publication boundary successfully.
        //
        // Recovery data may now be discarded because its
        // identities were committed by integrity.
        self.trade_buffer = None;

        // Trade readiness requires a successfully published
        // accepted batch and admissible integrity state.
        //
        // This does not imply globally complete history.
        if !accepted.trades.is_empty() {
            self.accepted_trade_publication = true;
        }

        Ok(())
    }
    /// Read-only access to unaccepted live batches retained
    /// during an unresolved trade coverage review.
    pub fn trade_recovery_buffer(&self) -> Option<&SourceTradeLiveBuffer> {
        self.trade_buffer.as_ref()
    }

    fn ensure_trade_recovery_buffer(
        &mut self,
        epoch: crate::live::integrity::TradeSourceEpoch,
    ) -> Result<(), SourceRunnerError> {
        if self.trade_buffer.is_none() {
            self.trade_buffer = Some(SourceTradeLiveBuffer::new(
                self.stream_id(),
                epoch,
                Self::TRADE_BUFFER_LIMITS,
            )?);
        }

        if self.trade_buffer.as_ref().unwrap().epoch() != epoch {
            return Err(SourceTradeBufferError::ObsoleteEpoch.into());
        }

        Ok(())
    }

    fn buffer_held_trade_message(
        &mut self,
        payload: &Value,
        accounted_bytes: usize,
        epoch: crate::live::integrity::TradeSourceEpoch,
    ) -> Result<(), SourceRunnerError> {
        self.ensure_trade_recovery_buffer(epoch)?;

        let normalized = self
            .instance
            .normalizer()
            .normalize_ws(payload)
            .map_err(SourceProcessorError::Normalization)?;

        let NormalizedMessage::Trades(message) = normalized else {
            return Err(SourceRunnerError::Processor(
                SourceProcessorError::WrongChannel,
            ));
        };

        if message.source() != NormalizationSource::WebSocket {
            return Err(SourceTradeBufferError::InvalidSource.into());
        }

        self.trade_buffer
            .as_mut()
            .unwrap()
            .push(epoch, message, accounted_bytes)?;

        Ok(())
    }
}

fn epoch_generation(epoch: SourceProcessingEpoch) -> u64 {
    match epoch {
        SourceProcessingEpoch::Depth(epoch) => epoch.stream_generation,

        SourceProcessingEpoch::Trades(epoch) => epoch.stream_generation,
    }
}

fn lifecycle_matches(lifecycle: &IntegrityLifecycleEvent, epoch: SourceProcessingEpoch) -> bool {
    match (lifecycle, epoch) {
        (
            IntegrityLifecycleEvent::DepthGeneration(binding),
            SourceProcessingEpoch::Depth(epoch),
        ) => binding.epoch() == epoch,

        (
            IntegrityLifecycleEvent::TradeGeneration(binding),
            SourceProcessingEpoch::Trades(epoch),
        ) => binding.epoch() == epoch,

        _ => false,
    }
}
