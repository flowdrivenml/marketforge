use super::{
    CompletedDepthAttempt, CompletedTradeAttempt, SourceAcquisitionBinder, SourceAcquisitionError,
    SourceAttachment, SourceAttachmentError, SourceDepthAttempt, SourceDepthAttemptError,
    SourceDepthRecoveryError, SourceDepthRecoveryPolicy, SourceProcessingEpoch,
    SourceProcessorError, SourceReadiness, SourceRecoveryMailboxError, SourceRunnerError,
    SourceStreamRunner, SourceTradeAttempt, SourceTradeAttemptContext, SourceTradeCompletionError,
    SourceTradeRecoveryPolicy, SourceTransportEvent, SourceTransportGate, SourceTransportGateError,
    complete_depth_recovery, validate_trade_completion, validate_trade_response,
};
use crate::live::infrastructure::normalization::NormalizedMessage;
use crate::live::infrastructure::websocket::StreamInput;
use crate::live::integrity::DepthPublicationPhase;
use crate::live::integrity::{DepthPublicationEvent, IntegrityOutcome};
use crate::live::integrity::{DepthSyncToken, RestDepthRequestContext};
use crate::live::recovery::StreamRecoveryHandle;
use crate::live::stream::request::SourceChannel;
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};

/// A trusted integrity binding for one confirmed acquisition.
///
/// Constructed by the owning source runtime, not from
/// exchange-controlled payloads.
///
/// On initial acquisition, lifecycle may be None only when
/// the integrity handle already owns the matching epoch.
///
/// On reconnection, the runtime must provide a newer epoch
/// and its corresponding generation lifecycle event.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceExecutorExit {
    ShutdownRequested,
    TransportClosed,
}

#[derive(Debug, Error)]
pub enum SourceExecutorError {
    #[error(transparent)]
    Attachment(#[from] SourceAttachmentError),

    #[error(transparent)]
    Transport(#[from] SourceTransportGateError),

    #[error(transparent)]
    Runner(#[from] SourceRunnerError),

    #[error("cannot bind trusted acquisition: {0}")]
    Binding(String),

    #[error("invalid websocket JSON payload: {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Acquisition(#[from] SourceAcquisitionError),

    #[error(transparent)]
    RecoveryMailbox(#[from] SourceRecoveryMailboxError),

    #[error("REST recovery completion failed: {0}")]
    RecoveryCompletion(String),

    #[error("REST recovery failed: {0}")]
    RecoveryRequest(String),

    #[error(transparent)]
    DepthAttempt(#[from] SourceDepthAttemptError),

    #[error(transparent)]
    DepthRecovery(#[from] SourceDepthRecoveryError),

    #[error("depth recovery is already installed")]
    DepthRecoveryAlreadyInstalled,

    #[error("trade recovery is already installed")]
    TradeRecoveryAlreadyInstalled,

    #[error("REST trade recovery policy is not installed")]
    MissingTradeRecoveryPolicy,

    #[error(transparent)]
    TradeCompletion(#[from] SourceTradeCompletionError),

    #[error("REST trade recovery completion could not be accepted: {0}")]
    TradeCompletionRejected(String),
}

const MAX_DEPTH_ATTEMPTS_PER_ACQUISITION: u64 = 3;
const DEPTH_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_TRADE_ATTEMPTS_PER_ACQUISITION: u64 = 3;
const MAX_TRADE_RECONCILIATION_RECORDS: usize = 1000;
/// Single-owner asynchronous execution context for one
/// logical source-stream attachment.
///
/// The WebSocket manager owns physical connections.
///
/// The runtime registry owns stream lifecycle authority.
///
/// This executor owns sequential transport-event processing,
/// normalization admission and ordered shutdown.
pub struct SourceStreamExecutor {
    runner: SourceStreamRunner,
    attachment: SourceAttachment,
    gate: SourceTransportGate,
    binder: SourceAcquisitionBinder,
    depth_recovery: Option<SourceDepthAttempt>,
    recovery_handle: Option<StreamRecoveryHandle>,
    depth_policy: Option<SourceDepthRecoveryPolicy>,
    next_depth_attempt: u64,
    depth_retry_at: Option<Instant>,
    trade_recovery: Option<SourceTradeAttempt>,
    trade_recovery_handle: Option<StreamRecoveryHandle>,
    trade_policy: Option<SourceTradeRecoveryPolicy>,
    next_trade_attempt: u64,
    completed_trade_recovery: Option<CompletedTradeAttempt>,
}

impl SourceStreamExecutor {
    pub fn new(
        runner: SourceStreamRunner,
        attachment: SourceAttachment,
        binder: SourceAcquisitionBinder,
    ) -> Self {
        Self {
            runner,
            attachment,
            gate: SourceTransportGate::new(),
            binder,
            depth_recovery: None,
            recovery_handle: None,
            depth_policy: None,
            next_depth_attempt: 1,
            depth_retry_at: None,
            trade_recovery: None,
            trade_recovery_handle: None,
            trade_policy: None,
            next_trade_attempt: 1,
            completed_trade_recovery: None,
        }
    }
    /// Install depth recovery resources for the current acquisition.
    ///
    /// The handle and attempt must belong to the same logical
    /// stream and source epoch.
    ///
    /// A new acquisition requires a new recovery handle.
    pub fn with_depth_recovery(
        mut self,
        attempt: SourceDepthAttempt,
        handle: StreamRecoveryHandle,
    ) -> Result<Self, SourceExecutorError> {
        if self.depth_recovery.is_some() {
            return Err(SourceExecutorError::DepthRecoveryAlreadyInstalled);
        }

        if attempt.epoch() != handle.binding().epoch {
            return Err(SourceExecutorError::RecoveryCompletion(
                "depth recovery epoch mismatch".into(),
            ));
        }

        if handle.binding().stream_id != self.runner.stream_id() {
            return Err(SourceExecutorError::RecoveryCompletion(
                "depth recovery stream identity mismatch".into(),
            ));
        }

        self.depth_recovery = Some(attempt);
        self.recovery_handle = Some(handle);

        Ok(self)
    }
    /// Install trade REST recovery ownership.
    ///
    /// This does not schedule a request or authorize
    /// recovered trade publication.
    ///
    /// The shared recovery coordinator remains the sole
    /// scheduler for both depth and trade acquisitions.
    pub fn with_trade_recovery(
        mut self,
        attempt: SourceTradeAttempt,
        handle: StreamRecoveryHandle,
    ) -> Result<Self, SourceExecutorError> {
        if self.trade_recovery.is_some() || self.trade_recovery_handle.is_some() {
            return Err(SourceExecutorError::TradeRecoveryAlreadyInstalled);
        }

        if self.depth_recovery.is_some() {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery cannot be installed on a depth executor".into(),
            ));
        }

        if attempt.epoch() != handle.binding().epoch {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery attempt and handle epochs differ".into(),
            ));
        }

        if handle.binding().stream_id != self.runner.stream_id() {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery stream identity mismatch".into(),
            ));
        }
        if handle.binding().source.channel != SourceChannel::Trades {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery requires a trade source binding".into(),
            ));
        }
        self.trade_recovery = Some(attempt);
        self.trade_recovery_handle = Some(handle);

        Ok(self)
    }
    /// Install the permitted trade REST acquisition strategy.
    ///
    /// Does not start recovery or authorize publication.
    pub fn with_trade_recovery_policy(
        mut self,
        policy: SourceTradeRecoveryPolicy,
    ) -> Result<Self, SourceExecutorError> {
        if self.trade_recovery_handle.is_none() || self.trade_recovery.is_none() {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery resources must be installed before policy".into(),
            ));
        }

        // Reconstruct the policy from the authoritative source
        // to prevent installing a policy for another exchange.
        let expected = SourceTradeRecoveryPolicy::for_source(self.runner.instance().request())
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        if policy != expected {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery policy does not match source configuration".into(),
            ));
        }

        self.trade_policy = Some(policy);

        Ok(self)
    }
    pub fn with_depth_recovery_policy(mut self, policy: SourceDepthRecoveryPolicy) -> Self {
        self.depth_policy = Some(policy);
        self
    }
    /// Attempt to recover from an uncertain depth publication.
    ///
    /// Only a failed downstream handoff that actually placed
    /// the publication fence into ResetRequired is recoverable
    /// through this path.
    ///
    /// All other errors retain their existing terminal behavior.
    fn handle_publication_failure(
        &mut self,
        error: SourceExecutorError,
        publish_depth: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceExecutorError> {
        let handoff_failed = matches!(
            &error,
            SourceExecutorError::Runner(SourceRunnerError::Processor(
                SourceProcessorError::Handoff(_)
            )) | SourceExecutorError::DepthRecovery(SourceDepthRecoveryError::Runner(
                SourceRunnerError::Processor(SourceProcessorError::Handoff(_))
            ))
        );

        if !handoff_failed {
            return Err(error);
        }

        // Only REST-backed depth streams can use the automatic
        // REST replacement path implemented here.
        if self.depth_policy.is_none() {
            return Err(error);
        }

        let reset_required = self
            .runner
            .processor()
            .publication()
            .is_some_and(|publication| publication.phase() == DepthPublicationPhase::ResetRequired);

        if !reset_required {
            return Err(error);
        }

        // The downstream consumer must acknowledge invalidation.
        //
        // If this fails, do not continue processing or schedule
        // an ordinary REST retry.
        self.runner.reset_depth_publication(&mut *publish_depth)?;

        let invalidation_complete = self
            .runner
            .processor()
            .publication()
            .is_some_and(|publication| publication.reset_invalidation_complete());

        if !invalidation_complete {
            return Err(SourceExecutorError::RecoveryCompletion(
                "depth reset invalidation was not acknowledged".into(),
            ));
        }

        // Do not process another ordinary delta as a replacement.
        //
        // The retry timer will prepare a fresh REST attempt
        // on the same serialized source actor.
        self.schedule_depth_retry();

        Ok(())
    }
    /// Begin REST synchronization for the current trusted
    /// depth acquisition.
    ///
    /// This method runs on the executor's owning task.
    ///
    /// Integrity is prepared before the REST request is
    /// submitted, so WebSocket deltas can enter the existing
    /// synchronization buffer.
    async fn schedule_depth_snapshot(
        &mut self,
        epoch: SourceProcessingEpoch,
        publish_depth: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceExecutorError> {
        let Some(policy) = self.depth_policy.as_ref() else {
            // WebSocket-snapshot depth streams and trade streams
            // do not use this REST synchronization policy.
            return Ok(());
        };

        let SourceProcessingEpoch::Depth(depth_epoch) = epoch else {
            return Err(SourceExecutorError::RecoveryCompletion(
                "REST depth policy configured for a trade stream".into(),
            ));
        };

        let handle = self.recovery_handle.as_ref().ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion("REST depth recovery handle is missing".into())
        })?;

        let attempt = self.depth_recovery.as_mut().ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion("REST depth attempt owner is missing".into())
        })?;

        if attempt.has_pending() {
            return Ok(());
        }
        if self.next_depth_attempt > MAX_DEPTH_ATTEMPTS_PER_ACQUISITION {
            return Err(SourceExecutorError::RecoveryCompletion(
                "REST depth synchronization retry budget exhausted".into(),
            ));
        }
        // All values originate from the trusted acquisition
        // and the configured REST depth policy.
        let context = RestDepthRequestContext {
            source_epoch: depth_epoch,

            sync_token: DepthSyncToken {
                stream_generation: depth_epoch.stream_generation,
                attempt: self.next_depth_attempt,
            },

            endpoint_key: policy.endpoint_key.clone(),

            requested_limit: policy.requested_limit,
        };

        // Validate before mutating the integrity controller.
        context.validate(depth_epoch).map_err(|error| {
            SourceExecutorError::RecoveryCompletion(format!(
                "invalid depth recovery context: {error:?}"
            ))
        })?;

        let next = self.next_depth_attempt.checked_add(1).ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion(
                "REST synchronization attempt counter exhausted".into(),
            )
        })?;

        // The integrity coordinator owns the synchronization
        // buffer and invalidates any previous book.
        self.runner
            .prepare_rest_depth_recovery(context.clone(), publish_depth)?;

        // Reserve the attempt ID before asynchronous scheduling.
        // A failed request must never cause reuse of an attempt
        // already installed in the integrity controller.
        self.next_depth_attempt = next;

        // Schedules acquisition through the existing shared
        // recovery infrastructure. Does not await HTTP completion.
        attempt.schedule(handle, context).await?;

        Ok(())
    }
    /// Schedule REST depth reconciliation when integrity has
    /// lost synchronization and no REST request is outstanding.
    ///
    /// Runs only on the single-owner source executor.
    async fn recover_unsynchronized_depth(
        &mut self,
        publish_depth: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceExecutorError> {
        // WebSocket-snapshot streams do not require REST recovery.
        if self.depth_policy.is_none() {
            return Ok(());
        }

        let Some(SourceProcessingEpoch::Depth(epoch)) = self.runner.active_epoch() else {
            return Ok(());
        };

        // Never schedule another request while one is pending.
        if self
            .depth_recovery
            .as_ref()
            .is_some_and(SourceDepthAttempt::has_pending)
        {
            return Ok(());
        }

        // The integrity controller is authoritative about
        // whether the book is synchronized.
        let publication = self.runner.processor().publication();

        let resetting = publication
            .is_some_and(|publication| publication.phase() == DepthPublicationPhase::ResetRequired);

        if resetting {
            // The reset-specific REST path is available only after
            // downstream invalidation has been acknowledged.
            if !publication.is_some_and(|publication| publication.reset_invalidation_complete()) {
                return Err(SourceExecutorError::RecoveryCompletion(
                    "depth publication reset is not yet acknowledged".into(),
                ));
            }
        } else if self
            .runner
            .processor()
            .integrity()
            .status()
            .depth_synchronized
            == Some(true)
        {
            // Ordinary REST recovery is unnecessary while integrity
            // and publication remain synchronized.
            return Ok(());
        }

        // Publication uncertainty requires the explicit
        // reset protocol, not ordinary REST synchronization.
        if self
            .runner
            .processor()
            .publication()
            .is_some_and(|publication| publication.phase() == DepthPublicationPhase::ResetRequired)
        {
            return Err(SourceExecutorError::RecoveryCompletion(
                "depth publication reset required before REST recovery".into(),
            ));
        }
        if self.depth_retry_at.is_some() {
            return Ok(());
        }
        self.schedule_depth_snapshot(SourceProcessingEpoch::Depth(epoch), publish_depth)
            .await
    }
    /// Replace REST recovery ownership after a trusted
    /// acquisition transition.
    ///
    /// The old REST epoch is cancelled before the replacement
    /// becomes available for scheduling.
    ///
    /// Cancellation failure terminates the executor rather than
    /// silently allowing overlapping recovery authority.
    async fn rotate_depth_recovery(
        &mut self,
        epoch: SourceProcessingEpoch,
    ) -> Result<(), SourceExecutorError> {
        let Some(old_handle) = self.recovery_handle.clone() else {
            return Ok(());
        };

        let SourceProcessingEpoch::Depth(_) = epoch else {
            return Err(SourceExecutorError::RecoveryCompletion(
                "depth recovery configured for a trade stream".into(),
            ));
        };

        let next_epoch = epoch.recovery_epoch();

        // Construct the replacement before modifying ownership.
        let next_handle = old_handle
            .rebind_epoch(next_epoch)
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        // Revoke local completion admission first.
        if let Some(attempt) = self.depth_recovery.as_mut() {
            attempt.revoke_pending();
        }
        if let Some(attempt) = self.trade_recovery.as_mut() {
            attempt.revoke_pending();
        }
        // Cancellation is scoped to the old stream epoch.
        old_handle
            .cancel_epoch()
            .await
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        // Install fresh ownership only after cancellation succeeds.
        self.depth_recovery = Some(SourceDepthAttempt::new(self.runner.stream_id(), next_epoch));

        self.recovery_handle = Some(next_handle);

        Ok(())
    }
    /// Schedule another depth synchronization attempt.
    ///
    /// Replaces an existing deadline instead of accumulating
    /// multiple retry tasks.
    fn schedule_depth_retry(&mut self) {
        self.depth_retry_at = Some(Instant::now() + DEPTH_RETRY_DELAY);
    }
    /// Rebind trade recovery to an ACK-confirmed acquisition.
    ///
    /// All requests belonging to the previous epoch are
    /// locally revoked before remote cancellation.
    ///
    /// The new owner is installed only after old-epoch
    /// cancellation succeeds.
    async fn rotate_trade_recovery(
        &mut self,
        epoch: SourceProcessingEpoch,
    ) -> Result<(), SourceExecutorError> {
        let Some(old_handle) = self.trade_recovery_handle.clone() else {
            return Ok(());
        };
        self.completed_trade_recovery = None;
        let SourceProcessingEpoch::Trades(_) = epoch else {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade recovery configured for a depth stream".into(),
            ));
        };

        let next_epoch = epoch.recovery_epoch();

        let next_handle = old_handle
            .rebind_epoch(next_epoch)
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        // Revoke completion admission before awaiting
        // cancellation of the previous acquisition.
        if let Some(attempt) = self.trade_recovery.as_mut() {
            attempt.revoke_pending();
        }

        old_handle
            .cancel_epoch()
            .await
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        self.trade_recovery = Some(SourceTradeAttempt::new(self.runner.stream_id(), next_epoch));

        self.trade_recovery_handle = Some(next_handle);
        // Each ACK-confirmed acquisition has an independent
        // bounded recovery-attempt budget.
        self.next_trade_attempt = 1;
        Ok(())
    }
    /// Revoke and cancel outstanding trade REST work
    /// when the current acquisition is interrupted.
    ///
    /// This preserves the integrity coverage uncertainty.
    /// It does not clear a coverage hold or restore readiness.
    async fn cancel_trade_recovery(&mut self) -> Result<(), SourceExecutorError> {
        if let Some(attempt) = self.trade_recovery.as_mut() {
            attempt.revoke_pending();
        }

        if let Some(handle) = self.trade_recovery_handle.as_ref() {
            handle
                .cancel_epoch()
                .await
                .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;
        }

        Ok(())
    }
    /// Cancel a pending retry when acquisition ownership changes.
    fn clear_depth_retry(&mut self) {
        self.depth_retry_at = None;
    }
    /// Execute until explicit shutdown, channel closure,
    /// or a processing error.
    ///
    /// All callbacks run synchronously on the owning task.
    /// They must not return before their handoff is resolved.
    ///
    /// An error terminates this execution context. Recovery
    /// and retry decisions belong to the runtime supervisor.
    ///
    /// `bind` must obtain epochs from the trusted runtime.
    /// It must never derive them from exchange message JSON.
    pub async fn run(
        mut self,
        mut shutdown: oneshot::Receiver<()>,
        readiness_tx: watch::Sender<SourceReadiness>,
        mut publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
        mut publish_trades: impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<SourceExecutorExit, SourceExecutorError> {
        readiness_tx.send_replace(self.runner.readiness());
        let execution_result = loop {
            let retry_deadline = self.depth_retry_at;
            tokio::select! {
            biased;

            _ = &mut shutdown => {
                break Ok(SourceExecutorExit::ShutdownRequested);
            }
            _ = async {
                match retry_deadline {
                    Some(deadline) => {
                        tokio::time::sleep_until(deadline).await;
                    }

                    None => {
                        std::future::pending::<()>().await;
                    }
                }
            } => {
                self.clear_depth_retry();

                let result = self
                    .recover_unsynchronized_depth(&mut publish_depth)
                    .await;
                let result = match result {
                    Ok(()) => Ok(()),

                    Err(error) => self.handle_publication_failure(
                        error,
                        &mut publish_depth,
                    ),
                };

                readiness_tx.send_replace(self.runner.readiness());

                if let Err(error) = result {
                    break Err(error);
                }
            }
                input = self.attachment.recv() => {
                    let Some(input) = input else {
                        break Ok(SourceExecutorExit::TransportClosed);
                    };

                let result = self
                    .process_input(
                        input,
                        &mut publish_depth,
                        &mut publish_trades,
                    )
                    .await;

                    readiness_tx.send_replace(self.runner.readiness());

                    if let Err(error) = result {
                        break Err(error);
                    }
                }
                completed = async {
                    match self.depth_recovery.as_mut() {
                        Some(attempt) => attempt.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let result = match completed {
                        Ok(Some(completed)) => {
                            self.process_completed_depth_attempt(
                                completed,
                                &mut publish_depth,
                            )
                        }

                        Ok(None) => Ok(()),

                        Err(error) => {
                            Err(SourceExecutorError::DepthAttempt(error))
                        }
                    };

                    let result = match result {
                        Ok(()) => {
                            // A REST response can complete successfully
                            // without establishing a synchronized book.
                            //
                            // The integrity controller remains authoritative.
                            if self.depth_policy.is_some()
                                && self.runner.readiness() != SourceReadiness::Ready
                                && self
                                    .runner
                                    .processor()
                                    .integrity()
                                    .status()
                                    .depth_synchronized
                                    == Some(false)
                                && !self
                                    .runner
                                    .processor()
                                    .publication()
                                    .is_some_and(|publication| {
                                        publication.phase() == DepthPublicationPhase::ResetRequired
                                        })
                                {
                                    self.schedule_depth_retry();
                                }

                                Ok(())
                            }

                            Err(SourceExecutorError::DepthRecovery(
                                SourceDepthRecoveryError::Acquisition(_),
                            )) => {
                                // A failed HTTP acquisition is retryable.
                                //
                                // This does not include normalization failures,
                                // integrity failures, or publication handoff
                                // failures.
                                self.schedule_depth_retry();
                                Ok(())
                            }

                            Err(error) => Err(error),
                        };
                        let result = match result {
                            Ok(()) => Ok(()),

                            Err(error) => self.handle_publication_failure(
                                error,
                                &mut publish_depth,
                            ),
                        };
                        readiness_tx.send_replace(self.runner.readiness());

                        if let Err(error) = result {
                            break Err(error);
                        }
                    }

                    // ==========================================
                    // REST TRADE RECOVERY COMPLETION
                    // ==========================================
                    completed = async {
                        match self.trade_recovery.as_mut() {
                            Some(attempt) => attempt.recv().await,
                            None => std::future::pending().await,
                        }
                    }, if self.completed_trade_recovery.is_none() => {
                        let result = match completed {
                            Ok(Some(completed)) => {
                                self.process_completed_trade_attempt(completed)
                            }

                            Ok(None) => Ok(()),

                            Err(error) => {
                                Err(SourceExecutorError::TradeCompletionRejected(
                                    error.to_string(),
                                ))
                            }
                        };

                        readiness_tx.send_replace(self.runner.readiness());

                        if let Err(error) = result {
                            break Err(error);
                        }
                    }
                }
        };

        // ==========================================
        // REVOKE PENDING RECOVERY COMPLETIONS
        // ==========================================

        if let Some(attempt) = self.depth_recovery.as_mut() {
            attempt.revoke_pending();
        }

        if let Some(attempt) = self.trade_recovery.as_mut() {
            attempt.revoke_pending();
        }

        // A completed REST acquisition must never survive
        // shutdown of the source acquisition that authorized it.
        self.completed_trade_recovery = None;

        // Close local admission before final lifecycle work.
        self.attachment.close_input();
        self.gate.channel_closed();

        // Always attempt both cleanup operations.
        //
        // Lifecycle invalidation must not be skipped just
        // because transport detachment fails, or vice versa.
        let stop_result = self.runner.stop(&mut publish_depth);

        readiness_tx.send_replace(self.runner.readiness());

        // Cancel outstanding REST recovery for this acquisition.
        let recovery_cancel_result = match self.recovery_handle.as_ref() {
            Some(handle) => handle
                .cancel_epoch()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),

            None => Ok(()),
        };
        let trade_cancel_result = match self.trade_recovery_handle.as_ref() {
            Some(handle) => handle
                .cancel_epoch()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),

            None => Ok(()),
        };
        // Always attempt WebSocket detachment, even if
        // REST cancellation or integrity shutdown failed.
        let detach_result = self.attachment.detach().await;

        // Preserve the original processing failure.
        //
        // Cleanup failures are returned when execution
        // otherwise completed successfully.
        match execution_result {
            Err(error) => Err(error),

            Ok(exit) => {
                stop_result?;

                if let Err(error) = recovery_cancel_result {
                    return Err(SourceExecutorError::RecoveryCompletion(format!(
                        "REST recovery cancellation failed: {error}"
                    )));
                }
                if let Err(error) = trade_cancel_result {
                    return Err(SourceExecutorError::RecoveryCompletion(format!(
                        "REST trade recovery cancellation failed: {error}"
                    )));
                }
                detach_result?;

                Ok(exit)
            }
        }
    }

    async fn process_input(
        &mut self,
        input: StreamInput,
        publish_depth: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
        publish_trades: &mut impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<(), SourceExecutorError> {
        let event = self.gate.process(input)?;

        match event {
            SourceTransportEvent::Connected(acquisition) => {
                let is_reconnection = self.binder.last_acquisition().is_some();

                let trusted = self.binder.bind(&acquisition)?;

                self.runner
                    .connected(trusted.epoch, trusted.lifecycle, &mut *publish_depth)?;

                // The new integrity epoch is now authoritative.
                //
                // Cancel REST operations belonging to the previous
                // acquisition and install a newly bound recovery handle.
                self.clear_depth_retry();
                self.rotate_depth_recovery(trusted.epoch).await?;
                self.rotate_trade_recovery(trusted.epoch).await?;
                // A new ACK-confirmed depth acquisition must establish
                // its own authoritative snapshot before becoming Ready.
                self.schedule_depth_snapshot(trusted.epoch, publish_depth)
                    .await?;

                if is_reconnection {
                    self.runner.instance().metrics().record_reconnection();
                }
            }

            SourceTransportEvent::Disconnected(_) => {
                self.clear_depth_retry();
                self.completed_trade_recovery = None;
                // Immediately revoke ordinary source admission.
                // Any unresolved trade coverage remains unverified.
                self.runner.disconnected(publish_depth)?;

                // Retire REST work belonging to this acquisition.
                // Cancellation failure propagates instead of leaving
                // an apparently healthy recovery state.
                self.cancel_trade_recovery().await?;
            }

            SourceTransportEvent::Data { message, .. } => {
                let result = self.process_message(message, &mut *publish_depth, publish_trades);

                match result {
                    Ok(()) => {}

                    Err(error) => {
                        self.handle_depth_buffer_failure(error).await?;
                    }
                }

                self.recover_unsynchronized_depth(&mut *publish_depth)
                    .await?;

                // Trade integrity may have created a coverage hold while
                // processing this WebSocket message.
                //
                // Schedule one trusted REST acquisition without admitting
                // the held or buffered trades into ordinary publication.
                self.recover_held_trades().await?;
            }

            SourceTransportEvent::Ignored => {}
        }

        Ok(())
    }

    fn process_message(
        &mut self,
        message: tokio_tungstenite::tungstenite::Message,
        publish_depth: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
        publish_trades: &mut impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<(), SourceExecutorError> {
        use tokio_tungstenite::tungstenite::Message;

        let started = StdInstant::now();

        let (payload, accounted_bytes) = match message {
            Message::Text(text) => {
                let size = text.len();

                let parsed = serde_json::from_str::<serde_json::Value>(text.as_ref());

                match parsed {
                    Ok(payload) => (payload, size),

                    Err(error) => {
                        let metrics = self.runner.instance().metrics();

                        metrics.record_normalization_error();

                        metrics.record_processing_latency(started.elapsed());

                        return Err(SourceExecutorError::Json(error));
                    }
                }
            }

            Message::Binary(bytes) => {
                let size = bytes.len();

                let parsed = serde_json::from_slice::<serde_json::Value>(bytes.as_ref());

                match parsed {
                    Ok(payload) => (payload, size),

                    Err(error) => {
                        let metrics = self.runner.instance().metrics();

                        metrics.record_normalization_error();

                        metrics.record_processing_latency(started.elapsed());

                        return Err(SourceExecutorError::Json(error));
                    }
                }
            }

            // Control frames are not canonical market data.
            _ => return Ok(()),
        };

        let received_at_ns = system_time_ns();

        {
            let metrics = self.runner.instance().metrics();

            metrics.record_message_received(received_at_ns);
        }

        let reset_buffering = self.depth_policy.is_some()
            && self
                .runner
                .processor()
                .publication()
                .is_some_and(|publication| {
                    publication.phase() == DepthPublicationPhase::ResetRequired
                        && publication.reset_invalidation_complete()
                });

        let result = if reset_buffering {
            self.runner
                .data_during_depth_reset(&payload, accounted_bytes)
        } else {
            self.runner
                .data(&payload, accounted_bytes, publish_depth, publish_trades)
        };

        {
            let metrics = self.runner.instance().metrics();

            if matches!(
                &result,
                Err(SourceRunnerError::Processor(
                    SourceProcessorError::Normalization(_)
                ))
            ) {
                metrics.record_normalization_error();
            }

            metrics.record_processing_latency(started.elapsed());
        }

        result?;

        Ok(())
    }
    /// Complete one already-validated REST depth attempt.
    ///
    /// The REST result is processed on the source actor,
    /// serialized with WebSocket depth messages.
    fn process_completed_depth_attempt(
        &mut self,
        completed: CompletedDepthAttempt,
        publish_depth: &mut impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
    ) -> Result<(), SourceExecutorError> {
        let restored = complete_depth_recovery(
            &mut self.runner,
            completed.completion,
            &completed.context,
            publish_depth,
        )?;

        // `restored == false` is not a successful synchronization.
        // The existing integrity/readiness state remains authoritative.
        if !restored {
            return Ok(());
        }

        debug_assert_eq!(
            self.runner.readiness(),
            SourceReadiness::Ready,
            "accepted and published depth snapshot should establish readiness"
        );

        Ok(())
    }
    /// Retire a failed REST synchronization attempt.
    ///
    /// The old request is cancelled before another attempt
    /// is allowed to begin.
    ///
    /// This operation runs on the source executor actor.
    async fn handle_depth_buffer_failure(
        &mut self,
        error: SourceExecutorError,
    ) -> Result<(), SourceExecutorError> {
        let needs_restart = matches!(
            &error,
            SourceExecutorError::Runner(SourceRunnerError::Processor(
                SourceProcessorError::DepthBufferRecoveryRequired
            ))
        );

        if !needs_restart {
            return Err(error);
        }

        // This recovery path is valid only for a REST-backed
        // depth stream.
        if self.depth_policy.is_none() {
            return Err(error);
        }

        let handle = self.recovery_handle.as_ref().ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion("REST recovery handle is missing".into())
        })?;

        let attempt = self.depth_recovery.as_mut().ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion("REST depth attempt owner is missing".into())
        })?;

        // Cancel the outstanding HTTP request and revoke its
        // completion receiver.
        //
        // Cancellation failure is terminal: the old request
        // must not race with a replacement synchronization.
        attempt.cancel_pending(handle).await?;

        // The next attempt receives a new DepthSyncToken.
        // No WebSocket delta is published during this interval.
        self.schedule_depth_retry();

        Ok(())
    }
    /// Schedule a trade REST acquisition when Integrity has
    /// suspended ordinary trade admission for coverage review.
    ///
    /// A successful request is only an acquisition.
    /// No recovered data is committed or published here.
    async fn recover_held_trades(&mut self) -> Result<(), SourceExecutorError> {
        // Trade recovery is optional and independently configured.
        let Some(policy) = self.trade_policy.as_ref() else {
            return Ok(());
        };

        let Some(SourceProcessingEpoch::Trades(epoch)) = self.runner.active_epoch() else {
            return Ok(());
        };

        // Do not schedule another request while one is pending.
        if self
            .trade_recovery
            .as_ref()
            .is_some_and(SourceTradeAttempt::has_pending)
        {
            return Ok(());
        }

        // Capture the authoritative review only while Integrity
        // has an unresolved coverage hold.
        let review = match self.runner.pending_trade_recovery() {
            Ok(review) => review,
            Err(_)
                if self
                    .runner
                    .processor()
                    .integrity()
                    .trade_coverage_hold()
                    .is_none() =>
            {
                return Ok(());
            }
            Err(error) => {
                return Err(SourceExecutorError::RecoveryCompletion(error.to_string()));
            }
        };

        if !review.is_current(self.runner.processor().integrity()) {
            return Err(SourceExecutorError::RecoveryCompletion(
                "trade coverage review changed before scheduling".into(),
            ));
        }

        if self.next_trade_attempt > MAX_TRADE_ATTEMPTS_PER_ACQUISITION {
            return Err(SourceExecutorError::RecoveryCompletion(
                "REST trade recovery attempt budget exhausted".into(),
            ));
        }

        // The operation is selected by the installed exchange
        // policy, never inferred from untrusted trade payloads.
        let operation = policy
            .operation_for_review(self.runner.stream_id(), &review)
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        let handle = self.trade_recovery_handle.as_ref().ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion("trade recovery handle is missing".into())
        })?;

        let attempt = self.trade_recovery.as_mut().ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion(
                "trade recovery attempt owner is missing".into(),
            )
        })?;

        let attempt_id = self.next_trade_attempt;

        let next = attempt_id.checked_add(1).ok_or_else(|| {
            SourceExecutorError::RecoveryCompletion(
                "trade recovery attempt counter exhausted".into(),
            )
        })?;

        let request = policy
            .request_for_source(self.runner.instance().request(), &review)
            .map_err(|error| SourceExecutorError::RecoveryCompletion(error.to_string()))?;

        let context = SourceTradeAttemptContext {
            epoch,
            attempt: crate::live::recovery::RecoveryAttemptId(attempt_id),
            review,
            operation,
            request,
        };

        // Reserve the ID before an asynchronous operation.
        // An attempted request must not reuse its identity.
        self.next_trade_attempt = next;

        attempt
            .schedule(handle, context)
            .await
            .map_err(|error| SourceExecutorError::RecoveryRequest(error.to_string()))?;

        Ok(())
    }
    /// Normalize and preflight one authorized REST trade completion.
    ///
    /// The recovery mailbox and source-level completion checks
    /// must have validated the acquisition before this method
    /// is invoked.
    ///
    /// This method:
    /// - Validates the original REST request binding.
    /// - Normalizes the complete REST response.
    /// - Reconciles REST and buffered live trades.
    /// - Validates the candidate against authoritative Integrity.
    ///
    /// It does NOT:
    /// - Commit trade identities.
    /// - Clear the coverage hold.
    /// - Publish recovered trades.
    /// - Establish global historical completeness.
    fn preflight_completed_trade_attempt(
        &self,
        completed: &CompletedTradeAttempt,
    ) -> Result<(), SourceExecutorError> {
        // --------------------------------------------------------
        // REST RESPONSE
        // --------------------------------------------------------

        let response = validate_trade_response(completed, "binance_spot")?;

        // --------------------------------------------------------
        // REST NORMALIZATION
        // --------------------------------------------------------

        // The request is the original logical request retained
        // when the REST acquisition was scheduled.
        //
        // Do not construct a new request from response contents.
        let normalized = self
            .runner
            .instance()
            .normalizer()
            .normalize_rest_trades(response, &completed.context.request)
            .map_err(|error| {
                SourceExecutorError::TradeCompletionRejected(format!(
                    "REST trade normalization failed: {error}"
                ))
            })?;

        let NormalizedMessage::Trades(trades) = normalized else {
            return Err(SourceExecutorError::TradeCompletionRejected(
                "REST trade normalization returned the wrong channel".into(),
            ));
        };

        // --------------------------------------------------------
        // CURRENT RECOVERY AUTHORITY
        // --------------------------------------------------------

        let context = &completed.context;

        let review = self.runner.pending_trade_recovery().map_err(|error| {
            SourceExecutorError::TradeCompletionRejected(format!(
                "trade coverage review unavailable: {error}"
            ))
        })?;

        if !review.is_current(self.runner.processor().integrity()) {
            return Err(SourceExecutorError::TradeCompletionRejected(
                "trade coverage review is no longer authoritative".into(),
            ));
        }

        // --------------------------------------------------------
        // RECONCILIATION AND INTEGRITY PREFLIGHT
        // --------------------------------------------------------

        // This operation combines:
        //
        // 1. REST-recovered trades.
        // 2. The original triggering batch retained by Integrity.
        // 3. Subsequent WebSocket batches retained by Source.
        //
        // It then performs the controller's authoritative
        // generation-fenced numeric recovery preflight.
        let prepared = self.runner.preflight_numeric_trade_recovery(
            context.epoch,
            &context.review,
            &trades,
            MAX_TRADE_RECONCILIATION_RECORDS,
        )?;

        // Successful preflight is necessary but insufficient
        // for publication or recovery completion.
        debug_assert!(prepared.record_count() > 0);

        Ok(())
    }
    /// Accept a REST trade completion for further reconciliation.
    ///
    /// The recovery mailbox has already validated request ownership.
    /// This method additionally validates current source authority.
    ///
    /// It deliberately does not clear the coverage hold.
    fn process_completed_trade_attempt(
        &mut self,
        completed: CompletedTradeAttempt,
    ) -> Result<(), SourceExecutorError> {
        if self.completed_trade_recovery.is_some() {
            return Err(SourceExecutorError::TradeCompletionRejected(
                "another completed trade recovery is awaiting reconciliation".into(),
            ));
        }

        let Some(SourceProcessingEpoch::Trades(epoch)) = self.runner.active_epoch() else {
            return Err(SourceTradeCompletionError::ObsoleteEpoch.into());
        };

        let review = self
            .runner
            .pending_trade_recovery()
            .map_err(|error| SourceExecutorError::TradeCompletionRejected(error.to_string()))?;

        if !review.is_current(self.runner.processor().integrity()) {
            return Err(SourceTradeCompletionError::ObsoleteReview.into());
        }

        validate_trade_completion(&completed, self.runner.stream_id(), epoch, &review)?;
        // The current automatic trade policy supports Binance Spot.
        //
        // Validate the REST acquisition envelope before retaining
        // the result for normalization and reconciliation.
        let policy = self.trade_policy.as_ref().ok_or_else(|| {
            SourceExecutorError::TradeCompletionRejected("trade recovery policy is missing".into())
        })?;

        let expected_policy =
            SourceTradeRecoveryPolicy::for_source(self.runner.instance().request())
                .map_err(|error| SourceExecutorError::TradeCompletionRejected(error.to_string()))?;

        if policy != &expected_policy {
            return Err(SourceExecutorError::TradeCompletionRejected(
                "trade recovery policy changed during acquisition".into(),
            ));
        }

        validate_trade_response(&completed, "binance_spot")?;
        // Validate the REST acquisition envelope.
        validate_trade_response(&completed, "binance_spot")?;

        // Normalize REST trades and verify the combined
        // recovery candidate against authoritative Integrity.
        //
        // No identities are committed and no trades are published.
        self.preflight_completed_trade_attempt(&completed)?;

        // Retain the original completed acquisition.
        //
        // Future recovery authorization must revalidate
        // against the final live buffer before commit.
        self.completed_trade_recovery = Some(completed);

        Ok(())
    }
}

fn system_time_ns() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };

    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::metrics::SourceMetrics;

    #[test]
    fn native_message_metrics_are_independent() {
        let metrics = SourceMetrics::new();

        metrics.record_message_received(1_000);
        metrics.record_normalization_error();

        assert_eq!(metrics.messages_received_total(), 1,);

        assert_eq!(metrics.normalization_errors_total(), 1,);

        assert_eq!(metrics.events_produced_total(), 0,);
    }

    #[test]
    fn processing_latency_is_recorded() {
        let metrics = SourceMetrics::new();

        metrics.record_processing_latency_ns(250);

        assert_eq!(metrics.last_processing_latency_ns(), Some(250),);
    }

    #[test]
    fn initial_connection_is_not_a_reconnection() {
        let metrics = SourceMetrics::new();

        assert_eq!(metrics.reconnections_total(), 0,);

        metrics.record_reconnection();

        assert_eq!(metrics.reconnections_total(), 1,);
    }

    #[test]
    fn system_timestamp_is_positive() {
        assert!(system_time_ns() > 0);
    }
    #[test]
    fn handoff_failure_is_not_a_rest_acquisition_failure() {
        let handoff = SourceExecutorError::Runner(SourceRunnerError::Processor(
            SourceProcessorError::Handoff("downstream unavailable".into()),
        ));

        assert!(matches!(
            handoff,
            SourceExecutorError::Runner(SourceRunnerError::Processor(
                SourceProcessorError::Handoff(_)
            ))
        ));

        let rest_handoff = SourceExecutorError::DepthRecovery(SourceDepthRecoveryError::Runner(
            SourceRunnerError::Processor(SourceProcessorError::Handoff(
                "snapshot publication unavailable".into(),
            )),
        ));

        assert!(matches!(
            rest_handoff,
            SourceExecutorError::DepthRecovery(SourceDepthRecoveryError::Runner(
                SourceRunnerError::Processor(SourceProcessorError::Handoff(_))
            ))
        ));
    }
    #[test]
    fn failed_rest_buffering_is_distinct_from_publication_failure() {
        let error = SourceExecutorError::Runner(SourceRunnerError::Processor(
            SourceProcessorError::DepthBufferRecoveryRequired,
        ));

        assert!(matches!(
            error,
            SourceExecutorError::Runner(SourceRunnerError::Processor(
                SourceProcessorError::DepthBufferRecoveryRequired
            ))
        ));
    }
}
