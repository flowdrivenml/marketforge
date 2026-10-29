use thiserror::Error;

use crate::live::{
    integrity::RestDepthRequestContext,
    recovery::{
        RecoveryAttemptId, RecoveryCompletion, RecoveryRequestId, RecoverySourceEpoch,
        RecoveryTicket, StreamRecoveryError, StreamRecoveryHandle,
    },
};

use super::{SourceRecoveryEvent, SourceRecoveryMailbox, SourceRecoveryMailboxError};

#[derive(Debug, Error)]
pub enum SourceDepthAttemptError {
    #[error("depth recovery context belongs to another source epoch")]
    WrongEpoch,

    #[error("depth synchronization token does not match the REST attempt")]
    WrongAttempt,

    #[error("depth recovery already has an outstanding request")]
    AlreadyPending,

    #[error(transparent)]
    Mailbox(#[from] SourceRecoveryMailboxError),

    #[error("REST handle belongs to another stream")]
    WrongStream,

    #[error("REST handle belongs to another acquisition")]
    WrongRecoveryBinding,

    #[error("invalid requested depth")]
    InvalidDepth,

    #[error(transparent)]
    Scheduling(#[from] StreamRecoveryError),
}

/// A REST completion paired with the exact synchronization
/// context that authorized its acquisition.
///
/// This is not yet an accepted integrity snapshot.
pub struct CompletedDepthAttempt {
    pub context: RestDepthRequestContext,
    pub completion: RecoveryCompletion,
}

/// One outstanding REST depth attempt per source actor.
///
/// The mailbox owns the completion receiver.
///
/// This structure owns its associated integrity context.
///
/// Neither component owns the shared REST scheduler.
pub struct SourceDepthAttempt {
    mailbox: SourceRecoveryMailbox,
    context: Option<RestDepthRequestContext>,
}

impl SourceDepthAttempt {
    pub fn new(stream_id: impl Into<String>, epoch: RecoverySourceEpoch) -> Self {
        Self {
            mailbox: SourceRecoveryMailbox::new(stream_id, epoch),
            context: None,
        }
    }

    pub fn has_pending(&self) -> bool {
        self.mailbox.has_pending()
    }

    pub fn epoch(&self) -> RecoverySourceEpoch {
        self.mailbox.epoch()
    }

    /// Install the ticket and its original trusted context
    /// as one logical operation.
    ///
    /// Context validation happens before mailbox mutation.
    pub fn install(
        &mut self,
        ticket: RecoveryTicket,
        context: RestDepthRequestContext,
        attempt: RecoveryAttemptId,
    ) -> Result<(), SourceDepthAttemptError> {
        if self.has_pending() {
            return Err(SourceDepthAttemptError::AlreadyPending);
        }

        let expected_epoch = RecoverySourceEpoch {
            stream_generation: context.source_epoch.stream_generation,
            connection_generation: context.source_epoch.connection_generation,
            subscription_generation: context.source_epoch.subscription_generation,
        };

        if expected_epoch != self.mailbox.epoch()
            || context.sync_token.stream_generation != context.source_epoch.stream_generation
        {
            return Err(SourceDepthAttemptError::WrongEpoch);
        }

        if attempt.0 == 0 || context.sync_token.attempt != attempt.0 {
            return Err(SourceDepthAttemptError::WrongAttempt);
        }

        self.mailbox.install(ticket, expected_epoch, attempt)?;

        self.context = Some(context);

        Ok(())
    }
    fn validate_request(
        &self,
        handle: &StreamRecoveryHandle,
        context: &RestDepthRequestContext,
    ) -> Result<RecoveryAttemptId, SourceDepthAttemptError> {
        if self.has_pending() {
            return Err(SourceDepthAttemptError::AlreadyPending);
        }

        if handle.binding().stream_id != self.mailbox.stream_id() {
            return Err(SourceDepthAttemptError::WrongStream);
        }

        if handle.binding().epoch != self.mailbox.epoch() {
            return Err(SourceDepthAttemptError::WrongRecoveryBinding);
        }

        let epoch = RecoverySourceEpoch {
            stream_generation: context.source_epoch.stream_generation,
            connection_generation: context.source_epoch.connection_generation,
            subscription_generation: context.source_epoch.subscription_generation,
        };

        if epoch != self.mailbox.epoch() {
            return Err(SourceDepthAttemptError::WrongEpoch);
        }

        if context.sync_token.stream_generation != context.source_epoch.stream_generation {
            return Err(SourceDepthAttemptError::WrongEpoch);
        }

        let attempt = RecoveryAttemptId(context.sync_token.attempt);

        if attempt.0 == 0 {
            return Err(SourceDepthAttemptError::WrongAttempt);
        }

        if context.requested_limit == 0 {
            return Err(SourceDepthAttemptError::InvalidDepth);
        }

        Ok(attempt)
    }
    /// Schedule an authorized REST depth snapshot.
    ///
    /// The original integrity synchronization context is retained
    /// alongside the returned recovery ticket.
    ///
    /// This method schedules acquisition through the shared
    /// RecoveryCoordinator. It does not wait for the HTTP
    /// response itself.
    ///
    /// The source actor must already have prepared the matching
    /// integrity recovery attempt.
    pub async fn schedule(
        &mut self,
        handle: &StreamRecoveryHandle,
        context: RestDepthRequestContext,
    ) -> Result<(), SourceDepthAttemptError> {
        let attempt = self.validate_request(handle, &context)?;

        let requested_depth = u64::try_from(context.requested_limit)
            .map_err(|_| SourceDepthAttemptError::InvalidDepth)?;

        let ticket = handle
            .request_depth_snapshot(attempt, Some(requested_depth))
            .await?;

        let request_id = ticket.request_id;

        // Installation must preserve the original context.
        //
        // If local installation fails after scheduling succeeds,
        // explicitly cancel the newly scheduled REST operation.
        if let Err(error) = self.install(ticket, context, attempt) {
            handle.cancel(attempt, request_id).await?;

            return Err(error);
        }

        Ok(())
    }

    /// Cancel the currently outstanding REST attempt.
    ///
    /// Local ownership is revoked before awaiting cancellation,
    /// preventing the stopped source from accepting its result.
    ///
    /// This method does not silently ignore cancellation failures.
    pub async fn cancel_pending(
        &mut self,
        handle: &StreamRecoveryHandle,
    ) -> Result<(), SourceDepthAttemptError> {
        if handle.binding().stream_id != self.mailbox.stream_id() {
            return Err(SourceDepthAttemptError::WrongStream);
        }

        if handle.binding().epoch != self.mailbox.epoch() {
            return Err(SourceDepthAttemptError::WrongRecoveryBinding);
        }

        let Some(context) = self.context.as_ref() else {
            return Ok(());
        };

        let attempt = RecoveryAttemptId(context.sync_token.attempt);

        let Some(request_id) = self.revoke_pending() else {
            return Ok(());
        };

        handle.cancel(attempt, request_id).await?;

        Ok(())
    }
    /// Receive a completed REST operation.
    ///
    /// The mailbox validates requester identity and attempt.
    ///
    /// An obsolete result does not receive an integrity context
    /// and cannot be applied to the depth controller.
    pub async fn recv(&mut self) -> Result<Option<CompletedDepthAttempt>, SourceDepthAttemptError> {
        let result = self.mailbox.recv().await;

        match result {
            Ok(SourceRecoveryEvent::Accepted(completion)) => {
                let context = self
                    .context
                    .take()
                    .expect("pending depth attempt owns its context");

                Ok(Some(CompletedDepthAttempt {
                    context,
                    completion,
                }))
            }

            Ok(SourceRecoveryEvent::Obsolete) => {
                self.context = None;
                Ok(None)
            }

            Err(error) => {
                self.context = None;
                Err(SourceDepthAttemptError::Mailbox(error))
            }
        }
    }

    /// Revoke the locally pending attempt.
    ///
    /// The returned request ID must be cancelled through
    /// the corresponding StreamRecoveryHandle.
    ///
    /// Dropping the mailbox receiver is not HTTP cancellation.
    pub fn revoke_pending(&mut self) -> Option<RecoveryRequestId> {
        self.context = None;
        self.mailbox.revoke_pending()
    }

    /// Revoke an obsolete source epoch.
    ///
    /// Return the previous request ID so the executor can
    /// explicitly cancel its outstanding REST operation.
    pub fn replace_epoch(&mut self, epoch: RecoverySourceEpoch) -> Option<RecoveryRequestId> {
        if self.mailbox.epoch() == epoch {
            return None;
        }

        let old_request = self.revoke_pending();
        self.mailbox.replace_epoch(epoch);

        old_request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::oneshot;

    use crate::live::{
        integrity::{DepthSourceEpoch, DepthSyncToken},
        recovery::RecoveryJobId,
    };

    fn epoch(generation: u64) -> RecoverySourceEpoch {
        RecoverySourceEpoch {
            stream_generation: generation,
            connection_generation: generation,
            subscription_generation: generation,
        }
    }

    fn context(generation: u64, attempt: u64) -> RestDepthRequestContext {
        RestDepthRequestContext {
            source_epoch: DepthSourceEpoch {
                stream_generation: generation,
                connection_generation: generation,
                subscription_generation: generation,
            },
            sync_token: DepthSyncToken {
                stream_generation: generation,
                attempt,
            },
            endpoint_key: "depth_snapshot".into(),
            requested_limit: 200,
        }
    }

    fn ticket(id: u64) -> RecoveryTicket {
        let (_tx, rx) = oneshot::channel();

        RecoveryTicket {
            request_id: RecoveryRequestId(id),
            job_id: RecoveryJobId(id),
            completion: rx,
        }
    }

    #[test]
    fn preserves_exact_depth_context() {
        let mut attempt = SourceDepthAttempt::new("btc-depth", epoch(10));

        attempt
            .install(ticket(1), context(10, 1), RecoveryAttemptId(1))
            .unwrap();

        assert!(attempt.has_pending());

        assert_eq!(attempt.revoke_pending(), Some(RecoveryRequestId(1)),);

        assert!(!attempt.has_pending());
    }

    #[test]
    fn rejects_context_from_another_epoch() {
        let mut attempt = SourceDepthAttempt::new("btc-depth", epoch(10));

        let result = attempt.install(ticket(1), context(11, 1), RecoveryAttemptId(1));

        assert!(matches!(result, Err(SourceDepthAttemptError::WrongEpoch)));

        assert!(!attempt.has_pending());
    }

    #[test]
    fn rejects_mismatched_sync_attempt() {
        let mut attempt = SourceDepthAttempt::new("btc-depth", epoch(10));

        let result = attempt.install(ticket(1), context(10, 2), RecoveryAttemptId(1));

        assert!(matches!(result, Err(SourceDepthAttemptError::WrongAttempt)));

        assert!(!attempt.has_pending());
    }

    #[test]
    fn reconnect_revokes_pending_rest_result() {
        let mut attempt = SourceDepthAttempt::new("btc-depth", epoch(10));

        attempt
            .install(ticket(7), context(10, 1), RecoveryAttemptId(1))
            .unwrap();

        let revoked = attempt.replace_epoch(epoch(11));

        assert_eq!(revoked, Some(RecoveryRequestId(7)),);

        assert!(!attempt.has_pending());
        assert_eq!(attempt.epoch(), epoch(11));
    }

    #[test]
    fn duplicate_pending_attempt_is_rejected() {
        let mut attempt = SourceDepthAttempt::new("btc-depth", epoch(10));

        attempt
            .install(ticket(1), context(10, 1), RecoveryAttemptId(1))
            .unwrap();

        let result = attempt.install(ticket(2), context(10, 2), RecoveryAttemptId(2));

        assert!(matches!(
            result,
            Err(SourceDepthAttemptError::AlreadyPending)
        ));
    }
}
