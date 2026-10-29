use thiserror::Error;
use tokio::sync::oneshot;

use crate::live::recovery::{
    RecoveryAttemptId, RecoveryCompletion, RecoveryRequestId, RecoverySourceEpoch, RecoveryTicket,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceRecoveryMailboxError {
    #[error("a REST recovery request is already outstanding")]
    AlreadyPending,

    #[error("recovery request belongs to an obsolete source epoch")]
    ObsoleteEpoch,

    #[error("recovery attempt is obsolete")]
    ObsoleteAttempt,

    #[error("recovery completion receiver closed")]
    CompletionClosed,
}

/// Result of collecting a REST recovery completion.
///
/// Accepted means the response belongs to the currently
/// authorized source epoch and recovery attempt.
///
/// It does NOT mean the REST response is valid market data
/// or that depth synchronization has succeeded.
#[derive(Debug)]
pub enum SourceRecoveryEvent {
    Accepted(RecoveryCompletion),

    /// Completion arrived after its ownership was revoked.
    Obsolete,
}

struct PendingRecovery {
    request_id: RecoveryRequestId,
    epoch: RecoverySourceEpoch,
    attempt: RecoveryAttemptId,
    completion: oneshot::Receiver<RecoveryCompletion>,
}

/// One outstanding REST recovery operation per source actor.
///
/// This component owns only completion delivery and
/// request-identity validation.
///
/// The shared RecoveryCoordinator continues to own REST
/// execution, scheduling, retries, and cancellation.
///
/// Integrity and publication must be modified exclusively
/// by the source executor after receiving Accepted.
pub struct SourceRecoveryMailbox {
    stream_id: String,
    epoch: RecoverySourceEpoch,
    highest_attempt: u64,
    pending: Option<PendingRecovery>,
}

impl SourceRecoveryMailbox {
    pub fn new(stream_id: impl Into<String>, epoch: RecoverySourceEpoch) -> Self {
        Self {
            stream_id: stream_id.into(),
            epoch,
            highest_attempt: 0,
            pending: None,
        }
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn epoch(&self) -> RecoverySourceEpoch {
        self.epoch
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Install a ticket returned by the shared recovery
    /// coordinator.
    ///
    /// The caller supplies the trusted requester identity
    /// associated with the original request.
    pub fn install(
        &mut self,
        ticket: RecoveryTicket,
        epoch: RecoverySourceEpoch,
        attempt: RecoveryAttemptId,
    ) -> Result<(), SourceRecoveryMailboxError> {
        if self.pending.is_some() {
            return Err(SourceRecoveryMailboxError::AlreadyPending);
        }

        if epoch != self.epoch {
            return Err(SourceRecoveryMailboxError::ObsoleteEpoch);
        }

        if attempt.0 == 0 || attempt.0 <= self.highest_attempt {
            return Err(SourceRecoveryMailboxError::ObsoleteAttempt);
        }

        self.highest_attempt = attempt.0;

        self.pending = Some(PendingRecovery {
            request_id: ticket.request_id,
            epoch,
            attempt,
            completion: ticket.completion,
        });

        Ok(())
    }

    /// Revoke recovery ownership when a new trusted source
    /// acquisition is established.
    ///
    /// Dropping the receiver does not necessarily cancel an
    /// HTTP operation already executing in RecoveryCoordinator.
    ///
    /// The runtime must separately cancel the previous
    /// request or epoch through StreamRecoveryHandle.
    pub fn replace_epoch(&mut self, epoch: RecoverySourceEpoch) {
        if epoch == self.epoch {
            return;
        }

        self.pending = None;
        self.epoch = epoch;
        self.highest_attempt = 0;
    }

    /// Remove the locally outstanding completion receiver.
    ///
    /// Returns its request ID so the owning executor can
    /// coordinate explicit cancellation upstream.
    pub fn revoke_pending(&mut self) -> Option<RecoveryRequestId> {
        self.pending.take().map(|pending| pending.request_id)
    }

    /// Await the currently outstanding recovery result.
    ///
    /// When no request exists, remain pending until this
    /// future is cancelled by the surrounding select! loop.
    ///
    /// No integrity mutation occurs here.
    pub async fn recv(&mut self) -> Result<SourceRecoveryEvent, SourceRecoveryMailboxError> {
        let Some(pending) = self.pending.as_mut() else {
            return std::future::pending().await;
        };

        let result = (&mut pending.completion).await;

        // Take ownership only after the receiver completes.
        let pending = self
            .pending
            .take()
            .expect("completion receiver belonged to pending recovery");

        let completion = result.map_err(|_| SourceRecoveryMailboxError::CompletionClosed)?;

        if completion.request_id != pending.request_id
            || completion.requester.stream_id != self.stream_id
            || completion.requester.epoch != pending.epoch
            || completion.requester.attempt != pending.attempt
            || self.epoch != pending.epoch
        {
            return Ok(SourceRecoveryEvent::Obsolete);
        }

        Ok(SourceRecoveryEvent::Accepted(completion))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::live::recovery::{RecoveryExecutionError, RecoveryJobId, RecoveryRequester};

    fn epoch(generation: u64) -> RecoverySourceEpoch {
        RecoverySourceEpoch {
            stream_generation: generation,
            connection_generation: generation,
            subscription_generation: generation,
        }
    }

    fn ticket(request_id: u64) -> (RecoveryTicket, oneshot::Sender<RecoveryCompletion>) {
        let (tx, rx) = oneshot::channel();

        (
            RecoveryTicket {
                request_id: RecoveryRequestId(request_id),
                job_id: RecoveryJobId(request_id),
                completion: rx,
            },
            tx,
        )
    }

    fn completion(
        stream_id: &str,
        source_epoch: RecoverySourceEpoch,
        attempt: u64,
        request_id: u64,
    ) -> RecoveryCompletion {
        RecoveryCompletion {
            request_id: RecoveryRequestId(request_id),
            job_id: RecoveryJobId(request_id),
            requester: RecoveryRequester {
                stream_id: stream_id.into(),
                epoch: source_epoch,
                attempt: RecoveryAttemptId(attempt),
            },
            result: Err(Arc::new(RecoveryExecutionError::Cancelled)),
        }
    }

    #[tokio::test]
    async fn matching_completion_is_accepted() {
        let mut mailbox = SourceRecoveryMailbox::new("btc-depth", epoch(10));

        let (ticket, tx) = ticket(5);

        mailbox
            .install(ticket, epoch(10), RecoveryAttemptId(1))
            .unwrap();

        tx.send(completion("btc-depth", epoch(10), 1, 5)).unwrap();

        let result = mailbox.recv().await.unwrap();

        assert!(matches!(result, SourceRecoveryEvent::Accepted(_)));

        assert!(!mailbox.has_pending());
    }

    #[tokio::test]
    async fn wrong_requester_is_rejected() {
        let mut mailbox = SourceRecoveryMailbox::new("btc-depth", epoch(10));

        let (ticket, tx) = ticket(5);

        mailbox
            .install(ticket, epoch(10), RecoveryAttemptId(1))
            .unwrap();

        tx.send(completion("eth-depth", epoch(10), 1, 5)).unwrap();

        assert!(matches!(
            mailbox.recv().await.unwrap(),
            SourceRecoveryEvent::Obsolete
        ));
    }

    #[test]
    fn duplicate_pending_request_is_rejected() {
        let mut mailbox = SourceRecoveryMailbox::new("btc-depth", epoch(10));

        let (first, _first_tx) = ticket(1);
        let (second, _second_tx) = ticket(2);

        mailbox
            .install(first, epoch(10), RecoveryAttemptId(1))
            .unwrap();

        assert!(matches!(
            mailbox.install(second, epoch(10), RecoveryAttemptId(2),),
            Err(SourceRecoveryMailboxError::AlreadyPending)
        ));
    }

    #[test]
    fn obsolete_attempt_cannot_be_reinstalled() {
        let mut mailbox = SourceRecoveryMailbox::new("btc-depth", epoch(10));

        let (first, _first_tx) = ticket(1);

        mailbox
            .install(first, epoch(10), RecoveryAttemptId(3))
            .unwrap();

        mailbox.revoke_pending();

        let (obsolete, _obsolete_tx) = ticket(2);

        assert!(matches!(
            mailbox.install(obsolete, epoch(10), RecoveryAttemptId(2),),
            Err(SourceRecoveryMailboxError::ObsoleteAttempt)
        ));
    }

    #[test]
    fn epoch_replacement_revokes_outstanding_request() {
        let mut mailbox = SourceRecoveryMailbox::new("btc-depth", epoch(10));

        let (first, _first_tx) = ticket(1);

        mailbox
            .install(first, epoch(10), RecoveryAttemptId(1))
            .unwrap();

        mailbox.replace_epoch(epoch(11));

        assert!(!mailbox.has_pending());
        assert_eq!(mailbox.epoch(), epoch(11));

        let (second, _second_tx) = ticket(2);

        mailbox
            .install(second, epoch(11), RecoveryAttemptId(1))
            .unwrap();

        assert!(mailbox.has_pending());
    }

    #[tokio::test]
    async fn dropped_completion_sender_is_an_explicit_error() {
        let mut mailbox = SourceRecoveryMailbox::new("btc-depth", epoch(10));

        let (ticket, tx) = ticket(1);

        mailbox
            .install(ticket, epoch(10), RecoveryAttemptId(1))
            .unwrap();

        drop(tx);

        assert!(matches!(
            mailbox.recv().await,
            Err(SourceRecoveryMailboxError::CompletionClosed)
        ));

        assert!(!mailbox.has_pending());
    }
}
