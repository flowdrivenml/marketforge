use super::{
    SourceRecoveryEvent, SourceRecoveryMailbox, SourceRecoveryMailboxError,
    SourceTradeRecoveryReview,
};
use crate::live::infrastructure::rest::HistoricalTradeCursor;
use crate::live::infrastructure::rest::RestRequest;
use crate::live::{
    integrity::TradeSourceEpoch,
    recovery::{
        RecoveryAttemptId, RecoveryCompletion, RecoveryOperation, RecoveryRequestId,
        RecoverySourceEpoch, RecoveryTicket, StreamRecoveryError, StreamRecoveryHandle,
    },
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceTradeAttemptError {
    #[error("trade recovery already has an outstanding request")]
    AlreadyPending,

    #[error("trade recovery belongs to another stream")]
    WrongStream,

    #[error("trade recovery belongs to another acquisition")]
    WrongEpoch,

    #[error("REST recovery handle has an obsolete binding")]
    WrongRecoveryBinding,

    #[error("trade recovery attempt ID must be positive")]
    InvalidAttempt,

    #[error("trade recovery requires a trade REST operation")]
    WrongOperation,

    #[error("trade recovery review does not match the source epoch")]
    WrongReview,

    #[error(transparent)]
    Mailbox(#[from] SourceRecoveryMailboxError),

    #[error(transparent)]
    Scheduling(#[from] StreamRecoveryError),
}

/// Exact context retained alongside one REST trade request.
///
/// The review describes the original coverage hold.
/// The epoch and attempt identify the authorized acquisition.
///
/// None of these fields independently proves complete
/// historical trade coverage.
#[derive(Debug, Clone)]
pub struct SourceTradeAttemptContext {
    pub epoch: TradeSourceEpoch,
    pub attempt: RecoveryAttemptId,
    pub review: SourceTradeRecoveryReview,
    pub operation: RecoveryOperation,

    /// The authoritative logical REST request associated
    /// with this acquisition.
    ///
    /// Never reconstruct this from exchange response data.
    pub request: RestRequest,
}

impl SourceTradeAttemptContext {
    pub fn recovery_epoch(&self) -> RecoverySourceEpoch {
        RecoverySourceEpoch {
            stream_generation: self.epoch.stream_generation,
            connection_generation: self.epoch.connection_generation,
            subscription_generation: self.epoch.subscription_generation,
        }
    }
}

/// Matching REST completion together with the original
/// trusted request context.
///
/// The executor must still validate the HTTP response,
/// normalize its trades, establish permitted coverage,
/// and perform integrity recovery admission.
pub struct CompletedTradeAttempt {
    pub context: SourceTradeAttemptContext,
    pub completion: RecoveryCompletion,
}

/// One outstanding REST trade request per source actor.
///
/// Owns the original recovery context and completion mailbox.
/// Does not own an HTTP client, REST scheduler, integrity
/// controller, live buffer, or publication destination.
pub struct SourceTradeAttempt {
    mailbox: SourceRecoveryMailbox,
    context: Option<SourceTradeAttemptContext>,
}

impl SourceTradeAttempt {
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

    pub fn pending_context(&self) -> Option<&SourceTradeAttemptContext> {
        self.context.as_ref()
    }

    fn validate_context(
        &self,
        context: &SourceTradeAttemptContext,
    ) -> Result<(), SourceTradeAttemptError> {
        if self.has_pending() {
            return Err(SourceTradeAttemptError::AlreadyPending);
        }

        if context.recovery_epoch() != self.mailbox.epoch() {
            return Err(SourceTradeAttemptError::WrongEpoch);
        }

        if context.review.stream_id != self.mailbox.stream_id() {
            return Err(SourceTradeAttemptError::WrongStream);
        }

        if context.review.stream_generation != context.epoch.stream_generation {
            return Err(SourceTradeAttemptError::WrongReview);
        }

        if context.attempt.0 == 0 {
            return Err(SourceTradeAttemptError::InvalidAttempt);
        }

        match &context.operation {
            RecoveryOperation::RecentTrades { .. } | RecoveryOperation::HistoricalTrades { .. } => {
            }

            RecoveryOperation::DepthSnapshot { .. } => {
                return Err(SourceTradeAttemptError::WrongOperation);
            }
        }
        let expected_endpoint = match &context.operation {
            RecoveryOperation::RecentTrades { .. } => "recent_trades",
            RecoveryOperation::HistoricalTrades { .. } => "historical_trades",
            RecoveryOperation::DepthSnapshot { .. } => {
                return Err(SourceTradeAttemptError::WrongOperation);
            }
        };
        if let RecoveryOperation::HistoricalTrades { page } = &context.operation {
            if let HistoricalTradeCursor::TradeId { from_id } = &page.cursor {
                let expected = context
                    .review
                    .baseline
                    .numeric_trade_id
                    .and_then(|id| id.checked_add(1));

                if expected != Some(*from_id) {
                    return Err(SourceTradeAttemptError::WrongReview);
                }

                if context
                    .request
                    .parameters()
                    .get("fromId")
                    .map(String::as_str)
                    != Some(from_id.to_string().as_str())
                {
                    return Err(SourceTradeAttemptError::WrongOperation);
                }
            }
        }
        if context.request.endpoint() != expected_endpoint {
            return Err(SourceTradeAttemptError::WrongOperation);
        }
        Ok(())
    }

    /// Install an already scheduled REST ticket.
    ///
    /// This is also useful for deterministic tests.
    ///
    /// The mailbox independently enforces monotonic
    /// attempt identity and completion ownership.
    pub fn install(
        &mut self,
        ticket: RecoveryTicket,
        context: SourceTradeAttemptContext,
    ) -> Result<(), SourceTradeAttemptError> {
        self.validate_context(&context)?;

        self.mailbox
            .install(ticket, context.recovery_epoch(), context.attempt)?;

        self.context = Some(context);

        Ok(())
    }

    /// Schedule one trade REST operation through the shared
    /// recovery infrastructure.
    ///
    /// The caller chooses RecentTrades or HistoricalTrades
    /// according to an explicit exchange recovery policy.
    ///
    /// A successful schedule does not establish coverage.
    pub async fn schedule(
        &mut self,
        handle: &StreamRecoveryHandle,
        context: SourceTradeAttemptContext,
    ) -> Result<(), SourceTradeAttemptError> {
        self.validate_context(&context)?;

        if handle.binding().stream_id != self.mailbox.stream_id() {
            return Err(SourceTradeAttemptError::WrongStream);
        }

        if handle.binding().epoch != self.mailbox.epoch() {
            return Err(SourceTradeAttemptError::WrongRecoveryBinding);
        }

        let ticket = handle
            .request(context.attempt, context.operation.clone())
            .await?;

        let request_id = ticket.request_id;
        let attempt = context.attempt;

        // If local installation fails after REST scheduling,
        // revoke the remote request explicitly.
        if let Err(error) = self.install(ticket, context) {
            handle.cancel(attempt, request_id).await?;
            return Err(error);
        }

        Ok(())
    }

    /// Receive a REST completion after the mailbox validates
    /// stream ID, complete epoch, attempt ID and request ID.
    ///
    /// An obsolete result receives no recovery context and
    /// must never reach integrity.
    pub async fn recv(&mut self) -> Result<Option<CompletedTradeAttempt>, SourceTradeAttemptError> {
        match self.mailbox.recv().await {
            Ok(SourceRecoveryEvent::Accepted(completion)) => {
                let context = self
                    .context
                    .take()
                    .expect("pending trade request owns its context");

                Ok(Some(CompletedTradeAttempt {
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
                Err(SourceTradeAttemptError::Mailbox(error))
            }
        }
    }

    /// Revoke the outstanding completion locally.
    ///
    /// Dropping the receiver alone does not cancel HTTP work.
    pub fn revoke_pending(&mut self) -> Option<RecoveryRequestId> {
        self.context = None;
        self.mailbox.revoke_pending()
    }

    /// Revoke locally before awaiting remote cancellation.
    ///
    /// A cancellation error is returned to the caller,
    /// rather than silently ignored.
    pub async fn cancel_pending(
        &mut self,
        handle: &StreamRecoveryHandle,
    ) -> Result<(), SourceTradeAttemptError> {
        if handle.binding().stream_id != self.mailbox.stream_id() {
            return Err(SourceTradeAttemptError::WrongStream);
        }

        if handle.binding().epoch != self.mailbox.epoch() {
            return Err(SourceTradeAttemptError::WrongRecoveryBinding);
        }

        let Some(context) = self.context.as_ref() else {
            return Ok(());
        };

        let attempt = context.attempt;

        if let Some(request_id) = self.revoke_pending() {
            handle.cancel(attempt, request_id).await?;
        }

        Ok(())
    }

    /// Establish a different trusted acquisition epoch.
    ///
    /// Returns the old request ID if one was outstanding.
    /// The executor must cancel that old request using its
    /// original recovery handle before discarding the handle.
    pub fn replace_epoch(&mut self, epoch: RecoverySourceEpoch) -> Option<RecoveryRequestId> {
        if self.mailbox.epoch() == epoch {
            return None;
        }

        let old = self.revoke_pending();
        self.mailbox.replace_epoch(epoch);
        old
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::infrastructure::rest::{
        HistoricalTradeCursor, HistoricalTradePage, RestRequest,
    };
    use crate::live::{
        integrity::{
            TradeIntegrityBaseline, TradeIntegrityObservation, TradeIntegrityObservationKind,
        },
        recovery::{RecoveryJobId, RecoveryRequester},
    };
    use tokio::sync::oneshot;

    const STREAM: &str = "binance:spot:BTCUSDT:trades";

    fn epoch(generation: u64) -> TradeSourceEpoch {
        TradeSourceEpoch {
            stream_generation: generation,
            connection_generation: generation + 10,
            subscription_generation: generation + 20,
        }
    }

    fn review(generation: u64) -> SourceTradeRecoveryReview {
        SourceTradeRecoveryReview {
            stream_id: STREAM.into(),
            stream_generation: generation,
            baseline: TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: None,
            },
            triggering_records: 1,
            observations: vec![TradeIntegrityObservation {
                index: 0,
                kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                previous: 100,
                current: 105,
            }],
        }
    }

    fn context(generation: u64, attempt: u64) -> SourceTradeAttemptContext {
        SourceTradeAttemptContext {
            epoch: epoch(generation),
            attempt: RecoveryAttemptId(attempt),
            review: review(generation),
            operation: RecoveryOperation::HistoricalTrades {
                page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::TradeId {
                    from_id: 101,
                })
                .with_limit(1000),
            },

            request: RestRequest::new("binance_spot", "historical_trades")
                .with_parameter("symbol", "BTCUSDT")
                .with_parameter("fromId", "101")
                .with_parameter("limit", "1000"),
        }
    }

    fn ticket(id: u64) -> (RecoveryTicket, oneshot::Sender<RecoveryCompletion>) {
        let (sender, completion) = oneshot::channel();

        (
            RecoveryTicket {
                request_id: RecoveryRequestId(id),
                job_id: RecoveryJobId(id),
                completion,
            },
            sender,
        )
    }

    fn completion(id: u64, context: &SourceTradeAttemptContext) -> RecoveryCompletion {
        use std::sync::Arc;

        use crate::live::recovery::RecoveryExecutionError;

        RecoveryCompletion {
            request_id: RecoveryRequestId(id),
            job_id: RecoveryJobId(id),
            requester: RecoveryRequester {
                stream_id: STREAM.into(),
                epoch: context.recovery_epoch(),
                attempt: context.attempt,
            },
            result: Err(Arc::new(RecoveryExecutionError::Cancelled)),
        }
    }

    #[tokio::test]
    async fn matching_trade_completion_preserves_context() {
        let original = context(10, 1);

        let mut owner = SourceTradeAttempt::new(STREAM, original.recovery_epoch());

        let (ticket, sender) = ticket(7);

        owner.install(ticket, original.clone()).unwrap();

        sender.send(completion(7, &original)).unwrap();

        let completed = owner.recv().await.unwrap().unwrap();

        assert_eq!(completed.context.epoch, epoch(10));
        assert_eq!(completed.context.attempt, RecoveryAttemptId(1));
        assert_eq!(
            completed.context.review.baseline.numeric_trade_id,
            Some(100)
        );

        assert!(matches!(
            completed.context.operation,
            RecoveryOperation::HistoricalTrades { .. }
        ));

        assert!(!owner.has_pending());
    }

    #[test]
    fn wrong_connection_epoch_is_rejected() {
        let mut owner = SourceTradeAttempt::new(STREAM, context(10, 1).recovery_epoch());

        let (ticket, _sender) = ticket(7);

        let mut wrong = context(10, 1);
        wrong.epoch.connection_generation += 1;

        assert!(matches!(
            owner.install(ticket, wrong),
            Err(SourceTradeAttemptError::WrongEpoch)
        ));

        assert!(!owner.has_pending());
    }

    #[test]
    fn wrong_subscription_epoch_is_rejected() {
        let mut owner = SourceTradeAttempt::new(STREAM, context(10, 1).recovery_epoch());

        let (ticket, _sender) = ticket(7);

        let mut wrong = context(10, 1);
        wrong.epoch.subscription_generation += 1;

        assert!(matches!(
            owner.install(ticket, wrong),
            Err(SourceTradeAttemptError::WrongEpoch)
        ));

        assert!(!owner.has_pending());
    }

    #[test]
    fn duplicate_outstanding_trade_attempt_is_rejected() {
        let mut owner = SourceTradeAttempt::new(STREAM, context(10, 1).recovery_epoch());

        let (first, _first_sender) = ticket(1);
        owner.install(first, context(10, 1)).unwrap();

        let (second, _second_sender) = ticket(2);

        assert!(matches!(
            owner.install(second, context(10, 2)),
            Err(SourceTradeAttemptError::AlreadyPending)
        ));

        assert!(owner.has_pending());
    }

    #[test]
    fn obsolete_attempt_number_cannot_be_reused() {
        let mut owner = SourceTradeAttempt::new(STREAM, context(10, 3).recovery_epoch());

        let (first, _first_sender) = ticket(1);
        owner.install(first, context(10, 3)).unwrap();

        assert_eq!(owner.revoke_pending(), Some(RecoveryRequestId(1)));

        let (second, _second_sender) = ticket(2);

        assert!(matches!(
            owner.install(second, context(10, 2)),
            Err(SourceTradeAttemptError::Mailbox(
                SourceRecoveryMailboxError::ObsoleteAttempt
            ))
        ));
    }

    #[test]
    fn depth_operation_cannot_enter_trade_attempt() {
        let mut owner = SourceTradeAttempt::new(STREAM, context(10, 1).recovery_epoch());

        let mut invalid = context(10, 1);

        invalid.operation = RecoveryOperation::DepthSnapshot { depth: Some(200) };

        let (ticket, _sender) = ticket(1);

        assert!(matches!(
            owner.install(ticket, invalid),
            Err(SourceTradeAttemptError::WrongOperation)
        ));

        assert!(!owner.has_pending());
    }

    #[test]
    fn generation_change_revokes_pending_request() {
        let mut owner = SourceTradeAttempt::new(STREAM, context(10, 1).recovery_epoch());

        let (first, _sender) = ticket(7);

        owner.install(first, context(10, 1)).unwrap();

        let revoked = owner.replace_epoch(context(11, 1).recovery_epoch());

        assert_eq!(revoked, Some(RecoveryRequestId(7)));
        assert!(!owner.has_pending());

        let (second, _sender) = ticket(8);

        owner.install(second, context(11, 1)).unwrap();

        assert!(owner.has_pending());
    }

    #[tokio::test]
    async fn incorrect_completion_request_is_obsolete() {
        let original = context(10, 1);

        let mut owner = SourceTradeAttempt::new(STREAM, original.recovery_epoch());

        let (ticket, sender) = ticket(7);

        owner.install(ticket, original.clone()).unwrap();

        sender.send(completion(8, &original)).unwrap();

        assert!(owner.recv().await.unwrap().is_none());
        assert!(!owner.has_pending());
    }
}
