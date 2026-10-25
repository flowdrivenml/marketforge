use std::{collections::VecDeque, time::Duration};

use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use tokio_tungstenite::tungstenite::Message;

use super::{
    CompiledSubscriptionAck, ExpectedSubscriptionAck, OutboundMessage, StreamRecoveryCoordinator,
    StreamRecoveryToken, SubscriptionReceiptError, SubscriptionSession, SubscriptionSessionError,
    SubscriptionSessionEvent, SubscriptionTransactionId, subscription_send_receipt,
};

// ============================================================
// OPERATIONAL LIMITS
// ============================================================

/// Maximum inbound messages staged while awaiting the
/// transmission receipt.
///
/// Reaching this limit fails the transaction rather than
/// silently discarding exchange data.
const MAX_EARLY_FRAMES: usize = 64;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionExecutionError {
    #[error(transparent)]
    Session(#[from] SubscriptionSessionError),

    #[error(transparent)]
    Receipt(#[from] SubscriptionReceiptError),

    #[error("outbound subscription queue is closed")]
    OutboundClosed,

    #[error("outbound subscription queue admission timed out")]
    OutboundTimeout,

    #[error("inbound WebSocket channel closed")]
    InboundClosed,

    #[error("market-data routing queue exhausted")]
    RoutingBackpressure,

    #[error("too many inbound frames before send receipt")]
    EarlyFrameOverflow,

    #[error("invalid JSON subscription response: {0}")]
    InvalidJson(String),

    #[error("subscription ACK deadline missing")]
    MissingDeadline,
    #[error("subscription session already has a pending transaction")]
    SessionBusy,
}

// ============================================================
// RESULT
// ============================================================

#[derive(Debug)]
pub struct SubscriptionExecutionResult {
    pub transaction_id: SubscriptionTransactionId,
    pub event: SubscriptionSessionEvent,
}

// ============================================================
// EXECUTOR
// ============================================================

/// Executes one subscription transaction over an existing
/// WebSocket supervisor.
///
/// The supervisor remains the sole socket owner.
///
/// This executor owns neither the socket nor a detached task.
///
/// The caller must serialize execution with any other
/// subscription transaction executor on the same inbound
/// channel.
///
/// A fatal execution error requires terminating the current
/// connection generation before retrying the subscription.
pub struct SubscriptionExecutor;

impl SubscriptionExecutor {
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_one(
        session: &mut SubscriptionSession,
        recovery: &mut StreamRecoveryCoordinator,
        protocol: &CompiledSubscriptionAck,
        token: &StreamRecoveryToken,
        expected: ExpectedSubscriptionAck,
        wire_text: String,
        outbound: &mpsc::Sender<OutboundMessage>,
        inbound: &mut mpsc::Receiver<Message>,
        unrelated: &mpsc::Sender<Message>,
        queue_timeout: Duration,
        receipt_timeout: Duration,
    ) -> Result<SubscriptionExecutionResult, SubscriptionExecutionError> {
        if session.pending_count() != 0 {
            return Err(SubscriptionExecutionError::SessionBusy);
        }
        let transaction_id = session.reserve(recovery, token, expected)?;

        let result = Self::execute_reserved(
            session,
            recovery,
            protocol,
            transaction_id,
            wire_text,
            outbound,
            inbound,
            unrelated,
            queue_timeout,
            receipt_timeout,
        )
        .await;

        // An unsuccessful operation cannot retain pending
        // transaction authority.
        //
        // If this future is externally cancelled, the
        // connection/session owner must invalidate the
        // generation and its outstanding transactions.
        if result.is_err() {
            let _ = session.cancel(transaction_id);
        }

        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_reserved(
        session: &mut SubscriptionSession,
        recovery: &mut StreamRecoveryCoordinator,
        protocol: &CompiledSubscriptionAck,
        transaction_id: SubscriptionTransactionId,
        wire_text: String,
        outbound: &mpsc::Sender<OutboundMessage>,
        inbound: &mut mpsc::Receiver<Message>,
        unrelated: &mpsc::Sender<Message>,
        queue_timeout: Duration,
        receipt_timeout: Duration,
    ) -> Result<SubscriptionExecutionResult, SubscriptionExecutionError> {
        let token = session
            .pending_token(transaction_id)
            .ok_or(SubscriptionSessionError::StaleRecoveryToken)?;

        let (receipt_sender, receipt_receiver) =
            subscription_send_receipt(transaction_id, token.connection().clone());

        if queue_timeout.is_zero() {
            return Err(SubscriptionExecutionError::OutboundTimeout);
        }

        // Reserve before queueing.
        //
        // Queue acceptance is NOT socket transmission.
        match time::timeout(
            queue_timeout,
            outbound.send(OutboundMessage::SubscriptionTracked {
                text: wire_text,
                receipt: receipt_sender,
            }),
        )
        .await
        {
            Ok(Ok(())) => {}

            Ok(Err(_)) => {
                return Err(SubscriptionExecutionError::OutboundClosed);
            }

            Err(_) => {
                return Err(SubscriptionExecutionError::OutboundTimeout);
            }
        }

        // ====================================================
        // WAIT FOR TRANSMISSION RECEIPT
        // ====================================================

        // An exchange can send an ACK immediately after
        // receiving the subscription.
        //
        // At that moment the application's receipt future
        // may not yet have been polled.
        //
        // Stage inbound frames until the receipt is known,
        // preserving their order.
        let mut early = VecDeque::new();

        let receipt_future = receipt_receiver.wait(receipt_timeout);
        tokio::pin!(receipt_future);

        let receipt = loop {
            tokio::select! {
                result = &mut receipt_future => {
                    break result?;
                }

                frame = inbound.recv() => {
                    let frame = frame.ok_or(
                        SubscriptionExecutionError::InboundClosed
                    )?;

                    if early.len() >= MAX_EARLY_FRAMES {
                        return Err(
                            SubscriptionExecutionError::EarlyFrameOverflow
                        );
                    }

                    early.push_back(frame);
                }
            }
        };

        // This transition starts the ACK deadline at
        // the actual socket-send completion timestamp.
        session.transmitted(receipt)?;

        // ====================================================
        // PROCESS EARLY FRAMES
        // ====================================================

        while let Some(frame) = early.pop_front() {
            if let Some(event) = Self::process_frame(session, recovery, protocol, frame, unrelated)?
            {
                // If any staged frames remain after a
                // matching ACK, we cannot discard them.
                Self::forward_remaining(early, unrelated)?;

                return Ok(SubscriptionExecutionResult {
                    transaction_id,
                    event,
                });
            }
        }

        // ====================================================
        // WAIT FOR ACK OR DEADLINE
        // ====================================================

        loop {
            let deadline = session
                .next_deadline()
                .ok_or(SubscriptionExecutionError::MissingDeadline)?;

            tokio::select! {
                _ = time::sleep_until(deadline) => {
                    let mut expired =
                        session.expire(Instant::now());

                    let completion = expired
                        .drain(..)
                        .find(|entry| {
                            entry.transaction_id
                                == transaction_id
                        });

                    if let Some(completion) = completion {
                        return Ok(SubscriptionExecutionResult {
                            transaction_id,
                            event: SubscriptionSessionEvent::Failed {
                                completion,
                            },
                        });
                    }
                }

                frame = inbound.recv() => {
                    let frame = frame.ok_or(
                        SubscriptionExecutionError::InboundClosed
                    )?;

                    if let Some(event) = Self::process_frame(
                        session,
                        recovery,
                        protocol,
                        frame,
                        unrelated,
                    )? {
                        return Ok(SubscriptionExecutionResult {
                            transaction_id,
                            event,
                        });
                    }
                }
            }
        }
    }

    // ========================================================
    // FRAME PROCESSING
    // ========================================================

    fn process_frame(
        session: &mut SubscriptionSession,
        recovery: &mut StreamRecoveryCoordinator,
        protocol: &CompiledSubscriptionAck,
        frame: Message,
        unrelated: &mpsc::Sender<Message>,
    ) -> Result<Option<SubscriptionSessionEvent>, SubscriptionExecutionError> {
        let Message::Text(ref text) = frame else {
            unrelated
                .try_send(frame)
                .map_err(|_| SubscriptionExecutionError::RoutingBackpressure)?;

            return Ok(None);
        };

        let native: Value = match serde_json::from_str(text.as_str()) {
            Ok(value) => value,

            // Some exchanges can send non-JSON application
            // messages. They are not subscription ACKs.
            Err(_) => {
                unrelated
                    .try_send(frame)
                    .map_err(|_| SubscriptionExecutionError::RoutingBackpressure)?;

                return Ok(None);
            }
        };

        match session.on_message(recovery, protocol, &native, Instant::now())? {
            SubscriptionSessionEvent::Unrelated => {
                unrelated
                    .try_send(frame)
                    .map_err(|_| SubscriptionExecutionError::RoutingBackpressure)?;

                Ok(None)
            }

            event => Ok(Some(event)),
        }
    }

    fn forward_remaining(
        remaining: VecDeque<Message>,
        unrelated: &mpsc::Sender<Message>,
    ) -> Result<(), SubscriptionExecutionError> {
        for frame in remaining {
            unrelated
                .try_send(frame)
                .map_err(|_| SubscriptionExecutionError::RoutingBackpressure)?;
        }

        Ok(())
    }
}
