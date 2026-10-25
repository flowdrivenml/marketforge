use std::{
    collections::{BTreeMap, VecDeque},
    future::{self, Future},
    pin::Pin,
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};

use serde_json::Value;
use thiserror::Error;

use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

use tokio_tungstenite::tungstenite::Message;

use super::{
    CompiledSubscriptionAck, ExpectedSubscriptionAck, OutboundMessage, SharedStreamRecovery,
    StreamRecoveryCoordinator, StreamRecoveryToken, SubscriptionCompletion,
    SubscriptionReceiptError, SubscriptionSendReceipt, SubscriptionSession,
    SubscriptionSessionError, SubscriptionSessionEvent, SubscriptionTransactionId,
    subscription_send_receipt,
};

// ============================================================
// COMMANDS
// ============================================================

/// Already serialized native subscription request.
///
/// Native argument resolution is handled separately.
///
/// Request identity must match `expected`.
#[derive(Debug)]
pub struct SubscriptionCommand {
    pub token: StreamRecoveryToken,
    pub expected: ExpectedSubscriptionAck,
    pub wire_text: String,
}

// ============================================================
// DRIVER EVENTS
// ============================================================

#[derive(Debug)]
pub enum SubscriptionDriverEvent {
    Confirmed {
        transaction_id: SubscriptionTransactionId,
        token: StreamRecoveryToken,
    },

    Failed {
        completion: SubscriptionCompletion,
    },
}

// ============================================================
// LIMITS
// ============================================================

#[derive(Debug, Clone, Copy)]
pub struct SubscriptionDriverLimits {
    pub queue_timeout: Duration,
    pub receipt_timeout: Duration,
    pub maximum_early_frames: usize,
}

impl SubscriptionDriverLimits {
    pub fn validate(&self) -> Result<(), SubscriptionDriverError> {
        if self.queue_timeout.is_zero()
            || self.receipt_timeout.is_zero()
            || self.maximum_early_frames == 0
        {
            return Err(SubscriptionDriverError::InvalidLimits);
        }

        Ok(())
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionDriverError {
    #[error("invalid subscription-driver limits")]
    InvalidLimits,

    #[error(transparent)]
    Session(#[from] SubscriptionSessionError),

    #[error(transparent)]
    Receipt(#[from] SubscriptionReceiptError),

    #[error("subscription outbound channel closed")]
    OutboundClosed,

    #[error("subscription outbound queue timed out")]
    OutboundTimeout,

    #[error("subscription inbound channel closed")]
    InboundClosed,

    #[error("subscription completion receiver closed")]
    CompletionClosed,

    #[error("unrelated-message routing queue exhausted")]
    RoutingBackpressure,

    #[error("early inbound frame buffer exhausted")]
    EarlyFrameOverflow,

    #[error("subscription transmission identity mismatch")]
    ReceiptIdentityMismatch,

    #[error("subscription transmission became uncertain")]
    TransmissionUncertain,

    #[error("subscription session has an invalid deadline state")]
    InvalidDeadlineState,

    #[error(transparent)]
    SharedRecovery(#[from] super::SharedRecoveryError),
}

// ============================================================
// INTERNAL RECEIPT EVENT
// ============================================================

type ReceiptResult = (
    SubscriptionTransactionId,
    Result<SubscriptionSendReceipt, SubscriptionReceiptError>,
);

type ReceiptFuture = Pin<Box<dyn Future<Output = ReceiptResult> + Send>>;

// ============================================================
// OPTIONAL DEADLINE
// ============================================================

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(deadline).await,
        None => future::pending::<()>().await,
    }
}

// ============================================================
// DRIVER
// ============================================================

pub struct SubscriptionDriver;

impl SubscriptionDriver {
    /// Run the connection-scoped subscription transaction loop.
    ///
    /// The driver owns `session` and `recovery` for the
    /// duration of the connection generation.
    ///
    /// The WebSocket supervisor owns all socket I/O.
    ///
    /// A fatal error requires the connection owner to
    /// revoke the physical generation before reconnecting.
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        session: SubscriptionSession,
        recovery: StreamRecoveryCoordinator,
        protocol: CompiledSubscriptionAck,
        commands: mpsc::Receiver<SubscriptionCommand>,
        outbound: mpsc::Sender<OutboundMessage>,
        inbound: mpsc::Receiver<Message>,
        unrelated: mpsc::Sender<Message>,
        completions: mpsc::Sender<SubscriptionDriverEvent>,
        shutdown: oneshot::Receiver<()>,
        limits: SubscriptionDriverLimits,
    ) -> Result<(), SubscriptionDriverError> {
        Self::run_shared(
            session,
            SharedStreamRecovery::from_coordinator(recovery),
            protocol,
            commands,
            outbound,
            inbound,
            unrelated,
            completions,
            shutdown,
            limits,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn run_shared(
        mut session: SubscriptionSession,
        recovery: SharedStreamRecovery,
        protocol: CompiledSubscriptionAck,
        mut commands: mpsc::Receiver<SubscriptionCommand>,
        outbound: mpsc::Sender<OutboundMessage>,
        mut inbound: mpsc::Receiver<Message>,
        unrelated: mpsc::Sender<Message>,
        completions: mpsc::Sender<SubscriptionDriverEvent>,
        mut shutdown: oneshot::Receiver<()>,
        limits: SubscriptionDriverLimits,
    ) -> Result<(), SubscriptionDriverError> {
        limits.validate()?;

        let mut receipts: FuturesUnordered<ReceiptFuture> = FuturesUnordered::new();

        // Transactions awaiting their send receipt.
        //
        // An ACK for any of these transactions may already
        // be queued by the WebSocket supervisor.
        let mut awaiting_receipt = BTreeMap::<SubscriptionTransactionId, ()>::new();

        let mut early_frames = VecDeque::<Message>::new();

        let mut commands_closed = false;

        loop {
            // ------------------------------------------------
            // EXPIRE ACK DEADLINES
            // ------------------------------------------------

            let now = Instant::now();

            for completion in session.expire(now) {
                Self::emit_completion(
                    &completions,
                    SubscriptionDriverEvent::Failed { completion },
                )?;
            }

            // ------------------------------------------------
            // DRAIN STAGED FRAMES
            // ------------------------------------------------

            // Do not classify any staged frame while some
            // transmission receipt is unresolved.
            //
            // This avoids processing a correct ACK before
            // its transaction enters AwaitingAck.
            if awaiting_receipt.is_empty() {
                while let Some(frame) = early_frames.pop_front() {
                    {
                        let mut recovery_guard = recovery.lock()?;

                        Self::process_frame(
                            &mut session,
                            &mut recovery_guard,
                            &protocol,
                            frame,
                            &unrelated,
                            &completions,
                        )?;
                    }
                }
            }

            // ------------------------------------------------
            // NEXT ACK DEADLINE
            // ------------------------------------------------

            let deadline = session.next_deadline();

            tokio::select! {
                // ============================================
                // SHUTDOWN
                // ============================================

                _ = &mut shutdown => {
                    // Drop all unresolved receipt futures.
                    // The connection owner is responsible
                    // for stopping its WebSocket generation.
                    session.invalidate();
                    return Ok(());
                }

                // ============================================
                // NEW SUBSCRIPTION COMMAND
                // ============================================

                command = commands.recv(),
                if !commands_closed
                    && awaiting_receipt.is_empty() =>
                {
                    let Some(command) = command else {
                        commands_closed = true;
                        continue;
                    };

                    // Reservation is performed before
                    // the message enters the outbound queue.
                let id = {
                    let recovery_guard = recovery.lock()?;

                    session.reserve(
                        &recovery_guard,
                        &command.token,
                        command.expected,
                    )?
                };

                    let (receipt_sender, receipt_receiver) =
                        subscription_send_receipt(
                            id,
                            command.token.connection().clone(),
                        );

                    let outbound_message =
                        OutboundMessage::SubscriptionTracked {
                            text: command.wire_text,
                            receipt: receipt_sender,
                        };

                    match time::timeout(
                        limits.queue_timeout,
                        outbound.send(outbound_message),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}

                        Ok(Err(_)) => {
                            let _ = session.cancel(id);
                            return Err(
                                SubscriptionDriverError::OutboundClosed
                            );
                        }

                        Err(_) => {
                            let _ = session.cancel(id);
                            return Err(
                                SubscriptionDriverError::OutboundTimeout
                            );
                        }
                    }

                    awaiting_receipt.insert(id, ());

                    let timeout = limits.receipt_timeout;

                    receipts.push(Box::pin(async move {
                        (id, receipt_receiver.wait(timeout).await)
                    }));
                }

                // ============================================
                // SOCKET SEND RECEIPT
                // ============================================

                result = receipts.next(),
                if !receipts.is_empty() =>
                {
                    let Some((id, receipt_result)) = result
                    else {
                        continue;
                    };

                    awaiting_receipt.remove(&id);

                    let receipt = match receipt_result {
                        Ok(receipt) => receipt,

                        Err(error) => {
                            let completion = session.cancel(id)?;

                            Self::emit_completion(
                                &completions,
                                SubscriptionDriverEvent::Failed {
                                    completion,
                                },
                            )?;

                            return Err(
                                SubscriptionDriverError::Receipt(error)
                            );
                        }
                    };

                    if receipt.transaction_id != id {
                        return Err(
                            SubscriptionDriverError::
                                ReceiptIdentityMismatch
                        );
                    }

                    // ACK timeout starts at the actual
                    // socket-send timestamp.
                    session.transmitted(receipt)?;
                }

                // ============================================
                // INCOMING EXCHANGE FRAME
                // ============================================

                frame = inbound.recv() => {
                    let frame = frame.ok_or(
                        SubscriptionDriverError::InboundClosed
                    )?;

                    if !awaiting_receipt.is_empty() {
                        if early_frames.len()
                            >= limits.maximum_early_frames
                        {
                            return Err(
                                SubscriptionDriverError::
                                    EarlyFrameOverflow
                            );
                        }

                        early_frames.push_back(frame);
                    } else {
                    {
                        let mut recovery_guard = recovery.lock()?;

                        Self::process_frame(
                            &mut session,
                            &mut recovery_guard,
                            &protocol,
                            frame,
                            &unrelated,
                            &completions,
                        )?;
                    }
                    }
                }

                // ============================================
                // ACK DEADLINE
                // ============================================

                _ = wait_until(deadline),
                if deadline.is_some() =>
                {
                    // The next iteration expires all
                    // transactions with reached deadlines.
                }
            }
        }
    }

    // ========================================================
    // PROCESS NATIVE INBOUND FRAME
    // ========================================================

    fn process_frame(
        session: &mut SubscriptionSession,
        recovery: &mut StreamRecoveryCoordinator,
        protocol: &CompiledSubscriptionAck,
        frame: Message,
        unrelated: &mpsc::Sender<Message>,
        completions: &mpsc::Sender<SubscriptionDriverEvent>,
    ) -> Result<(), SubscriptionDriverError> {
        let Message::Text(ref text) = frame else {
            Self::forward(unrelated, frame)?;
            return Ok(());
        };

        let native: Value = match serde_json::from_str(text.as_str()) {
            Ok(value) => value,

            Err(_) => {
                Self::forward(unrelated, frame)?;
                return Ok(());
            }
        };

        let result = session.on_message(recovery, protocol, &native, Instant::now())?;

        match result {
            SubscriptionSessionEvent::Unrelated => {
                Self::forward(unrelated, frame)?;
            }

            SubscriptionSessionEvent::Confirmed {
                transaction_id,
                token,
            } => {
                Self::emit_completion(
                    completions,
                    SubscriptionDriverEvent::Confirmed {
                        transaction_id,
                        token,
                    },
                )?;
            }

            SubscriptionSessionEvent::Failed { completion } => {
                Self::emit_completion(completions, SubscriptionDriverEvent::Failed { completion })?;
            }
        }

        Ok(())
    }

    // ========================================================
    // BOUNDED DELIVERY
    // ========================================================

    fn forward(
        sender: &mpsc::Sender<Message>,
        frame: Message,
    ) -> Result<(), SubscriptionDriverError> {
        sender
            .try_send(frame)
            .map_err(|_| SubscriptionDriverError::RoutingBackpressure)
    }

    fn emit_completion(
        sender: &mpsc::Sender<SubscriptionDriverEvent>,
        event: SubscriptionDriverEvent,
    ) -> Result<(), SubscriptionDriverError> {
        sender
            .try_send(event)
            .map_err(|_| SubscriptionDriverError::CompletionClosed)
    }
}
