// src/live/infrastructure/websocket/supervisor.rs

use std::{future, time::Duration};

use futures_util::{SinkExt, StreamExt};

use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

use tokio_tungstenite::tungstenite::Message;

use crate::live::infrastructure::limiter::{ConnectionRateLimiter, RequestPriority};

use super::connection::WebSocketConnection;
use super::error::WebSocketTransportError;
use crate::live::infrastructure::websocket::heartbeat::policy::HeartbeatPolicy;
use crate::live::infrastructure::websocket::heartbeat::protocol::ApplicationHeartbeatPolicy;

// ============================================================
// SUPERVISOR EXIT
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorExit {
    Shutdown,
    PeerClosed,
}

// ============================================================
// OUTBOUND MESSAGES
// ============================================================

#[derive(Debug)]
pub enum OutboundMessage {
    Text(String),

    /// Existing untracked subscription path.
    Subscription(String),

    /// Subscription with an authoritative transmission receipt.
    ///
    /// The receipt is completed only after the socket
    /// send operation succeeds.
    SubscriptionTracked {
        text: String,
        receipt: crate::live::infrastructure::websocket::subscriptions::send_receipt::SubscriptionSendReceiptSender,
    },
} // ============================================================
// SUPERVISOR
// ============================================================

pub struct WebSocketSupervisor;

impl WebSocketSupervisor {
    // ========================================================
    // BACKWARD-COMPATIBLE CONTROL-FRAME SUPERVISION
    // ========================================================

    pub async fn run(
        connection: WebSocketConnection,
        inbound: mpsc::Sender<Message>,
        outbound: mpsc::Receiver<OutboundMessage>,
        shutdown: oneshot::Receiver<()>,
        heartbeat: HeartbeatPolicy,
    ) -> Result<SupervisorExit, WebSocketTransportError> {
        Self::run_with_application_heartbeat(
            connection, inbound, outbound, shutdown, heartbeat, None,
        )
        .await
    }

    // ========================================================
    // CONTROL + APPLICATION HEARTBEAT SUPERVISION
    // ========================================================

    pub async fn run_with_application_heartbeat(
        connection: WebSocketConnection,
        inbound: mpsc::Sender<Message>,
        mut outbound: mpsc::Receiver<OutboundMessage>,
        mut shutdown: oneshot::Receiver<()>,
        control_heartbeat: HeartbeatPolicy,
        application_heartbeat: Option<ApplicationHeartbeatPolicy>,
    ) -> Result<SupervisorExit, WebSocketTransportError> {
        let mut socket = connection.socket;

        let limiter = connection.limiter;

        // ----------------------------------------------------
        // CONTROL HEARTBEAT TIMER
        // ----------------------------------------------------

        let mut control_ticker = time::interval(control_heartbeat.interval);

        control_ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        // Tokio's first interval tick occurs immediately.
        control_ticker.tick().await;

        // ----------------------------------------------------
        // APPLICATION HEARTBEAT TIMER
        // ----------------------------------------------------

        let mut application_ticker = application_heartbeat.as_ref().map(|policy| {
            let mut ticker = time::interval(policy.interval);

            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

            ticker
        });

        if let Some(ticker) = application_ticker.as_mut() {
            ticker.tick().await;
        }

        // ----------------------------------------------------
        // HEARTBEAT STATE
        // ----------------------------------------------------

        let mut control_sequence = 0_u64;

        // Expected Pong payload and its absolute deadline.
        let mut pending_control: Option<(Vec<u8>, Instant)> = None;

        // Client-initiated application heartbeat deadline.
        //
        // Only one application heartbeat may be outstanding.
        let mut pending_application: Option<Instant> = None;

        let mut outbound_closed = false;

        // ----------------------------------------------------
        // SINGLE-OWNER SOCKET LOOP
        // ----------------------------------------------------

        loop {
            let control_deadline = pending_control.as_ref().map(|(_, deadline)| *deadline);

            let application_deadline = pending_application;

            tokio::select! {
                biased;

                // ============================================
                // SHUTDOWN
                // ============================================

                _ = &mut shutdown => {
                    let _ = time::timeout(
                        control_heartbeat.send_timeout,
                        socket.close(None),
                    )
                    .await;

                    return Ok(SupervisorExit::Shutdown);
                }

                // ============================================
                // CONTROL PONG DEADLINE
                // ============================================

                _ = wait_until(control_deadline) => {
                    return Err(
                        WebSocketTransportError::HeartbeatTimeout
                    );
                }

                // ============================================
                // APPLICATION RESPONSE DEADLINE
                // ============================================

                _ = wait_until(application_deadline) => {
                    return Err(
                        WebSocketTransportError::
                            ApplicationHeartbeatTimeout
                    );
                }

                // ============================================
                // RECEIVE
                // ============================================

                frame = socket.next() => {
                    match frame {
                        Some(Ok(Message::Pong(payload))) => {
                            // Only the current control heartbeat
                            // token can clear its deadline.

                            let matched = pending_control
                                .as_ref()
                                .is_some_and(|(expected, _)| {
                                    payload.as_ref()
                                        == expected.as_slice()
                                });

                            if matched {
                                pending_control = None;
                            }
                        }

                        Some(Ok(Message::Ping(_))) => {
                            // Tungstenite automatically queues
                            // protocol Pong frames.
                            //
                            // Explicitly flush to avoid
                            // waiting for another application
                            // write.

                            time::timeout(
                                control_heartbeat.send_timeout,
                                socket.flush(),
                            )
                            .await
                            .map_err(|_| {
                                WebSocketTransportError::
                                    OperationTimeout
                            })??;
                        }

                        Some(Ok(Message::Close(_))) => {
                            return Ok(SupervisorExit::PeerClosed);
                        }

                        Some(Ok(Message::Text(text))) => {
                            // Check application heartbeat
                            // responses before forwarding
                            // messages to normal routing.

                            let matched = pending_application
                                .is_some()
                                && application_heartbeat
                                    .as_ref()
                                    .is_some_and(|policy| {
                                        policy.protocol
                                            .matches_response(
                                                text.as_str()
                                            )
                                    });

                            if matched {
                                pending_application = None;
                                continue;
                            }

                            // Never silently discard market
                            // data when the consumer is slow.
                            inbound
                                .try_send(Message::Text(text))
                                .map_err(|_| {
                                    WebSocketTransportError::
                                        InboundBackpressure
                                })?;
                        }

                        Some(Ok(message @ Message::Binary(_))) => {
                            inbound
                                .try_send(message)
                                .map_err(|_| {
                                    WebSocketTransportError::
                                        InboundBackpressure
                                })?;
                        }

                        Some(Ok(_)) => {}

                        Some(Err(error)) => {
                            return Err(error.into());
                        }

                        None => {
                            return Ok(SupervisorExit::PeerClosed);
                        }
                    }
                }

                // ============================================
                // CONTROL PING
                // ============================================

                _ = control_ticker.tick(),
                    if pending_control.is_none() =>
                {
                    control_sequence =
                        control_sequence.checked_add(1)
                            .ok_or_else(|| {
                                WebSocketTransportError::
                                    Configuration(
                                        "control heartbeat sequence exhausted"
                                            .into(),
                                    )
                            })?;

                    let payload =
                        control_sequence.to_be_bytes().to_vec();

                    send_limited(
                        &limiter,
                        &mut socket,
                        Message::Ping(payload.clone().into()),
                        false,
                        RequestPriority::High,
                        control_heartbeat.send_timeout,
                    )
                    .await?;

                    pending_control = Some((
                        payload,
                        Instant::now()
                            + control_heartbeat.response_timeout,
                    ));
                }

                // ============================================
                // APPLICATION HEARTBEAT
                // ============================================

                _ = async {
                    match application_ticker.as_mut() {
                        Some(ticker) => {
                            ticker.tick().await;
                        }

                        None => {
                            future::pending::<()>().await;
                        }
                    }
                },
                if application_heartbeat.is_some()
                    && pending_application.is_none() =>
                {
                    let policy = application_heartbeat
                        .as_ref()
                        .expect(
                            "application heartbeat enabled"
                        );

                    let request =
                        policy.protocol.request_text().to_owned();

                    send_limited(
                        &limiter,
                        &mut socket,
                        Message::Text(request.into()),
                        false,
                        RequestPriority::High,
                        policy.send_timeout,
                    )
                    .await?;

                    pending_application = Some(
                        Instant::now() + policy.response_timeout
                    );
                }

                // ============================================
                // OUTBOUND APPLICATION MESSAGES
                // ============================================

                outgoing = outbound.recv(),
                    if !outbound_closed =>
                {
                    match outgoing {
                        Some(OutboundMessage::Text(text)) => {
                            send_limited(
                                &limiter,
                                &mut socket,
                                Message::Text(text.into()),
                                false,
                                RequestPriority::Normal,
                                control_heartbeat.send_timeout,
                            )
                            .await?;
                        }

                        Some(OutboundMessage::Subscription(text)) => {
                            send_limited(
                                &limiter,
                                &mut socket,
                                Message::Text(text.into()),
                                true,
                                RequestPriority::Normal,
                                control_heartbeat.send_timeout,
                            )
                            .await?;
                        }
                        Some(OutboundMessage::SubscriptionTracked {
                            text,
                            receipt,
                        }) => {
                            // If admission or transmission fails, `receipt`
                            // is dropped and the receiver observes cancellation.
                            send_limited(
                                &limiter,
                                &mut socket,
                                Message::Text(text.into()),
                                true,
                                RequestPriority::Normal,
                                control_heartbeat.send_timeout,
                            )
                            .await?;

                            // Only successful socket transmission completes
                            // the receipt.
                            receipt.sent();
                        }
                        None => {
                            // Keep receiving even after all
                            // outbound producers disappear.
                            outbound_closed = true;
                        }
                    }
                }
            }
        }
    }
}

// ============================================================
// OPTIONAL ABSOLUTE DEADLINE
// ============================================================

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => {
            time::sleep_until(deadline).await;
        }

        None => {
            future::pending::<()>().await;
        }
    }
}

// ============================================================
// SINGLE OUTBOUND ADMISSION + TRANSMISSION
// ============================================================

async fn send_limited<S>(
    limiter: &ConnectionRateLimiter,
    socket: &mut S,
    message: Message,
    subscription: bool,
    priority: RequestPriority,
    timeout: Duration,
) -> Result<(), WebSocketTransportError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    time::timeout(timeout, async {
        if subscription {
            // Subscription admission includes the ordinary
            // outbound quota when one is configured.
            limiter.acquire_subscription(priority).await?;
        } else {
            limiter.acquire_outbound(priority).await?;
        }

        socket.send(message).await?;

        Ok::<(), WebSocketTransportError>(())
    })
    .await
    .map_err(|_| WebSocketTransportError::OperationTimeout)?
}
