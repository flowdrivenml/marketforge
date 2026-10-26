use std::{
    future::{self, Future},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};

use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

use tokio_tungstenite::tungstenite::Message;

use crate::live::infrastructure::limiter::{
    ConnectionLimiterError, ConnectionRateLimiter, RequestPriority,
};

use super::{
    ApplicationHeartbeatSchedule, CompiledHeartbeatPlan, OutboundMessage, SupervisorExit,
    WebSocketConnection, WebSocketSupervisor, WebSocketTransportError,
};

// ============================================================
// OPERATIONAL BOUNDS
// ============================================================

// These are MarketForge resource-safety bounds, not
// exchange-specific heartbeat requirements.
const ADMISSION_TIMEOUT: Duration = Duration::from_secs(2);
const WIRE_SEND_TIMEOUT: Duration = Duration::from_millis(500);

// ============================================================
// OUTBOUND JOBS
// ============================================================

#[derive(Debug)]
enum SendJob {
    ControlPing(Vec<u8>),
    ApplicationPing(String),
    Text(String),
    Subscription(String),
    SubscriptionTracked {
        text: String,
        receipt: super::SubscriptionSendReceiptSender,
    },
}

impl SendJob {
    fn is_heartbeat(&self) -> bool {
        matches!(self, Self::ControlPing(_) | Self::ApplicationPing(_))
    }

    fn priority(&self) -> RequestPriority {
        if self.is_heartbeat() {
            RequestPriority::High
        } else {
            RequestPriority::Normal
        }
    }

    fn is_subscription(&self) -> bool {
        matches!(
            self,
            Self::Subscription(_) | Self::SubscriptionTracked { .. }
        )
    }

    fn message(&self) -> Message {
        match self {
            Self::ControlPing(payload) => Message::Ping(payload.clone().into()),

            Self::ApplicationPing(text) | Self::Text(text) | Self::Subscription(text) => {
                Message::Text(text.clone().into())
            }

            Self::SubscriptionTracked { text, .. } => Message::Text(text.clone().into()),
        }
    }
}

// ============================================================
// CANCELLATION-SAFE LIMITER ADMISSION
// ============================================================

type AdmissionFuture = Pin<Box<dyn Future<Output = Result<(), ConnectionLimiterError>> + Send>>;

struct PendingSend {
    job: SendJob,
    admission: AdmissionFuture,
    deadline: Instant,
}

impl PendingSend {
    fn new(job: SendJob, limiter: Arc<ConnectionRateLimiter>) -> Self {
        let subscription = job.is_subscription();
        let priority = job.priority();

        let admission: AdmissionFuture = Box::pin(async move {
            if subscription {
                limiter.acquire_subscription(priority).await
            } else {
                limiter.acquire_outbound(priority).await
            }
        });

        Self {
            job,
            admission,
            deadline: Instant::now() + ADMISSION_TIMEOUT,
        }
    }
}

// ============================================================
// OPTIONAL TIMER
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
// SUPERVISOR IMPLEMENTATION
// ============================================================

impl WebSocketSupervisor {
    /// Execute one compiled, exchange-independent heartbeat
    /// plan while continuously servicing the WebSocket.
    ///
    /// No detached tasks are created.
    ///
    /// Application data is never intentionally dropped:
    /// inbound queue exhaustion terminates the connection.
    pub async fn run_with_plan(
        connection: WebSocketConnection,
        inbound: mpsc::Sender<Message>,
        mut outbound: mpsc::Receiver<OutboundMessage>,
        mut shutdown: oneshot::Receiver<()>,
        plan: CompiledHeartbeatPlan,
    ) -> Result<SupervisorExit, WebSocketTransportError> {
        let mut socket = connection.socket;
        let limiter = Arc::new(connection.limiter);

        let now = Instant::now();

        // ----------------------------------------------------
        // SCHEDULING
        // ----------------------------------------------------

        let mut next_control = match &plan {
            CompiledHeartbeatPlan::ClientControl(policy) => Some(now + policy.interval),

            _ => None,
        };

        let mut next_application = match &plan {
            CompiledHeartbeatPlan::Application(policy) => {
                let delay = match policy.schedule {
                    ApplicationHeartbeatSchedule::Periodic { interval } => interval,

                    ApplicationHeartbeatSchedule::ReceiveInactivity { threshold } => threshold,
                };

                Some(now + delay)
            }

            _ => None,
        };

        // ----------------------------------------------------
        // HEARTBEAT RESPONSE STATE
        // ----------------------------------------------------

        let mut sequence = 0_u64;

        let mut pending_control: Option<(Vec<u8>, Instant)> = None;

        let mut pending_application: Option<Instant> = None;

        // ----------------------------------------------------
        // OUTBOUND ADMISSION STATE
        // ----------------------------------------------------

        let mut pending_send: Option<PendingSend> = None;

        // Only one ordinary message can be displaced by
        // a higher-priority heartbeat.
        let mut deferred: Option<SendJob> = None;

        let mut outbound_closed = false;

        loop {
            // =================================================
            // RESUME A DEFERRED ORDINARY MESSAGE
            // =================================================

            if pending_send.is_none() {
                if let Some(job) = deferred.take() {
                    pending_send = Some(PendingSend::new(job, limiter.clone()));
                }
            }

            let control_response_deadline = pending_control.as_ref().map(|(_, deadline)| *deadline);

            let application_response_deadline = pending_application;

            // Admission itself is bounded. A saturated
            // limiter must not occupy the entire supervisor
            // indefinitely.
            let admission_deadline = pending_send.as_ref().map(|pending| pending.deadline);

            tokio::select! {

                // =============================================
                // SHUTDOWN
                // =============================================

                _ = &mut shutdown => {
                    let _ = time::timeout(
                        WIRE_SEND_TIMEOUT,
                        socket.close(None),
                    )
                    .await;

                    return Ok(SupervisorExit::Shutdown);
                }

                // =============================================
                // CONTROL RESPONSE TIMEOUT
                // =============================================

                _ = wait_until(control_response_deadline) => {
                    return Err(
                        WebSocketTransportError::HeartbeatTimeout
                    );
                }

                // =============================================
                // APPLICATION RESPONSE TIMEOUT
                // =============================================

                _ = wait_until(application_response_deadline) => {
                    return Err(
                        WebSocketTransportError::
                            ApplicationHeartbeatTimeout
                    );
                }

                // =============================================
                // INCOMING FRAMES
                // =============================================

                frame = socket.next() => {
                    match frame {
                        Some(Ok(Message::Ping(_))) => {
                            // Tungstenite queues the appropriate
                            // echoed Pong automatically.
                            //
                            // Promptly flush that response.
                            time::timeout(
                                WIRE_SEND_TIMEOUT,
                                socket.flush(),
                            )
                            .await
                            .map_err(|_| {
                                WebSocketTransportError::
                                    OperationTimeout
                            })??;
                        }

                        Some(Ok(Message::Pong(payload))) => {
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

                        Some(Ok(Message::Close(_))) => {
                            return Ok(SupervisorExit::PeerClosed);
                        }

                        Some(Ok(Message::Text(text))) => {
                            let matched =
                                pending_application.is_some()
                                && match &plan {
                                    CompiledHeartbeatPlan::Application(
                                        policy
                                    ) => {
                                        policy.protocol.matches_response(
                                            text.as_str()
                                        )
                                    }

                                    _ => false,
                                };

                        if matched {
                            pending_application = None;

                            if let CompiledHeartbeatPlan::Application(policy) = &plan {
                                if let ApplicationHeartbeatSchedule::ReceiveInactivity {
                                    threshold,
                                } = policy.schedule
                                {
                                    next_application = Some(Instant::now() + threshold);
                                }
                            }
                        } else {
                            inbound
                                .try_send(Message::Text(text))
                                .map_err(|_| {
                                    WebSocketTransportError::InboundBackpressure
                                })?;
                        }

                            // An inactivity-triggered heartbeat
                            // resets on incoming application
                            // messages, not on outgoing traffic.
                            //
                            // Crucially, unrelated traffic cannot
                            // satisfy a pending Pong deadline.
                            if let CompiledHeartbeatPlan::Application(
                                policy
                            ) = &plan
                            {
                                if let ApplicationHeartbeatSchedule::
                                    ReceiveInactivity {
                                        threshold,
                                    } = policy.schedule
                                {
                                    if pending_application.is_none() {
                                        next_application =
                                            Some(Instant::now() + threshold);
                                    }
                                }
                            }
                        }

                        Some(Ok(message @ Message::Binary(_))) => {
                            inbound
                                .try_send(message)
                                .map_err(|_| {
                                    WebSocketTransportError::
                                        InboundBackpressure
                                })?;

                            if let CompiledHeartbeatPlan::Application(
                                policy
                            ) = &plan
                            {
                                if let ApplicationHeartbeatSchedule::
                                    ReceiveInactivity {
                                        threshold,
                                    } = policy.schedule
                                {
                                    if pending_application.is_none() {
                                        next_application =
                                            Some(Instant::now() + threshold);
                                    }
                                }
                            }
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

                // =============================================
                // CONTROL HEARTBEAT DUE
                // =============================================

                _ = wait_until(next_control),
                if next_control.is_some()
                    && pending_control.is_none()
                    && !pending_send
                        .as_ref()
                        .is_some_and(|pending| pending.job.is_heartbeat())=>
                {
                    // Do not enqueue a second heartbeat while
                    // the first still awaits admission.
                    if pending_send.as_ref().is_some_and(
                        |pending| pending.job.is_heartbeat()
                    ) {
                        continue;
                    }

                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| {
                            WebSocketTransportError::
                                Configuration(
                                    "heartbeat sequence exhausted".into()
                                )
                        })?;

                    let payload =
                        sequence.to_be_bytes().to_vec();

                    // Heartbeats preempt waiting ordinary
                    // admission, not socket transmission.
                    if let Some(previous) = pending_send.take() {
                        deferred = Some(previous.job);
                    }

                    pending_send = Some(PendingSend::new(
                        SendJob::ControlPing(payload),
                        limiter.clone(),
                    ));

                    next_control = None;
                }

                // =============================================
                // APPLICATION HEARTBEAT DUE
                // =============================================

                _ = wait_until(next_application),
                if next_application.is_some()
                    && pending_application.is_none()
                    && !pending_send
                        .as_ref()
                        .is_some_and(|pending| pending.job.is_heartbeat()) =>
                {
                    if pending_send.as_ref().is_some_and(
                        |pending| pending.job.is_heartbeat()
                    ) {
                        continue;
                    }

                    let CompiledHeartbeatPlan::Application(
                        policy
                    ) = &plan
                    else {
                        unreachable!(
                            "application timer without application plan"
                        );
                    };

                    if let Some(previous) = pending_send.take() {
                        deferred = Some(previous.job);
                    }

                    pending_send = Some(PendingSend::new(
                        SendJob::ApplicationPing(
                            policy.protocol.request_text().to_owned()
                        ),
                        limiter.clone(),
                    ));

                    next_application = None;
                }

                // =============================================
                // OUTBOUND ADMISSION TIMEOUT
                // =============================================

                _ = wait_until(admission_deadline),
                    if pending_send.is_some() =>
                {
                    return Err(
                        WebSocketTransportError::OperationTimeout
                    );
                }

                // =============================================
                // OUTBOUND ADMISSION COMPLETED
                // =============================================
                    admitted = async {
                        match pending_send.as_mut() {
                            Some(pending) => pending.admission.as_mut().await,

                            None => {
                                future::pending().await
                            }
                        }
                    } => {
                    admitted?;

                    let completed = pending_send
                        .take()
                        .expect("completed admission has a job");

                    let job = completed.job;

                    let control_payload = match &job {
                        SendJob::ControlPing(payload) => {
                            Some(payload.clone())
                        }

                        _ => None,
                    };

                    let application_ping =
                        matches!(&job, SendJob::ApplicationPing(_));

                    // Construct the wire frame without consuming the job.
                    // Any attached receipt remains alive until after send.
                    let message = job.message();

                    // The socket writer remains exclusively owned by
                    // this supervisor.
                    //
                    // The receipt is not completed merely because limiter
                    // admission succeeded.
                    time::timeout(
                        WIRE_SEND_TIMEOUT,
                        socket.send(message),
                    )
                    .await
                    .map_err(|_| {
                        WebSocketTransportError::OperationTimeout
                    })??;


                    let sent_at = Instant::now();
                    // Complete the transmission receipt only after
                    // successful socket.send().
                    //
                    // All other jobs retain their previous behavior.
                    if let SendJob::SubscriptionTracked { receipt, .. } = job {
                        receipt.sent();
                    }

                    if let Some(payload) = control_payload {
                        let CompiledHeartbeatPlan::ClientControl(
                            policy
                        ) = &plan
                        else {
                            unreachable!();
                        };

                        pending_control = Some((
                            payload,
                            sent_at + policy.response_timeout,
                        ));

                        next_control = Some(
                            sent_at + policy.interval
                        );
                    }

                    if application_ping {
                        let CompiledHeartbeatPlan::Application(
                            policy
                        ) = &plan
                        else {
                            unreachable!();
                        };

                        pending_application = Some(
                            sent_at + policy.response_timeout
                        );

                        // Periodic scheduling starts after
                        // transmission. Inactivity scheduling
                        // restarts when the response arrives
                        // or other incoming traffic resets
                        // the inactivity clock.
                        if let ApplicationHeartbeatSchedule::
                            Periodic {
                                interval,
                            } = policy.schedule
                        {
                            next_application = Some(
                                sent_at + interval
                            );
                        }
                    }
                }

                // =============================================
                // NEXT ORDINARY OUTBOUND MESSAGE
                // =============================================

                outgoing = outbound.recv(),
                    if !outbound_closed
                        && pending_send.is_none()
                        && deferred.is_none() =>
                {
                    match outgoing {
                        Some(OutboundMessage::Text(text)) => {
                            pending_send = Some(PendingSend::new(
                                SendJob::Text(text),
                                limiter.clone(),
                            ));
                        }

                        Some(OutboundMessage::Subscription(text)) => {
                            pending_send = Some(PendingSend::new(
                                SendJob::Subscription(text),
                                limiter.clone(),
                            ));
                        }
                        Some(OutboundMessage::SubscriptionTracked {
                                text,
                                receipt,
                            }) => {
                                pending_send = Some(PendingSend::new(
                                    SendJob::SubscriptionTracked {
                                        text,
                                        receipt,
                                    },
                                    limiter.clone(),
                                ));
                            }

                        None => {
                            outbound_closed = true;
                        }
                    }
                }
            }
        }
    }
}
