use super::{ConnectionFence, ConnectionFenceError, ConnectionStamp, ConnectionState};
use super::{
    ConnectionGeneration, ConnectionLifecycleError, SupervisorExit, WebSocketConnection,
    WebSocketConnector, WebSocketTransportError,
};
use crate::live::infrastructure::limiter::{RateLimitManager, factory::CompiledWebSocketLimiters};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

// ============================================================
// CONNECTION ID AUTHORITY
// ============================================================

/// Connection-local limiter identities must never be reused
/// by separate reconnect coordinators in one process.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn allocate_connection_id() -> Result<u64, ReconnectError> {
    NEXT_CONNECTION_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| ReconnectError::ConnectionIdExhausted)
}

// ============================================================
// RETRY POLICY
// ============================================================
// Add near the reconnect coordinator.

/// Revokes connection authority whenever the runner ends,
/// including when its future is externally cancelled.
///
/// Dropping the runner must never leave a generation Ready.
struct FenceShutdownGuard {
    fence: ConnectionFence,
}

impl FenceShutdownGuard {
    fn new(fence: ConnectionFence) -> Self {
        Self { fence }
    }
}

impl Drop for FenceShutdownGuard {
    fn drop(&mut self) {
        self.fence.stop();
    }
}
#[derive(Debug, Clone, Copy)]

pub struct ReconnectPolicy {
    pub initial_delay: Duration,
    pub maximum_delay: Duration,

    /// A connection surviving this long resets the
    /// consecutive-failure counter.
    ///
    /// Successful handshakes alone do not reset retries.
    pub stable_connection_duration: Duration,

    pub connection_timeout: Duration,

    /// None means retry indefinitely until shutdown.
    ///
    /// Some(N) allows at most N failed attempts/sessions
    /// before returning RetryExhausted.
    pub maximum_consecutive_failures: Option<u32>,
}

impl ReconnectPolicy {
    pub fn new(
        initial_delay: Duration,
        maximum_delay: Duration,
        stable_connection_duration: Duration,
        connection_timeout: Duration,
        maximum_consecutive_failures: Option<u32>,
    ) -> Result<Self, ReconnectError> {
        if initial_delay.is_zero()
            || maximum_delay.is_zero()
            || stable_connection_duration.is_zero()
            || connection_timeout.is_zero()
        {
            return Err(ReconnectError::Configuration(
                "reconnect durations must be positive".into(),
            ));
        }

        if initial_delay > maximum_delay {
            return Err(ReconnectError::Configuration(
                "initial reconnect delay exceeds maximum delay".into(),
            ));
        }

        if maximum_consecutive_failures == Some(0) {
            return Err(ReconnectError::Configuration(
                "maximum_consecutive_failures must be positive".into(),
            ));
        }

        Ok(Self {
            initial_delay,
            maximum_delay,
            stable_connection_duration,
            connection_timeout,
            maximum_consecutive_failures,
        })
    }

    pub fn delay_for_failure(&self, failures: u32) -> Duration {
        let mut delay = self.initial_delay;

        for _ in 1..failures.min(64) {
            delay = delay.saturating_mul(2).min(self.maximum_delay);

            if delay == self.maximum_delay {
                break;
            }
        }

        delay
    }
}

// ============================================================
// OBSERVABLE EVENTS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconnectEvent {
    Connecting {
        generation: ConnectionGeneration,
    },

    /// Physical handshake completed.
    ///
    /// This does NOT mean subscription readiness.
    Connected {
        generation: ConnectionGeneration,
        connection_id: u64,
    },

    /// The generation has already been invalidated when
    /// this event is emitted.
    Disconnected {
        generation: ConnectionGeneration,
        reason: String,
    },

    Backoff {
        after_generation: ConnectionGeneration,
        delay: Duration,
        consecutive_failures: u32,
    },

    Stopped,
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum ReconnectError {
    #[error("invalid reconnect configuration: {0}")]
    Configuration(String),

    #[error("physical connection identifier exhausted")]
    ConnectionIdExhausted,

    #[error("reconnect event receiver closed")]
    EventReceiverClosed,

    #[error("reconnect event delivery timed out")]
    EventDeliveryTimeout,

    #[error("reconnect retry limit reached after {0} failures")]
    RetryExhausted(u32),

    #[error(transparent)]
    Lifecycle(#[from] ConnectionLifecycleError),

    #[error(transparent)]
    Transport(#[from] WebSocketTransportError),
    #[error(transparent)]
    Fence(#[from] ConnectionFenceError),
}

// ============================================================
// RECONNECT COORDINATOR
// ============================================================

pub struct ReconnectRunner;

impl ReconnectRunner {
    /// Legacy entry point.
    ///
    /// Preserved for existing W3.2 callers and tests.
    ///
    /// Its fence is private to the runner. Production code
    /// requiring downstream generation validation should
    /// use run_with_fence() instead.
    pub async fn run<F, Fut>(
        url: &str,
        compiled: &CompiledWebSocketLimiters,
        shared_limiter: &RateLimitManager,
        policy: ReconnectPolicy,
        shutdown: oneshot::Receiver<()>,
        events: mpsc::Sender<ReconnectEvent>,
        mut session: F,
    ) -> Result<(), ReconnectError>
    where
        F: FnMut(ConnectionGeneration, WebSocketConnection) -> Fut,
        Fut: Future<Output = Result<SupervisorExit, WebSocketTransportError>>,
    {
        static NEXT_LEGACY_SLOT: AtomicU64 = AtomicU64::new(1);

        let id = NEXT_LEGACY_SLOT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ReconnectError::ConnectionIdExhausted)?;

        // This identity exists for backward compatibility,
        // not for persisted or cross-restart fencing.
        let fence = ConnectionFence::new("legacy-process", format!("legacy-slot-{id}"))?;

        Self::run_with_fence(
            url,
            compiled,
            shared_limiter,
            policy,
            shutdown,
            events,
            fence,
            move |stamp, connection| session(stamp.generation(), connection),
        )
        .await
    }

    /// Production reconnect entry point.
    ///
    /// The caller supplies the authoritative connection
    /// fence shared with its consumers.
    ///
    /// Every attempt receives a unique generation stamp
    /// within that fence's runtime and connection slot.
    ///
    /// A successful handshake advances only to Connected.
    /// The session initializer remains responsible for
    /// explicitly establishing readiness.
    pub async fn run_with_fence<F, Fut>(
        url: &str,
        compiled: &CompiledWebSocketLimiters,
        shared_limiter: &RateLimitManager,
        policy: ReconnectPolicy,
        mut shutdown: oneshot::Receiver<()>,
        events: mpsc::Sender<ReconnectEvent>,
        fence: ConnectionFence,
        mut session: F,
    ) -> Result<(), ReconnectError>
    where
        F: FnMut(ConnectionStamp, WebSocketConnection) -> Fut,
        Fut: Future<Output = Result<SupervisorExit, WebSocketTransportError>>,
    {
        // An already-running or stopped fence cannot be
        // silently adopted by another reconnect runner.
        if fence.state() != ConnectionState::Idle {
            return Err(ReconnectError::Configuration(
                "reconnect fence must initially be Idle".into(),
            ));
        }

        // The guard revokes authority if this async function
        // returns, panics during unwinding, or is cancelled.
        let _shutdown_guard = FenceShutdownGuard::new(fence.clone());

        let mut consecutive_failures = 0_u32;

        loop {
            // -----------------------------------------------
            // BEGIN PHYSICAL GENERATION
            // -----------------------------------------------

            let stamp = fence.begin_connect()?;

            let generation = stamp.generation();

            let connection_id = allocate_connection_id()?;

            send_event(&events, ReconnectEvent::Connecting { generation }).await?;

            // -----------------------------------------------
            // CONNECTION ATTEMPT
            // -----------------------------------------------

            let connection = tokio::select! {
                _ = &mut shutdown => {
                    send_event(
                        &events,
                        ReconnectEvent::Stopped,
                    )
                    .await?;

                    return Ok(());
                }

                result = WebSocketConnector::connect(
                    url,
                    connection_id,
                    compiled,
                    shared_limiter,
                    policy.connection_timeout,
                ) => result,
            };

            // -----------------------------------------------
            // EXECUTE ONE PHYSICAL SESSION
            // -----------------------------------------------

            let failure_reason = match connection {
                Ok(connection) => {
                    // Only the matching generation may
                    // complete this handshake transition.
                    fence.connected(&stamp)?;

                    send_event(
                        &events,
                        ReconnectEvent::Connected {
                            generation,
                            connection_id,
                        },
                    )
                    .await?;

                    let connected_at = Instant::now();

                    let outcome = tokio::select! {
                        _ = &mut shutdown => {
                            send_event(
                                &events,
                                ReconnectEvent::Stopped,
                            )
                            .await?;

                            return Ok(());
                        }

                        result = session(
                            stamp.clone(),
                            connection,
                        ) => result,
                    };

                    if connected_at.elapsed() >= policy.stable_connection_duration {
                        consecutive_failures = 0;
                    }

                    match outcome {
                        Ok(SupervisorExit::PeerClosed) => "peer closed WebSocket".to_owned(),

                        Ok(SupervisorExit::Shutdown) => {
                            // The session requested terminal
                            // shutdown, not reconnection.
                            send_event(&events, ReconnectEvent::Stopped).await?;

                            return Ok(());
                        }

                        Err(error) => error.to_string(),
                    }
                }

                Err(error) => {
                    if matches!(
                        error,
                        WebSocketTransportError::Configuration(_)
                            | WebSocketTransportError::LimiterConfiguration(_)
                            | WebSocketTransportError::ConnectionLimiter(_)
                    ) {
                        // Terminal configuration failures
                        // must not leave Connecting authority.
                        return Err(ReconnectError::Transport(error));
                    }

                    error.to_string()
                }
            };

            // -----------------------------------------------
            // REVOKE BEFORE NOTIFICATION
            // -----------------------------------------------

            // All processing that requires Ready authority
            // must now reject this generation.
            fence.failed(&stamp)?;

            consecutive_failures = consecutive_failures.saturating_add(1);

            send_event(
                &events,
                ReconnectEvent::Disconnected {
                    generation,
                    reason: failure_reason,
                },
            )
            .await?;

            // -----------------------------------------------
            // RETRY EXHAUSTION
            // -----------------------------------------------

            if policy
                .maximum_consecutive_failures
                .is_some_and(|maximum| consecutive_failures >= maximum)
            {
                return Err(ReconnectError::RetryExhausted(consecutive_failures));
            }

            // -----------------------------------------------
            // BACKOFF
            // -----------------------------------------------

            let delay = policy.delay_for_failure(consecutive_failures);

            send_event(
                &events,
                ReconnectEvent::Backoff {
                    after_generation: generation,
                    delay,
                    consecutive_failures,
                },
            )
            .await?;

            tokio::select! {
                _ = &mut shutdown => {
                    send_event(
                        &events,
                        ReconnectEvent::Stopped,
                    )
                    .await?;

                    return Ok(());
                }

                _ = time::sleep(delay) => {}
            }
        }
    }
}
// ============================================================
// BOUNDED EVENT DELIVERY
// ============================================================

async fn send_event(
    sender: &mpsc::Sender<ReconnectEvent>,
    event: ReconnectEvent,
) -> Result<(), ReconnectError> {
    const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

    time::timeout(EVENT_TIMEOUT, sender.send(event))
        .await
        .map_err(|_| ReconnectError::EventDeliveryTimeout)?
        .map_err(|_| ReconnectError::EventReceiverClosed)
}
