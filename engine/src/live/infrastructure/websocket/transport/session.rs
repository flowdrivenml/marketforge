use std::time::Duration;

use thiserror::Error;

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::tungstenite::Message;

use super::connection::WebSocketConnection;
use super::error::WebSocketTransportError;
use super::supervisor::{OutboundMessage, SupervisorExit, WebSocketSupervisor};
use crate::live::infrastructure::websocket::heartbeat::plan::CompiledHeartbeatPlan;
use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, ConnectionStamp};
use crate::live::infrastructure::websocket::streams::shared_recovery::SharedStreamRecovery;
use crate::live::infrastructure::websocket::subscriptions::ack::CompiledSubscriptionAck;
use crate::live::infrastructure::websocket::subscriptions::driver::{
    SubscriptionCommand, SubscriptionDriver, SubscriptionDriverError, SubscriptionDriverEvent,
    SubscriptionDriverLimits,
};
use crate::live::infrastructure::websocket::subscriptions::event_monitor::{
    SubscriptionEventMonitor, SubscriptionEventMonitorError,
};
use crate::live::infrastructure::websocket::subscriptions::restoration_dispatch::{
    RestorationDispatchError, SubscriptionRestorationDispatcher,
};
use crate::live::infrastructure::websocket::subscriptions::session::SubscriptionSession;

// ============================================================
// CHANNEL LIMITS
// ============================================================

/// Operational queue capacities for one physical
/// WebSocket connection.
///
/// These bounds are independent of exchange limits.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionSessionLimits {
    pub inbound_capacity: usize,
    pub outbound_capacity: usize,
    pub command_capacity: usize,

    pub dispatch_timeout: Duration,
    pub shutdown_timeout: Duration,

    pub driver: SubscriptionDriverLimits,
    pub event_capacity: usize,
}

impl ConnectionSessionLimits {
    pub fn validate(&self) -> Result<(), ConnectionSessionError> {
        if self.inbound_capacity == 0
            || self.outbound_capacity == 0
            || self.command_capacity == 0
            || self.dispatch_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.event_capacity == 0
        {
            return Err(ConnectionSessionError::InvalidLimits);
        }

        self.driver
            .validate()
            .map_err(ConnectionSessionError::Driver)?;

        Ok(())
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum ConnectionSessionError {
    #[error("invalid connection session limits")]
    InvalidLimits,

    #[error("connection generation is stale")]
    StaleGeneration,

    #[error("subscription driver exited unexpectedly")]
    DriverExited,

    #[error("graceful WebSocket shutdown timed out")]
    ShutdownTimeout,

    #[error(transparent)]
    Driver(#[from] SubscriptionDriverError),

    #[error(transparent)]
    Dispatch(#[from] RestorationDispatchError),

    #[error(transparent)]
    Transport(#[from] WebSocketTransportError),
    #[error("restoration demand revision is stale")]
    StaleDemand,
    #[error(transparent)]
    EventMonitor(#[from] SubscriptionEventMonitorError),

    #[error("subscription event monitor exited unexpectedly")]
    EventMonitorExited,
}

// ============================================================
// SESSION INPUTS
// ============================================================

/// Resources owned by one physical connection session.
///
/// The caller retains the authoritative ConnectionFence.
///
/// Recovery state is moved into SubscriptionDriver.
/// It must not be recreated independently while the
/// same logical connection slot remains active.
pub struct ConnectionSessionInputs {
    pub stamp: ConnectionStamp,

    pub connection: WebSocketConnection,

    pub heartbeat: CompiledHeartbeatPlan,
    pub acknowledgements: CompiledSubscriptionAck,

    pub subscription_session: SubscriptionSession,
    pub recovery: SharedStreamRecovery,

    pub restoration_commands: Vec<SubscriptionCommand>,

    pub demand_revision: u64,
}

// ============================================================
// SESSION OUTPUTS
// ============================================================

/// Downstream consumers receive messages through bounded
/// channels owned by their authoritative runtime.
///
/// The connection session does not normalize or publish
/// these messages.
pub struct ConnectionSessionOutputs {
    pub market_data: mpsc::Sender<Message>,

    pub subscription_events: mpsc::Sender<SubscriptionDriverEvent>,
}

// ============================================================
// SESSION ORCHESTRATOR
// ============================================================

pub struct ConnectionSession;

impl ConnectionSession {
    /// Execute one physical WebSocket generation.
    ///
    /// ReconnectRunner remains the physical lifecycle
    /// authority and must revoke the generation after
    /// this session returns.
    ///
    /// Any driver failure, supervisor failure, or restoration
    /// dispatch failure terminates the session.
    ///
    /// Successful command dispatch is NOT subscription
    /// confirmation.
    ///
    /// The downstream owner must consume market-data and
    /// subscription-event channels for the entire session.
    #[allow(clippy::too_many_arguments)]
    pub async fn run<F>(
        inputs: ConnectionSessionInputs,
        outputs: ConnectionSessionOutputs,
        fence: &ConnectionFence,
        current_demand_revision: F,
        mut shutdown: oneshot::Receiver<()>,
        limits: ConnectionSessionLimits,
    ) -> Result<SupervisorExit, ConnectionSessionError>
    where
        F: Fn() -> u64,
    {
        limits.validate()?;

        // ----------------------------------------------------
        // PHYSICAL GENERATION
        // ----------------------------------------------------

        if fence.current_stamp().as_ref() != Some(&inputs.stamp) {
            return Err(ConnectionSessionError::StaleGeneration);
        }

        if !matches!(
            fence.state(),
            crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Connected
                | crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Ready
        ) {
            return Err(ConnectionSessionError::StaleGeneration);
        }
        if current_demand_revision() != inputs.demand_revision {
            return Err(ConnectionSessionError::StaleDemand);
        }

        if inputs
            .restoration_commands
            .iter()
            .any(|command| command.token.connection() != &inputs.stamp)
        {
            return Err(ConnectionSessionError::StaleGeneration);
        }
        // ----------------------------------------------------
        // DESTRUCTURE SESSION INPUTS
        // ----------------------------------------------------

        let ConnectionSessionInputs {
            stamp,
            connection,
            heartbeat,
            acknowledgements,
            subscription_session,
            recovery,
            restoration_commands,
            demand_revision,
        } = inputs;

        // ----------------------------------------------------
        // BOUNDED INTERNAL CHANNELS
        // ----------------------------------------------------

        let (inbound_tx, inbound_rx) = mpsc::channel(limits.inbound_capacity);

        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundMessage>(limits.outbound_capacity);

        let (command_tx, command_rx) = mpsc::channel(limits.command_capacity);
        let (internal_events_tx, internal_events_rx) = mpsc::channel(limits.event_capacity);
        let (supervisor_shutdown_tx, supervisor_shutdown_rx) = oneshot::channel();

        let (driver_shutdown_tx, driver_shutdown_rx) = oneshot::channel();

        // ----------------------------------------------------
        // PHYSICAL SOCKET SUPERVISOR
        // ----------------------------------------------------

        let supervisor = WebSocketSupervisor::run_with_plan(
            connection,
            inbound_tx,
            outbound_rx,
            supervisor_shutdown_rx,
            heartbeat,
        );

        tokio::pin!(supervisor);

        // ----------------------------------------------------
        // SUBSCRIPTION DRIVER
        // ----------------------------------------------------
        let ConnectionSessionOutputs {
            market_data,
            subscription_events,
        } = outputs;

        let recovery_for_monitor = recovery.clone();

        let driver = SubscriptionDriver::run_shared(
            subscription_session,
            recovery,
            acknowledgements,
            command_rx,
            outbound_tx,
            inbound_rx,
            market_data,
            internal_events_tx,
            driver_shutdown_rx,
            limits.driver,
        );

        tokio::pin!(driver);

        let event_monitor = SubscriptionEventMonitor::run(
            internal_events_rx,
            subscription_events,
            fence,
            &stamp,
            &recovery_for_monitor,
            demand_revision,
            &current_demand_revision,
        );

        tokio::pin!(event_monitor);
        // ----------------------------------------------------
        // INITIAL RESTORATION DISPATCH
        // ----------------------------------------------------

        let dispatcher = SubscriptionRestorationDispatcher::new(limits.dispatch_timeout)?;

        let dispatch = dispatcher.dispatch(
            restoration_commands,
            &command_tx,
            fence,
            &stamp,
            demand_revision,
            &current_demand_revision,
        );

        tokio::pin!(dispatch);

        let mut dispatch_complete = false;

        // ----------------------------------------------------
        // SINGLE SESSION LIFECYCLE
        // ----------------------------------------------------

        loop {
            tokio::select! {
                biased;

                // ============================================
                // TERMINAL SESSION SHUTDOWN
                // ============================================

                _ = &mut shutdown => {
                    // Request both components to terminate.
                    //
                    // The supervisor owns the actual WebSocket
                    // close handshake.
                    let _ = supervisor_shutdown_tx.send(());
                    let _ = driver_shutdown_tx.send(());

                    let outcome = time::timeout(
                        limits.shutdown_timeout,
                        &mut supervisor,
                    )
                    .await
                    .map_err(|_| {
                        ConnectionSessionError::ShutdownTimeout
                    })??;

                    return Ok(outcome);
                }

                // ============================================
                // PHYSICAL SOCKET EXIT
                // ============================================

                outcome = &mut supervisor => {
                    // Dropping the session also drops its
                    // driver, pending operations, and queues.
                    //
                    // ReconnectRunner revokes the physical
                    // generation before starting another.
                    return Ok(outcome?);
                }
                // ============================================
                // SUBSCRIPTION EVENT MONITOR
                // ============================================

                outcome = &mut event_monitor => {
                    match outcome {
                        Err(error) => {
                            // Any invalid completion, uncertain
                            // subscription failure, or downstream
                            // backpressure terminates this session.
                            //
                            // ReconnectRunner will revoke the
                            // physical generation.
                            return Err(
                                ConnectionSessionError::EventMonitor(
                                    error
                                )
                            );
                        }

                        Ok(()) => {
                            // The monitor must not independently
                            // terminate while this connection is
                            // still running.
                            return Err(
                                ConnectionSessionError::EventMonitorExited
                            );
                        }
                    }
                }
                // ============================================
                // SUBSCRIPTION DRIVER EXIT
                // ============================================

                outcome = &mut driver => {
                    match outcome {
                        Err(error) => {
                            return Err(
                                ConnectionSessionError::Driver(
                                    error
                                )
                            );
                        }

                        Ok(()) => {
                            // The driver must not terminate
                            // independently while its physical
                            // connection remains active.
                            return Err(
                                ConnectionSessionError::DriverExited
                            );
                        }
                    }
                }

                // ============================================
                // INITIAL DEMAND RESTORATION
                // ============================================

                outcome = &mut dispatch,
                    if !dispatch_complete =>
                {
                    match outcome {
                        Ok(receipt) => {
                            // Admission is complete.
                            //
                            // Actual socket transmission and
                            // ACK confirmation remain owned
                            // by the driver and supervisor.
                            debug_assert_eq!(
                                receipt.demand_revision,
                                demand_revision,
                            );

                            dispatch_complete = true;
                        }

                        Err(error) => {
                            // Including PartialAdmission:
                            // terminate rather than permit a
                            // partially restored session to
                            // continue indefinitely.
                            return Err(
                                ConnectionSessionError::Dispatch(
                                    error
                                )
                            );
                        }
                    }
                }
            }
        }
    }

    /// Adapter for the ReconnectRunner session callback.
    ///
    /// Session errors become retryable physical-session
    /// failures, not static connection configuration errors.
    pub fn into_transport_error(error: ConnectionSessionError) -> WebSocketTransportError {
        WebSocketTransportError::Session(error.to_string())
    }
}
