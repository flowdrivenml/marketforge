use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use thiserror::Error;

use tokio::{
    sync::{mpsc, oneshot, watch},
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

use crate::live::config::ExchangeConfig;
use crate::live::infrastructure::websocket::{
    streams::recovery::{RecoveryStreamKind, StreamRecoveryToken},
    subscriptions::{
        ack::SubscriptionAction,
        command_factory::SubscriptionCommandFactory,
        resolver::SubscriptionResolver,
        restoration::{DesiredSubscription, DesiredSubscriptionSnapshot},
    },
};

pub(crate) struct DemandReconciliation<'a> {
    pub exchange: &'a ExchangeConfig,
    pub factory: SubscriptionCommandFactory,
    pub initial: DesiredSubscriptionSnapshot,
    pub desired: &'a (dyn Fn() -> DesiredSubscriptionSnapshot + Sync),
    pub changes: Option<watch::Receiver<DesiredSubscriptionSnapshot>>,
}

impl DemandReconciliation<'_> {
    async fn run(
        mut self,
        initial_commands: Vec<SubscriptionCommand>,
        commands: mpsc::Sender<SubscriptionCommand>,
        mut events: mpsc::Receiver<SubscriptionDriverEvent>,
        downstream: mpsc::Sender<SubscriptionDriverEvent>,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        recovery: &SharedStreamRecovery,
        send_timeout: Duration,
    ) -> Result<(), ConnectionSessionError> {
        let failure = |detail: &str| ConnectionSessionError::Reconciliation(detail.into());
        let config = self
            .exchange
            .websocket
            .subscription
            .as_ref()
            .ok_or_else(|| failure("missing subscription configuration"))?;
        let mut acknowledged: BTreeMap<String, (DesiredSubscription, StreamRecoveryToken)> =
            BTreeMap::new();
        let mut initial_commands = initial_commands.into_iter();
        let mut checks = time::interval(Duration::from_millis(25));
        checks.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut latest = self
            .changes
            .as_mut()
            .map_or_else(self.desired, |changes| changes.borrow_and_update().clone());
        loop {
            if let Some(changes) = &mut self.changes {
                if changes
                    .has_changed()
                    .map_err(|_| failure("desired demand closed"))?
                {
                    latest = changes.borrow_and_update().clone();
                }
            } else {
                latest = (self.desired)();
            }
            let next = if let Some(mut command) = initial_commands.next() {
                let id = command.token.stream_id();
                let subscription = self
                    .initial
                    .get(id)
                    .expect("initial command has desired subscription");
                if latest.get(id) != Some(subscription) {
                    recovery
                        .lock()
                        .map_err(|e| failure(&e.to_string()))?
                        .retire_subscription(&command.token);
                    continue;
                }
                self.refresh_initial_timestamp(subscription, &mut command)?;
                Some((subscription.clone(), command))
            } else {
                if let Some(retirements) = &latest.retirements {
                    retirements
                        .lock()
                        .map_err(|_| failure("retirement state poisoned"))?
                        .retain(|id| latest.get(id).is_some() || acknowledged.contains_key(id));
                }
                if let Some((id, (subscription, token))) = acknowledged
                    .iter()
                    .find(|(id, (subscription, _))| latest.get(id) != Some(subscription))
                {
                    if config.supports_unsubscribe != Some(true) {
                        return Err(failure(
                            "native unsubscribe unavailable; restoration required",
                        ));
                    }
                    if latest.get(id).is_some() {
                        return Err(failure("native identity replaced; restoration required"));
                    }
                    Some((
                        subscription.clone(),
                        self.operation(
                            subscription,
                            token.clone(),
                            SubscriptionAction::Unsubscribe,
                        )?,
                    ))
                } else if let Some(subscription) = latest
                    .iter()
                    .find(|subscription| !acknowledged.contains_key(&subscription.stream_id))
                {
                    if config.supports_dynamic_subscriptions != Some(true) {
                        return Err(failure(
                            "dynamic subscribe unavailable; restoration required",
                        ));
                    }
                    let kind = match subscription.channel_name.as_str() {
                        "trades" => RecoveryStreamKind::Trades,
                        "depth" => RecoveryStreamKind::Depth,
                        _ => return Err(failure("unsupported Source channel")),
                    };
                    let token = {
                        let mut recovery = recovery.lock().map_err(|e| failure(&e.to_string()))?;
                        recovery
                            .register(&subscription.stream_id, kind)
                            .map_err(|e| failure(&e.to_string()))?;
                        recovery
                            .begin_binding(fence, stamp, &subscription.stream_id)
                            .map_err(|e| failure(&e.to_string()))?
                    };
                    Some((
                        subscription.clone(),
                        self.operation(subscription, token, SubscriptionAction::Subscribe)?,
                    ))
                } else {
                    None
                }
            };

            let Some((subscription, command)) = next else {
                tokio::select! {
                    outcome = async {
                        if let Some(changes) = &mut self.changes {
                            changes.changed().await.map_err(|_| failure("desired demand closed"))?;
                            latest = changes.borrow_and_update().clone();
                        } else {
                            checks.tick().await;
                        }
                        Ok::<(), ConnectionSessionError>(())
                    } => {
                        outcome?;
                        continue;
                    },
                    _ = events.recv() => return Err(failure("unexpected subscription completion")),
                }
            };
            let action = command.expected.action;
            let token = command.token.clone();
            time::timeout(send_timeout, commands.send(command))
                .await
                .map_err(|_| failure("subscription command admission timed out"))?
                .map_err(|_| failure("subscription driver unavailable"))?;
            // A wire outcome belongs to the original token even when demand changes.
            // Serializing through ACK completion also preserves ID-less correlation.
            let event = events
                .recv()
                .await
                .ok_or_else(|| failure("subscription event monitor unavailable"))?;
            match &event {
                SubscriptionDriverEvent::Confirmed { token: actual, .. } if actual == &token => {}
                _ => {
                    return Err(failure(
                        "subscription failed or completed with obsolete authority",
                    ));
                }
            }
            match action {
                SubscriptionAction::Subscribe => {
                    acknowledged.insert(
                        subscription.stream_id.clone(),
                        (subscription.clone(), token),
                    );
                    let still_desired = self.changes.as_ref().map_or_else(
                        || (self.desired)().get(&subscription.stream_id) == Some(&subscription),
                        |changes| {
                            changes.borrow().get(&subscription.stream_id) == Some(&subscription)
                        },
                    );
                    if still_desired {
                        downstream
                            .try_send(event)
                            .map_err(|_| failure("subscription event backpressure"))?;
                    }
                }
                SubscriptionAction::Unsubscribe => {
                    acknowledged.remove(&subscription.stream_id);
                    recovery
                        .lock()
                        .map_err(|e| failure(&e.to_string()))?
                        .retire_subscription(&token);
                }
            }
        }
    }

    fn refresh_initial_timestamp(
        &self,
        subscription: &DesiredSubscription,
        command: &mut SubscriptionCommand,
    ) -> Result<(), ConnectionSessionError> {
        use crate::live::infrastructure::websocket::subscriptions::protocol::SubscriptionStrategy;
        let error = |detail: String| ConnectionSessionError::Reconciliation(detail);
        if self
            .exchange
            .websocket
            .subscription
            .as_ref()
            .is_none_or(|config| config.strategy != "gate_v4_channel_event")
        {
            return Ok(());
        }
        let context = subscription
            .resolution_context()
            .map_err(|e| error(e.to_string()))?;
        let mut resolved =
            SubscriptionResolver::resolve(self.exchange, &subscription.channel_name, &context)
                .map_err(|e| error(e.to_string()))?;
        debug_assert_eq!(
            resolved.protocol.strategy(),
            SubscriptionStrategy::GateV4ChannelEvent
        );
        resolved.request.request_id = command.expected.request_id.clone();
        resolved.request.unix_seconds = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| error(e.to_string()))?
                .as_secs(),
        );
        command.wire_text = resolved
            .protocol
            .subscribe(&resolved.request)
            .map_err(|e| error(e.to_string()))?;
        Ok(())
    }

    fn operation(
        &mut self,
        subscription: &DesiredSubscription,
        token: StreamRecoveryToken,
        action: SubscriptionAction,
    ) -> Result<SubscriptionCommand, ConnectionSessionError> {
        let error = |detail: String| ConnectionSessionError::Reconciliation(detail);
        let context = subscription
            .resolution_context()
            .map_err(|e| error(e.to_string()))?;
        let resolved =
            SubscriptionResolver::resolve(self.exchange, &subscription.channel_name, &context)
                .map_err(|e| error(e.to_string()))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| error(e.to_string()))?;
        self.factory
            .operation(self.exchange, resolved, token, action, now.as_secs())
            .map_err(|e| error(e.to_string()))
    }
}

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

    #[error("subscription reconciliation failed: {0}")]
    Reconciliation(String),
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
        shutdown: oneshot::Receiver<()>,
        limits: ConnectionSessionLimits,
    ) -> Result<SupervisorExit, ConnectionSessionError>
    where
        F: Fn() -> u64,
    {
        Self::run_internal(
            inputs,
            outputs,
            fence,
            current_demand_revision,
            shutdown,
            limits,
            None,
        )
        .await
    }

    pub(crate) async fn run_reconciled(
        inputs: ConnectionSessionInputs,
        outputs: ConnectionSessionOutputs,
        fence: &ConnectionFence,
        shutdown: oneshot::Receiver<()>,
        limits: ConnectionSessionLimits,
        reconciliation: DemandReconciliation<'_>,
    ) -> Result<SupervisorExit, ConnectionSessionError> {
        let revision = inputs.demand_revision;
        Self::run_internal(
            inputs,
            outputs,
            fence,
            move || revision,
            shutdown,
            limits,
            Some(reconciliation),
        )
        .await
    }

    async fn run_internal<F: Fn() -> u64>(
        inputs: ConnectionSessionInputs,
        outputs: ConnectionSessionOutputs,
        fence: &ConnectionFence,
        current_demand_revision: F,
        mut shutdown: oneshot::Receiver<()>,
        limits: ConnectionSessionLimits,
        reconciliation: Option<DemandReconciliation<'_>>,
    ) -> Result<SupervisorExit, ConnectionSessionError> {
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

        let (validated_tx, validated_rx) = mpsc::channel(limits.event_capacity);
        let monitored = if reconciliation.is_some() {
            validated_tx
        } else {
            subscription_events.clone()
        };
        let event_monitor = SubscriptionEventMonitor::run(
            internal_events_rx,
            monitored,
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

        let dispatch = async {
            if let Some(reconciliation) = reconciliation {
                return reconciliation
                    .run(
                        restoration_commands,
                        command_tx,
                        validated_rx,
                        subscription_events,
                        fence,
                        &stamp,
                        &recovery_for_monitor,
                        limits.dispatch_timeout,
                    )
                    .await;
            }
            let dispatcher = SubscriptionRestorationDispatcher::new(limits.dispatch_timeout)?;
            dispatcher
                .dispatch(
                    restoration_commands,
                    &command_tx,
                    fence,
                    &stamp,
                    demand_revision,
                    &current_demand_revision,
                )
                .await?;
            Ok::<_, ConnectionSessionError>(())
        };
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
                        Ok(()) => dispatch_complete = true,
                        Err(error) => return Err(error),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::live::infrastructure::websocket::{
        ConnectionSessionError, ConnectionSessionLimits, SubscriptionDriverLimits,
    };

    fn valid_limits() -> ConnectionSessionLimits {
        ConnectionSessionLimits {
            inbound_capacity: 1024,
            outbound_capacity: 256,
            command_capacity: 128,
            event_capacity: 128,

            dispatch_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(2),

            driver: SubscriptionDriverLimits {
                queue_timeout: Duration::from_secs(2),
                receipt_timeout: Duration::from_secs(2),
                maximum_early_frames: 1024,
            },
        }
    }

    #[test]
    fn accepts_bounded_session_configuration() {
        valid_limits().validate().unwrap();
    }

    #[test]
    fn rejects_zero_inbound_capacity() {
        let mut limits = valid_limits();

        limits.inbound_capacity = 0;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::InvalidLimits),
        ));
    }

    #[test]
    fn rejects_zero_outbound_capacity() {
        let mut limits = valid_limits();

        limits.outbound_capacity = 0;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::InvalidLimits),
        ));
    }

    #[test]
    fn rejects_zero_command_capacity() {
        let mut limits = valid_limits();

        limits.command_capacity = 0;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::InvalidLimits),
        ));
    }

    #[test]
    fn rejects_zero_dispatch_timeout() {
        let mut limits = valid_limits();

        limits.dispatch_timeout = Duration::ZERO;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::InvalidLimits),
        ));
    }

    #[test]
    fn rejects_zero_shutdown_timeout() {
        let mut limits = valid_limits();

        limits.shutdown_timeout = Duration::ZERO;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::InvalidLimits),
        ));
    }

    #[test]
    fn rejects_invalid_subscription_driver_limits() {
        let mut limits = valid_limits();

        limits.driver.maximum_early_frames = 0;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::Driver(_)),
        ));
    }

    #[test]
    fn rejects_zero_event_capacity() {
        let mut limits = valid_limits();

        limits.event_capacity = 0;

        assert!(matches!(
            limits.validate(),
            Err(ConnectionSessionError::InvalidLimits),
        ));
    }
}
