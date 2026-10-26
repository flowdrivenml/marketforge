use std::{
    collections::BTreeMap,
    future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::tungstenite::Message;

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::limiter::{RateLimitManager, factory::CompiledWebSocketLimiters},
};

use crate::live::infrastructure::websocket::heartbeat::plan::CompiledHeartbeatPlan;
use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, FencedMessage};
use crate::live::infrastructure::websocket::lifecycle::frame_relay::GenerationFrameRelay;
use crate::live::infrastructure::websocket::streams::shared_recovery::SharedStreamRecovery;
use crate::live::infrastructure::websocket::subscriptions::ack_binding::CompiledSubscriptionAckBinding;
use crate::live::infrastructure::websocket::subscriptions::driver::SubscriptionDriverEvent;
use crate::live::infrastructure::websocket::subscriptions::restoration::DesiredSubscriptionSnapshot;
use crate::live::infrastructure::websocket::transport::error::WebSocketTransportError;
use crate::live::infrastructure::websocket::transport::generation_bootstrap::{
    ConnectionBootstrapLimits, ConnectionGenerationBootstrap,
};
use crate::live::infrastructure::websocket::transport::reconnect::{
    ReconnectError, ReconnectEvent, ReconnectPolicy, ReconnectRunner,
};
use crate::live::infrastructure::websocket::transport::session::{
    ConnectionSession, ConnectionSessionLimits, ConnectionSessionOutputs,
};

// ============================================================
// DEMAND RECONCILIATION
// ============================================================

/// Maximum delay between periodic demand-revision checks
/// while a physical WebSocket connection is running.
///
/// This is a MarketForge operational setting, not an
/// exchange protocol requirement.
///
/// A production runtime may eventually replace polling
/// with revision-change notifications.
const DEMAND_RECONCILE_INTERVAL: Duration = Duration::from_millis(100);

// ============================================================
// MARKET-DATA OUTPUT MODE
// ============================================================

/// Internal market-data delivery strategy.
///
/// Raw mode preserves compatibility with existing callers.
///
/// Fenced mode delivers frames carrying the exact physical
/// connection-generation identity that received them.
///
/// This type is intentionally private.
enum MarketDataOutput {
    Raw(mpsc::Sender<Message>),

    Fenced(mpsc::Sender<FencedMessage<Message>>),
}

// ============================================================
// LOGICAL CONNECTION-SLOT RUNTIME
// ============================================================

/// Supervises one logical WebSocket connection slot.
///
/// One slot may contain multiple source subscriptions.
///
/// The logical slot survives physical reconnections.
///
/// ConnectionSlotRunner does not:
/// - Own authoritative consumer demand.
/// - Normalize market data.
/// - Validate trade/depth sequences.
/// - Publish canonical events.
///
/// ReconnectRunner remains the physical lifecycle authority.
pub struct ConnectionSlotRunner;

impl ConnectionSlotRunner {
    // ========================================================
    // LEGACY RAW OUTPUT
    // ========================================================

    /// Run one logical WebSocket connection slot using
    /// the existing raw market-data output contract.
    ///
    /// This entry point is retained for compatibility with
    /// existing MarketForge callers and regression tests.
    ///
    /// New manager integration should use run_fenced().
    ///
    /// The caller remains responsible for continuously
    /// consuming both output channels.
    #[allow(clippy::too_many_arguments)]
    pub async fn run<D, H>(
        url: &str,
        exchange: Arc<ExchangeConfig>,
        compiled_limiters: &CompiledWebSocketLimiters,
        shared_limiter: &RateLimitManager,
        reconnect_policy: ReconnectPolicy,
        shutdown: oneshot::Receiver<()>,
        reconnect_events: mpsc::Sender<ReconnectEvent>,
        fence: ConnectionFence,
        recovery: SharedStreamRecovery,
        connection_route: Option<String>,
        demand_snapshot: D,
        heartbeat_plan: H,
        ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
        outputs: ConnectionSessionOutputs,
        bootstrap_limits: ConnectionBootstrapLimits,
        session_limits: ConnectionSessionLimits,
    ) -> Result<(), ReconnectError>
    where
        D: Fn() -> DesiredSubscriptionSnapshot + Send + Sync + 'static,
        H: Fn() -> CompiledHeartbeatPlan + Send + Sync + 'static,
    {
        let ConnectionSessionOutputs {
            market_data,
            subscription_events,
        } = outputs;

        Self::run_internal(
            url,
            exchange,
            compiled_limiters,
            shared_limiter,
            reconnect_policy,
            shutdown,
            reconnect_events,
            fence,
            recovery,
            connection_route,
            demand_snapshot,
            heartbeat_plan,
            ack_bindings,
            MarketDataOutput::Raw(market_data),
            subscription_events,
            bootstrap_limits,
            session_limits,
        )
        .await
    }

    // ========================================================
    // FENCED OUTPUT
    // ========================================================

    /// Run one logical WebSocket connection slot with
    /// generation-aware native market-data output.
    ///
    /// Each physical connection generation receives its own
    /// bounded relay.
    ///
    /// Frames are stamped with their originating generation,
    /// never with whichever generation happens to be current
    /// when downstream processing occurs.
    ///
    /// Relay failures terminate the physical session and
    /// return control to ReconnectRunner.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_fenced<D, H>(
        url: &str,
        exchange: Arc<ExchangeConfig>,
        compiled_limiters: &CompiledWebSocketLimiters,
        shared_limiter: &RateLimitManager,
        reconnect_policy: ReconnectPolicy,
        shutdown: oneshot::Receiver<()>,
        reconnect_events: mpsc::Sender<ReconnectEvent>,
        fence: ConnectionFence,
        recovery: SharedStreamRecovery,
        connection_route: Option<String>,
        demand_snapshot: D,
        heartbeat_plan: H,
        ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
        market_data: mpsc::Sender<FencedMessage<Message>>,
        subscription_events: mpsc::Sender<SubscriptionDriverEvent>,
        bootstrap_limits: ConnectionBootstrapLimits,
        session_limits: ConnectionSessionLimits,
    ) -> Result<(), ReconnectError>
    where
        D: Fn() -> DesiredSubscriptionSnapshot + Send + Sync + 'static,
        H: Fn() -> CompiledHeartbeatPlan + Send + Sync + 'static,
    {
        Self::run_internal(
            url,
            exchange,
            compiled_limiters,
            shared_limiter,
            reconnect_policy,
            shutdown,
            reconnect_events,
            fence,
            recovery,
            connection_route,
            demand_snapshot,
            heartbeat_plan,
            ack_bindings,
            MarketDataOutput::Fenced(market_data),
            subscription_events,
            bootstrap_limits,
            session_limits,
        )
        .await
    }

    // ========================================================
    // SHARED IMPLEMENTATION
    // ========================================================

    /// Shared implementation for raw and fenced output.
    ///
    /// This method owns the internal orchestration contract.
    ///
    /// The future WebSocketManager will hide this complexity
    /// behind its attach/detach interface.
    #[allow(clippy::too_many_arguments)]
    async fn run_internal<D, H>(
        url: &str,
        exchange: Arc<ExchangeConfig>,
        compiled_limiters: &CompiledWebSocketLimiters,
        shared_limiter: &RateLimitManager,
        reconnect_policy: ReconnectPolicy,
        shutdown: oneshot::Receiver<()>,
        reconnect_events: mpsc::Sender<ReconnectEvent>,
        fence: ConnectionFence,
        recovery: SharedStreamRecovery,
        connection_route: Option<String>,
        demand_snapshot: D,
        heartbeat_plan: H,
        ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
        market_data_output: MarketDataOutput,
        subscription_events: mpsc::Sender<SubscriptionDriverEvent>,
        bootstrap_limits: ConnectionBootstrapLimits,
        session_limits: ConnectionSessionLimits,
    ) -> Result<(), ReconnectError>
    where
        D: Fn() -> DesiredSubscriptionSnapshot + Send + Sync + 'static,
        H: Fn() -> CompiledHeartbeatPlan + Send + Sync + 'static,
    {
        // ====================================================
        // PERSISTENT LOGICAL-SLOT RESOURCES
        // ====================================================

        // These closures survive physical reconnects.
        let demand_snapshot = Arc::new(demand_snapshot);
        let heartbeat_plan = Arc::new(heartbeat_plan);

        // The output destination also survives reconnects.
        //
        // Each physical generation creates its own local
        // raw-frame relay when fenced delivery is enabled.
        let market_data_output = Arc::new(market_data_output);

        // ReconnectRunner owns the authoritative fence.
        //
        // Session callbacks use clones of the same
        // underlying lifecycle authority.
        let session_fence = fence.clone();

        // ====================================================
        // PHYSICAL CONNECTION LIFECYCLE
        // ====================================================

        ReconnectRunner::run_with_fence(
            url,
            compiled_limiters,
            shared_limiter,
            reconnect_policy,
            shutdown,
            reconnect_events,
            fence,
            move |stamp, connection| {
                // ============================================
                // CLONE PERSISTENT SLOT RESOURCES
                // ============================================

                let exchange = exchange.clone();

                let recovery = recovery.clone();

                let fence = session_fence.clone();

                let demand_snapshot = demand_snapshot.clone();

                let heartbeat_plan = heartbeat_plan.clone();

                let ack_bindings = ack_bindings.clone();

                let connection_route = connection_route.clone();

                let market_data_output = market_data_output.clone();

                let subscription_events = subscription_events.clone();

                // ============================================
                // EXECUTE ONE PHYSICAL GENERATION
                // ============================================

                async move {
                    // ----------------------------------------
                    // READ CURRENT AUTHORITATIVE DEMAND
                    // ----------------------------------------

                    // Read demand after the physical handshake.
                    //
                    // Never restore subscriptions from a
                    // previous generation's command queue.
                    let desired = demand_snapshot();

                    let revision = desired.revision();

                    let current_revision_at_bootstrap = demand_snapshot().revision();

                    // ----------------------------------------
                    // GENERATE NATIVE TIMESTAMP
                    // ----------------------------------------

                    let unix_seconds = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|error| {
                            session_error(format!("system clock is before Unix epoch: {error}"))
                        })?
                        .as_secs();

                    // ----------------------------------------
                    // PREPARE THIS PHYSICAL GENERATION
                    // ----------------------------------------

                    let bootstrap = ConnectionGenerationBootstrap::prepare(
                        &exchange,
                        &fence,
                        &stamp,
                        connection_route.as_deref(),
                        &desired,
                        current_revision_at_bootstrap,
                        &recovery,
                        ack_bindings,
                        unix_seconds,
                        bootstrap_limits,
                    )
                    .map_err(|error| {
                        session_error(format!("connection bootstrap failed: {error}"))
                    })?;

                    // ----------------------------------------
                    // COMPILE SUBSCRIPTION ACK PROTOCOL
                    // ----------------------------------------

                    let acknowledgements = crate::live::infrastructure::websocket::subscriptions::ack::SubscriptionAckCompiler::compile(&exchange)
                        .map_err(|error| {
                            session_error(format!("ACK compilation failed: {error}"))
                        })?;

                    // ----------------------------------------
                    // CONSTRUCT PHYSICAL SESSION INPUTS
                    // ----------------------------------------

                    let inputs = bootstrap.into_session_inputs(
                        connection,
                        heartbeat_plan(),
                        acknowledgements,
                    );

                    // ========================================
                    // OUTPUT CONFIGURATION
                    // ========================================

                    // Raw mode:
                    //
                    //   ConnectionSession
                    //       -> original mpsc::Sender<Message>
                    //
                    // Fenced mode:
                    //
                    //   ConnectionSession
                    //       -> generation-local raw channel
                    //       -> GenerationFrameRelay
                    //       -> FencedMessage<Message>
                    //
                    // The relay is created for this physical
                    // generation only.

                    let (market_data, relay_input) = match market_data_output.as_ref() {
                        MarketDataOutput::Raw(sender) => (sender.clone(), None),

                        MarketDataOutput::Fenced(_) => {
                            let (sender, receiver) =
                                mpsc::channel::<Message>(session_limits.inbound_capacity);

                            (sender, Some(receiver))
                        }
                    };

                    let outputs = ConnectionSessionOutputs {
                        market_data,
                        subscription_events,
                    };

                    // ----------------------------------------
                    // CURRENT DEMAND REVISION PROVIDER
                    // ----------------------------------------

                    let current_revision = || demand_snapshot().revision();

                    // ========================================
                    // PHYSICAL SESSION SHUTDOWN
                    // ========================================

                    let (session_shutdown_sender, session_shutdown_receiver) = oneshot::channel();

                    // ========================================
                    // PHYSICAL SESSION
                    // ========================================

                    let session = ConnectionSession::run(
                        inputs,
                        outputs,
                        &fence,
                        current_revision,
                        session_shutdown_receiver,
                        session_limits,
                    );

                    tokio::pin!(session);

                    // ========================================
                    // GENERATION-LOCAL FRAME RELAY
                    // ========================================

                    // No relay is required for legacy raw
                    // output mode.
                    //
                    // Fenced mode creates one relay using
                    // this exact session stamp.
                    //
                    // Never replace this stamp with
                    // fence.current_stamp() while draining.

                    let relay = match (relay_input, market_data_output.as_ref()) {
                        (Some(receiver), MarketDataOutput::Fenced(sender)) => {
                            Some(GenerationFrameRelay::run(
                                receiver,
                                sender.clone(),
                                &fence,
                                stamp.clone(),
                            ))
                        }

                        (None, MarketDataOutput::Raw(_)) => None,

                        _ => {
                            return Err(session_error(
                                "inconsistent market-data output configuration".into(),
                            ));
                        }
                    };

                    // Pin the optional relay future so it
                    // can be polled repeatedly by select!.
                    tokio::pin!(relay);

                    // ========================================
                    // DEMAND REVISION WATCH
                    // ========================================

                    let mut demand_checks = time::interval(DEMAND_RECONCILE_INTERVAL);

                    demand_checks.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

                    // ========================================
                    // RUN UNTIL TERMINATION
                    // ========================================

                    let outcome = loop {
                        tokio::select! {
                            // --------------------------------
                            // PHYSICAL SESSION COMPLETED
                            // --------------------------------

                            result = &mut session => {
                                break result.map_err(
                                    ConnectionSession::into_transport_error,
                                );
                            }

                            // --------------------------------
                            // FRAME RELAY TERMINATED
                            // --------------------------------

                            result = async {
                                match relay.as_mut().as_pin_mut() {
                                    Some(relay_future) => {
                                        relay_future.await
                                    }

                                    None => {
                                        future::pending().await
                                    }
                                }
                            } => {
                                // The relay must not exit while
                                // the physical session is active.
                                //
                                // This includes:
                                // - Input channel closure.
                                // - Output backpressure.
                                // - Output receiver closure.
                                // - Stale generation.
                                //
                                // Returning a session error
                                // causes ReconnectRunner to
                                // revoke this generation.

                                break Err(session_error(format!(
                                    "generation frame relay terminated: {result:?}"
                                )));
                            }

                            // --------------------------------
                            // DESIRED SUBSCRIPTIONS CHANGED
                            // --------------------------------

                            _ = demand_checks.tick() => {
                                let latest_revision =
                                    demand_snapshot().revision();

                                if latest_revision != revision {
                                    // The current physical
                                    // subscription set is no
                                    // longer authoritative.
                                    //
                                    // ConnectionSlotRunner
                                    // currently reconciles
                                    // demand changes by
                                    // recycling the session.
                                    //
                                    // ReconnectRunner will
                                    // revoke this generation
                                    // before reconnecting.

                                    break Err(session_error(
                                        format!(
                                            "subscription demand changed \
                                             from revision {} to {}; \
                                             connection restoration required",
                                            revision,
                                            latest_revision,
                                        ),
                                    ));
                                }
                            }
                        }
                    };

                    // ========================================
                    // TERMINATE GENERATION-LOCAL RESOURCES
                    // ========================================

                    // Keep the shutdown sender alive until
                    // the session has finished or been
                    // cancelled.
                    //
                    // Dropping the session future cancels
                    // its supervisor and subscription driver.
                    //
                    // Dropping the relay future ensures no
                    // old-generation relay continues running.

                    drop(session);

                    drop(relay);

                    drop(session_shutdown_sender);

                    // ----------------------------------------
                    // RETURN TO RECONNECT AUTHORITY
                    // ----------------------------------------

                    outcome
                }
            },
        )
        .await
    }
}

// ============================================================
// ERROR CONVERSION
// ============================================================

fn session_error(message: String) -> WebSocketTransportError {
    WebSocketTransportError::Session(message)
}
