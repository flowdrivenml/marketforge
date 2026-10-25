use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::{mpsc, oneshot};

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::limiter::{RateLimitManager, factory::CompiledWebSocketLimiters},
};

use super::{
    CompiledHeartbeatPlan, CompiledSubscriptionAckBinding, ConnectionBootstrapLimits,
    ConnectionFence, ConnectionGenerationBootstrap, ConnectionSession, ConnectionSessionLimits,
    ConnectionSessionOutputs, DesiredSubscriptionSnapshot, ReconnectError, ReconnectEvent,
    ReconnectPolicy, ReconnectRunner, SharedStreamRecovery, WebSocketTransportError,
};

// ============================================================
// LOGICAL CONNECTION-SLOT RUNTIME
// ============================================================

pub struct ConnectionSlotRunner;

impl ConnectionSlotRunner {
    /// Run one logical WebSocket connection slot across
    /// multiple physical connection generations.
    ///
    /// ReconnectRunner remains the physical lifecycle owner.
    ///
    /// SharedStreamRecovery must be the persistent recovery
    /// authority for this logical slot.
    ///
    /// demand_snapshot must return a complete, internally
    /// consistent snapshot assigned to this connection route.
    ///
    /// The authoritative demand owner must maintain
    /// monotonically increasing revisions.
    ///
    /// Subscription events and market data must be consumed
    /// continuously by downstream owners.
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
        // ----------------------------------------------------
        // PERSISTENT SLOT RESOURCES
        // ----------------------------------------------------

        let demand_snapshot = Arc::new(demand_snapshot);
        let heartbeat_plan = Arc::new(heartbeat_plan);

        let market_data = outputs.market_data;
        let subscription_events = outputs.subscription_events;

        let session_fence = fence.clone();

        // ----------------------------------------------------
        // EXISTING RECONNECT AUTHORITY
        // ----------------------------------------------------

        ReconnectRunner::run_with_fence(
            url,
            compiled_limiters,
            shared_limiter,
            reconnect_policy,
            shutdown,
            reconnect_events,
            fence,
            move |stamp, connection| {
                // These clones retain handles to persistent
                // logical-slot resources.
                //
                // They do not create new recovery authorities.

                let exchange = exchange.clone();
                let recovery = recovery.clone();
                let fence = session_fence.clone();

                let demand_snapshot = demand_snapshot.clone();
                let heartbeat_plan = heartbeat_plan.clone();

                let ack_bindings = ack_bindings.clone();

                let connection_route = connection_route.clone();

                let market_data = market_data.clone();
                let subscription_events = subscription_events.clone();

                async move {
                    // ----------------------------------------
                    // READ CURRENT AUTHORITATIVE DEMAND
                    // ----------------------------------------

                    // This happens AFTER the physical
                    // connection handshake.
                    //
                    // Never restore subscriptions from a
                    // previous generation's command queue.

                    let desired = demand_snapshot();

                    let revision = desired.revision();

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
                    // BOOTSTRAP THIS GENERATION
                    // ----------------------------------------

                    let bootstrap = ConnectionGenerationBootstrap::prepare(
                        &exchange,
                        &fence,
                        &stamp,
                        connection_route.as_deref(),
                        &desired,
                        revision,
                        &recovery,
                        ack_bindings,
                        unix_seconds,
                        bootstrap_limits,
                    )
                    .map_err(|error| {
                        session_error(format!("connection bootstrap failed: {error}"))
                    })?;

                    // ----------------------------------------
                    // CREATE PHYSICAL SESSION INPUTS
                    // ----------------------------------------

                    let acknowledgements = super::SubscriptionAckCompiler::compile(&exchange)
                        .map_err(|error| {
                            session_error(format!("ACK compilation failed: {error}"))
                        })?;

                    let inputs = bootstrap.into_session_inputs(
                        connection,
                        heartbeat_plan(),
                        acknowledgements,
                    );

                    let outputs = ConnectionSessionOutputs {
                        market_data,
                        subscription_events,
                    };

                    // ----------------------------------------
                    // DEMAND REVISION PROVIDER
                    // ----------------------------------------

                    let current_revision = || demand_snapshot().revision();

                    // ----------------------------------------
                    // PHYSICAL SESSION LIFETIME
                    // ----------------------------------------

                    // The outer ReconnectRunner owns terminal
                    // shutdown.
                    //
                    // Keep the sender alive while this
                    // physical session runs.
                    //
                    // Dropping the sender prematurely would
                    // close the receiver and trigger immediate
                    // ConnectionSession shutdown.

                    let (session_shutdown_sender, session_shutdown_receiver) = oneshot::channel();

                    let outcome = ConnectionSession::run(
                        inputs,
                        outputs,
                        &fence,
                        current_revision,
                        session_shutdown_receiver,
                        session_limits,
                    )
                    .await;

                    drop(session_shutdown_sender);

                    // ----------------------------------------
                    // RETURN CONTROL TO RECONNECT RUNNER
                    // ----------------------------------------

                    outcome.map_err(|error| ConnectionSession::into_transport_error(error))
                }
            },
        )
        .await
    }
}

fn session_error(message: String) -> WebSocketTransportError {
    WebSocketTransportError::Session(message)
}
