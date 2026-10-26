use std::{collections::BTreeMap, sync::Arc};

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::limiter::{RateLimitManager, factory::CompiledWebSocketLimiters},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};
use tokio_tungstenite::tungstenite::Message;

use super::{
    CompiledHeartbeatPlan, CompiledSubscriptionAckBinding, ConnectionBootstrapLimits,
    ConnectionFence, ConnectionFenceError, ConnectionSessionLimits, ConnectionSlotRunner,
    DesiredSubscriptionSnapshot, FencedMessage, ReconnectError, ReconnectEvent, ReconnectPolicy,
    SharedStreamRecovery, SubscriptionDriverEvent,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum ConnectionSlotTaskError {
    #[error("subscription demand revision must increase")]
    StaleRevision,

    #[error(transparent)]
    Fence(#[from] ConnectionFenceError),

    #[error(transparent)]
    Runner(#[from] ReconnectError),

    #[error(transparent)]
    Join(#[from] JoinError),
}

// ============================================================
// DESIRED SUBSCRIPTION STATE
// ============================================================

/// Runtime-owned desired subscriptions for one connection slot.
///
/// The existing ConnectionSlotRunner reads this through a
/// synchronous closure.
///
/// Updates are atomic from the reader's perspective.
///
/// This does not create a second consumer-demand authority.
/// The manager supplies projections from authoritative demand.
struct SlotDemand {
    sender: watch::Sender<DesiredSubscriptionSnapshot>,
    receiver: watch::Receiver<DesiredSubscriptionSnapshot>,
}

impl SlotDemand {
    fn new(initial: DesiredSubscriptionSnapshot) -> Self {
        let (sender, receiver) = watch::channel(initial);

        Self { sender, receiver }
    }

    fn update(
        &mut self,
        desired: DesiredSubscriptionSnapshot,
    ) -> Result<(), ConnectionSlotTaskError> {
        if desired.revision() <= self.receiver.borrow().revision() {
            return Err(ConnectionSlotTaskError::StaleRevision);
        }

        self.sender.send_replace(desired);

        Ok(())
    }

    fn snapshot(&self) -> DesiredSubscriptionSnapshot {
        self.receiver.borrow().clone()
    }

    fn reader(&self) -> watch::Receiver<DesiredSubscriptionSnapshot> {
        self.receiver.clone()
    }
}

// ============================================================
// SLOT CONFIGURATION
// ============================================================

/// Complete internal dependencies for one logical connection slot.
///
/// This is NOT part of the public stream-facing API.
///
/// WebSocketManager will construct this from exchange profiles,
/// compiled configuration, shared limiter infrastructure,
/// and the existing connection-slot assignment.
pub(crate) struct ConnectionSlotTaskConfig {
    pub runtime_id: String,
    pub slot_id: String,

    pub url: String,
    pub exchange: Arc<ExchangeConfig>,
    pub connection_route: Option<String>,

    pub compiled_limiters: CompiledWebSocketLimiters,
    pub shared_limiter: RateLimitManager,

    pub reconnect_policy: ReconnectPolicy,

    pub heartbeat_plan: CompiledHeartbeatPlan,

    pub ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,

    pub bootstrap_limits: ConnectionBootstrapLimits,
    pub session_limits: ConnectionSessionLimits,

    pub initial_demand: DesiredSubscriptionSnapshot,
}

// ============================================================
// CONNECTION SLOT TASK
// ============================================================

/// Owns the background task executing one logical
/// WebSocket connection slot.
///
/// Owns:
/// - The slot's connection fence.
/// - Its persistent WebSocket recovery coordinator.
/// - Its authoritative task shutdown handle.
/// - Its desired subscription projection.
/// - Its task join handle.
///
/// Does NOT own:
/// - Consumer demand.
/// - Normalization.
/// - Sequence integrity.
/// - Canonical publication.
pub(crate) struct ConnectionSlotTask {
    demand: SlotDemand,

    fence: ConnectionFence,

    shutdown: Option<oneshot::Sender<()>>,

    join: Option<JoinHandle<Result<(), ReconnectError>>>,
}

impl ConnectionSlotTask {
    // ========================================================
    // START
    // ========================================================

    pub fn demand_revision(&self) -> u64 {
        self.demand.snapshot().revision()
    }
    /// Internal cancellation handle for the supervised slot runner.
    ///
    /// Aborting the runner invokes its existing fence shutdown
    /// guard, revoking transport-generation authority.
    pub fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.join
            .as_ref()
            .expect("connection slot task must have a join handle")
            .abort_handle()
    }
    pub fn start_fenced(
        config: ConnectionSlotTaskConfig,
        market_data: mpsc::Sender<FencedMessage<Message>>,
        subscription_events: mpsc::Sender<SubscriptionDriverEvent>,
        reconnect_events: mpsc::Sender<ReconnectEvent>,
    ) -> Result<Self, ConnectionSlotTaskError> {
        // Persistent logical-slot authority.
        //
        // These objects must survive physical reconnects.
        let fence = ConnectionFence::new(config.runtime_id, config.slot_id)?;
        let runner_fence = fence.clone();

        let recovery = SharedStreamRecovery::new();

        let demand = SlotDemand::new(config.initial_demand);

        let demand_reader = demand.reader();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Move the complete slot resources into one
        // supervised Tokio task.
        let join = tokio::spawn(async move {
            let heartbeat = config.heartbeat_plan;

            let result = ConnectionSlotRunner::run_fenced(
                &config.url,
                config.exchange,
                &config.compiled_limiters,
                &config.shared_limiter,
                config.reconnect_policy,
                shutdown_rx,
                reconnect_events,
                runner_fence,
                recovery,
                config.connection_route,
                // The existing runner requests complete
                // snapshots and checks their revisions.
                move || demand_reader.borrow().clone(),
                // The heartbeat plan is compiled once.
                // Each connection generation receives a clone.
                move || heartbeat.clone(),
                config.ack_bindings,
                market_data,
                subscription_events,
                config.bootstrap_limits,
                config.session_limits,
            )
            .await;

            eprintln!("[MarketForge WebSocket] ConnectionSlotRunner terminated: {result:?}");

            result
        });

        Ok(Self {
            demand,
            fence,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        })
    }
    /// Shared authoritative lifecycle for this connection slot.
    ///
    /// The returned handle references the same state used by
    /// ConnectionSlotRunner.
    ///
    /// This is an internal interface, not part of the public
    /// WebSocketManager API.
    pub fn fence(&self) -> ConnectionFence {
        self.fence.clone()
    }
    // ========================================================
    // UPDATE DEMAND
    // ========================================================

    /// Replace the complete desired subscription snapshot.
    ///
    /// ConnectionSlotRunner detects the changed revision
    /// and restores subscriptions using its existing logic.
    pub fn update_demand(
        &mut self,
        desired: DesiredSubscriptionSnapshot,
    ) -> Result<(), ConnectionSlotTaskError> {
        self.demand.update(desired)
    }

    // ========================================================
    // SHUTDOWN
    // ========================================================

    /// Request shutdown and await the slot runner.
    ///
    /// No detached connection-slot task is left running
    /// after successful completion.
    pub async fn shutdown(mut self) -> Result<(), ConnectionSlotTaskError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(join) = self.join.take() {
            join.await??;
        }

        Ok(())
    }
}

// ============================================================
// DROP SAFETY
// ============================================================

impl Drop for ConnectionSlotTask {
    fn drop(&mut self) {
        // Prefer explicit shutdown(). This is a last-resort
        // cancellation path when the owner is dropped.
        //
        // Aborting the runner triggers the existing fence
        // shutdown guard, revoking its generation authority.
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::infrastructure::websocket::DesiredSubscription;

    fn snapshot(revision: u64, count: usize) -> DesiredSubscriptionSnapshot {
        let subscriptions = (0..count)
            .map(|index| {
                DesiredSubscription::new(
                    format!("stream-{index}"),
                    "trades",
                    format!("SYMBOL{index}"),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        DesiredSubscriptionSnapshot::new(revision, subscriptions).unwrap()
    }

    #[test]
    fn initial_demand_is_visible() {
        let demand = SlotDemand::new(snapshot(1, 2));

        let reader = demand.reader();

        assert_eq!(reader.borrow().revision(), 1);
        assert_eq!(reader.borrow().len(), 2);
    }

    #[test]
    fn demand_update_is_visible_to_existing_reader() {
        let mut demand = SlotDemand::new(snapshot(1, 1));

        let reader = demand.reader();

        demand.update(snapshot(2, 3)).unwrap();

        assert_eq!(reader.borrow().revision(), 2);
        assert_eq!(reader.borrow().len(), 3);

        assert_eq!(demand.snapshot().revision(), 2);
    }

    #[test]
    fn stale_revision_is_rejected() {
        let mut demand = SlotDemand::new(snapshot(5, 1));

        assert!(matches!(
            demand.update(snapshot(4, 2)),
            Err(ConnectionSlotTaskError::StaleRevision)
        ));

        assert!(matches!(
            demand.update(snapshot(5, 2)),
            Err(ConnectionSlotTaskError::StaleRevision)
        ));

        assert_eq!(demand.snapshot().revision(), 5);
        assert_eq!(demand.snapshot().len(), 1);
    }

    #[test]
    fn demand_updates_are_complete_snapshots() {
        let mut demand = SlotDemand::new(snapshot(1, 3));

        demand.update(snapshot(2, 1)).unwrap();

        let current = demand.snapshot();

        assert_eq!(current.revision(), 2);
        assert_eq!(current.len(), 1);

        assert!(current.get("stream-0").is_some());
        assert!(current.get("stream-1").is_none());
        assert!(current.get("stream-2").is_none());
    }
    #[test]
    fn cloned_connection_fence_shares_lifecycle_authority() {
        let original = ConnectionFence::new("test-runtime", "test-slot").unwrap();

        let shared = original.clone();

        let stamp = original.begin_connect().unwrap();

        // A transition through one handle must be visible
        // through the other.
        assert_eq!(shared.current_stamp(), Some(stamp.clone()));

        shared.connected(&stamp).unwrap();

        assert_eq!(original.state(), super::super::ConnectionState::Connected);

        original.failed(&stamp).unwrap();

        assert_eq!(shared.state(), super::super::ConnectionState::Backoff);
    }
}
