use std::{collections::BTreeMap, sync::Arc};

use crate::live::infrastructure::websocket::streams::request::StreamRequest;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};

use crate::live::config::exchange::ExchangeConfig;
use crate::live::infrastructure::websocket::streams::resolver::StreamSourceResolver;

use crate::live::infrastructure::websocket::routing::stream_dispatcher::{
    StreamBinding, StreamInput,
};
use crate::live::infrastructure::websocket::subscriptions::restoration::DesiredSubscriptionSnapshot;

use crate::live::infrastructure::websocket::slots::allocation::{SlotId, SlotKey};
use crate::live::infrastructure::websocket::slots::task::ConnectionSlotTaskConfig;

use crate::live::infrastructure::websocket::routing::message_router::NativeMessageRouter;
use crate::live::infrastructure::websocket::slots::managed::ManagedConnectionSlot;

use crate::live::infrastructure::websocket::streams::source::StreamSource;

use super::state::WebSocketManagerState;

// ============================================================
// INTERNAL SLOT CONFIGURATION FACTORY
// ============================================================

/// Resolves complete runner dependencies for one slot.
///
/// The infrastructure bootstrap supplies this factory.
///
/// It must use the existing compiled exchange profiles,
/// limiter infrastructure, and subscription protocols.
///
/// The factory must not open a WebSocket connection.
type SlotConfigFactory = dyn Fn(SlotId, &SlotKey, DesiredSubscriptionSnapshot) -> Result<ConnectionSlotTaskConfig, String>
    + Send
    + Sync;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum WebSocketManagerError {
    #[error("websocket manager is shut down")]
    Shutdown,

    #[error("websocket manager state is inconsistent: {0}")]
    InconsistentState(String),

    #[error("websocket manager initialization failed: {0}")]
    Initialization(String),

    #[error("connection-slot configuration failed: {0}")]
    Configuration(String),

    #[error("connection-slot operation failed: {0}")]
    ConnectionSlot(String),

    #[error("stream attachment failed: {0}")]
    Attachment(String),
    #[error("unknown stream binding")]
    UnknownBinding,

    #[error("cannot detach from a shared slot until coordinated slot replacement is implemented")]
    SharedSlotDetachRequiresReplacement,
}

// ============================================================
// MANAGER STATE
// ============================================================

struct ManagerInner {
    ownership: WebSocketManagerState,

    slots: BTreeMap<SlotId, ManagedConnectionSlot>,

    closed: bool,
}

// ============================================================
// WEBSOCKET MANAGER
// ============================================================

/// Runtime-facing WebSocket transport facade.
///
/// Owns logical attachments and managed connection slots.
///
/// Stream processors receive only StreamInput.
///
/// Connection creation, reconnection, ACK handling,
/// native routing, and limiters remain internal.
pub struct WebSocketManager {
    inner: Arc<Mutex<ManagerInner>>,

    slot_factory: Arc<SlotConfigFactory>,

    event_capacity: usize,

    maximum_early_frames: usize,
}

impl WebSocketManager {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    /// Construct the manager from validated runtime resources.
    ///
    /// This constructor is infrastructure-internal.
    ///
    /// Stream-facing callers use attach()/detach()/shutdown().
    pub(crate) fn new<F>(
        profiles: impl IntoIterator<Item = Arc<ExchangeConfig>>,
        slot_factory: F,
        event_capacity: usize,
        maximum_early_frames: usize,
    ) -> Result<Self, WebSocketManagerError>
    where
        F: Fn(
                SlotId,
                &SlotKey,
                DesiredSubscriptionSnapshot,
            ) -> Result<ConnectionSlotTaskConfig, String>
            + Send
            + Sync
            + 'static,
    {
        if event_capacity == 0 {
            return Err(WebSocketManagerError::Initialization(
                "event capacity must be positive".into(),
            ));
        }

        if maximum_early_frames == 0 {
            return Err(WebSocketManagerError::Initialization(
                "early-frame capacity must be positive".into(),
            ));
        }

        let ownership = WebSocketManagerState::new(profiles)
            .map_err(|error| WebSocketManagerError::Initialization(error.to_string()))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(ManagerInner {
                ownership,
                slots: BTreeMap::new(),
                closed: false,
            })),
            slot_factory: Arc::new(slot_factory),

            event_capacity,

            maximum_early_frames,
        })
    }
    /// Attach a declarative stream request.
    ///
    /// Exchange-native subscriptions and routing selectors
    /// are resolved internally from the already-loaded profile.
    ///
    /// No configuration files are loaded here.
    ///
    /// No limiter infrastructure is constructed here.
    ///
    /// This delegates physical connection management to
    /// the existing attach() implementation.
    pub async fn attach_request(
        &self,
        request: StreamRequest,
        destination: mpsc::Sender<StreamInput>,
    ) -> Result<StreamBinding, WebSocketManagerError> {
        // Obtain the already-loaded exchange profile.
        //
        // Do not hold the manager lock while performing the
        // asynchronous attachment operation.
        let exchange = {
            let inner = self.inner.lock().await;

            if inner.closed {
                return Err(WebSocketManagerError::Shutdown);
            }

            inner
                .ownership
                .profile(request.profile())
                .cloned()
                .ok_or_else(|| {
                    WebSocketManagerError::Configuration(format!(
                        "unknown exchange profile: {}",
                        request.profile(),
                    ))
                })?
        };

        let source = StreamSourceResolver::resolve(&exchange, &request)
            .map_err(|error| WebSocketManagerError::Configuration(error.to_string()))?;

        self.attach(source, destination).await
    }
    /// Observe terminal failure of one managed connection slot.
    ///
    /// This task does not retain the WebSocketManager itself.
    ///
    /// If the manager or managed slot has already been removed,
    /// the supervisor exits without modifying anything.
    ///
    /// On terminal failure, all attachments belonging to that
    /// specific slot are retired.
    fn supervise_slot(&self, slot_id: SlotId, slot: &mut ManagedConnectionSlot) {
        let Some((liveness, failure_rx)) = slot.take_failure_monitor() else {
            return;
        };

        // Weak ownership prevents this background task from
        // keeping the entire manager alive indefinitely.
        let manager = Arc::downgrade(&self.inner);

        tokio::spawn(async move {
            // Wait for an explicit terminal event-loop failure.
            //
            // A dropped sender means the task was cancelled
            // without emitting a failure notification.
            if failure_rx.await.is_err() {
                return;
            }

            let Some(manager) = manager.upgrade() else {
                return;
            };

            let mut inner = manager.lock().await;

            // The old slot may have been removed and replaced
            // while this supervisor waited for the manager lock.
            if liveness.upgrade().is_none() {
                return;
            }

            // Manager shutdown takes ownership of all slots
            // before stopping them. In that case there is
            // nothing for this supervisor to retire.
            if inner.closed {
                return;
            }

            let Some(failed_slot) = inner.slots.remove(&slot_id) else {
                return;
            };

            eprintln!(
                "[MarketForge WebSocket] Retiring failed slot {}",
                slot_id.get()
            );

            // Close only streams associated with the failed
            // connection slot.
            //
            // Other exchange connections are unaffected.
            if let Err(error) = inner.ownership.retire_slot_local(slot_id) {
                eprintln!(
                    "[MarketForge WebSocket] Failed to retire \
                 slot {} attachments: {error}",
                    slot_id.get()
                );
            }

            // Dropping the managed slot also cancels any
            // remaining task handles.
            drop(failed_slot);
        });
    }
    /// Detach one stream attachment.
    ///
    /// This implementation supports detaching the last stream
    /// assigned to a connection slot.
    ///
    /// Shared-slot detachment is deliberately rejected until
    /// coordinated slot replacement is implemented.
    ///
    /// Rejection does not modify local attachment state.
    /// Detach one stream from WebSocket transport.
    ///
    /// If other streams share the same connection slot,
    /// the slot is stopped and rebuilt with the remaining demand.
    ///
    /// No old physical generation remains active when the
    /// replacement connection starts.
    ///
    /// On a failed replacement, all remaining attachments
    /// belonging to this slot are retired.
    pub async fn detach(&self, binding: StreamBinding) -> Result<(), WebSocketManagerError> {
        let mut inner = self.inner.lock().await;

        if inner.closed {
            return Err(WebSocketManagerError::Shutdown);
        }

        let slot_id = inner
            .ownership
            .slot_for_binding(binding)
            .ok_or(WebSocketManagerError::UnknownBinding)?;

        // ========================================================
        // STOP OLD CONNECTION SLOT
        // ========================================================

        let slot = inner.slots.remove(&slot_id).ok_or_else(|| {
            WebSocketManagerError::InconsistentState(format!(
                "missing managed connection for slot {}",
                slot_id.get(),
            ))
        })?;

        // The old connection must stop before its subscriptions
        // and routing table are changed.
        //
        // shutdown() consumes the managed slot and terminates
        // its owned runner/event-loop tasks.
        let shutdown_result = slot.shutdown().await;

        // ========================================================
        // RETIRE REQUESTED ATTACHMENT
        // ========================================================

        let removal = match inner.ownership.detach_local(binding) {
            Ok(removal) => removal,

            Err(error) => {
                // Physical slot has already stopped.
                //
                // Do not leave its remaining logical streams
                // attached to a nonexistent connection.
                let cleanup = inner.ownership.retire_slot_local(slot_id);

                return Err(WebSocketManagerError::InconsistentState(format!(
                    "detachment failed: {error}; \
                         slot cleanup: {cleanup:?}"
                )));
            }
        };

        // ========================================================
        // LAST STREAM REMOVED
        // ========================================================

        let Some(desired) = removal.desired else {
            return shutdown_result
                .map_err(|error| WebSocketManagerError::ConnectionSlot(error.to_string()));
        };

        // ========================================================
        // NOTIFY SURVIVING STREAMS
        // ========================================================

        // The old transport authority has stopped.
        //
        // All surviving streams must observe a boundary
        // before their replacement connection is started.
        if let Err(error) = inner.ownership.disconnect_slot_local(slot_id) {
            let cleanup = inner.ownership.retire_slot_local(slot_id);

            return Err(WebSocketManagerError::ConnectionSlot(format!(
                "cannot notify surviving streams: {error}; \
                     retired attachments: {cleanup:?}"
            )));
        }

        // ========================================================
        // START REPLACEMENT SLOT
        // ========================================================

        // Rebuild the native router from the surviving sources.
        //
        // The new runner receives only the surviving
        // subscription snapshot.
        let replacement = self.start_slot(&inner.ownership, slot_id, desired);

        match replacement {
            Ok(slot) => {
                inner.slots.insert(slot_id, slot);
                let replacement_slot = inner
                    .slots
                    .get_mut(&slot_id)
                    .expect("replacement managed slot exists");

                self.supervise_slot(slot_id, replacement_slot);
                // A shutdown error is still reported. The
                // replacement was nevertheless constructed.
                if let Err(error) = shutdown_result {
                    return Err(WebSocketManagerError::ConnectionSlot(format!(
                        "old slot shutdown reported an error \
                             before replacement: {error}"
                    )));
                }

                Ok(())
            }

            Err(error) => {
                // Restart failed.
                //
                // A surviving stream must never retain an
                // attachment to a nonexistent managed slot.
                let cleanup = inner.ownership.retire_slot_local(slot_id);

                Err(WebSocketManagerError::ConnectionSlot(format!(
                    "replacement slot failed: {error}; \
                         remaining attachments retired: \
                         {cleanup:?}"
                )))
            }
        }
    }
    /// Shut down the entire WebSocket subsystem.
    ///
    /// Contract:
    ///
    /// - Prevent new attachments.
    /// - Stop every managed connection slot.
    /// - Retire every stream attachment.
    /// - Close stream input channels.
    /// - Leave no managed slot tasks running.
    ///
    /// This method is idempotent.
    ///
    /// A failure in one slot does not prevent attempts to shut
    /// down other slots.
    ///
    /// Once called, this manager cannot be restarted.
    pub async fn shutdown(&self) -> Result<(), WebSocketManagerError> {
        let mut inner = self.inner.lock().await;

        if inner.closed {
            return Ok(());
        }

        // Prevent subsequent attach operations.
        inner.closed = true;

        // Take ownership of every running slot.
        let slots = std::mem::take(&mut inner.slots);

        let mut first_error: Option<String> = None;

        // Shut down every slot, even if one reports failure.
        for (slot_id, slot) in slots {
            if let Err(error) = slot.shutdown().await {
                if first_error.is_none() {
                    first_error = Some(format!("slot {} shutdown failed: {error}", slot_id.get()));
                }
            }
        }

        // All managed connection slots have now been stopped
        // or had their task owners consumed by shutdown.
        //
        // Retire the stream destinations even if a slot
        // reported an error.
        inner.ownership.retire_all_local().map_err(|error| {
            WebSocketManagerError::InconsistentState(format!(
                "terminal attachment cleanup failed: {error}"
            ))
        })?;

        if let Some(error) = first_error {
            return Err(WebSocketManagerError::ConnectionSlot(error));
        }

        Ok(())
    }
    // ========================================================
    // ATTACH
    // ========================================================

    /// Attach one logical source stream to WebSocket transport.
    ///
    /// A successful return means that the stream has been
    /// registered and its subscription demand accepted.
    ///
    /// It does NOT mean the physical socket is connected
    /// or the exchange has confirmed its subscription.
    ///
    /// The stream receives StreamInput::Connected only after
    /// the existing subscription machinery validates the ACK.
    pub async fn attach(
        &self,
        source: StreamSource,
        destination: mpsc::Sender<StreamInput>,
    ) -> Result<StreamBinding, WebSocketManagerError> {
        let mut inner = self.inner.lock().await;

        if inner.closed {
            return Err(WebSocketManagerError::Shutdown);
        }

        let stream_id = source.stream_id().to_owned();

        // ====================================================
        // RESERVE LOCAL ATTACHMENT
        // ====================================================

        let attachment = inner
            .ownership
            .attach_local(source, destination)
            .map_err(|error| WebSocketManagerError::Attachment(error.to_string()))?;

        let binding = attachment.binding;

        let slot_id = attachment.slot_id;

        // ====================================================
        // NEW CONNECTION SLOT
        // ====================================================

        if attachment.created_slot {
            let start_result = self.start_slot(&inner.ownership, slot_id, attachment.desired);

            match start_result {
                Ok(managed_slot) => {
                    inner.slots.insert(slot_id, managed_slot);

                    let slot = inner
                        .slots
                        .get_mut(&slot_id)
                        .expect("newly inserted managed slot exists");

                    self.supervise_slot(slot_id, slot);

                    return Ok(binding);
                }

                Err(error) => {
                    // No managed slot was committed.
                    //
                    // Roll back the local attachment.
                    inner
                        .ownership
                        .detach_local(binding)
                        .map_err(|rollback_error| {
                            WebSocketManagerError::InconsistentState(format!(
                                "startup failed: {error}; \
                                     rollback failed: {rollback_error}"
                            ))
                        })?;

                    return Err(error);
                }
            }
        }

        // ====================================================
        // EXISTING CONNECTION SLOT
        // ====================================================

        let routing = inner
            .ownership
            .source(&stream_id)
            .ok_or_else(|| {
                WebSocketManagerError::InconsistentState("newly attached source is missing".into())
            })?
            .routing()
            .clone();

        let Some(managed_slot) = inner.slots.get_mut(&slot_id) else {
            // A logical assignment exists without its
            // physical-slot owner.
            //
            // Roll back the new attachment.
            inner.ownership.detach_local(binding).map_err(|error| {
                WebSocketManagerError::InconsistentState(format!(
                    "missing managed slot; rollback failed: {error}"
                ))
            })?;

            return Err(WebSocketManagerError::InconsistentState(format!(
                "no managed connection for slot {}",
                slot_id.get(),
            )));
        };

        // ====================================================
        // EXTEND ROUTING BEFORE SUBSCRIPTION DEMAND
        // ====================================================

        let result = managed_slot
            .extend_demand(attachment.desired, vec![(stream_id, routing)])
            .await;

        match result {
            Ok(()) => Ok(binding),

            Err(error) => {
                // The routing extension and desired-demand update
                // are separate operations.
                //
                // We cannot assume the slot is unchanged after
                // extend_demand() returns an error.
                //
                // Fail closed: retire the entire connection slot.

                let failed_slot = inner.slots.remove(&slot_id);

                // Stop the existing transport before removing
                // its remaining stream destinations.
                let shutdown_error = if let Some(slot) = failed_slot {
                    slot.shutdown().await.err().map(|error| error.to_string())
                } else {
                    None
                };

                // Retire ALL attachments belonging to this slot,
                // including the newly reserved attachment.
                //
                // This closes their input channels after queued
                // messages have been drained.
                let cleanup = inner.ownership.retire_slot_local(slot_id);

                match cleanup {
                    Ok(retired) => Err(WebSocketManagerError::ConnectionSlot(format!(
                        "subscription/routing update failed: {error}; \
                         retired {retired} affected attachments; \
                         shutdown error: {shutdown_error:?}"
                    ))),

                    Err(cleanup_error) => Err(WebSocketManagerError::InconsistentState(format!(
                        "subscription/routing update failed: {error}; \
                         failed to retire affected slot: \
                         {cleanup_error}; \
                         shutdown error: {shutdown_error:?}"
                    ))),
                }
            }
        }
    }

    // ========================================================
    // INTERNAL SLOT STARTUP
    // ========================================================

    fn start_slot(
        &self,
        ownership: &WebSocketManagerState,
        slot_id: SlotId,
        desired: DesiredSubscriptionSnapshot,
    ) -> Result<ManagedConnectionSlot, WebSocketManagerError> {
        let key = ownership.slot_key(slot_id).ok_or_else(|| {
            WebSocketManagerError::InconsistentState("connection slot key is missing".into())
        })?;

        // Build routing from exactly the source streams
        // assigned to this logical connection slot.
        let mut router = NativeMessageRouter::new();

        for source in ownership.sources_for_slot(slot_id) {
            router
                .register(source.stream_id(), source.routing().clone())
                .map_err(|error| {
                    WebSocketManagerError::Configuration(format!("invalid native routing: {error}"))
                })?;
        }

        // Resolve all existing connection-runner dependencies.
        //
        // The factory is infrastructure-owned, not part
        // of the stream-facing API.
        let config = (self.slot_factory)(slot_id, key, desired)
            .map_err(WebSocketManagerError::Configuration)?;

        ManagedConnectionSlot::start(
            config,
            router,
            ownership.dispatcher(),
            self.event_capacity,
            self.maximum_early_frames,
        )
        .map_err(|error| WebSocketManagerError::ConnectionSlot(error.to_string()))
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    pub async fn slot_count(&self) -> usize {
        self.inner.lock().await.slots.len()
    }

    pub async fn stream_count(&self) -> usize {
        self.inner.lock().await.ownership.stream_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_profile() -> Arc<ExchangeConfig> {
        let config = r#"
schema_version = 1

[exchange]
id = "binance"
market = "linear"
profile = "binance_linear"

[websocket]
base_url = "wss://example.invalid"
max_subscriptions_per_connection = 10

[channels.trades]
enabled = true
format = "trades/binance_linear"

[rest]
base_url = "https://example.invalid"
"#;

        Arc::new(toml::from_str(config).unwrap())
    }

    fn test_source(stream_id: &str, symbol: &str) -> StreamSource {
        StreamSource::new(
            "binance_linear",
            crate::live::infrastructure::websocket::subscriptions::restoration::DesiredSubscription::new(stream_id, "trades", symbol).unwrap(),
            vec![(
                "/stream".to_owned(),
                serde_json::json!(format!("{}@trade", symbol.to_ascii_lowercase())),
            )],
        )
        .unwrap()
    }
    #[test]
    fn rejects_zero_event_capacity() {
        let result = WebSocketManager::new(
            Vec::<Arc<ExchangeConfig>>::new(),
            |_slot_id, _key, _desired| unreachable!("factory must not run during construction"),
            0,
            16,
        );

        assert!(matches!(
            result,
            Err(WebSocketManagerError::Initialization(_))
        ));
    }

    #[test]
    fn rejects_zero_early_frame_capacity() {
        let result = WebSocketManager::new(
            Vec::<Arc<ExchangeConfig>>::new(),
            |_slot_id, _key, _desired| unreachable!("factory must not run during construction"),
            16,
            0,
        );

        assert!(matches!(
            result,
            Err(WebSocketManagerError::Initialization(_))
        ));
    }

    #[tokio::test]
    async fn new_manager_has_no_connections_or_streams() {
        let manager = WebSocketManager::new(
            Vec::<Arc<ExchangeConfig>>::new(),
            |_slot_id, _key, _desired| unreachable!("factory must not run during construction"),
            16,
            16,
        )
        .unwrap();

        assert_eq!(manager.slot_count().await, 0);
        assert_eq!(manager.stream_count().await, 0);
    }
    #[tokio::test]
    async fn shutdown_empty_manager_is_idempotent() {
        let manager = WebSocketManager::new(
            Vec::<Arc<ExchangeConfig>>::new(),
            |_slot_id, _key, _desired| unreachable!("no slot should be started"),
            16,
            16,
        )
        .unwrap();

        manager.shutdown().await.unwrap();
        manager.shutdown().await.unwrap();

        assert_eq!(manager.slot_count().await, 0);
        assert_eq!(manager.stream_count().await, 0);
    }

    #[tokio::test]
    async fn shutdown_manager_rejects_new_attachments() {
        let manager = WebSocketManager::new(
            Vec::<Arc<ExchangeConfig>>::new(),
            |_slot_id, _key, _desired| unreachable!("no slot should be started"),
            16,
            16,
        )
        .unwrap();

        manager.shutdown().await.unwrap();

        let source = StreamSource::new(
            "binance_linear",
            crate::live::infrastructure::websocket::subscriptions::restoration::DesiredSubscription::new("btc", "trades", "BTCUSDT").unwrap(),
            vec![("/stream".to_owned(), serde_json::json!("btcusdt@trade"))],
        )
        .unwrap();

        let (tx, _rx) = mpsc::channel(8);

        assert!(matches!(
            manager.attach(source, tx).await,
            Err(WebSocketManagerError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn failed_slot_startup_rolls_back_attachment() {
        let manager = WebSocketManager::new(
            [test_profile()],
            |_slot_id, _key, _desired| Err("injected slot configuration failure".into()),
            16,
            16,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(16);

        let result = manager
            .attach(test_source("btc-trades", "BTCUSDT"), tx)
            .await;

        assert!(matches!(
            result,
            Err(WebSocketManagerError::Configuration(_))
        ));

        // No physical connection was committed.
        assert_eq!(manager.slot_count().await, 0);

        // No phantom logical subscription remains.
        assert_eq!(manager.stream_count().await, 0);

        // The failed attachment no longer owns the sender.
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));

        manager.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn failed_attachment_does_not_poison_manager_state() {
        let manager = WebSocketManager::new(
            [test_profile()],
            |_slot_id, _key, _desired| Err("injected startup failure".into()),
            16,
            16,
        )
        .unwrap();

        let (btc_tx, _btc_rx) = mpsc::channel(16);

        assert!(
            manager
                .attach(test_source("btc-trades", "BTCUSDT"), btc_tx,)
                .await
                .is_err()
        );

        let (eth_tx, _eth_rx) = mpsc::channel(16);

        let result = manager
            .attach(test_source("eth-trades", "ETHUSDT"), eth_tx)
            .await;

        // ETH reaches the injected slot factory too.
        // It is not rejected because BTC left an attachment.
        assert!(matches!(
            result,
            Err(WebSocketManagerError::Configuration(_))
        ));

        assert_eq!(manager.stream_count().await, 0);
        assert_eq!(manager.slot_count().await, 0);

        manager.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn unknown_profile_does_not_allocate_slot() {
        let manager = WebSocketManager::new(
            Vec::<Arc<ExchangeConfig>>::new(),
            |_slot_id, _key, _desired| unreachable!("unknown profile must fail before factory"),
            16,
            16,
        )
        .unwrap();

        let (tx, _rx) = mpsc::channel(16);

        let result = manager
            .attach(test_source("btc-trades", "BTCUSDT"), tx)
            .await;

        assert!(matches!(result, Err(WebSocketManagerError::Attachment(_))));

        assert_eq!(manager.slot_count().await, 0);
        assert_eq!(manager.stream_count().await, 0);

        manager.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn shutdown_after_failed_startup_is_idempotent() {
        let manager = WebSocketManager::new(
            [test_profile()],
            |_slot_id, _key, _desired| Err("startup unavailable".into()),
            16,
            16,
        )
        .unwrap();

        let (tx, _rx) = mpsc::channel(16);

        assert!(
            manager
                .attach(test_source("btc-trades", "BTCUSDT"), tx,)
                .await
                .is_err()
        );

        manager.shutdown().await.unwrap();
        manager.shutdown().await.unwrap();

        assert_eq!(manager.slot_count().await, 0);
        assert_eq!(manager.stream_count().await, 0);
    }
}
