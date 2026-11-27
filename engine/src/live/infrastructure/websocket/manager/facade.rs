use std::{collections::BTreeMap, sync::Arc};

use crate::live::infrastructure::websocket::streams::request::StreamRequest;
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

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

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum MutationPoint {
    Reserved,
    RoutesInstalled,
    ReplyQueued,
    RouteRetired,
    RemovedForReplacement,
    ShuttingDown,
}

#[cfg(test)]
struct MutationBarrier {
    point: MutationPoint,
    arrived: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

type ShutdownResult = watch::Receiver<Option<Result<(), String>>>;

struct ManagerOperations {
    mutations: JoinSet<()>,
    shutdown: Option<(ShutdownResult, JoinHandle<()>)>,
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
#[derive(Clone)]
pub struct WebSocketManager {
    inner: Arc<Mutex<ManagerInner>>,

    operations: Arc<std::sync::Mutex<ManagerOperations>>,
    admission: Arc<Semaphore>,
    closing: watch::Sender<bool>,
    #[cfg(test)]
    barrier: Arc<std::sync::Mutex<Option<MutationBarrier>>>,

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
            operations: Arc::new(std::sync::Mutex::new(ManagerOperations {
                mutations: JoinSet::new(),
                shutdown: None,
            })),
            admission: Arc::new(Semaphore::new(event_capacity)),
            closing: watch::channel(false).0,
            #[cfg(test)]
            barrier: Arc::default(),

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
        self.attach_request_configured(request, destination, None)
            .await
    }

    /// Resolve instrument-aware native payloads using the same immutable format
    /// generation used by Source normalization. Performs no canonical routing.
    pub async fn attach_request_with_formats(
        &self,
        request: StreamRequest,
        destination: mpsc::Sender<StreamInput>,
        formats: &crate::live::config::FormatConfigStore,
    ) -> Result<StreamBinding, WebSocketManagerError> {
        self.attach_request_configured(request, destination, Some(formats))
            .await
    }

    async fn attach_request_configured(
        &self,
        request: StreamRequest,
        destination: mpsc::Sender<StreamInput>,
        formats: Option<&crate::live::config::FormatConfigStore>,
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

        let source = match formats {
            Some(formats) => {
                StreamSourceResolver::resolve_with_formats(&exchange, &request, Some(formats))
            }
            None => StreamSourceResolver::resolve(&exchange, &request),
        }
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
    #[cfg(test)]
    async fn checkpoint(&self, point: MutationPoint) {
        let barrier = {
            let mut slot = self.barrier.lock().unwrap();
            if slot.as_ref().is_some_and(|barrier| barrier.point == point) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(barrier) = barrier {
            let _ = barrier.arrived.send(());
            let _ = barrier.resume.await;
        }
    }

    /// Admit a mutation before spawning its owner. Waiting callers own no slot state.
    async fn start_mutation(
        &self,
        mutation: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), WebSocketManagerError> {
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| WebSocketManagerError::Shutdown)?;
        let mut operations = self.operations.lock().expect("manager operations lock");
        if operations.shutdown.is_some() {
            return Err(WebSocketManagerError::Shutdown);
        }
        while let Some(result) = operations.mutations.try_join_next() {
            if let Err(error) = result {
                tracing::error!(%error, "WebSocket manager mutation task failed");
            }
        }
        operations.mutations.spawn(async move {
            mutation.await;
            drop(permit);
        });
        Ok(())
    }

    /// Revoke local delivery while the slot reconciles surviving native demand.
    pub async fn detach(&self, binding: StreamBinding) -> Result<(), WebSocketManagerError> {
        let (reply, result) = oneshot::channel();
        let manager = self.clone();
        self.start_mutation(async move {
            let _ = reply.send(manager.detach_owned(binding).await);
        })
        .await?;
        result.await.map_err(|_| {
            WebSocketManagerError::InconsistentState("detachment owner exited".into())
        })?
    }

    async fn detach_owned(&self, binding: StreamBinding) -> Result<(), WebSocketManagerError> {
        let mut inner = self.inner.lock().await;

        if inner.closed {
            return Err(WebSocketManagerError::Shutdown);
        }

        let slot_id = inner
            .ownership
            .slot_for_binding(binding)
            .ok_or(WebSocketManagerError::UnknownBinding)?;

        let source = inner
            .ownership
            .source_for_binding(binding)
            .ok_or(WebSocketManagerError::UnknownBinding)?;
        let stream_id = source.stream_id().to_owned();
        let selector = source.routing().clone();
        let last = inner
            .ownership
            .slot_stream_count(slot_id)
            .map_err(|error| WebSocketManagerError::Attachment(error.to_string()))?
            == 1;

        if last {
            if let Some(slot) = inner.slots.get_mut(&slot_id) {
                // Revoke the route before removing the final destination.
                let _ = slot.retire_route(stream_id, selector).await;
            }
            inner
                .ownership
                .detach_local(binding)
                .map_err(|error| WebSocketManagerError::Attachment(error.to_string()))?;
            let slot = inner.slots.remove(&slot_id).ok_or_else(|| {
                WebSocketManagerError::InconsistentState("missing final managed slot".into())
            })?;
            drop(inner);
            return slot
                .shutdown()
                .await
                .map_err(|error| WebSocketManagerError::ConnectionSlot(error.to_string()));
        }

        {
            let slot = inner.slots.get_mut(&slot_id).ok_or_else(|| {
                WebSocketManagerError::InconsistentState("missing managed slot".into())
            })?;
            if slot.has_retirement_capacity()
                && slot.retire_route(stream_id, selector).await.is_ok()
            {
                #[cfg(test)]
                self.checkpoint(MutationPoint::RouteRetired).await;
                let desired = inner
                    .ownership
                    .detach_local(binding)
                    .map_err(|error| WebSocketManagerError::Attachment(error.to_string()))?
                    .desired
                    .expect("surviving subscriptions");
                let slot = inner
                    .slots
                    .get_mut(&slot_id)
                    .expect("managed slot checked above");
                if let Err(error) = slot.publish_demand(desired) {
                    let failed = inner.slots.remove(&slot_id);
                    let _ = inner.ownership.retire_slot_local(slot_id);
                    drop(inner);
                    drop(failed);
                    return Err(WebSocketManagerError::ConnectionSlot(error.to_string()));
                }
                return Ok(());
            }
        }

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
        #[cfg(test)]
        self.checkpoint(MutationPoint::RemovedForReplacement).await;
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
        let mut result = {
            let mut operations = self.operations.lock().expect("manager operations lock");
            if let Some((result, _)) = &operations.shutdown {
                result.clone()
            } else {
                self.admission.close();
                self.closing.send_replace(true);
                let mut mutations = std::mem::take(&mut operations.mutations);
                let (finish, result) = watch::channel(None);
                let manager = self.clone();
                let task = tokio::spawn(async move {
                    let mut failure = None;
                    while let Some(outcome) = mutations.join_next().await {
                        if let Err(error) = outcome {
                            failure.get_or_insert_with(|| error.to_string());
                        }
                    }
                    let outcome = manager
                        .shutdown_owned()
                        .await
                        .map_err(|error| error.to_string());
                    finish.send_replace(Some(outcome.and(failure.map_or(Ok(()), Err))));
                });
                operations.shutdown = Some((result.clone(), task));
                result
            }
        };
        loop {
            if let Some(outcome) = result.borrow_and_update().clone() {
                return outcome.map_err(WebSocketManagerError::ConnectionSlot);
            }
            result.changed().await.map_err(|_| {
                WebSocketManagerError::InconsistentState("shutdown owner exited".into())
            })?;
        }
    }

    async fn shutdown_owned(&self) -> Result<(), WebSocketManagerError> {
        let mut inner = self.inner.lock().await;

        if inner.closed {
            return Ok(());
        }

        // Prevent subsequent attach operations.
        inner.closed = true;

        // Take ownership of every running slot.
        let slots = std::mem::take(&mut inner.slots);
        #[cfg(test)]
        self.checkpoint(MutationPoint::ShuttingDown).await;

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
        let (reply, result) = oneshot::channel();
        let (claim, claimed) = oneshot::channel();
        let manager = self.clone();
        let mut closing = self.closing.subscribe();
        self.start_mutation(async move {
            let outcome = manager
                .attach_owned(source, destination, || reply.is_closed())
                .await;
            let binding = match outcome {
                Ok(binding) => binding,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return;
                }
            };
            if reply.send(Ok(binding)).is_ok() {
                #[cfg(test)]
                manager.checkpoint(MutationPoint::ReplyQueued).await;
                // A successful send is insufficient. The reply may be queued when
                // its receiver is dropped. The caller claims it before returning.
                tokio::select! {
                    claimed = claimed => if claimed.is_ok() { return; },
                    _ = closing.wait_for(|closed| *closed) => return,
                }
            }
            if let Err(error) = manager.detach_owned(binding).await {
                tracing::debug!(%error, "abandoned WebSocket attachment cleanup");
            }
        })
        .await?;
        let binding = result.await.map_err(|_| {
            WebSocketManagerError::InconsistentState("attachment owner exited".into())
        })??;
        claim
            .send(())
            .map_err(|_| WebSocketManagerError::Shutdown)?;
        Ok(binding)
    }

    async fn attach_owned(
        &self,
        source: StreamSource,
        destination: mpsc::Sender<StreamInput>,
        abandoned: impl FnOnce() -> bool,
    ) -> Result<StreamBinding, WebSocketManagerError> {
        let mut inner = self.inner.lock().await;

        if inner.closed || abandoned() {
            return Err(WebSocketManagerError::Shutdown);
        }

        let stream_id = source.stream_id().to_owned();

        // ====================================================
        // RESERVE LOCAL ATTACHMENT
        // ====================================================

        let attachment = {
            let selector = source.routing().clone();
            let ManagerInner {
                ownership, slots, ..
            } = &mut *inner;
            ownership
                .attach_local_eligible(source, destination, |id, remaining| {
                    slots
                        .get(&id)
                        .is_some_and(|slot| slot.accepts(&stream_id, &selector, remaining))
                })
                .map_err(|error| WebSocketManagerError::Attachment(error.to_string()))?
        };

        #[cfg(test)]
        self.checkpoint(MutationPoint::Reserved).await;
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

        let result = match managed_slot
            .install_routes(attachment.desired.revision(), vec![(stream_id, routing)])
            .await
        {
            Ok(()) => {
                #[cfg(test)]
                self.checkpoint(MutationPoint::RoutesInstalled).await;
                managed_slot.publish_demand(attachment.desired)
            }
            Err(error) => Err(error),
        };

        match result {
            Ok(()) => Ok(binding),

            Err(crate::live::infrastructure::websocket::slots::managed::ManagedConnectionSlotError::Routing(error)) => {
                inner.ownership.detach_local(binding)
                    .map_err(|rollback| WebSocketManagerError::InconsistentState(rollback.to_string()))?;
                Err(WebSocketManagerError::Attachment(error.to_string()))
            }

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

    async fn cancellation_fixture() -> (WebSocketManager, tokio::net::TcpListener) {
        use crate::live::infrastructure::limiter::{
            RateLimitManager,
            factory::{WebSocketLimiterBindings, WebSocketLimiterCompiler},
        };
        use crate::live::infrastructure::websocket::{
            ConnectionBootstrapLimits, ConnectionSessionLimits, HeartbeatPlanCompiler,
            ReconnectPolicy, SubscriptionDriverLimits,
        };
        use std::time::Duration;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let mut config = (*test_profile()).clone();
        config.websocket.heartbeat = Some(
            toml::from_str(
                r#"
            strategy = "websocket_control_frames"
            respond_to_ping = true
            echo_ping_payload = true
        "#,
            )
            .unwrap(),
        );
        config.websocket.subscription = Some(
            toml::from_str(
                r#"
            strategy = "json_rpc"
            subscribe_method = "SUBSCRIBE"
            unsubscribe_method = "UNSUBSCRIBE"
            request_id_field = "id"
            request_id_type = "signed_integer"
            success_field = "result"
            success_is_null = true
            supports_dynamic_subscriptions = true
            supports_unsubscribe = true
        "#,
            )
            .unwrap(),
        );
        config.channels.get_mut("trades").unwrap().stream_template =
            Some("{symbol_lower}@trade".into());
        let exchange = Arc::new(config);
        let compiled =
            WebSocketLimiterCompiler::compile(&exchange, &WebSocketLimiterBindings::default())
                .unwrap();
        let limiter = RateLimitManager::new(compiled.shared.policies.clone(), 16).unwrap();
        let heartbeat_plan = HeartbeatPlanCompiler::compile(&exchange).unwrap();
        let profile = exchange.clone();
        let manager = WebSocketManager::new(
            [profile],
            move |id, key, initial_demand| {
                Ok(ConnectionSlotTaskConfig {
                    runtime_id: "cancel-test".into(),
                    slot_id: id.get().to_string(),
                    url: url.clone(),
                    exchange: exchange.clone(),
                    connection_route: key.route.clone(),
                    compiled_limiters: compiled.clone(),
                    shared_limiter: limiter.clone(),
                    reconnect_policy: ReconnectPolicy::new(
                        Duration::from_millis(10),
                        Duration::from_millis(20),
                        Duration::from_secs(1),
                        Duration::from_secs(1),
                        None,
                    )
                    .unwrap(),
                    heartbeat_plan: heartbeat_plan.clone(),
                    ack_bindings: BTreeMap::new(),
                    bootstrap_limits: ConnectionBootstrapLimits {
                        maximum_pending: 4,
                        acknowledgement_timeout: Duration::from_secs(1),
                    },
                    session_limits: ConnectionSessionLimits {
                        inbound_capacity: 8,
                        outbound_capacity: 8,
                        command_capacity: 8,
                        event_capacity: 8,
                        dispatch_timeout: Duration::from_secs(1),
                        shutdown_timeout: Duration::from_secs(1),
                        driver: SubscriptionDriverLimits {
                            queue_timeout: Duration::from_secs(1),
                            receipt_timeout: Duration::from_secs(1),
                            maximum_early_frames: 8,
                        },
                    },
                    initial_demand,
                })
            },
            8,
            8,
        )
        .unwrap();
        (manager, listener)
    }

    fn arm(
        manager: &WebSocketManager,
        point: MutationPoint,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived, arrival) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        *manager.barrier.lock().unwrap() = Some(MutationBarrier {
            point,
            arrived,
            resume,
        });
        (arrival, release)
    }

    async fn bounded<T>(future: impl Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(5), future)
            .await
            .unwrap()
    }

    async fn mutations_finished(manager: &WebSocketManager) {
        let _permits = bounded(manager.admission.acquire_many(8)).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_attach_never_leaves_an_unclaimed_binding() {
        use std::task::Poll;
        let (manager, _listener) = cancellation_fixture().await;
        let (keep, _input) = mpsc::channel(8);
        let survivor = manager
            .attach(test_source("btc", "BTCUSDT"), keep)
            .await
            .unwrap();
        let locked = manager.inner.lock().await;
        let (tx, _rx) = mpsc::channel(8);
        let mut before = Box::pin(manager.attach(test_source("before", "XRPUSDT"), tx));
        assert!(matches!(futures_util::poll!(&mut before), Poll::Pending));
        drop(before);
        drop(locked);
        mutations_finished(&manager).await;
        for (index, point) in [
            MutationPoint::Reserved,
            MutationPoint::RoutesInstalled,
            MutationPoint::ReplyQueued,
        ]
        .into_iter()
        .enumerate()
        {
            let (arrival, release) = arm(&manager, point);
            let (tx, mut rx) = mpsc::channel(8);
            let mut attach = Box::pin(manager.attach(
                test_source(&format!("eth-{index}"), &format!("SYN{index}USDT")),
                tx,
            ));
            assert!(matches!(futures_util::poll!(&mut attach), Poll::Pending));
            bounded(arrival).await.unwrap();
            drop(attach);
            release.send(()).unwrap();
            mutations_finished(&manager).await;
            assert!(bounded(rx.recv()).await.is_none());
            assert_eq!(manager.stream_count().await, 1);
            assert_eq!(manager.slot_count().await, 1);
        }
        manager.detach(survivor).await.unwrap();
        assert_eq!(manager.slot_count().await, 0);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_unclaimed_attachment_is_unsubscribed_after_native_ack() {
        use futures_util::{SinkExt, StreamExt};
        use serde_json::{Value, json};
        use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
        async fn command(
            socket: &mut WebSocketStream<tokio::net::TcpStream>,
            method: &str,
            arg: &str,
        ) {
            let frame = bounded(socket.next()).await.unwrap().unwrap();
            let request: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            assert_eq!(request["method"], method);
            assert_eq!(request["params"], json!([arg]));
            socket
                .send(Message::Text(
                    json!({"result":null,"id":request["id"]}).to_string().into(),
                ))
                .await
                .unwrap();
        }

        let (manager, listener) = cancellation_fixture().await;
        let (btc, mut btc_rx) = mpsc::channel(8);
        manager
            .attach(test_source("btc", "BTCUSDT"), btc)
            .await
            .unwrap();
        let (tcp, _) = bounded(listener.accept()).await.unwrap();
        let mut socket = bounded(accept_async(tcp)).await.unwrap();
        command(&mut socket, "SUBSCRIBE", "btcusdt@trade").await;
        let Some(StreamInput::Connected(btc_epoch)) = bounded(btc_rx.recv()).await else {
            panic!("BTC must be active before the unclaimed mutation");
        };

        let (arrived, release) = arm(&manager, MutationPoint::ReplyQueued);
        let (eth, mut eth_rx) = mpsc::channel(8);
        let mut request = Box::pin(manager.attach(test_source("eth", "ETHUSDT"), eth));
        assert!(futures_util::poll!(&mut request).is_pending());
        bounded(arrived).await.unwrap();
        command(&mut socket, "SUBSCRIBE", "ethusdt@trade").await;
        assert!(matches!(
            bounded(eth_rx.recv()).await,
            Some(StreamInput::Connected(_))
        ));
        drop(request);
        release.send(()).unwrap();
        command(&mut socket, "UNSUBSCRIBE", "ethusdt@trade").await;
        assert!(bounded(eth_rx.recv()).await.is_none());
        socket
            .send(Message::Text(
                json!({"stream":"btcusdt@trade"}).to_string().into(),
            ))
            .await
            .unwrap();
        let Some(StreamInput::Data { acquisition, .. }) = bounded(btc_rx.recv()).await else {
            panic!("BTC must keep receiving data");
        };
        assert_eq!(acquisition, btc_epoch);
        assert_eq!(manager.stream_count().await, 1);
        drop(socket);
        bounded(manager.shutdown()).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_detach_retires_delivery_and_finishes_slot_replacement() {
        use std::task::Poll;
        let (manager, _listener) = cancellation_fixture().await;
        let (tx, _btc) = mpsc::channel(8);
        manager
            .attach(test_source("btc", "BTCUSDT"), tx)
            .await
            .unwrap();
        for (index, point) in [
            MutationPoint::RouteRetired,
            MutationPoint::RemovedForReplacement,
        ]
        .into_iter()
        .enumerate()
        {
            let (tx, mut rx) = mpsc::channel(8);
            let binding = manager
                .attach(
                    test_source(&format!("eth-{index}"), &format!("SYN{index}USDT")),
                    tx,
                )
                .await
                .unwrap();
            if point == MutationPoint::RemovedForReplacement {
                let mut inner = manager.inner.lock().await;
                let id = inner.ownership.slot_for_binding(binding).unwrap();
                inner
                    .slots
                    .get_mut(&id)
                    .unwrap()
                    .exhaust_retirement_capacity();
            }
            let (arrival, release) = arm(&manager, point);
            let mut detach = Box::pin(manager.detach(binding));
            assert!(matches!(futures_util::poll!(&mut detach), Poll::Pending));
            bounded(arrival).await.unwrap();
            drop(detach);
            release.send(()).unwrap();
            mutations_finished(&manager).await;
            assert!(bounded(rx.recv()).await.is_none());
            assert_eq!(manager.slot_count().await, 1);
            assert_eq!(manager.stream_count().await, 1);
        }
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_shutdown_waiter_leaves_owned_cleanup() {
        let (manager, _listener) = cancellation_fixture().await;
        let (tx, mut rx) = mpsc::channel(8);
        manager
            .attach(test_source("btc", "BTCUSDT"), tx)
            .await
            .unwrap();
        let (arrival, release) = arm(&manager, MutationPoint::ShuttingDown);
        let mut shutdown = Box::pin(manager.shutdown());
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        bounded(arrival).await.unwrap();
        drop(shutdown);
        release.send(()).unwrap();
        bounded(manager.shutdown()).await.unwrap();
        assert!(bounded(rx.recv()).await.is_none());
        assert_eq!(manager.stream_count().await, 0);
        assert_eq!(manager.slot_count().await, 0);
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
