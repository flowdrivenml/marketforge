use crate::live::infrastructure::websocket::lifecycle::fence::{
    ConnectionFence, ConnectionStamp, FencedMessage,
};
use crate::live::infrastructure::websocket::routing::availability::{
    StreamAvailability, StreamAvailabilityError,
};
use crate::live::infrastructure::websocket::routing::frame_dispatcher::{
    NativeFrameDispatchError, NativeFrameDispatchOutcome, NativeFrameDispatcher,
};
use crate::live::infrastructure::websocket::routing::message_router::NativeMessageSelector;
use crate::live::infrastructure::websocket::subscriptions::driver::SubscriptionDriverEvent;
use crate::live::infrastructure::websocket::subscriptions::restoration::PendingRetirements;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum SlotEventCoordinatorError {
    #[error("invalid early-frame buffer capacity")]
    InvalidCapacity,

    #[error("early market-data buffer exhausted")]
    EarlyFrameOverflow,

    #[error("stale connection generation")]
    StaleGeneration,

    #[error("market-data delivery was unavailable after confirmation")]
    DeliveryUnavailable,

    #[error("subscription failed: {0}")]
    SubscriptionFailed(String),

    #[error(transparent)]
    Frame(#[from] NativeFrameDispatchError),

    #[error(transparent)]
    Availability(#[from] StreamAvailabilityError),
}

enum RetirementBoundary {
    Unpublished,
    Published(Option<ConnectionStamp>),
}

struct RetiredRoute {
    selector: NativeMessageSelector,
    boundary: RetirementBoundary,
}

/// The event loop removes selectors before releasing the manager's exclusion.
#[derive(Default)]
pub(crate) struct RouteRetirements {
    routes: Mutex<BTreeMap<String, RetiredRoute>>,
    pub changed: Notify,
}

impl RouteRetirements {
    pub fn blocks(&self, stream_id: &str, selector: &NativeMessageSelector) -> bool {
        let routes = self.routes.lock().expect("retired routes lock");
        routes.contains_key(stream_id) || routes.values().any(|route| &route.selector == selector)
    }

    pub fn len(&self) -> usize {
        self.routes.lock().expect("retired routes lock").len()
    }

    pub fn publish(&self, cutoff: Option<ConnectionStamp>) {
        let mut routes = self.routes.lock().expect("retired routes lock");
        for route in routes.values_mut() {
            if matches!(route.boundary, RetirementBoundary::Unpublished) {
                route.boundary = RetirementBoundary::Published(cutoff.clone());
            }
        }
        self.changed.notify_one();
    }
}

// ============================================================
// COORDINATOR
// ============================================================

/// Coordinates native frames and validated subscription
/// events for one logical connection slot.
///
/// All methods are synchronous.
///
/// The owning WebSocketManager must serialize calls.
/// Do not invoke these methods concurrently from
/// independent receiver tasks.
///
/// This component does not normalize market data or
/// perform trade/depth integrity checks.
pub(crate) struct SlotEventCoordinator {
    fence: ConnectionFence,

    frames: NativeFrameDispatcher,

    availability: StreamAvailability,

    early: BTreeMap<String, VecDeque<FencedMessage<Message>>>,

    early_count: usize,
    retired: Arc<RouteRetirements>,
    pending_retirements: PendingRetirements,

    maximum_early_frames: usize,
}

impl SlotEventCoordinator {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        fence: ConnectionFence,
        frames: NativeFrameDispatcher,
        availability: StreamAvailability,
        maximum_early_frames: usize,
    ) -> Result<Self, SlotEventCoordinatorError> {
        if maximum_early_frames == 0 {
            return Err(SlotEventCoordinatorError::InvalidCapacity);
        }

        Ok(Self {
            fence,
            frames,
            availability,
            early: BTreeMap::new(),
            early_count: 0,
            retired: Arc::default(),
            pending_retirements: PendingRetirements::default(),
            maximum_early_frames,
        })
    }

    // ========================================================
    // NATIVE FRAME
    // ========================================================
    /// Install additional native routing identities.
    ///
    /// This is serialized with incoming frame processing
    /// because SlotEventLoop is the exclusive mutable owner.
    ///
    /// Existing routes cannot be removed or changed here.
    pub fn extend_routes(
        &mut self,
        entries: Vec<(String, NativeMessageSelector)>,
    ) -> Result<(), SlotEventCoordinatorError> {
        self.frames.extend_routes(entries)?;

        Ok(())
    }
    pub fn retired_routes(&self) -> Arc<RouteRetirements> {
        self.retired.clone()
    }

    pub fn retirement_state(&mut self, pending: PendingRetirements) -> Arc<RouteRetirements> {
        self.pending_retirements = pending;
        self.retired.clone()
    }

    pub fn retire_route(&mut self, stream_id: String, selector: NativeMessageSelector) {
        if let Some(frames) = self.early.remove(&stream_id) {
            self.early_count -= frames.len();
        }
        self.availability.retire(&stream_id);
        self.retired
            .routes
            .lock()
            .expect("retired routes lock")
            .insert(
                stream_id,
                RetiredRoute {
                    selector,
                    boundary: RetirementBoundary::Unpublished,
                },
            );
    }

    pub fn reclaim_routes(&mut self) -> Result<(), SlotEventCoordinatorError> {
        let mut routes = self.retired.routes.lock().expect("retired routes lock");
        if routes.is_empty() {
            return Ok(());
        }
        let current = self.fence.current_stamp();
        let reclaim: Vec<_> = routes
            .iter()
            .filter_map(|(id, route)| match &route.boundary {
                RetirementBoundary::Published(None) => Some(id.clone()),
                RetirementBoundary::Published(Some(cutoff))
                    if current.as_ref().is_none_or(|stamp| {
                        stamp.generation().get() > cutoff.generation().get()
                    }) =>
                {
                    Some(id.clone())
                }
                _ => None,
            })
            .collect();
        for id in reclaim {
            self.frames.remove_route(&id)?;
            self.pending_retirements
                .lock()
                .expect("pending retirement lock")
                .remove(&id);
            routes.remove(&id);
        }
        Ok(())
    }

    pub fn on_frame(
        &mut self,
        frame: FencedMessage<Message>,
    ) -> Result<(), SlotEventCoordinatorError> {
        let (stamp, message) = frame.into_parts();

        // Do not process an obsolete physical generation.
        //
        // Connected is sufficient for native transport
        // delivery. Integrity readiness remains separate.
        let current = self.fence.with_connected(&stamp, || ());

        if current.is_none() {
            return Err(SlotEventCoordinatorError::StaleGeneration);
        }

        let mut routed = self.frames.route(message)?;
        {
            let retired = self.retired.routes.lock().expect("retired routes lock");
            routed.retain(|(id, _)| !retired.contains_key(id));
        }
        let needed = routed
            .iter()
            .filter(|(stream_id, _)| !self.availability.is_connected(stream_id))
            .count();
        if self.early_count.saturating_add(needed) > self.maximum_early_frames {
            return Err(SlotEventCoordinatorError::EarlyFrameOverflow);
        }
        // All records have been validated before admitting any native fragment.
        // Every fragment retains its original physical acquisition provenance.
        for (stream_id, message) in routed {
            if self.availability.is_connected(&stream_id) {
                let outcome = self.frames.dispatch_to(&stream_id, &stamp, message)?;
                if outcome != NativeFrameDispatchOutcome::Delivered {
                    return Err(SlotEventCoordinatorError::DeliveryUnavailable);
                }
            } else {
                self.early
                    .entry(stream_id)
                    .or_default()
                    .push_back(FencedMessage::new(stamp.clone(), message));
                self.early_count += 1;
            }
        }

        Ok(())
    }

    // ========================================================
    // VALIDATED SUBSCRIPTION EVENT
    // ========================================================

    pub fn on_subscription(
        &mut self,
        event: SubscriptionDriverEvent,
    ) -> Result<(), SlotEventCoordinatorError> {
        match event {
            SubscriptionDriverEvent::Confirmed { token, .. } => {
                let stream_id = token.stream_id().to_owned();

                if self
                    .retired
                    .routes
                    .lock()
                    .expect("retired routes lock")
                    .contains_key(&stream_id)
                {
                    return Ok(());
                }
                let stamp = token.connection().clone();

                // The event originates from the existing
                // SubscriptionEventMonitor, which validates
                // ACK correlation and recovery-token identity.
                //
                // The coordinator validates physical
                // generation authority again.

                self.availability
                    .subscription_confirmed(&stream_id, stamp.clone(), &self.fence)?;

                // Connected has now been admitted to the
                // stream's channel.
                //
                // Replay this stream's buffered frames
                // in their original arrival order.

                if let Some(mut pending) = self.early.remove(&stream_id) {
                    self.early_count -= pending.len();

                    while let Some(frame) = pending.pop_front() {
                        let (stamp, message) = frame.into_parts();
                        let outcome = self.frames.dispatch_to(&stream_id, &stamp, message)?;

                        if outcome != NativeFrameDispatchOutcome::Delivered {
                            return Err(SlotEventCoordinatorError::DeliveryUnavailable);
                        }
                    }
                }

                Ok(())
            }

            SubscriptionDriverEvent::Failed { completion } => {
                // The existing session machinery already
                // treats subscription failure as fatal.
                //
                // Do not establish stream availability.

                Err(SlotEventCoordinatorError::SubscriptionFailed(format!(
                    "{}: {:?}",
                    completion.stream_id, completion.kind,
                )))
            }
        }
    }

    // ========================================================
    // CONNECTION LOST
    // ========================================================

    /// Called after the physical generation has been revoked.
    ///
    /// No buffered frame from that generation may survive
    /// into the next physical connection.
    pub fn on_connection_lost(
        &mut self,
        stamp: &ConnectionStamp,
    ) -> Result<(), SlotEventCoordinatorError> {
        for queue in self.early.values_mut() {
            queue.retain(|frame| frame.stamp() != stamp);
        }

        self.early.retain(|_, queue| !queue.is_empty());

        self.early_count = self.early.values().map(VecDeque::len).sum();

        self.availability.connection_lost(stamp)?;

        Ok(())
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    #[cfg(test)]
    pub fn early_frame_count(&self) -> usize {
        self.early_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    use crate::live::infrastructure::websocket::routing::stream_dispatcher::StreamDispatcher;
    use crate::live::infrastructure::websocket::routing::stream_dispatcher::StreamInput;

    use crate::live::infrastructure::websocket::routing::message_router::{
        NativeMessageRouter, NativeMessageSelector,
    };

    // ========================================================
    // TEST FIXTURE
    // ========================================================

    fn setup(
        capacity: usize,
    ) -> (
        SlotEventCoordinator,
        ConnectionFence,
        ConnectionStamp,
        mpsc::Receiver<StreamInput>,
    ) {
        let fence = ConnectionFence::new("test-runtime", "test-slot").unwrap();

        let stamp = fence.begin_connect().unwrap();
        fence.connected(&stamp).unwrap();

        let streams = StreamDispatcher::new();

        let (tx, rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        let mut router = NativeMessageRouter::new();

        let selector = NativeMessageSelector::new(vec![(
            "/stream".to_owned(),
            Value::String("btcusdt@trade".into()),
        )])
        .unwrap();

        router.register("btc", selector).unwrap();

        let frames = NativeFrameDispatcher::new(fence.clone(), router, streams.clone());

        let availability = StreamAvailability::new(streams);

        let coordinator =
            SlotEventCoordinator::new(fence.clone(), frames, availability, capacity).unwrap();

        (coordinator, fence, stamp, rx)
    }

    fn native_frame(stamp: ConnectionStamp) -> FencedMessage<Message> {
        FencedMessage::new(
            stamp,
            Message::Text(
                json!({
                    "stream": "btcusdt@trade",
                    "data": {"p": "80000"}
                })
                .to_string()
                .into(),
            ),
        )
    }

    // ========================================================
    // EARLY-FRAME BUFFERING
    // ========================================================

    #[tokio::test]
    async fn reclamation_preserves_fenced_delivery_after_selector_reuse() {
        let (mut coordinator, fence, old, mut rx) = setup(8);
        let selector =
            NativeMessageSelector::new(vec![("/stream".into(), json!("btcusdt@trade"))]).unwrap();
        let state = coordinator.retired_routes();
        coordinator.retire_route("btc".into(), selector.clone());
        state.publish(Some(old.clone()));
        coordinator.reclaim_routes().unwrap();
        assert!(state.blocks("btc", &selector));
        coordinator.on_frame(native_frame(old.clone())).unwrap();
        assert_eq!(coordinator.early_frame_count(), 0);

        fence.failed(&old).unwrap();
        coordinator.on_connection_lost(&old).unwrap();
        let current = fence.begin_connect().unwrap();
        fence.connected(&current).unwrap();
        coordinator.reclaim_routes().unwrap();
        assert_eq!(state.len(), 0);
        coordinator
            .extend_routes(vec![("btc".into(), selector)])
            .unwrap();
        assert!(matches!(
            coordinator.on_frame(native_frame(old)),
            Err(SlotEventCoordinatorError::StaleGeneration)
        ));
        assert!(rx.try_recv().is_err());
        coordinator.on_frame(native_frame(current)).unwrap();
        assert_eq!(coordinator.early_frame_count(), 1);
    }

    #[tokio::test]
    async fn buffers_data_before_subscription_confirmation() {
        let (mut coordinator, _fence, stamp, mut rx) = setup(8);

        coordinator.on_frame(native_frame(stamp)).unwrap();

        assert_eq!(coordinator.early_frame_count(), 1);

        // No stream-facing message before confirmation.
        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // BUFFER CAPACITY
    // ========================================================

    #[tokio::test]
    async fn early_frame_capacity_is_bounded() {
        let (mut coordinator, _fence, stamp, _rx) = setup(1);

        coordinator.on_frame(native_frame(stamp.clone())).unwrap();

        let result = coordinator.on_frame(native_frame(stamp));

        assert!(matches!(
            result,
            Err(SlotEventCoordinatorError::EarlyFrameOverflow)
        ));
    }

    // ========================================================
    // DISCONNECTION CLEANUP
    // ========================================================

    #[tokio::test]
    async fn disconnect_retires_buffered_generation() {
        let (mut coordinator, fence, stamp, mut rx) = setup(8);

        coordinator.on_frame(native_frame(stamp.clone())).unwrap();

        assert_eq!(coordinator.early_frame_count(), 1);

        fence.failed(&stamp).unwrap();

        coordinator.on_connection_lost(&stamp).unwrap();

        assert_eq!(coordinator.early_frame_count(), 0);

        // This stream was never confirmed, so it was never
        // marked Connected.
        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // STALE GENERATION
    // ========================================================

    #[tokio::test]
    async fn stale_generation_cannot_buffer_data() {
        let (mut coordinator, fence, stamp, _rx) = setup(8);

        fence.failed(&stamp).unwrap();

        let result = coordinator.on_frame(native_frame(stamp));

        assert!(matches!(
            result,
            Err(SlotEventCoordinatorError::StaleGeneration)
        ));

        assert_eq!(coordinator.early_frame_count(), 0);
    }
}
