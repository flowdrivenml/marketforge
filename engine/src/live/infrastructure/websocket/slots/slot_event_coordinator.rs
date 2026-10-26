use super::native_frame_dispatcher::{
    NativeFrameDispatchError, NativeFrameDispatchOutcome, NativeFrameDispatcher,
};
use super::native_message_router::NativeMessageSelector;
use super::stream_availability::{StreamAvailability, StreamAvailabilityError};
use super::{ConnectionFence, ConnectionStamp, FencedMessage, SubscriptionDriverEvent};
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;
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

        let stream_id = self.frames.classify(&message)?;

        if self.availability.is_connected(&stream_id) {
            let outcome = self.frames.dispatch(FencedMessage::new(stamp, message))?;

            return match outcome {
                NativeFrameDispatchOutcome::Delivered => Ok(()),

                NativeFrameDispatchOutcome::StreamUnavailable => {
                    Err(SlotEventCoordinatorError::DeliveryUnavailable)
                }
            };
        }

        // The stream has not yet received its confirmed
        // subscription notification.
        //
        // Buffer the original stamped message rather than
        // dropping it.
        //
        // Buffer capacity is shared across all streams
        // assigned to this connection slot.

        if self.early_count >= self.maximum_early_frames {
            return Err(SlotEventCoordinatorError::EarlyFrameOverflow);
        }

        self.early
            .entry(stream_id)
            .or_default()
            .push_back(FencedMessage::new(stamp, message));

        self.early_count += 1;

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
                        let outcome = self.frames.dispatch(frame)?;

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

    pub fn early_frame_count(&self) -> usize {
        self.early_count
    }

    pub fn connected_stream_count(&self) -> usize {
        self.availability.connected_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    use super::super::StreamInput;
    use super::super::stream_dispatch::StreamDispatcher;

    use super::super::native_message_router::{NativeMessageRouter, NativeMessageSelector};

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
