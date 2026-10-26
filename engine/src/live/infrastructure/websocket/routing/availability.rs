use std::collections::BTreeMap;

use thiserror::Error;

use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, ConnectionStamp};
use crate::live::infrastructure::websocket::lifecycle::state::ConnectionState;

use super::stream_dispatcher::{StreamDispatchError, StreamDispatcher};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum StreamAvailabilityError {
    #[error("subscription confirmation belongs to a stale connection")]
    StaleConnection,

    #[error("stream is already bound to another connection generation: {0}")]
    ConflictingBinding(String),

    #[error(transparent)]
    Delivery(#[from] StreamDispatchError),
}

// ============================================================
// STREAM AVAILABILITY COORDINATOR
// ============================================================

/// Translates validated subscription and connection-lifecycle
/// events into stream-facing availability notifications.
///
/// This component does not:
/// - Open WebSocket connections.
/// - Classify subscription ACKs.
/// - Perform normalization.
/// - Check trade or depth sequences.
/// - Perform market-data integrity recovery.
///
/// Only the existing subscription protocol authority may
/// establish that an exchange subscription was confirmed.
///
/// The future WebSocketManager owns this component.
pub(crate) struct StreamAvailability {
    streams: StreamDispatcher,

    /// Last confirmed transport generation for each stream.
    active: BTreeMap<String, ConnectionStamp>,
}

impl StreamAvailability {
    pub fn new(streams: StreamDispatcher) -> Self {
        Self {
            streams,
            active: BTreeMap::new(),
        }
    }

    // ========================================================
    // SUBSCRIPTION CONFIRMED
    // ========================================================

    /// Called only after SubscriptionEventMonitor has
    /// validated a positive subscription ACK and its
    /// current recovery token.
    ///
    /// Physical connection establishment alone must never
    /// invoke this method.
    pub fn subscription_confirmed(
        &mut self,
        stream_id: &str,
        stamp: ConnectionStamp,
        fence: &ConnectionFence,
    ) -> Result<(), StreamAvailabilityError> {
        // Check that this ACK belongs to the current
        // physical connection generation.
        if fence.current_stamp().as_ref() != Some(&stamp)
            || !matches!(
                fence.state(),
                ConnectionState::Connected | ConnectionState::Ready
            )
        {
            return Err(StreamAvailabilityError::StaleConnection);
        }

        if let Some(existing) = self.active.get(stream_id) {
            // Duplicate confirmation from the same generation
            // does not produce a second Connected event.
            if existing == &stamp {
                return Ok(());
            }

            // The previous generation must first be revoked.
            return Err(StreamAvailabilityError::ConflictingBinding(
                stream_id.to_owned(),
            ));
        }

        // StreamDispatcher serializes notification admission
        // with the stream's native data delivery.
        //
        // Failed notification admission must not register
        // this generation as active.
        self.streams.connected(stream_id, stamp.clone())?;

        self.active.insert(stream_id.to_owned(), stamp);

        Ok(())
    }

    // ========================================================
    // PHYSICAL CONNECTION LOST
    // ========================================================

    /// Invalidate every logical stream whose confirmed
    /// subscription belongs to the given physical generation.
    ///
    /// ReconnectRunner must already have revoked the
    /// connection's transport authority before this is called.
    ///
    /// Any delivery error is fatal to the owning slot's
    /// coordination task; it must not be silently ignored.
    pub fn connection_lost(
        &mut self,
        stamp: &ConnectionStamp,
    ) -> Result<usize, StreamAvailabilityError> {
        // Collect all streams bound to the revoked generation.
        //
        // Do not modify the map while iterating over it.
        let affected: Vec<String> = self
            .active
            .iter()
            .filter_map(|(stream_id, current)| (current == stamp).then_some(stream_id.clone()))
            .collect();

        let affected_count = affected.len();

        // Remember the first notification failure, but continue
        // revoking every affected stream.
        //
        // One slow or closed destination must not leave other
        // streams authorized to receive stale data.
        let mut first_error = None;

        for stream_id in affected {
            let result = self.streams.disconnected(&stream_id, stamp);

            // Remove availability authority regardless of
            // notification delivery success.
            self.active.remove(&stream_id);

            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        // Report failed notification delivery after every
        // affected stream has been processed.
        //
        // The owning connection supervisor must treat this as
        // an explicit terminal delivery failure.
        if let Some(error) = first_error {
            return Err(StreamAvailabilityError::Delivery(error));
        }

        Ok(affected_count)
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    pub fn is_connected(&self, stream_id: &str) -> bool {
        self.active.contains_key(stream_id)
    }

    #[cfg(test)]
    pub fn connected_count(&self) -> usize {
        self.active.len()
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::mpsc;

    use crate::live::infrastructure::websocket::routing::stream_dispatcher::StreamInput;

    fn connected_fence(slot: &str) -> (ConnectionFence, ConnectionStamp) {
        let fence = ConnectionFence::new("test-runtime", slot).unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        (fence, stamp)
    }

    // ========================================================
    // VALID CONFIRMATION
    // ========================================================

    #[tokio::test]
    async fn confirmed_subscription_notifies_stream() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(8);

        dispatcher.attach("btc".into(), tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher);

        let (fence, stamp) = connected_fence("slot-1");

        availability
            .subscription_confirmed("btc", stamp, &fence)
            .unwrap();

        assert!(availability.is_connected("btc"));

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // DUPLICATE CONFIRMATION
    // ========================================================

    #[tokio::test]
    async fn duplicate_confirmation_is_idempotent() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(8);

        dispatcher.attach("btc".into(), tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher);

        let (fence, stamp) = connected_fence("slot-1");

        availability
            .subscription_confirmed("btc", stamp.clone(), &fence)
            .unwrap();

        availability
            .subscription_confirmed("btc", stamp, &fence)
            .unwrap();

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        // No duplicate Connected notification.
        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // STALE CONFIRMATION
    // ========================================================

    #[tokio::test]
    async fn stale_confirmation_is_rejected() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(8);

        dispatcher.attach("btc".into(), tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher);

        let (fence, stamp) = connected_fence("slot-1");

        fence.failed(&stamp).unwrap();

        let result = availability.subscription_confirmed("btc", stamp, &fence);

        assert!(matches!(
            result,
            Err(StreamAvailabilityError::StaleConnection)
        ));

        assert!(!availability.is_connected("btc"));
        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // DISCONNECT MULTIPLE STREAMS
    // ========================================================

    #[tokio::test]
    async fn disconnect_notifies_all_affected_streams() {
        let dispatcher = StreamDispatcher::new();

        let (btc_tx, mut btc_rx) = mpsc::channel(8);
        let (eth_tx, mut eth_rx) = mpsc::channel(8);

        dispatcher.attach("btc".into(), btc_tx).unwrap();
        dispatcher.attach("eth".into(), eth_tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher);

        let (fence, stamp) = connected_fence("slot-1");

        availability
            .subscription_confirmed("btc", stamp.clone(), &fence)
            .unwrap();

        availability
            .subscription_confirmed("eth", stamp.clone(), &fence)
            .unwrap();

        // ReconnectRunner revokes the physical generation
        // before emitting the disconnect notification.
        fence.failed(&stamp).unwrap();

        let affected = availability.connection_lost(&stamp).unwrap();

        assert_eq!(affected, 2);
        assert_eq!(availability.connected_count(), 0);

        assert!(matches!(btc_rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(
            btc_rx.recv().await,
            Some(StreamInput::Disconnected)
        ));

        assert!(matches!(eth_rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(
            eth_rx.recv().await,
            Some(StreamInput::Disconnected)
        ));
    }

    // ========================================================
    // CONNECTION ISOLATION
    // ========================================================

    #[tokio::test]
    async fn disconnect_only_affects_matching_slot() {
        let dispatcher = StreamDispatcher::new();

        let (btc_tx, mut btc_rx) = mpsc::channel(8);
        let (eth_tx, mut eth_rx) = mpsc::channel(8);

        dispatcher.attach("btc".into(), btc_tx).unwrap();
        dispatcher.attach("eth".into(), eth_tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher);

        let (fence_a, stamp_a) = connected_fence("slot-a");
        let (fence_b, stamp_b) = connected_fence("slot-b");

        availability
            .subscription_confirmed("btc", stamp_a.clone(), &fence_a)
            .unwrap();

        availability
            .subscription_confirmed("eth", stamp_b.clone(), &fence_b)
            .unwrap();

        fence_a.failed(&stamp_a).unwrap();

        let affected = availability.connection_lost(&stamp_a).unwrap();

        assert_eq!(affected, 1);

        assert!(!availability.is_connected("btc"));
        assert!(availability.is_connected("eth"));

        assert!(matches!(btc_rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(
            btc_rx.recv().await,
            Some(StreamInput::Disconnected)
        ));

        assert!(matches!(eth_rx.recv().await, Some(StreamInput::Connected)));

        assert!(eth_rx.try_recv().is_err());
    }

    // ========================================================
    // NOTIFICATION BACKPRESSURE
    // ========================================================

    #[tokio::test]
    async fn connected_notification_backpressure_is_explicit() {
        let dispatcher = StreamDispatcher::new();

        let (tx, _rx) = mpsc::channel(1);

        dispatcher.attach("btc".into(), tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher.clone());

        let (fence, stamp) = connected_fence("slot-1");

        // Fill the destination before confirmation.
        dispatcher.connected("btc", stamp.clone()).unwrap();

        let result = availability.subscription_confirmed("btc", stamp, &fence);

        assert!(result.is_ok());
        assert!(availability.is_connected("btc"));
    }
    #[tokio::test]
    async fn failed_notification_does_not_leave_other_streams_connected() {
        let dispatcher = StreamDispatcher::new();

        // Stream A has enough capacity for Connected,
        // but not enough for Disconnected.
        let (a_tx, mut a_rx) = mpsc::channel(1);

        // Stream B has enough capacity for both events.
        let (b_tx, mut b_rx) = mpsc::channel(8);

        dispatcher.attach("a".into(), a_tx).unwrap();
        dispatcher.attach("b".into(), b_tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher.clone());

        let (fence, stamp) = connected_fence("slot-1");

        availability
            .subscription_confirmed("a", stamp.clone(), &fence)
            .unwrap();

        availability
            .subscription_confirmed("b", stamp.clone(), &fence)
            .unwrap();

        // A's queue still contains Connected and is full.
        // B's queue has available capacity.
        fence.failed(&stamp).unwrap();

        let result = availability.connection_lost(&stamp);

        assert!(matches!(
            result,
            Err(StreamAvailabilityError::Delivery(
                StreamDispatchError::Backpressure(_)
            ))
        ));

        // Both streams must lose their availability state.
        assert!(!availability.is_connected("a"));
        assert!(!availability.is_connected("b"));
        assert_eq!(availability.connected_count(), 0);

        // Stream A received Connected, but Disconnected
        // could not be queued.
        assert!(matches!(a_rx.recv().await, Some(StreamInput::Connected)));

        assert!(a_rx.try_recv().is_err());

        // Stream B received both events despite A's failure.
        assert!(matches!(b_rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(b_rx.recv().await, Some(StreamInput::Disconnected)));

        // A must not remain authorized to receive market data.
        assert!(
            !dispatcher
                .deliver(
                    "a",
                    &stamp,
                    tokio_tungstenite::tungstenite::Message::Text("stale".into(),),
                )
                .unwrap()
        );
    }
    #[tokio::test]
    async fn old_disconnect_does_not_revoke_new_generation() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(8);

        dispatcher.attach("btc".into(), tx).unwrap();

        let mut availability = StreamAvailability::new(dispatcher);

        let fence = ConnectionFence::new("test-runtime", "slot-1").unwrap();

        let first = fence.begin_connect().unwrap();
        fence.connected(&first).unwrap();

        availability
            .subscription_confirmed("btc", first.clone(), &fence)
            .unwrap();

        fence.failed(&first).unwrap();

        availability.connection_lost(&first).unwrap();

        let second = fence.begin_connect().unwrap();
        fence.connected(&second).unwrap();

        availability
            .subscription_confirmed("btc", second.clone(), &fence)
            .unwrap();

        // Delayed disconnect event from generation 1.
        let affected = availability.connection_lost(&first).unwrap();

        assert_eq!(affected, 0);

        // Generation 2 remains active.
        assert!(availability.is_connected("btc"));

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(rx.recv().await, Some(StreamInput::Disconnected)));

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        assert!(rx.try_recv().is_err());
    }
}
