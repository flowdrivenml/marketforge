use super::ConnectionStamp;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// ============================================================
// PUBLIC STREAM INPUT
// ============================================================

/// The only WebSocket information a stream processor receives.
///
/// This does not represent market-data integrity readiness.
#[derive(Debug)]
pub enum StreamInput {
    Connected,
    Disconnected,
    Data(Message),
}

/// Identifies a transport attachment.
///
/// The physical WebSocket connection is intentionally hidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamBinding {
    id: u64,
}

impl StreamBinding {
    pub fn id(&self) -> u64 {
        self.id
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum StreamDispatchError {
    #[error("stream is already attached: {0}")]
    AlreadyAttached(String),

    #[error("unknown stream: {0}")]
    UnknownStream(String),

    #[error("stale stream binding")]
    StaleBinding,

    #[error("stream attachment identifier exhausted")]
    BindingIdExhausted,

    #[error("stream destination is unavailable: {0}")]
    DestinationUnavailable(String),

    #[error("stream destination queue exhausted: {0}")]
    Backpressure(String),

    #[error("stream dispatcher lock poisoned")]
    Poisoned,
}

// ============================================================
// INTERNAL ATTACHMENT
// ============================================================

struct Attachment {
    binding: StreamBinding,

    destination: mpsc::Sender<StreamInput>,

    /// Current authorized transport generation.
    ///
    /// None means the source is unavailable.
    generation: Option<ConnectionStamp>,
}

struct DispatcherState {
    next_binding_id: u64,
    streams: HashMap<String, Attachment>,
}

// ============================================================
// STREAM DISPATCHER
// ============================================================

/// Routes already-classified native market data to logical
/// source-stream instances.
///
/// This component does not:
/// - Own physical WebSocket connections.
/// - Resolve exchange-native message identities.
/// - Perform normalization.
/// - Validate trade/depth sequences.
/// - Perform integrity recovery.
/// - Own authoritative consumer demand.
///
/// All operations are synchronous and serialized to preserve
/// availability/data ordering per logical stream.
///
/// This object is intended to remain private to WebSocketManager.
#[derive(Clone)]
pub(crate) struct StreamDispatcher {
    inner: Arc<Mutex<DispatcherState>>,
}

impl StreamDispatcher {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DispatcherState {
                next_binding_id: 0,
                streams: HashMap::new(),
            })),
        }
    }

    // ========================================================
    // ATTACH
    // ========================================================
    /// Revoke the current transport generation before a managed
    /// connection slot is replaced.
    ///
    /// If the stream is connected, queues Disconnected after
    /// previously accepted data.
    ///
    /// A notification failure is explicit. The manager must
    /// retire the affected binding rather than restart it.
    pub(crate) fn disconnect_for_replacement(
        &self,
        stream_id: &str,
    ) -> Result<(), StreamDispatchError> {
        let mut state = self.lock()?;

        let entry = state
            .streams
            .get_mut(stream_id)
            .ok_or_else(|| StreamDispatchError::UnknownStream(stream_id.to_owned()))?;

        if entry.generation.take().is_none() {
            return Ok(());
        }

        Self::send(stream_id, &entry.destination, StreamInput::Disconnected)
    }

    /// Look up an attachment without exposing the dispatcher's
    /// internal registry.
    pub(crate) fn binding_for(
        &self,
        stream_id: &str,
    ) -> Result<Option<StreamBinding>, StreamDispatchError> {
        let state = self.lock()?;

        Ok(state.streams.get(stream_id).map(|entry| entry.binding))
    }
    pub fn attach(
        &self,
        stream_id: String,
        destination: mpsc::Sender<StreamInput>,
    ) -> Result<StreamBinding, StreamDispatchError> {
        let mut state = self.lock()?;

        if state.streams.contains_key(&stream_id) {
            return Err(StreamDispatchError::AlreadyAttached(stream_id));
        }

        let next = state
            .next_binding_id
            .checked_add(1)
            .ok_or(StreamDispatchError::BindingIdExhausted)?;

        let binding = StreamBinding { id: next };

        state.next_binding_id = next;

        state.streams.insert(
            stream_id,
            Attachment {
                binding,
                destination,
                generation: None,
            },
        );

        Ok(binding)
    }
    /// Retire every attachment.
    ///
    /// Dropping the stored senders closes their receivers once
    /// already-queued messages have been consumed, provided no
    /// other sender clones remain.
    ///
    /// Used only when the entire WebSocketManager is shutting down.
    pub fn detach_all(&self) -> Result<usize, StreamDispatchError> {
        let mut state = self.lock()?;

        let count = state.streams.len();

        state.streams.clear();

        Ok(count)
    }
    // ========================================================
    // DETACH
    // ========================================================

    pub fn detach(
        &self,
        stream_id: &str,
        binding: StreamBinding,
    ) -> Result<(), StreamDispatchError> {
        let mut state = self.lock()?;

        let entry = state
            .streams
            .get(stream_id)
            .ok_or_else(|| StreamDispatchError::UnknownStream(stream_id.to_owned()))?;

        if entry.binding != binding {
            return Err(StreamDispatchError::StaleBinding);
        }

        state.streams.remove(stream_id);

        Ok(())
    }

    // ========================================================
    // TRANSPORT AVAILABLE
    // ========================================================

    /// Called after the existing subscription subsystem has
    /// confirmed the stream's subscription.
    ///
    /// The manager must validate the originating connection
    /// generation before invoking this transition.
    pub fn connected(
        &self,
        stream_id: &str,
        stamp: ConnectionStamp,
    ) -> Result<(), StreamDispatchError> {
        let mut state = self.lock()?;

        let entry = state
            .streams
            .get_mut(stream_id)
            .ok_or_else(|| StreamDispatchError::UnknownStream(stream_id.to_owned()))?;

        if entry.generation.as_ref() == Some(&stamp) {
            return Ok(());
        }

        if entry.generation.is_some() {
            return Err(StreamDispatchError::StaleBinding);
        }

        Self::send(stream_id, &entry.destination, StreamInput::Connected)?;

        entry.generation = Some(stamp);

        Ok(())
    }

    // ========================================================
    // TRANSPORT UNAVAILABLE
    // ========================================================

    /// Revoke only the matching transport generation.
    ///
    /// Old disconnect notifications cannot invalidate a
    /// newer, already-authorized generation.
    pub fn disconnected(
        &self,
        stream_id: &str,
        stamp: &ConnectionStamp,
    ) -> Result<(), StreamDispatchError> {
        let mut state = self.lock()?;

        let entry = state
            .streams
            .get_mut(stream_id)
            .ok_or_else(|| StreamDispatchError::UnknownStream(stream_id.to_owned()))?;

        if entry.generation.as_ref() != Some(stamp) {
            return Ok(());
        }

        entry.generation = None;

        Self::send(stream_id, &entry.destination, StreamInput::Disconnected)
    }

    // ========================================================
    // MARKET-DATA DELIVERY
    // ========================================================

    /// Deliver one native frame after the manager has
    /// classified its destination stream.
    ///
    /// Returns Ok(false) for stale/unavailable generations.
    ///
    /// Backpressure is an explicit error and MUST cause the
    /// owning connection-slot supervisor to fail closed.
    pub fn deliver(
        &self,
        stream_id: &str,
        stamp: &ConnectionStamp,
        message: Message,
    ) -> Result<bool, StreamDispatchError> {
        let state = self.lock()?;

        let entry = state
            .streams
            .get(stream_id)
            .ok_or_else(|| StreamDispatchError::UnknownStream(stream_id.to_owned()))?;

        if entry.generation.as_ref() != Some(stamp) {
            return Ok(false);
        }

        Self::send(stream_id, &entry.destination, StreamInput::Data(message))?;

        Ok(true)
    }

    // ========================================================
    // HELPERS
    // ========================================================

    fn send(
        stream_id: &str,
        destination: &mpsc::Sender<StreamInput>,
        input: StreamInput,
    ) -> Result<(), StreamDispatchError> {
        destination.try_send(input).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                StreamDispatchError::Backpressure(stream_id.to_owned())
            }

            mpsc::error::TrySendError::Closed(_) => {
                StreamDispatchError::DestinationUnavailable(stream_id.to_owned())
            }
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, DispatcherState>, StreamDispatchError> {
        self.inner.lock().map_err(|_| StreamDispatchError::Poisoned)
    }
}

impl Default for StreamDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================
    // TEST CONNECTION STAMPS
    // ========================================================

    fn stamp(slot: &str, generation: u64) -> ConnectionStamp {
        let fence = super::super::ConnectionFence::new("test-runtime", slot).unwrap();

        let mut current = fence.begin_connect().unwrap();

        for _ in 1..generation {
            fence.connected(&current).unwrap();
            fence.failed(&current).unwrap();

            current = fence.begin_connect().unwrap();
        }

        current
    }

    // ========================================================
    // ATTACHMENT AND DELIVERY
    // ========================================================

    #[tokio::test]
    async fn attached_stream_receives_data() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        dispatcher.attach("btc-trades".into(), tx).unwrap();

        let connection = stamp("slot-1", 1);

        dispatcher
            .connected("btc-trades", connection.clone())
            .unwrap();

        let delivered = dispatcher
            .deliver("btc-trades", &connection, Message::Text("trade".into()))
            .unwrap();

        assert!(delivered);

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(
            rx.recv().await,
            Some(StreamInput::Data(Message::Text(_)))
        ));
    }

    // ========================================================
    // STREAM ISOLATION
    // ========================================================

    #[tokio::test]
    async fn different_streams_are_isolated() {
        let dispatcher = StreamDispatcher::new();

        let (btc_tx, mut btc_rx) = mpsc::channel(16);
        let (eth_tx, mut eth_rx) = mpsc::channel(16);

        dispatcher.attach("btc".into(), btc_tx).unwrap();
        dispatcher.attach("eth".into(), eth_tx).unwrap();

        // Both streams share the same physical connection.
        let connection = stamp("slot-1", 1);

        dispatcher.connected("btc", connection.clone()).unwrap();

        dispatcher.connected("eth", connection.clone()).unwrap();

        dispatcher
            .deliver("btc", &connection, Message::Text("btc-message".into()))
            .unwrap();

        // BTC receives its availability event.
        assert!(matches!(btc_rx.recv().await, Some(StreamInput::Connected)));

        // BTC receives its own message.
        match btc_rx.recv().await.unwrap() {
            StreamInput::Data(Message::Text(text)) => {
                assert_eq!(text.as_str(), "btc-message");
            }

            other => panic!("unexpected BTC input: {other:?}"),
        }

        // ETH receives its availability event.
        assert!(matches!(eth_rx.recv().await, Some(StreamInput::Connected)));

        // ETH must NOT receive BTC market data.
        assert!(eth_rx.try_recv().is_err());
    }

    // ========================================================
    // STALE GENERATION REJECTION
    // ========================================================

    #[tokio::test]
    async fn stale_generation_is_rejected() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        dispatcher.attach("btc".into(), tx).unwrap();

        let first = stamp("slot-1", 1);
        let second = stamp("slot-1", 2);

        dispatcher.connected("btc", first.clone()).unwrap();

        dispatcher.disconnected("btc", &first).unwrap();

        dispatcher.connected("btc", second.clone()).unwrap();

        // An old-generation message must not be delivered.
        let delivered = dispatcher
            .deliver("btc", &first, Message::Text("stale".into()))
            .unwrap();

        assert!(!delivered);

        // The new generation remains authorized.
        let delivered = dispatcher
            .deliver("btc", &second, Message::Text("current".into()))
            .unwrap();

        assert!(delivered);

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        assert!(matches!(rx.recv().await, Some(StreamInput::Disconnected)));

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        match rx.recv().await.unwrap() {
            StreamInput::Data(Message::Text(text)) => {
                assert_eq!(text.as_str(), "current");
            }

            other => panic!("unexpected input: {other:?}"),
        }

        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // BACKPRESSURE
    // ========================================================

    #[tokio::test]
    async fn backpressure_is_not_silent() {
        let dispatcher = StreamDispatcher::new();

        let (tx, _rx) = mpsc::channel(1);

        dispatcher.attach("btc".into(), tx).unwrap();

        let connection = stamp("slot-1", 1);

        // Connected occupies the only queue slot.
        dispatcher.connected("btc", connection.clone()).unwrap();

        let result = dispatcher.deliver("btc", &connection, Message::Text("trade".into()));

        assert!(matches!(result, Err(StreamDispatchError::Backpressure(_))));
    }

    // ========================================================
    // STALE BINDING REJECTION
    // ========================================================

    #[tokio::test]
    async fn stale_binding_cannot_detach_replacement() {
        let dispatcher = StreamDispatcher::new();

        let (first_tx, _first_rx) = mpsc::channel(8);

        let first = dispatcher.attach("btc".into(), first_tx).unwrap();

        dispatcher.detach("btc", first).unwrap();

        let (second_tx, _second_rx) = mpsc::channel(8);

        let second = dispatcher.attach("btc".into(), second_tx).unwrap();

        assert_ne!(first, second);

        let result = dispatcher.detach("btc", first);

        assert!(matches!(result, Err(StreamDispatchError::StaleBinding)));
    }

    // ========================================================
    // CROSS-SLOT GENERATION ISOLATION
    // ========================================================

    #[tokio::test]
    async fn identical_generation_numbers_on_different_slots_are_isolated() {
        let dispatcher = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        dispatcher.attach("btc".into(), tx).unwrap();

        let slot_a = stamp("slot-a", 1);
        let slot_b = stamp("slot-b", 1);

        // Both slots have physical generation 1.
        assert_eq!(slot_a.generation().get(), slot_b.generation().get());

        // But their complete identities differ.
        assert_ne!(slot_a, slot_b);

        dispatcher.connected("btc", slot_a.clone()).unwrap();

        // Another slot's generation is not authorized.
        let delivered = dispatcher
            .deliver("btc", &slot_b, Message::Text("wrong-slot".into()))
            .unwrap();

        assert!(!delivered);

        // The correct slot remains authorized.
        let delivered = dispatcher
            .deliver("btc", &slot_a, Message::Text("correct-slot".into()))
            .unwrap();

        assert!(delivered);

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected)));

        match rx.recv().await.unwrap() {
            StreamInput::Data(Message::Text(text)) => {
                assert_eq!(text.as_str(), "correct-slot");
            }

            other => panic!("unexpected input: {other:?}"),
        }

        assert!(rx.try_recv().is_err());
    }
}
