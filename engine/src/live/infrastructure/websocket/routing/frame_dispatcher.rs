use thiserror::Error;

use tokio_tungstenite::tungstenite::Message;

use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, FencedMessage};

use super::message_router::{NativeMessageRouter, NativeMessageRouterError, NativeMessageSelector};
use super::stream_dispatcher::{StreamDispatchError, StreamDispatcher};
// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum NativeFrameDispatchError {
    #[error("connection generation is stale or not ready")]
    StaleGeneration,

    #[error(transparent)]
    Classification(#[from] NativeMessageRouterError),

    #[error(transparent)]
    Delivery(#[from] StreamDispatchError),
}

// ============================================================
// DISPATCH OUTCOME
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeFrameDispatchOutcome {
    Delivered,

    /// The stream is not currently authorized to receive
    /// data from this generation.
    StreamUnavailable,
}

// ============================================================
// NATIVE FRAME DISPATCHER
// ============================================================

/// Connects generation-aware WebSocket output to
/// individual logical source-stream destinations.
///
/// Responsibilities:
///
/// - Validate connection generation authority.
/// - Classify native market-data messages.
/// - Select the corresponding logical source stream.
/// - Deliver the original native message.
///
/// Does NOT:
///
/// - Normalize trades or depth.
/// - Validate market-data sequences.
/// - Own subscriptions.
/// - Perform connection recovery.
/// - Authorize canonical publication.
///
/// The WebSocket manager owns one dispatcher per
/// connection slot.
pub(crate) struct NativeFrameDispatcher {
    fence: ConnectionFence,

    router: NativeMessageRouter,

    streams: StreamDispatcher,
}

impl NativeFrameDispatcher {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        fence: ConnectionFence,
        router: NativeMessageRouter,
        streams: StreamDispatcher,
    ) -> Self {
        Self {
            fence,
            router,
            streams,
        }
    }
    /// Extend the routing configuration of this connection slot.
    ///
    /// Called only by its serialized event-processing owner.
    ///
    /// Must complete before corresponding subscription demand
    /// becomes visible to ConnectionSlotRunner.
    pub fn extend_routes(
        &mut self,
        entries: impl IntoIterator<Item = (String, NativeMessageSelector)>,
    ) -> Result<(), NativeFrameDispatchError> {
        self.router.extend_all(entries)?;

        Ok(())
    }
    pub fn route(
        &self,
        message: Message,
    ) -> Result<Vec<(String, Message)>, NativeFrameDispatchError> {
        Ok(self.router.route(message)?)
    }

    /// Deliver a previously classified native fragment under the original fence.
    pub fn dispatch_to(
        &self,
        stream_id: &str,
        stamp: &crate::live::infrastructure::websocket::ConnectionStamp,
        message: Message,
    ) -> Result<NativeFrameDispatchOutcome, NativeFrameDispatchError> {
        let delivered = self
            .fence
            .with_connected(stamp, || self.streams.deliver(stream_id, stamp, message))
            .ok_or(NativeFrameDispatchError::StaleGeneration)??;
        Ok(if delivered {
            NativeFrameDispatchOutcome::Delivered
        } else {
            NativeFrameDispatchOutcome::StreamUnavailable
        })
    }

    /// Process one generation-stamped native frame.
    ///
    /// Classification determines the logical stream.
    ///
    /// The original native message is delivered without
    /// normalization.
    ///
    /// The fence remains held during synchronous admission
    /// to the destination queue.
    ///
    /// No asynchronous operation occurs while the
    /// connection fence is held.
    pub fn dispatch(
        &self,
        frame: FencedMessage<Message>,
    ) -> Result<NativeFrameDispatchOutcome, NativeFrameDispatchError> {
        let (stamp, message) = frame.into_parts();

        // First, classify the native message.
        //
        // Classification never mutates stream state.
        //
        // Unknown or ambiguous messages are not guessed.
        let destination = self.router.classify(&message)?;

        self.dispatch_to(&destination.stream_id, &stamp, message)
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{Value, json};

    use tokio::sync::mpsc;

    use crate::live::infrastructure::websocket::lifecycle::fence::ConnectionStamp;
    use crate::live::infrastructure::websocket::routing::stream_dispatcher::StreamInput;

    use crate::live::infrastructure::websocket::routing::message_router::NativeMessageSelector;

    // ========================================================
    // HELPERS
    // ========================================================

    fn ready_connection() -> (ConnectionFence, ConnectionStamp) {
        let fence = ConnectionFence::new("test-runtime", "slot-1").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        fence.ready(&stamp).unwrap();

        (fence, stamp)
    }

    fn selector(stream: &str) -> NativeMessageSelector {
        NativeMessageSelector::new(vec![(
            "/stream".to_owned(),
            Value::String(stream.to_owned()),
        )])
        .unwrap()
    }

    fn native_frame(stamp: ConnectionStamp, stream: &str) -> FencedMessage<Message> {
        let message = json!({
            "stream": stream,
            "data": {
                "p": "80000",
                "q": "0.1"
            }
        });

        FencedMessage::new(stamp, Message::Text(message.to_string().into()))
    }

    // ========================================================
    // CORRECT STREAM DELIVERY
    // ========================================================

    #[tokio::test]
    async fn delivers_native_frame_to_matching_stream() {
        let (fence, stamp) = ready_connection();

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc-trades".into(), tx).unwrap();

        streams.connected("btc-trades", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router
            .register("btc-trades", selector("btcusdt@trade"))
            .unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence, router, streams);

        let outcome = dispatcher
            .dispatch(native_frame(stamp, "btcusdt@trade"))
            .unwrap();

        assert_eq!(outcome, NativeFrameDispatchOutcome::Delivered);

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected(_))));

        assert!(matches!(
            rx.recv().await,
            Some(StreamInput::Data {
                message: Message::Text(_),
                ..
            })
        ));
    }

    // ========================================================
    // MULTIPLE STREAMS
    // ========================================================

    #[tokio::test]
    async fn routes_interleaved_streams_independently() {
        let (fence, stamp) = ready_connection();

        let streams = StreamDispatcher::new();

        let (btc_tx, mut btc_rx) = mpsc::channel(16);
        let (eth_tx, mut eth_rx) = mpsc::channel(16);

        streams.attach("btc".into(), btc_tx).unwrap();
        streams.attach("eth".into(), eth_tx).unwrap();

        streams.connected("btc", stamp.clone()).unwrap();

        streams.connected("eth", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        router.register("eth", selector("ethusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence, router, streams);

        dispatcher
            .dispatch(native_frame(stamp.clone(), "ethusdt@trade"))
            .unwrap();

        dispatcher
            .dispatch(native_frame(stamp, "btcusdt@trade"))
            .unwrap();

        assert!(matches!(
            btc_rx.recv().await,
            Some(StreamInput::Connected(_))
        ));

        assert!(matches!(
            eth_rx.recv().await,
            Some(StreamInput::Connected(_))
        ));

        // Each logical stream receives exactly one
        // matching market-data message.
        assert!(matches!(
            btc_rx.recv().await,
            Some(StreamInput::Data { .. })
        ));

        assert!(matches!(
            eth_rx.recv().await,
            Some(StreamInput::Data { .. })
        ));

        assert!(btc_rx.try_recv().is_err());
        assert!(eth_rx.try_recv().is_err());
    }

    // ========================================================
    // STALE GENERATION
    // ========================================================

    #[tokio::test]
    async fn revoked_generation_cannot_deliver() {
        let (fence, stamp) = ready_connection();

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        streams.connected("btc", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence.clone(), router, streams);

        // Revoke the physical connection generation.
        fence.failed(&stamp).unwrap();

        let result = dispatcher.dispatch(native_frame(stamp, "btcusdt@trade"));

        assert!(matches!(
            result,
            Err(NativeFrameDispatchError::StaleGeneration)
        ));

        // Only the already-queued availability event.
        assert!(matches!(rx.recv().await, Some(StreamInput::Connected(_))));

        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // UNKNOWN MESSAGE
    // ========================================================

    #[tokio::test]
    async fn unknown_message_is_not_delivered() {
        let (fence, stamp) = ready_connection();

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        streams.connected("btc", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence, router, streams);

        let result = dispatcher.dispatch(native_frame(stamp, "unknown@trade"));

        assert!(matches!(
            result,
            Err(NativeFrameDispatchError::Classification(
                NativeMessageRouterError::UnknownMessage
            ))
        ));

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected(_))));

        assert!(rx.try_recv().is_err());
    }

    // ========================================================
    // BACKPRESSURE
    // ========================================================

    #[tokio::test]
    async fn destination_backpressure_is_propagated() {
        let (fence, stamp) = ready_connection();

        let streams = StreamDispatcher::new();

        // One queued message maximum.
        let (tx, _rx) = mpsc::channel(1);

        streams.attach("btc".into(), tx).unwrap();

        // Connected fills the queue.
        streams.connected("btc", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence, router, streams);

        let result = dispatcher.dispatch(native_frame(stamp, "btcusdt@trade"));

        assert!(matches!(
            result,
            Err(NativeFrameDispatchError::Delivery(
                StreamDispatchError::Backpressure(_)
            ))
        ));
    }

    // ========================================================
    // UNAVAILABLE STREAM
    // ========================================================

    #[tokio::test]
    async fn unconfirmed_stream_does_not_receive_data() {
        let (fence, stamp) = ready_connection();

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        // Deliberately do not mark the stream Connected.

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence, router, streams);

        let result = dispatcher
            .dispatch(native_frame(stamp, "btcusdt@trade"))
            .unwrap();

        assert_eq!(result, NativeFrameDispatchOutcome::StreamUnavailable);

        assert!(rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn delivers_after_ack_without_requiring_fence_ready() {
        let fence = ConnectionFence::new("test-runtime", "slot-connected").unwrap();

        let stamp = fence.begin_connect().unwrap();

        // Physical handshake completed.
        fence.connected(&stamp).unwrap();

        // Deliberately do NOT call fence.ready().
        assert_eq!(
            fence.state(),
            crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Connected
        );

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        // Represents validated subscription confirmation.
        streams.connected("btc", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence.clone(), router, streams);

        let outcome = dispatcher
            .dispatch(native_frame(stamp.clone(), "btcusdt@trade"))
            .unwrap();

        assert_eq!(outcome, NativeFrameDispatchOutcome::Delivered);

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected(_))));

        assert!(matches!(rx.recv().await, Some(StreamInput::Data { .. })));

        // Transport delivery does not imply publication readiness.
        assert!(fence.with_ready(&stamp, || ()).is_none());
    }

    #[tokio::test]
    async fn handshake_without_subscription_confirmation_cannot_deliver() {
        let fence = ConnectionFence::new("test-runtime", "slot-unconfirmed").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        // No subscription confirmation.

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence, router, streams);

        let result = dispatcher
            .dispatch(native_frame(stamp, "btcusdt@trade"))
            .unwrap();

        assert_eq!(result, NativeFrameDispatchOutcome::StreamUnavailable);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn revoked_connection_cannot_deliver_after_ack() {
        let fence = ConnectionFence::new("test-runtime", "slot-revoked").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        let streams = StreamDispatcher::new();

        let (tx, mut rx) = mpsc::channel(16);

        streams.attach("btc".into(), tx).unwrap();

        streams.connected("btc", stamp.clone()).unwrap();

        let mut router = NativeMessageRouter::new();

        router.register("btc", selector("btcusdt@trade")).unwrap();

        let dispatcher = NativeFrameDispatcher::new(fence.clone(), router, streams);

        fence.failed(&stamp).unwrap();

        let result = dispatcher.dispatch(native_frame(stamp, "btcusdt@trade"));

        assert!(matches!(
            result,
            Err(NativeFrameDispatchError::StaleGeneration)
        ));

        assert!(matches!(rx.recv().await, Some(StreamInput::Connected(_))));

        assert!(rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn partitioned_batch_cannot_cross_a_revoked_physical_fence() {
        use super::super::message_router::NativePayloadSelector;
        let (fence, stamp) = ready_connection();
        let streams = StreamDispatcher::new();
        let mut router = NativeMessageRouter::new();
        let mut receivers = Vec::new();
        for symbol in ["BTC_USDT", "ETH_USDT"] {
            let (tx, mut rx) = mpsc::channel(4);
            streams.attach(symbol.into(), tx).unwrap();
            streams.connected(symbol, stamp.clone()).unwrap();
            assert!(matches!(rx.recv().await, Some(StreamInput::Connected(_))));
            receivers.push(rx);
            router
                .register(
                    symbol,
                    NativeMessageSelector::new([
                        ("/channel".into(), json!("futures.trades")),
                        ("/event".into(), json!("update")),
                    ])
                    .unwrap()
                    .with_payload(NativePayloadSelector {
                        path: "/result".into(),
                        instrument: "/contract".into(),
                        symbol: symbol.into(),
                        array: true,
                    })
                    .unwrap(),
                )
                .unwrap();
        }
        let dispatcher = NativeFrameDispatcher::new(fence.clone(), router, streams);
        let fragments = dispatcher.route(Message::Text(json!({"channel":"futures.trades","event":"update","result":[{"contract":"BTC_USDT"},{"contract":"ETH_USDT"}]}).to_string().into())).unwrap();
        assert_eq!(fragments.len(), 2);
        fence.failed(&stamp).unwrap();
        let newer = fence.begin_connect().unwrap();
        fence.connected(&newer).unwrap();
        for (owner, frame) in fragments {
            assert!(matches!(
                dispatcher.dispatch_to(&owner, &stamp, frame),
                Err(NativeFrameDispatchError::StaleGeneration)
            ));
        }
        for mut rx in receivers {
            assert!(rx.try_recv().is_err());
        }
    }
}
