use thiserror::Error;

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::fence::{ConnectionFence, ConnectionStamp, FencedMessage};
use super::state::ConnectionState;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum GenerationFrameRelayError {
    #[error("frame belongs to a stale connection generation")]
    StaleGeneration,

    #[error("native frame output queue exhausted")]
    Backpressure,

    #[error("native frame output receiver closed")]
    OutputClosed,
    #[error("native frame input channel closed")]
    InputClosed,
}

// ============================================================
// GENERATION FRAME RELAY
// ============================================================

/// Attaches authoritative physical-connection provenance
/// to native WebSocket frames.
///
/// One relay belongs to exactly one physical connection
/// generation.
///
/// The stamp MUST be captured when the session is created,
/// never reconstructed from the currently active generation
/// while draining an older queue.
///
/// This component does not:
/// - Identify native streams.
/// - Normalize market data.
/// - Perform sequence validation.
/// - Authorize canonical publication.
/// - Manage connection recovery.
///
/// The downstream consumer must still verify generation
/// authority before applying or publishing data.
pub(crate) struct GenerationFrameRelay;

impl GenerationFrameRelay {
    /// Forward native messages from one physical session
    /// into a bounded provenance-aware output channel.
    ///
    /// A stale generation or downstream backpressure is
    /// a fatal session-level condition.
    ///
    /// The caller must supervise this future together
    /// with the corresponding ConnectionSession.
    pub async fn run(
        mut inbound: mpsc::Receiver<Message>,
        outbound: mpsc::Sender<FencedMessage<Message>>,
        fence: &ConnectionFence,
        stamp: ConnectionStamp,
    ) -> Result<(), GenerationFrameRelayError> {
        // Validate initial connection identity.
        Self::validate(fence, &stamp)?;

        while let Some(message) = inbound.recv().await {
            // Revalidate after every await.
            //
            // A disconnect may have occurred while
            // waiting for another native message.
            Self::validate(fence, &stamp)?;

            // The stamp is the original session stamp.
            //
            // Never replace it with fence.current_stamp().
            let envelope = FencedMessage::new(stamp.clone(), message);

            // Do not silently discard market data.
            //
            // A full output queue fails the relay.
            outbound.try_send(envelope).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => GenerationFrameRelayError::Backpressure,

                mpsc::error::TrySendError::Closed(_) => GenerationFrameRelayError::OutputClosed,
            })?;
        }

        Err(GenerationFrameRelayError::InputClosed)
    }

    fn validate(
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
    ) -> Result<(), GenerationFrameRelayError> {
        if fence.current_stamp().as_ref() != Some(stamp) {
            return Err(GenerationFrameRelayError::StaleGeneration);
        }

        if !matches!(
            fence.state(),
            ConnectionState::Connected | ConnectionState::Ready
        ) {
            return Err(GenerationFrameRelayError::StaleGeneration);
        }

        Ok(())
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn connected_fence() -> (ConnectionFence, ConnectionStamp) {
        let fence = ConnectionFence::new("test-runtime", "test-slot").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        (fence, stamp)
    }

    #[tokio::test]
    async fn forwards_frames_with_original_generation() {
        let (fence, stamp) = connected_fence();

        let (raw_tx, raw_rx) = mpsc::channel(8);
        let (fenced_tx, mut fenced_rx) = mpsc::channel(8);

        raw_tx.send(Message::Text("trade-1".into())).await.unwrap();

        raw_tx.send(Message::Text("trade-2".into())).await.unwrap();

        drop(raw_tx);

        let result = GenerationFrameRelay::run(raw_rx, fenced_tx, &fence, stamp.clone()).await;

        assert_eq!(result, Err(GenerationFrameRelayError::InputClosed));

        let first = fenced_rx.recv().await.unwrap();
        let second = fenced_rx.recv().await.unwrap();

        let (first_stamp, first_message) = first.into_parts();
        let (second_stamp, second_message) = second.into_parts();

        assert_eq!(first_stamp, stamp);
        assert_eq!(second_stamp, stamp);

        assert!(matches!(first_message, Message::Text(_)));

        assert!(matches!(second_message, Message::Text(_)));
    }

    #[tokio::test]
    async fn rejects_revoked_generation() {
        let (fence, stamp) = connected_fence();

        fence.failed(&stamp).unwrap();

        let (raw_tx, raw_rx) = mpsc::channel(8);
        let (fenced_tx, _fenced_rx) = mpsc::channel(8);

        raw_tx.send(Message::Text("stale".into())).await.unwrap();

        drop(raw_tx);

        let result = GenerationFrameRelay::run(raw_rx, fenced_tx, &fence, stamp).await;

        assert_eq!(result, Err(GenerationFrameRelayError::StaleGeneration));
    }

    #[tokio::test]
    async fn output_backpressure_is_explicit() {
        let (fence, stamp) = connected_fence();

        let (raw_tx, raw_rx) = mpsc::channel(8);
        let (fenced_tx, _fenced_rx) = mpsc::channel(1);

        raw_tx.send(Message::Text("first".into())).await.unwrap();

        raw_tx.send(Message::Text("second".into())).await.unwrap();

        drop(raw_tx);

        let result = GenerationFrameRelay::run(raw_rx, fenced_tx, &fence, stamp).await;

        assert_eq!(result, Err(GenerationFrameRelayError::Backpressure));
    }

    #[tokio::test]
    async fn closed_output_is_explicit_failure() {
        let (fence, stamp) = connected_fence();

        let (raw_tx, raw_rx) = mpsc::channel(8);
        let (fenced_tx, fenced_rx) = mpsc::channel(8);

        drop(fenced_rx);

        raw_tx.send(Message::Text("trade".into())).await.unwrap();

        drop(raw_tx);

        let result = GenerationFrameRelay::run(raw_rx, fenced_tx, &fence, stamp).await;

        assert_eq!(result, Err(GenerationFrameRelayError::OutputClosed));
    }

    #[tokio::test]
    async fn distinguishes_connection_slots() {
        let first_fence = ConnectionFence::new("test-runtime", "slot-a").unwrap();

        let second_fence = ConnectionFence::new("test-runtime", "slot-b").unwrap();

        let first = first_fence.begin_connect().unwrap();
        let second = second_fence.begin_connect().unwrap();

        // Both slots have generation 1, but their
        // connection stamps must be distinct.
        assert_eq!(first.generation().get(), second.generation().get());

        assert_ne!(first, second);
    }
}
