use std::sync::Arc;

use thiserror::Error;
use tokio::sync::mpsc;

use crate::live::infrastructure::websocket::{
    StreamBinding, StreamInput, StreamRequest, WebSocketManager, WebSocketManagerError,
};

#[derive(Debug, Error)]
pub enum SourceAttachmentError {
    #[error("source input channel capacity must be positive")]
    InvalidCapacity,

    #[error(transparent)]
    WebSocket(#[from] WebSocketManagerError),
}

/// Owns one logical WebSocket attachment.
///
/// Physical connections remain owned by WebSocketManager.
///
/// The input receiver is bounded. Its messages must be
/// consumed by the source's single-owner execution actor.
///
/// Explicit detach is required during shutdown.
pub struct SourceAttachment {
    manager: Arc<WebSocketManager>,
    binding: StreamBinding,
    receiver: mpsc::Receiver<StreamInput>,
}

impl SourceAttachment {
    /// Attach an already validated, identity-bound request.
    ///
    /// Successful attachment does not imply a connected
    /// or ready exchange subscription.
    pub async fn attach(
        manager: Arc<WebSocketManager>,
        request: StreamRequest,
        capacity: usize,
    ) -> Result<Self, SourceAttachmentError> {
        if capacity == 0 {
            return Err(SourceAttachmentError::InvalidCapacity);
        }

        let (sender, receiver) = mpsc::channel(capacity);

        let binding = manager.attach_request(request, sender).await?;

        Ok(Self {
            manager,
            binding,
            receiver,
        })
    }

    /// Receive the next transport event in delivery order.
    ///
    /// None means that all senders have been released and
    /// the remaining queued events have been consumed.
    pub async fn recv(&mut self) -> Option<StreamInput> {
        self.receiver.recv().await
    }

    /// Close local admission before draining or detaching.
    ///
    /// The executor determines how remaining events are
    /// discarded or accounted for during shutdown.
    pub fn close_input(&mut self) {
        self.receiver.close();
    }

    /// Explicitly release this logical attachment.
    ///
    /// Detachment may rebuild a shared connection slot.
    /// The manager owns that procedure.
    pub async fn detach(self) -> Result<(), SourceAttachmentError> {
        self.manager.detach(self.binding).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_capacity_must_be_positive() {
        assert_eq!(mpsc::channel::<StreamInput>(1).0.capacity(), 1);
    }
}
