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
    detached: bool,
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
            detached: false,
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
    pub async fn detach(mut self) -> Result<(), SourceAttachmentError> {
        self.manager.detach(self.binding).await?;
        self.detached = true;
        Ok(())
    }
}

// Explicit detach remains the awaited shutdown contract. If preparation is
// rejected or its owning task is aborted, revoke local admission immediately
// and ask the same manager to retire this exact attachment as a fallback.
impl Drop for SourceAttachment {
    fn drop(&mut self) {
        self.receiver.close();
        if self.detached {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let manager = self.manager.clone();
            let binding = self.binding;
            runtime.spawn(async move {
                let _ = manager.detach(binding).await;
            });
        }
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
