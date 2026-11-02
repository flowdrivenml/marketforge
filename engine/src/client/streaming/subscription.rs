use super::connection::*;
use crate::stream_protocol::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
pub struct Subscription {
    pub(super) inner: Arc<Inner>,
    pub(super) id: u64,
    pub(super) receiver: mpsc::Receiver<QueuedMessage>,
    pub(super) terminal: Arc<Mutex<Option<ClientError>>>,
    pub(super) bytes: Arc<AtomicUsize>,
    pub(super) ended: bool,
    pub(super) closed: bool,
}
impl Subscription {
    /// Retained payload charge currently queued, excluding channel/allocator overhead.
    pub fn queued_payload_bytes(&self) -> usize {
        self.bytes.load(Ordering::Acquire)
    }

    /// Cancellation-safe: framing belongs to the connection reader task.
    pub async fn recv(&mut self) -> Result<Option<StreamMessage>, ClientError> {
        if self.ended {
            return Ok(None);
        }
        match self.receiver.recv().await {
            Some(QueuedMessage { message: m, .. }) => {
                if matches!(m, StreamMessage::End(_)) {
                    self.ended = true;
                }
                Ok(Some(m))
            }
            None => {
                self.ended = true;
                Err(self
                    .terminal
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap_or(ClientError::ConnectionLost(
                        "subscription closed without End".into(),
                    )))
            }
        }
    }
    pub async fn close(&mut self) -> Result<(), ClientError> {
        if self.closed {
            return Ok(());
        }
        let client = MarketForgeStreamClient {
            inner: self.inner.clone(),
        };
        let result = client
            .request(
                Message::Unsubscribe {
                    subscription_id: self.id,
                },
                None,
            )
            .await?;
        if !matches!(result,Message::Unsubscribed{subscription_id} if subscription_id==self.id) {
            return Err(ClientError::Protocol(
                "expected unsubscribe boundary".into(),
            ));
        }
        self.closed = true;
        self.ended = true;
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        Ok(())
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.closed
            && self
                .inner
                .commands
                .try_send(Command::Release(self.id))
                .is_err()
        {
            self.inner.stop.notify_one();
        }
    }
}
