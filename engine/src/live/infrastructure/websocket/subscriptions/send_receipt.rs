use std::time::Duration;

use thiserror::Error;
use tokio::{
    sync::oneshot,
    time::{self, Instant},
};

use super::pending::SubscriptionTransactionId;
use crate::live::infrastructure::websocket::lifecycle::fence::ConnectionStamp;

#[derive(Debug, Clone)]
pub struct SubscriptionSendReceipt {
    pub transaction_id: SubscriptionTransactionId,
    pub stamp: ConnectionStamp,
    pub sent_at: Instant,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SubscriptionSendError {
    #[error("WebSocket transmission failed: {0}")]
    Transport(String),

    #[error("outbound admission failed: {0}")]
    Admission(String),

    #[error("subscription transmission was cancelled")]
    Cancelled,
}

#[derive(Debug, Error)]
pub enum SubscriptionReceiptError {
    #[error("transmission receipt channel closed")]
    ChannelClosed,

    #[error("transmission receipt wait timed out")]
    TimedOut,

    #[error(transparent)]
    Send(#[from] SubscriptionSendError),
}
#[derive(Debug)]
pub struct SubscriptionSendReceiptSender {
    transaction_id: SubscriptionTransactionId,
    stamp: ConnectionStamp,
    sender: Option<oneshot::Sender<Result<SubscriptionSendReceipt, SubscriptionSendError>>>,
}

impl SubscriptionSendReceiptSender {
    /// The socket-owning supervisor calls this only
    /// after successful completion of its send operation.
    pub fn sent(mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Ok(SubscriptionSendReceipt {
                transaction_id: self.transaction_id,
                stamp: self.stamp.clone(),
                sent_at: Instant::now(),
            }));
        }
    }

    /// The supervisor calls this when it knows that
    /// transmission failed before completion.
    pub fn failed(mut self, error: SubscriptionSendError) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(error));
        }
    }
}

impl Drop for SubscriptionSendReceiptSender {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(SubscriptionSendError::Cancelled));
        }
    }
}

pub struct SubscriptionSendReceiptReceiver {
    receiver: oneshot::Receiver<Result<SubscriptionSendReceipt, SubscriptionSendError>>,
}

impl SubscriptionSendReceiptReceiver {
    pub async fn wait(
        self,
        timeout: Duration,
    ) -> Result<SubscriptionSendReceipt, SubscriptionReceiptError> {
        if timeout.is_zero() {
            return Err(SubscriptionReceiptError::TimedOut);
        }

        let result = time::timeout(timeout, self.receiver)
            .await
            .map_err(|_| SubscriptionReceiptError::TimedOut)?
            .map_err(|_| SubscriptionReceiptError::ChannelClosed)?;

        Ok(result?)
    }
}

pub fn subscription_send_receipt(
    transaction_id: SubscriptionTransactionId,
    stamp: ConnectionStamp,
) -> (
    SubscriptionSendReceiptSender,
    SubscriptionSendReceiptReceiver,
) {
    let (sender, receiver) = oneshot::channel();

    (
        SubscriptionSendReceiptSender {
            transaction_id,
            stamp,
            sender: Some(sender),
        },
        SubscriptionSendReceiptReceiver { receiver },
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use crate::live::infrastructure::websocket::{
        ConnectionFence, ExpectedSubscriptionAck, SubscriptionPendingManager,
        SubscriptionReceiptError, SubscriptionSendError, SubscriptionTransactionPhase,
        subscription_send_receipt,
    };

    fn connected() -> (
        ConnectionFence,
        crate::live::infrastructure::websocket::ConnectionStamp,
    ) {
        let fence = ConnectionFence::new("test-runtime", "test-slot").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        (fence, stamp)
    }

    #[tokio::test]
    async fn unsent_job_drop_fails_receipt() {
        let (_fence, stamp) = connected();

        let mut pending = SubscriptionPendingManager::new(8, Duration::from_secs(2)).unwrap();

        let (fence, _) = connected();

        // Use the original fence identity for reservation.
        let current = fence.current_stamp().unwrap();

        let id = pending
            .reserve(
                &fence,
                &current,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        let (sender, receiver) = subscription_send_receipt(id, stamp);

        drop(sender);

        assert!(matches!(
            receiver.wait(Duration::from_secs(1)).await,
            Err(SubscriptionReceiptError::Send(
                SubscriptionSendError::Cancelled
            )),
        ));

        assert_eq!(pending.next_deadline(), None);
        assert_eq!(
            pending.phase(id),
            Some(SubscriptionTransactionPhase::Reserved),
        );

        // The transaction owner must release the reservation.
        pending.cancel(id).unwrap();

        assert_eq!(pending.pending_count(), 0);
    }

    #[tokio::test]
    async fn receipt_timeout_is_not_ack_timeout() {
        let (fence, stamp) = connected();

        let mut pending = SubscriptionPendingManager::new(8, Duration::from_secs(2)).unwrap();

        let id = pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        let (sender, receiver) = subscription_send_receipt(id, stamp);

        // The receipt exists, but transmission is unresolved.
        let result = receiver.wait(Duration::from_millis(1)).await;

        assert!(matches!(result, Err(SubscriptionReceiptError::TimedOut),));

        // Reserved subscription must not acquire an ACK
        // deadline simply because a receipt wait expired.
        assert_eq!(pending.next_deadline(), None);

        drop(sender);
        pending.cancel(id).unwrap();
    }
}
