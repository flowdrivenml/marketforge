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
