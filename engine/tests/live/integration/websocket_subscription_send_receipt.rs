use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;

use marketforge_engine::live::infrastructure::websocket::{
    ConnectionFence, ExpectedSubscriptionAck, SubscriptionPendingManager, SubscriptionReceiptError,
    SubscriptionSendError, SubscriptionTransactionPhase, subscription_send_receipt,
};

fn connected() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("test-runtime", "test-slot").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

#[tokio::test]
async fn successful_send_receipt_starts_ack_deadline() {
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

    let (sender, receiver) = subscription_send_receipt(id, stamp.clone());

    assert_eq!(pending.next_deadline(), None);

    let before = Instant::now();

    // Simulates completion of a socket send.
    sender.sent();

    let receipt = receiver.wait(Duration::from_secs(1)).await.unwrap();

    assert_eq!(receipt.transaction_id, id);
    assert_eq!(receipt.stamp, stamp);
    assert!(receipt.sent_at >= before);

    pending
        .mark_sent(
            &fence,
            &receipt.stamp,
            receipt.transaction_id,
            receipt.sent_at,
        )
        .unwrap();

    assert_eq!(
        pending.phase(id),
        Some(SubscriptionTransactionPhase::AwaitingAck),
    );

    assert_eq!(
        pending.next_deadline(),
        Some(receipt.sent_at + Duration::from_secs(2)),
    );
}

#[tokio::test]
async fn explicit_send_failure_does_not_start_ack_timer() {
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

    sender.failed(SubscriptionSendError::Admission(
        "subscription rate limit deadline exceeded".into(),
    ));

    assert!(matches!(
        receiver.wait(Duration::from_secs(1)).await,
        Err(SubscriptionReceiptError::Send(
            SubscriptionSendError::Admission(_)
        )),
    ));

    assert_eq!(pending.next_deadline(), None);

    pending.cancel(id).unwrap();
    assert_eq!(pending.pending_count(), 0);
}
