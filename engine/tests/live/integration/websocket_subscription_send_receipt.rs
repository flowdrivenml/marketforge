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
async fn w43_successful_send_receipt_starts_ack_deadline() {
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
async fn w43_unsent_job_drop_fails_receipt() {
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
async fn w43_explicit_send_failure_does_not_start_ack_timer() {
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

#[tokio::test]
async fn w43_receipt_timeout_is_not_ack_timeout() {
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
