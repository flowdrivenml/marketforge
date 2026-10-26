use serde_json::json;
use tokio::sync::mpsc;

use marketforge_engine::live::infrastructure::websocket::{
    ConnectionFence, ExpectedSubscriptionAck, RecoveryStreamKind, SharedStreamRecovery,
    StreamRecoveryPhase, SubscriptionDriverEvent, SubscriptionEventMonitor,
    SubscriptionEventMonitorError, SubscriptionPendingManager,
};

fn connected() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

fn registered_token(
    recovery: &SharedStreamRecovery,
    fence: &ConnectionFence,
    stamp: &marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) -> marketforge_engine::live::infrastructure::websocket::StreamRecoveryToken {
    let mut guard = recovery.lock().unwrap();

    guard
        .register("BTC:trades", RecoveryStreamKind::Trades)
        .unwrap();

    guard.begin_binding(fence, stamp, "BTC:trades").unwrap()
}

// ============================================================
// CONFIRMED SUBSCRIPTION
// ============================================================

#[tokio::test]
async fn forwards_valid_confirmed_event() {
    let (fence, stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let token = registered_token(&recovery, &fence, &stamp);

    // Simulate the transition already performed by
    // SubscriptionDriver after a valid native ACK.
    recovery
        .lock()
        .unwrap()
        .subscription_confirmed(&fence, &token)
        .unwrap();

    let (tx, rx) = mpsc::channel(4);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(4);

    let monitor =
        SubscriptionEventMonitor::run(rx, downstream_tx, &fence, &stamp, &recovery, 7, || 7);
    let transaction_id = {
        let mut pending =
            SubscriptionPendingManager::new(4, std::time::Duration::from_secs(2)).unwrap();

        pending
            .reserve(
                &fence,
                &stamp,
                "BTC:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap()
    };
    let producer = async move {
        tx.send(SubscriptionDriverEvent::Confirmed {
            transaction_id,
            token,
        })
        .await
        .unwrap();
    };

    let (result, ()) = tokio::join!(monitor, producer);

    // The sender has been dropped. The monitor forwards
    // the valid confirmation, then detects closure.
    assert!(matches!(
        result,
        Err(SubscriptionEventMonitorError::DriverEventChannelClosed)
    ));

    let event = downstream_rx.recv().await.unwrap();

    assert!(matches!(event, SubscriptionDriverEvent::Confirmed { .. }));
}

#[tokio::test]
async fn rejects_stale_demand_confirmation() {
    let (fence, stamp) = connected();
    let recovery = SharedStreamRecovery::new();

    let token = registered_token(&recovery, &fence, &stamp);

    recovery
        .lock()
        .unwrap()
        .subscription_confirmed(&fence, &token)
        .unwrap();

    let mut pending =
        SubscriptionPendingManager::new(4, std::time::Duration::from_secs(2)).unwrap();

    let transaction_id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    let (tx, rx) = mpsc::channel(4);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(4);

    tx.send(SubscriptionDriverEvent::Confirmed {
        transaction_id,
        token,
    })
    .await
    .unwrap();

    let result =
        SubscriptionEventMonitor::run(rx, downstream_tx, &fence, &stamp, &recovery, 7, || 8).await;

    assert!(matches!(
        result,
        Err(SubscriptionEventMonitorError::StaleDemand)
    ));

    assert!(downstream_rx.try_recv().is_err());
}

#[tokio::test]
async fn rejects_confirmation_from_revoked_generation() {
    let (fence, stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let token = registered_token(&recovery, &fence, &stamp);

    recovery
        .lock()
        .unwrap()
        .subscription_confirmed(&fence, &token)
        .unwrap();

    let mut pending =
        SubscriptionPendingManager::new(4, std::time::Duration::from_secs(2)).unwrap();

    let transaction_id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    let (tx, rx) = mpsc::channel(4);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(4);

    tx.send(SubscriptionDriverEvent::Confirmed {
        transaction_id,
        token,
    })
    .await
    .unwrap();

    // Old generation is revoked before the monitor
    // processes the queued confirmation.
    fence.failed(&stamp).unwrap();

    let result =
        SubscriptionEventMonitor::run(rx, downstream_tx, &fence, &stamp, &recovery, 7, || 7).await;

    assert!(matches!(
        result,
        Err(SubscriptionEventMonitorError::StaleGeneration)
    ));

    assert!(downstream_rx.try_recv().is_err());
}

#[tokio::test]
async fn rejects_confirmation_before_subscription_ack() {
    let (fence, stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let token = registered_token(&recovery, &fence, &stamp);

    assert_eq!(
        recovery.lock().unwrap().phase("BTC:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    let mut pending =
        SubscriptionPendingManager::new(4, std::time::Duration::from_secs(2)).unwrap();

    let transaction_id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    let (tx, rx) = mpsc::channel(4);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(4);

    tx.send(SubscriptionDriverEvent::Confirmed {
        transaction_id,
        token,
    })
    .await
    .unwrap();

    let result =
        SubscriptionEventMonitor::run(rx, downstream_tx, &fence, &stamp, &recovery, 7, || 7).await;

    assert!(matches!(
        result,
        Err(SubscriptionEventMonitorError::InvalidRecoveryPhase)
    ));

    assert!(downstream_rx.try_recv().is_err());
}
