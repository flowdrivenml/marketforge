use crate::live::infrastructure::websocket::{
    ConnectionFence, RecoveryStreamKind, SharedStreamRecovery, StreamRecoveryPhase,
};

fn connected() -> (
    ConnectionFence,
    crate::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

#[test]
fn shared_handles_preserve_one_authority() {
    let recovery = SharedStreamRecovery::new();

    let another = recovery.clone();

    assert!(recovery.same_authority(&another));

    {
        let mut guard = recovery.lock().unwrap();

        guard
            .register("BTC:trades", RecoveryStreamKind::Trades)
            .unwrap();
    }

    let guard = another.lock().unwrap();

    assert_eq!(guard.kind("BTC:trades"), Some(RecoveryStreamKind::Trades),);
}

#[test]
fn recovery_epochs_survive_physical_reconnect() {
    let (fence, first_stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let first_token = {
        let mut guard = recovery.lock().unwrap();

        guard
            .register("BTC:trades", RecoveryStreamKind::Trades)
            .unwrap();

        guard
            .begin_binding(&fence, &first_stamp, "BTC:trades")
            .unwrap()
    };

    {
        let mut guard = recovery.lock().unwrap();

        guard.subscription_confirmed(&fence, &first_token).unwrap();

        assert_eq!(
            guard.phase("BTC:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );
    }

    // Physical connection is revoked.
    fence.failed(&first_stamp).unwrap();

    let second_stamp = fence.begin_connect().unwrap();

    fence.connected(&second_stamp).unwrap();

    // Reuse the original coordinator.
    //
    // Do not recreate its generation counters.
    let second_token = {
        let mut guard = recovery.lock().unwrap();

        guard
            .begin_binding(&fence, &second_stamp, "BTC:trades")
            .unwrap()
    };

    assert_ne!(first_token.connection(), second_token.connection(),);

    assert!(second_token.stream_generation() > first_token.stream_generation());

    assert!(second_token.subscription_generation() > first_token.subscription_generation());

    let guard = recovery.lock().unwrap();

    assert_eq!(
        guard.phase("BTC:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    assert_eq!(guard.token("BTC:trades"), Some(&second_token),);

    assert!(!guard.may_publish(&fence, &first_token));
    assert!(!guard.may_publish(&fence, &second_token));
}

#[test]
fn old_generation_cannot_confirm_new_subscription() {
    let (fence, first_stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let first_token = {
        let mut guard = recovery.lock().unwrap();

        guard
            .register("BTC:depth", RecoveryStreamKind::Depth)
            .unwrap();

        guard
            .begin_binding(&fence, &first_stamp, "BTC:depth")
            .unwrap()
    };

    fence.failed(&first_stamp).unwrap();

    let second_stamp = fence.begin_connect().unwrap();

    fence.connected(&second_stamp).unwrap();

    let second_token = {
        let mut guard = recovery.lock().unwrap();

        guard
            .begin_binding(&fence, &second_stamp, "BTC:depth")
            .unwrap()
    };

    let mut guard = recovery.lock().unwrap();

    assert!(guard.subscription_confirmed(&fence, &first_token,).is_err());

    assert_eq!(guard.token("BTC:depth"), Some(&second_token),);

    assert_eq!(
        guard.phase("BTC:depth"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );
}

#[test]
fn new_logical_slot_has_independent_recovery_state() {
    let first = SharedStreamRecovery::new();
    let second = SharedStreamRecovery::new();

    assert!(!first.same_authority(&second));

    first
        .lock()
        .unwrap()
        .register("BTC:trades", RecoveryStreamKind::Trades)
        .unwrap();

    assert!(second.lock().unwrap().kind("BTC:trades").is_none());
}
