use marketforge_engine::live::infrastructure::websocket::{
    ConnectionFence, RecoveryStreamKind, StreamRecoveryCoordinator, StreamRecoveryError,
    StreamRecoveryPhase,
};

#[test]
fn w33_new_binding_requires_subscription_and_recovery() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register("BTCUSDT:depth", RecoveryStreamKind::Depth)
        .unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    let token = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:depth")
        .unwrap();

    assert_eq!(
        recovery.phase("BTCUSDT:depth"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    assert!(!recovery.may_publish(&fence, &token));

    // Simulates a subscription ACK validated by the
    // future subscription-protocol component.
    recovery.subscription_confirmed(&fence, &token).unwrap();

    assert_eq!(
        recovery.phase("BTCUSDT:depth"),
        Some(StreamRecoveryPhase::AwaitingRecovery),
    );

    assert!(!recovery.may_publish(&fence, &token));

    // Physical readiness alone is insufficient.
    fence.ready(&stamp).unwrap();

    assert!(!recovery.may_publish(&fence, &token));

    // Simulates recovery completion from the existing
    // depth integrity controller.
    recovery.recovery_confirmed(&fence, &token).unwrap();

    assert_eq!(
        recovery.phase("BTCUSDT:depth"),
        Some(StreamRecoveryPhase::Ready),
    );

    assert!(recovery.may_publish(&fence, &token));
}

#[test]
fn w33_reconnect_invalidates_previous_stream_binding() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register("BTCUSDT:trades", RecoveryStreamKind::Trades)
        .unwrap();

    let first = fence.begin_connect().unwrap();
    fence.connected(&first).unwrap();

    let old = recovery
        .begin_binding(&fence, &first, "BTCUSDT:trades")
        .unwrap();

    recovery.subscription_confirmed(&fence, &old).unwrap();

    recovery.recovery_confirmed(&fence, &old).unwrap();

    fence.ready(&first).unwrap();

    assert!(recovery.may_publish(&fence, &old));

    // Transport failure revokes physical authority.
    fence.failed(&first).unwrap();

    assert!(!recovery.may_publish(&fence, &old));

    let second = fence.begin_connect().unwrap();
    fence.connected(&second).unwrap();

    let new = recovery
        .begin_binding(&fence, &second, "BTCUSDT:trades")
        .unwrap();

    assert!(new.stream_generation() > old.stream_generation());

    assert!(new.subscription_generation() > old.subscription_generation());

    assert_ne!(new.connection_generation(), old.connection_generation(),);

    assert!(!recovery.may_publish(&fence, &old));
    assert!(!recovery.may_publish(&fence, &new));

    // Old subscription ACKs cannot authorize the new stream.
    assert!(matches!(
        recovery.subscription_confirmed(&fence, &old),
        Err(StreamRecoveryError::StaleConnection),
    ));

    recovery.subscription_confirmed(&fence, &new).unwrap();

    recovery.recovery_confirmed(&fence, &new).unwrap();

    fence.ready(&second).unwrap();

    assert!(recovery.may_publish(&fence, &new));
    assert!(!recovery.may_publish(&fence, &old));
}

#[test]
fn w33_duplicate_ack_and_recovery_are_rejected() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register("ETHUSDT:depth", RecoveryStreamKind::Depth)
        .unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    let token = recovery
        .begin_binding(&fence, &stamp, "ETHUSDT:depth")
        .unwrap();

    recovery.subscription_confirmed(&fence, &token).unwrap();

    assert!(matches!(
        recovery.subscription_confirmed(&fence, &token),
        Err(StreamRecoveryError::InvalidTransition),
    ));

    recovery.recovery_confirmed(&fence, &token).unwrap();

    assert!(matches!(
        recovery.recovery_confirmed(&fence, &token),
        Err(StreamRecoveryError::InvalidTransition),
    ));
}

#[test]
fn w33_rebinding_same_connection_revokes_previous_epoch() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register("BTCUSDT:depth", RecoveryStreamKind::Depth)
        .unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    let first = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:depth")
        .unwrap();

    // Stream-local resubscription or recovery reset can
    // happen without reconnecting the entire socket.
    let second = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:depth")
        .unwrap();

    assert_ne!(first, second);

    assert!(matches!(
        recovery.subscription_confirmed(&fence, &first),
        Err(StreamRecoveryError::StaleToken),
    ));

    recovery.subscription_confirmed(&fence, &second).unwrap();

    recovery.recovery_confirmed(&fence, &second).unwrap();

    fence.ready(&stamp).unwrap();

    assert!(!recovery.may_publish(&fence, &first));
    assert!(recovery.may_publish(&fence, &second));
}

#[test]
fn w33_recovery_cannot_start_for_unknown_stream() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    assert!(matches!(
        recovery.begin_binding(&fence, &stamp, "unknown:depth",),
        Err(StreamRecoveryError::UnknownStream(_)),
    ));
}
