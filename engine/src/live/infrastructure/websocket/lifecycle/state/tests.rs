use crate::live::infrastructure::websocket::{
    ConnectionLifecycle, ConnectionLifecycleError, ConnectionState,
};

// ============================================================
// INITIAL STATE
// ============================================================

#[test]
fn new_lifecycle_starts_idle() {
    let lifecycle = ConnectionLifecycle::new();

    assert_eq!(lifecycle.state(), ConnectionState::Idle,);

    assert_eq!(lifecycle.current_generation(), None);
    assert_eq!(lifecycle.last_generation(), 0);
}

// ============================================================
// NORMAL CONNECTION ESTABLISHMENT
// ============================================================

#[test]
fn connection_requires_explicit_readiness() {
    let mut lifecycle = ConnectionLifecycle::new();

    let generation = lifecycle.begin_connect().unwrap();

    assert_eq!(generation.get(), 1);

    assert_eq!(lifecycle.state(), ConnectionState::Connecting,);

    assert!(!lifecycle.accepts_data(generation));

    lifecycle.connected(generation).unwrap();

    assert_eq!(lifecycle.state(), ConnectionState::Connected,);

    // A successful handshake does not authorize publication.
    assert!(!lifecycle.accepts_data(generation));

    lifecycle.ready(generation).unwrap();

    assert_eq!(lifecycle.state(), ConnectionState::Ready,);

    assert!(lifecycle.accepts_data(generation));
}

// ============================================================
// RECONNECT GENERATION FENCING
// ============================================================

#[test]
fn reconnection_revokes_old_generation() {
    let mut lifecycle = ConnectionLifecycle::new();

    let first = lifecycle.begin_connect().unwrap();

    lifecycle.connected(first).unwrap();
    lifecycle.ready(first).unwrap();

    assert!(lifecycle.accepts_data(first));

    // Transport failure invalidates the current generation
    // immediately, before any retry attempt.
    lifecycle.failed(first).unwrap();

    assert_eq!(lifecycle.state(), ConnectionState::Backoff,);

    assert_eq!(lifecycle.current_generation(), None);

    assert!(!lifecycle.accepts_data(first));
    assert!(!lifecycle.is_current(first));

    // Allocate a new physical connection generation.
    let second = lifecycle.begin_connect().unwrap();

    assert_eq!(second.get(), first.get() + 1);

    lifecycle.connected(second).unwrap();
    lifecycle.ready(second).unwrap();

    assert!(lifecycle.accepts_data(second));

    // A buffered event from the previous connection
    // must never become valid again.
    assert!(!lifecycle.accepts_data(first));
}

// ============================================================
// STALE CALLBACKS
// ============================================================

#[test]
fn stale_connection_callback_cannot_change_state() {
    let mut lifecycle = ConnectionLifecycle::new();

    let first = lifecycle.begin_connect().unwrap();

    lifecycle.failed(first).unwrap();

    let second = lifecycle.begin_connect().unwrap();

    lifecycle.connected(second).unwrap();

    let result = lifecycle.connected(first);

    assert!(matches!(
        result,
        Err(ConnectionLifecycleError::StaleGeneration { .. }),
    ));

    // The stale callback must not modify the current state.
    assert_eq!(lifecycle.state(), ConnectionState::Connected,);

    assert_eq!(lifecycle.current_generation(), Some(second),);
}

// ============================================================
// INVALID TRANSITIONS
// ============================================================

#[test]
fn rejects_ready_before_handshake() {
    let mut lifecycle = ConnectionLifecycle::new();

    let generation = lifecycle.begin_connect().unwrap();

    let result = lifecycle.ready(generation);

    assert!(matches!(
        result,
        Err(ConnectionLifecycleError::InvalidTransition { .. }),
    ));

    assert_eq!(lifecycle.state(), ConnectionState::Connecting,);

    assert!(!lifecycle.accepts_data(generation));
}

#[test]
fn rejects_duplicate_connect_attempt() {
    let mut lifecycle = ConnectionLifecycle::new();

    let first = lifecycle.begin_connect().unwrap();

    assert!(matches!(
        lifecycle.begin_connect(),
        Err(ConnectionLifecycleError::InvalidTransition { .. }),
    ));

    // A rejected transition does not allocate an ID.
    assert_eq!(lifecycle.last_generation(), 1);
    assert_eq!(lifecycle.current_generation(), Some(first));
}

#[test]
fn rejects_duplicate_ready_transition() {
    let mut lifecycle = ConnectionLifecycle::new();

    let generation = lifecycle.begin_connect().unwrap();

    lifecycle.connected(generation).unwrap();
    lifecycle.ready(generation).unwrap();

    assert!(matches!(
        lifecycle.ready(generation),
        Err(ConnectionLifecycleError::InvalidTransition { .. }),
    ));

    assert!(lifecycle.accepts_data(generation));
}

// ============================================================
// FAILURE DURING CONNECT
// ============================================================

#[test]
fn handshake_failure_invalidates_generation() {
    let mut lifecycle = ConnectionLifecycle::new();

    let failed = lifecycle.begin_connect().unwrap();

    lifecycle.failed(failed).unwrap();

    assert_eq!(lifecycle.state(), ConnectionState::Backoff,);

    assert!(!lifecycle.accepts_data(failed));

    let retry = lifecycle.begin_connect().unwrap();

    assert_eq!(retry.get(), failed.get() + 1);
    assert!(!lifecycle.accepts_data(retry));
}

// ============================================================
// TERMINAL SHUTDOWN
// ============================================================

#[test]
fn shutdown_revokes_generation() {
    let mut lifecycle = ConnectionLifecycle::new();

    let generation = lifecycle.begin_connect().unwrap();

    lifecycle.connected(generation).unwrap();
    lifecycle.ready(generation).unwrap();

    lifecycle.stop();

    assert_eq!(lifecycle.state(), ConnectionState::Stopped,);

    assert_eq!(lifecycle.current_generation(), None);

    assert!(!lifecycle.accepts_data(generation));

    // A stopped lifecycle cannot be restarted implicitly.
    assert!(matches!(
        lifecycle.begin_connect(),
        Err(ConnectionLifecycleError::Stopped),
    ));

    // Late callbacks cannot reactivate the slot.
    assert!(matches!(
        lifecycle.ready(generation),
        Err(ConnectionLifecycleError::Stopped),
    ));
}

#[test]
fn shutdown_is_idempotent() {
    let mut lifecycle = ConnectionLifecycle::new();

    lifecycle.stop();
    lifecycle.stop();

    assert_eq!(lifecycle.state(), ConnectionState::Stopped,);
}

// ============================================================
// MANY RECONNECTIONS
// ============================================================

#[test]
fn generations_never_repeat_during_reconnects() {
    let mut lifecycle = ConnectionLifecycle::new();

    let mut previous = 0_u64;

    for _ in 0..1000 {
        let generation = lifecycle.begin_connect().unwrap();

        assert!(generation.get() > previous);

        assert_eq!(generation.get(), previous + 1,);

        lifecycle.connected(generation).unwrap();
        lifecycle.ready(generation).unwrap();

        assert!(lifecycle.accepts_data(generation));

        lifecycle.failed(generation).unwrap();

        assert!(!lifecycle.accepts_data(generation));

        previous = generation.get();
    }

    assert_eq!(lifecycle.last_generation(), 1000);
}
