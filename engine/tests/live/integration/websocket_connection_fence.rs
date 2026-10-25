use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
};

use marketforge_engine::live::infrastructure::websocket::{
    ConnectionFence, ConnectionState, FencedMessage,
};

// ============================================================
// SHARED AUTHORITY
// ============================================================

#[test]
fn w33_fence_clones_share_lifecycle() {
    let owner = ConnectionFence::new("runtime-1", "slot-A").unwrap();

    let consumer = owner.clone();

    let generation = owner.begin_connect().unwrap();

    assert_eq!(consumer.state(), ConnectionState::Connecting);

    owner.connected(&generation).unwrap();

    assert!(!consumer.accepts(&generation));

    owner.ready(&generation).unwrap();

    assert!(consumer.accepts(&generation));

    owner.failed(&generation).unwrap();

    assert!(!consumer.accepts(&generation));
}

// ============================================================
// STALE BUFFERED DATA
// ============================================================

#[test]
fn w33_old_generation_messages_are_rejected() {
    let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

    let first = fence.begin_connect().unwrap();

    fence.connected(&first).unwrap();
    fence.ready(&first).unwrap();

    let buffered = FencedMessage::new(first.clone(), "depth-update-500");

    assert!(fence.accepts(buffered.stamp()));

    // Old connection fails.
    fence.failed(&first).unwrap();

    // Replacement establishes a new generation.
    let second = fence.begin_connect().unwrap();

    fence.connected(&second).unwrap();
    fence.ready(&second).unwrap();

    assert!(fence.accepts(&second));

    // Previously buffered data cannot be accepted.
    assert!(!fence.accepts(buffered.stamp()));

    assert_eq!(
        fence.with_ready(buffered.stamp(), || buffered.payload(),),
        None,
    );
}

// ============================================================
// SLOT IDENTITY
// ============================================================

#[test]
fn w33_separate_connection_slots_do_not_share_generations() {
    let first = ConnectionFence::new("runtime-1", "slot-A").unwrap();

    let second = ConnectionFence::new("runtime-1", "slot-B").unwrap();

    let first_stamp = first.begin_connect().unwrap();
    let second_stamp = second.begin_connect().unwrap();

    assert_eq!(
        first_stamp.generation().get(),
        second_stamp.generation().get(),
    );

    first.connected(&first_stamp).unwrap();
    first.ready(&first_stamp).unwrap();

    second.connected(&second_stamp).unwrap();
    second.ready(&second_stamp).unwrap();

    assert!(first.accepts(&first_stamp));
    assert!(second.accepts(&second_stamp));

    assert!(!first.accepts(&second_stamp));
    assert!(!second.accepts(&first_stamp));
}

// ============================================================
// RUNTIME INCARNATION
// ============================================================

#[test]
fn w33_previous_runtime_generation_is_rejected() {
    let old = ConnectionFence::new("runtime-old", "slot-A").unwrap();

    let current = ConnectionFence::new("runtime-new", "slot-A").unwrap();

    let old_stamp = old.begin_connect().unwrap();

    let current_stamp = current.begin_connect().unwrap();

    current.connected(&current_stamp).unwrap();
    current.ready(&current_stamp).unwrap();

    // Generation numbers happen to be equal.
    assert_eq!(
        old_stamp.generation().get(),
        current_stamp.generation().get(),
    );

    // Runtime incarnation prevents accidental equality.
    assert!(!current.accepts(&old_stamp));
    assert!(current.accepts(&current_stamp));
}

// ============================================================
// NO PUBLICATION BEFORE READY
// ============================================================

#[test]
fn w33_connected_socket_cannot_accept_market_data() {
    let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    let accepted = fence.with_ready(&stamp, || {
        panic!("unready connection accepted market data");
    });

    assert!(accepted.is_none());

    fence.ready(&stamp).unwrap();

    assert_eq!(fence.with_ready(&stamp, || 42), Some(42),);
}

// ============================================================
// REVOCATION VS SYNCHRONOUS PROCESSING
// ============================================================

#[test]
fn w33_revocation_waits_for_inflight_fenced_operation() {
    let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();
    fence.ready(&stamp).unwrap();

    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));

    let revoked = Arc::new(AtomicBool::new(false));

    let worker_fence = fence.clone();
    let worker_stamp = stamp.clone();

    let worker_started = started.clone();
    let worker_release = release.clone();

    let worker = std::thread::spawn(move || {
        worker_fence.with_ready(&worker_stamp, || {
            worker_started.wait();

            // Keep the read lock held until the test
            // explicitly permits processing to finish.
            worker_release.wait();

            "accepted"
        })
    });

    // Wait until the processing closure has entered.
    started.wait();

    let revoke_fence = fence.clone();
    let revoke_stamp = stamp.clone();
    let revoke_complete = revoked.clone();

    let revoker = std::thread::spawn(move || {
        revoke_fence.failed(&revoke_stamp).unwrap();

        revoke_complete.store(true, Ordering::SeqCst);
    });

    // The processing closure is still holding the
    // lifecycle read lock at this point.
    assert!(!revoked.load(Ordering::SeqCst));

    release.wait();

    assert_eq!(worker.join().unwrap(), Some("accepted"));

    revoker.join().unwrap();

    assert!(revoked.load(Ordering::SeqCst));

    assert!(!fence.accepts(&stamp));
}

// ============================================================
// TERMINAL SHUTDOWN
// ============================================================

#[test]
fn w33_shutdown_invalidates_all_clones() {
    let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

    let consumer = fence.clone();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();
    fence.ready(&stamp).unwrap();

    fence.stop();

    assert_eq!(consumer.state(), ConnectionState::Stopped);

    assert!(!consumer.accepts(&stamp));
    assert!(consumer.current_stamp().is_none());
}

// ============================================================
// VALIDATE IDENTITIES
// ============================================================

#[test]
fn w33_rejects_empty_runtime_and_slot_identity() {
    assert!(ConnectionFence::new("", "slot-A").is_err());

    assert!(ConnectionFence::new("runtime-1", "").is_err());
}
