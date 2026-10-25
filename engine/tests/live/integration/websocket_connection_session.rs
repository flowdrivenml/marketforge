use std::time::Duration;

use marketforge_engine::live::infrastructure::websocket::{
    ConnectionSessionError, ConnectionSessionLimits, SubscriptionDriverLimits,
};

fn valid_limits() -> ConnectionSessionLimits {
    ConnectionSessionLimits {
        inbound_capacity: 1024,
        outbound_capacity: 256,
        command_capacity: 128,
        event_capacity: 128,

        dispatch_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),

        driver: SubscriptionDriverLimits {
            queue_timeout: Duration::from_secs(2),
            receipt_timeout: Duration::from_secs(2),
            maximum_early_frames: 1024,
        },
    }
}

#[test]
fn w46_accepts_bounded_session_configuration() {
    valid_limits().validate().unwrap();
}

#[test]
fn w46_rejects_zero_inbound_capacity() {
    let mut limits = valid_limits();

    limits.inbound_capacity = 0;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::InvalidLimits),
    ));
}

#[test]
fn w46_rejects_zero_outbound_capacity() {
    let mut limits = valid_limits();

    limits.outbound_capacity = 0;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::InvalidLimits),
    ));
}

#[test]
fn w46_rejects_zero_command_capacity() {
    let mut limits = valid_limits();

    limits.command_capacity = 0;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::InvalidLimits),
    ));
}

#[test]
fn w46_rejects_zero_dispatch_timeout() {
    let mut limits = valid_limits();

    limits.dispatch_timeout = Duration::ZERO;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::InvalidLimits),
    ));
}

#[test]
fn w46_rejects_zero_shutdown_timeout() {
    let mut limits = valid_limits();

    limits.shutdown_timeout = Duration::ZERO;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::InvalidLimits),
    ));
}

#[test]
fn w46_rejects_invalid_subscription_driver_limits() {
    let mut limits = valid_limits();

    limits.driver.maximum_early_frames = 0;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::Driver(_)),
    ));
}

#[test]
fn w46_rejects_zero_event_capacity() {
    let mut limits = valid_limits();

    limits.event_capacity = 0;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionSessionError::InvalidLimits),
    ));
}
