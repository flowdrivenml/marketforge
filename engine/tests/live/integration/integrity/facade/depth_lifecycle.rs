use marketforge_engine::{
    canonical::Exchange,
    live::integrity::{
        CompiledDepthContinuityPlan, CreateDepthIntegrity, DepthBridgeRule, DepthBufferLimits,
        DepthContinuityFamily, DepthResourceLimits, DepthSnapshotSource, DepthSourceBinding,
        DepthSourceEpoch, IntegrityAdmissionState, IntegrityFacadeFactory, IntegrityLifecycleEvent,
        IntegrityLifecycleOutcome, StreamIntegrityHandle,
    },
};

// ============================================================
// FIXTURES
// ============================================================

const STREAM: &str = "bybit:spot:BTCUSDT:depth";

fn epoch(stream: u64, connection: u64, subscription: u64) -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: stream,
        connection_generation: connection,
        subscription_generation: subscription,
    }
}

fn binding(current: DepthSourceEpoch) -> DepthSourceBinding {
    DepthSourceBinding::new(
        STREAM,
        Exchange::Bybit,
        "spot",
        42,
        "BTCUSDT",
        Some(50),
        current,
    )
    .unwrap()
}

fn plan() -> CompiledDepthContinuityPlan {
    CompiledDepthContinuityPlan {
        format_id: "bybit_spot".into(),
        family: DepthContinuityFamily::BybitV5,
        snapshot_source: DepthSnapshotSource::WebSocket,
        initial_bridge: DepthBridgeRule::WebSocketSnapshotBaseline,
        requires_update_buffer: false,
        accepts_websocket_snapshots: true,
        allows_overlapping_ranges: false,
        allows_equal_sequence_keepalive: true,
        allows_linked_sequence_decrease: false,
        requires_generation_fencing: true,
    }
}

fn integrity() -> StreamIntegrityHandle {
    IntegrityFacadeFactory::depth(CreateDepthIntegrity {
        binding: binding(epoch(10, 20, 30)),
        plan: plan(),

        buffer_limits: DepthBufferLimits {
            max_messages: 100,
            max_bytes: 1_000_000,
        },

        resource_limits: DepthResourceLimits::default(),

        rest_endpoint: None,
    })
    .unwrap()
}

// ============================================================
// TESTS
// ============================================================

#[test]
fn depth_handle_starts_active_but_unsynchronized() {
    let handle = integrity();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Active,);

    assert_eq!(handle.stream_generation(), 10);

    assert_eq!(handle.status().depth_synchronized, Some(false),);

    assert!(handle.book().is_none());
}

#[test]
fn interruption_closes_depth_admission() {
    let mut handle = integrity();

    let result = handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert_eq!(result, IntegrityLifecycleOutcome::DepthInvalidated,);

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );

    assert_eq!(handle.status().depth_synchronized, Some(false),);

    assert!(handle.book().is_none());
}

#[test]
fn trusted_generation_reopens_depth_admission() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let next = epoch(11, 21, 31);

    let result = handle
        .on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(binding(next)))
        .unwrap();

    assert_eq!(result, IntegrityLifecycleOutcome::GenerationAdvanced,);

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Active,);

    assert_eq!(handle.stream_generation(), 11);

    // Transport admission is restored.
    //
    // This does not establish an authoritative book.
    assert_eq!(handle.status().depth_synchronized, Some(false),);

    assert!(handle.book().is_none());
}

#[test]
fn obsolete_depth_generation_cannot_reopen_admission() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let result = handle.on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(binding(epoch(
        10, 20, 30,
    ))));

    assert!(result.is_err());

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );

    assert_eq!(handle.stream_generation(), 10);
}

#[test]
fn depth_generation_cannot_change_source_identity() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let wrong_binding = DepthSourceBinding::new(
        STREAM,
        Exchange::Bybit,
        "spot",
        42,
        "ETHUSDT",
        Some(50),
        epoch(11, 21, 31),
    )
    .unwrap();

    let result = handle.on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(wrong_binding));

    assert!(result.is_err());

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );

    assert_eq!(handle.stream_generation(), 10);
}

#[test]
fn stopped_depth_handle_is_terminal() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
        .unwrap();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);

    let next = binding(epoch(11, 21, 31));

    let result = handle.on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(next));

    assert!(result.is_err());

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);

    assert_eq!(handle.stream_generation(), 10);

    assert!(handle.book().is_none());
}

#[test]
fn repeated_interruption_cannot_downgrade_stopped_state() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
        .unwrap();

    let result = handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert_eq!(result, IntegrityLifecycleOutcome::SourceStopped,);

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);
}
