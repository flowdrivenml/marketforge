use marketforge_engine::live::recovery::{
    RecoveryJobState, RecoveryRegistry, RecoveryRegistryError, RecoveryRegistryLimits,
};

use super::model::depth_request;

fn registry() -> RecoveryRegistry {
    RecoveryRegistry::new(RecoveryRegistryLimits {
        max_requests: 100,
        max_jobs: 20,
    })
    .unwrap()
}

#[test]
fn identical_queued_requests_share_one_job() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let b = registry
        .register(depth_request("stream-b", 17, 3, Some(1000)))
        .unwrap();

    assert!(a.created_job);
    assert!(!b.created_job);

    assert_eq!(a.job_id, b.job_id);
    assert_ne!(a.request_id, b.request_id);

    assert_eq!(registry.job_count(), 1);
    assert_eq!(registry.request_count(), 2);

    assert_eq!(registry.requester_count(a.job_id), Some(2));
}

#[test]
fn incompatible_requests_create_separate_jobs() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(100)))
        .unwrap();

    let b = registry
        .register(depth_request("stream-b", 10, 1, Some(1000)))
        .unwrap();

    assert_ne!(a.job_id, b.job_id);

    assert_eq!(registry.job_count(), 2);
}

#[test]
fn running_jobs_do_not_accept_late_joiners() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    registry.start(a.job_id).unwrap();

    assert_eq!(
        registry.job_state(a.job_id),
        Some(RecoveryJobState::Running),
    );

    let b = registry
        .register(depth_request("stream-b", 10, 1, Some(1000)))
        .unwrap();

    assert!(b.created_job);
    assert_ne!(a.job_id, b.job_id);

    assert_eq!(registry.job_count(), 2);
    assert_eq!(registry.queued_jobs(), vec![b.job_id]);
}

#[test]
fn cancelling_one_waiter_preserves_shared_job() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let b = registry
        .register(depth_request("stream-b", 10, 1, Some(1000)))
        .unwrap();

    let cancelled = registry.cancel(a.request_id).unwrap();

    assert!(!cancelled.job_became_empty);

    assert_eq!(registry.job_count(), 1);
    assert_eq!(registry.request_count(), 1);

    assert_eq!(registry.requester_count(b.job_id), Some(1));
}

#[test]
fn cancelling_last_queued_waiter_removes_job() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let cancelled = registry.cancel(a.request_id).unwrap();

    assert!(cancelled.job_became_empty);
    assert!(!cancelled.was_running);

    assert_eq!(registry.job_count(), 0);
    assert_eq!(registry.request_count(), 0);

    assert!(registry.queued_jobs().is_empty());
}

#[test]
fn cancelling_last_running_waiter_requests_executor_cancellation() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    registry.start(a.job_id).unwrap();

    let cancelled = registry.cancel(a.request_id).unwrap();

    assert!(cancelled.job_became_empty);
    assert!(cancelled.was_running);

    assert_eq!(registry.job_count(), 0);

    // The coordinator must use these flags to abort
    // the corresponding asynchronous acquisition.
}

#[test]
fn completed_job_preserves_each_requesters_original_identity() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let b = registry
        .register(depth_request("stream-b", 17, 3, Some(1000)))
        .unwrap();

    registry.start(a.job_id).unwrap();

    let completion = registry.complete(a.job_id).unwrap();

    assert_eq!(completion.job_id, a.job_id);
    assert_eq!(completion.recipients.len(), 2);

    assert_eq!(completion.recipients[0].request_id, a.request_id,);

    assert_eq!(completion.recipients[1].request_id, b.request_id,);

    assert_eq!(
        completion.recipients[0].request.requester.stream_id,
        "stream-a",
    );

    assert_eq!(
        completion.recipients[1].request.requester.stream_id,
        "stream-b",
    );

    assert_eq!(
        completion.recipients[1]
            .request
            .requester
            .epoch
            .stream_generation,
        17,
    );

    assert_eq!(registry.job_count(), 0);
    assert_eq!(registry.request_count(), 0);
}

#[test]
fn generation_cancellation_detaches_only_matching_requests() {
    let mut registry = registry();

    let old = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let current = registry
        .register(depth_request("stream-a", 11, 1, Some(1000)))
        .unwrap();

    let other = registry
        .register(depth_request("stream-b", 10, 1, Some(1000)))
        .unwrap();

    let cancelled = registry.cancel_generation("stream-a", 10);

    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].request_id, old.request_id);

    assert!(registry.request(current.request_id).is_some());
    assert!(registry.request(other.request_id).is_some());

    assert_eq!(registry.request_count(), 2);
    assert_eq!(registry.job_count(), 1);
}

#[test]
fn duplicate_requester_is_rejected() {
    let mut registry = registry();

    registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let duplicate = registry.register(depth_request("stream-a", 10, 1, Some(1000)));

    assert!(matches!(
        duplicate,
        Err(RecoveryRegistryError::DuplicateRequester),
    ));

    assert_eq!(registry.request_count(), 1);
}

#[test]
fn completed_or_cancelled_job_cannot_deliver_stale_result() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    registry.start(a.job_id).unwrap();

    registry.cancel(a.request_id).unwrap();

    assert!(matches!(
        registry.complete(a.job_id),
        Err(RecoveryRegistryError::UnknownJob),
    ));

    // Same acquisition, different job identity.
    let b = registry
        .register(depth_request("stream-a", 11, 1, Some(1000)))
        .unwrap();

    assert_ne!(a.job_id, b.job_id);

    assert_eq!(registry.request_count(), 1);
    assert_eq!(registry.job_count(), 1);
}

#[test]
fn capacity_rejection_does_not_modify_registry() {
    let mut registry = RecoveryRegistry::new(RecoveryRegistryLimits {
        max_requests: 1,
        max_jobs: 1,
    })
    .unwrap();

    registry
        .register(depth_request("stream-a", 10, 1, Some(1000)))
        .unwrap();

    let result = registry.register(depth_request("stream-b", 10, 1, Some(1000)));

    assert!(matches!(
        result,
        Err(RecoveryRegistryError::RequestCapacityExceeded),
    ));

    assert_eq!(registry.request_count(), 1);
    assert_eq!(registry.job_count(), 1);
}

#[test]
fn queued_jobs_have_deterministic_order() {
    let mut registry = registry();

    let a = registry
        .register(depth_request("stream-a", 10, 1, Some(100)))
        .unwrap();

    let b = registry
        .register(depth_request("stream-b", 10, 1, Some(500)))
        .unwrap();

    let c = registry
        .register(depth_request("stream-c", 10, 1, Some(1000)))
        .unwrap();

    assert_eq!(registry.queued_jobs(), vec![a.job_id, b.job_id, c.job_id],);

    registry.start(b.job_id).unwrap();

    assert_eq!(registry.queued_jobs(), vec![a.job_id, c.job_id],);
}
