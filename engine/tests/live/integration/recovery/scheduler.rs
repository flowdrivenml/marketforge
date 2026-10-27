use marketforge_engine::live::recovery::{
    RecoveryJobId, RecoveryRegistryError, RecoveryScheduler, RecoverySchedulerError,
    RecoverySchedulerLimits,
};

use super::model::depth_request;

// ============================================================
// FIXTURE
// ============================================================

fn scheduler(concurrency: usize) -> RecoveryScheduler {
    RecoveryScheduler::new(RecoverySchedulerLimits {
        max_requests: 100,
        max_jobs: 100,
        max_concurrent_jobs: concurrency,
    })
    .unwrap()
}

// ============================================================
// TESTS
// ============================================================

#[test]
fn rejects_invalid_scheduler_limits() {
    for (requests, jobs, concurrency) in [(0, 10, 2), (10, 0, 2), (10, 10, 0)] {
        let result = RecoveryScheduler::new(RecoverySchedulerLimits {
            max_requests: requests,
            max_jobs: jobs,
            max_concurrent_jobs: concurrency,
        });

        assert!(matches!(result, Err(RecoverySchedulerError::InvalidLimits),));
    }
}

#[test]
fn dispatches_jobs_in_fifo_order() {
    let mut scheduler = scheduler(2);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(100)))
        .unwrap();

    let b = scheduler
        .register(depth_request("b", 10, 1, Some(500)))
        .unwrap();

    let c = scheduler
        .register(depth_request("c", 10, 1, Some(1000)))
        .unwrap();

    let ready = scheduler.start_ready_jobs().unwrap();

    assert_eq!(ready.len(), 2);

    assert_eq!(ready[0].job_id, a.job_id);
    assert_eq!(ready[1].job_id, b.job_id);

    assert_eq!(scheduler.running_count(), 2);
    assert_eq!(scheduler.available_slots(), 0);

    assert_eq!(scheduler.queued_jobs(), vec![c.job_id]);
}

#[test]
fn identical_requests_share_one_execution_slot() {
    let mut scheduler = scheduler(1);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(1000)))
        .unwrap();

    let b = scheduler
        .register(depth_request("b", 20, 2, Some(1000)))
        .unwrap();

    assert_eq!(a.job_id, b.job_id);

    let ready = scheduler.start_ready_jobs().unwrap();

    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].job_id, a.job_id);

    assert_eq!(scheduler.running_count(), 1);

    let completion = scheduler.complete(a.job_id).unwrap();

    assert_eq!(completion.recipients.len(), 2);

    assert_eq!(scheduler.running_count(), 0);
    assert_eq!(scheduler.job_count(), 0);
}

#[test]
fn completion_releases_capacity_for_next_job() {
    let mut scheduler = scheduler(1);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(100)))
        .unwrap();

    let b = scheduler
        .register(depth_request("b", 10, 1, Some(500)))
        .unwrap();

    let ready = scheduler.start_ready_jobs().unwrap();

    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].job_id, a.job_id);

    scheduler.complete(a.job_id).unwrap();

    assert_eq!(scheduler.available_slots(), 1);

    let next = scheduler.start_ready_jobs().unwrap();

    assert_eq!(next.len(), 1);
    assert_eq!(next[0].job_id, b.job_id);
}

#[test]
fn cancellation_of_one_waiter_keeps_shared_job_running() {
    let mut scheduler = scheduler(1);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(1000)))
        .unwrap();

    let b = scheduler
        .register(depth_request("b", 10, 1, Some(1000)))
        .unwrap();

    scheduler.start_ready_jobs().unwrap();

    let cancellation = scheduler.cancel(a.request_id).unwrap();

    assert_eq!(cancellation.abort_job, None);

    assert_eq!(scheduler.running_count(), 1);
    assert_eq!(scheduler.request_count(), 1);

    let completion = scheduler.complete(b.job_id).unwrap();

    assert_eq!(completion.recipients.len(), 1);

    assert_eq!(completion.recipients[0].request_id, b.request_id,);
}

#[test]
fn last_running_waiter_cancellation_requests_abort() {
    let mut scheduler = scheduler(1);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(1000)))
        .unwrap();

    scheduler.start_ready_jobs().unwrap();

    let cancellation = scheduler.cancel(a.request_id).unwrap();

    assert_eq!(cancellation.abort_job, Some(a.job_id));

    assert_eq!(scheduler.running_count(), 0);
    assert_eq!(scheduler.available_slots(), 1);

    assert_eq!(scheduler.job_count(), 0);
}

#[test]
fn cancelled_job_completion_cannot_affect_new_job() {
    let mut scheduler = scheduler(1);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(1000)))
        .unwrap();

    scheduler.start_ready_jobs().unwrap();

    let cancelled = scheduler.cancel(a.request_id).unwrap();

    assert_eq!(cancelled.abort_job, Some(a.job_id));

    let b = scheduler
        .register(depth_request("b", 20, 1, Some(1000)))
        .unwrap();

    assert_ne!(a.job_id, b.job_id);

    scheduler.start_ready_jobs().unwrap();

    // A late callback from the cancelled acquisition must
    // not complete the new job or release its slot.
    let result = scheduler.complete(a.job_id);

    assert!(matches!(
        result,
        Err(
            RecoverySchedulerError::InconsistentRunningState(id)
        ) if id == a.job_id
    ));

    assert!(scheduler.is_running(b.job_id));

    assert_eq!(scheduler.running_count(), 1);

    scheduler.complete(b.job_id).unwrap();
}

#[test]
fn generation_cancellation_preserves_other_streams() {
    let mut scheduler = scheduler(1);

    let a = scheduler
        .register(depth_request("a", 10, 1, Some(1000)))
        .unwrap();

    let b = scheduler
        .register(depth_request("b", 20, 1, Some(1000)))
        .unwrap();

    scheduler.start_ready_jobs().unwrap();

    let cancelled = scheduler.cancel_generation("a", 10);

    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].cancellation.request_id, a.request_id);

    assert_eq!(cancelled[0].abort_job, None);

    assert_eq!(scheduler.running_count(), 1);

    let completion = scheduler.complete(b.job_id).unwrap();

    assert_eq!(completion.recipients.len(), 1);

    assert_eq!(completion.recipients[0].request.requester.stream_id, "b",);
}

#[test]
fn queued_job_cancellation_requires_no_abort() {
    let mut scheduler = scheduler(1);

    let running = scheduler
        .register(depth_request("running", 10, 1, Some(100)))
        .unwrap();

    let queued = scheduler
        .register(depth_request("queued", 10, 1, Some(500)))
        .unwrap();

    scheduler.start_ready_jobs().unwrap();

    let cancellation = scheduler.cancel(queued.request_id).unwrap();

    assert_eq!(cancellation.abort_job, None);
    assert!(cancellation.cancellation.job_became_empty);
    assert!(!cancellation.cancellation.was_running);

    assert_eq!(scheduler.queued_jobs(), Vec::<RecoveryJobId>::new());

    assert!(scheduler.is_running(running.job_id));
}

#[test]
fn scheduler_enforces_registry_capacity() {
    let mut scheduler = RecoveryScheduler::new(RecoverySchedulerLimits {
        max_requests: 1,
        max_jobs: 1,
        max_concurrent_jobs: 1,
    })
    .unwrap();

    scheduler
        .register(depth_request("a", 10, 1, Some(100)))
        .unwrap();

    let result = scheduler.register(depth_request("b", 10, 1, Some(1000)));

    assert!(matches!(
        result,
        Err(RecoverySchedulerError::Registry(
            RecoveryRegistryError::RequestCapacityExceeded
        )),
    ));

    assert_eq!(scheduler.request_count(), 1);
    assert_eq!(scheduler.job_count(), 1);
}
