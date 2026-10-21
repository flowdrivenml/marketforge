//! Single mutable owner for the router's in-process delivery responsibilities.
//! Admission and reporting borrow these same owners; this is not a second
//! tracker, an executor, or a crash-durable recovery service.
use super::{
    acknowledgement::{
        AcknowledgementTracker, DeliveryObligationId, FinalizationJournal, PreparedObligations,
        PreparedObligationsAbortError,
    },
    admission::{DeadlineFuture, DeadlineTimer},
    completion::{PendingDeliveryError, RetainedDelivery},
    supervisor::{DeliverySupervisionSnapshot, DeliverySupervisor, SupervisionStep},
    tracking_recovery::{TrackingRecovery, TrackingRecoveryPass},
    worker::{DestinationWorker, WorkerOutcome, WorkerRegistrationError},
};
use crate::live::ports::routing::DestinationCompletion;
use std::task::{Context, Poll, Waker};

/// One bounded recovery pass followed by at most one supervision step.
/// Recovery errors remain observable even when delivery work is pending.
#[derive(Debug)]
#[must_use]
pub struct DeliveryDrive {
    pub recovery: TrackingRecoveryPass,
    pub delivery: Poll<SupervisionStep>,
}

/// One bounded runtime turn using the authoritative registry and delivery owners.
/// Membership errors are observations, not proof that delivery has drained.
#[derive(Debug)]
#[must_use]
pub struct RegisteredDeliveryDrive {
    pub registry: RegistryWorkerSync,
    pub delivery: DeliveryDrive,
    pub membership: Poll<Result<u64, super::router::RouterRegistryError>>,
}

#[derive(Debug)]
#[must_use]
pub enum DeliveryShutdown {
    Step(DeliveryDrive),
    Drained,
    DeadlineReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAdmissionError {
    Closed,
}

/// One bounded membership maintenance turn; no adapter operation is polled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct RegistryWorkerSync {
    pub workers_reaped: usize,
    pub retired_reaped: usize,
    pub attachment: Result<bool, RegistryWorkerSyncError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryWorkerSyncError {
    Closed,
    Registry(super::router::RegistryWorkerError),
    Scheduler(WorkerRegistrationError),
}

pub struct DeliveryDriver {
    closed: bool,
    completion_waiter: Option<Waker>,
    retry_timer: Option<(std::time::Instant, DeadlineFuture)>,
    supervisor: DeliverySupervisor,
    tracker: AcknowledgementTracker,
    recovery: TrackingRecovery,
    journal: FinalizationJournal,
}

impl DeliveryDriver {
    pub(super) fn registered_workers_in(&self, registry: &super::router::RouterRegistry) -> bool {
        self.supervisor.registered_workers_in(registry)
    }

    /// Commit ordered admission against the same owners used by delivery turns.
    /// Shutdown rejects before obligations or queues change and returns all
    /// prepared responsibility intact. No mutable owner escapes this operation.
    #[allow(clippy::result_large_err)]
    pub fn commit_ordered<'a>(
        &mut self,
        prepared: super::ordered::OrderedFanout<'a>,
    ) -> Result<
        (
            super::ordered::OrderedCommitOutcome,
            Option<Result<super::router::BestEffortFanout, super::router::BestEffortError>>,
        ),
        (DeliveryAdmissionError, super::ordered::OrderedFanout<'a>),
    > {
        if self.closed {
            return Err((DeliveryAdmissionError::Closed, prepared));
        }
        let result = prepared.commit_with_best_effort(&mut self.tracker, &mut self.recovery);
        // Queue notifications alone are insufficient: partial commitment can
        // leave tracking repair work even when no worker is waiting on a queue.
        // Rejections create no new responsibility and must preserve the waiter.
        if matches!(
            &result.0,
            super::ordered::OrderedCommitOutcome::Tracked(outcome)
                if !matches!(outcome, super::router::TrackedFanoutResult::Rejected { .. })
        ) {
            if let Some(waiter) = self.completion_waiter.take() {
                waiter.wake();
            }
        }
        Ok(result)
    }

    /// Transfer the authoritative owners, including any existing responsibilities.
    pub fn new(
        supervisor: DeliverySupervisor,
        tracker: AcknowledgementTracker,
        recovery: TrackingRecovery,
        journal: FinalizationJournal,
    ) -> Self {
        Self {
            closed: false,
            completion_waiter: None,
            retry_timer: None,
            supervisor,
            tracker,
            recovery,
            journal,
        }
    }

    /// Exclusive admission access to the same tracker and recovery capacity
    /// consumed by worker supervision. Never hold this borrow across external IO.
    pub fn admission_owners(
        &mut self,
    ) -> Result<(&mut AcknowledgementTracker, &mut TrackingRecovery), DeliveryAdmissionError> {
        if self.closed {
            return Err(DeliveryAdmissionError::Closed);
        }
        Ok((&mut self.tracker, &mut self.recovery))
    }

    /// Reconcile producer-owned tracking repairs using the same owners as
    /// delivery supervision. Also allowed after closure: shutdown fences new
    /// admissions, but cannot waive already accepted repair responsibility.
    pub fn reconcile_ingress_tracking(
        &mut self,
        outcome: &mut super::ingress::IngressOutcome,
    ) -> super::ingress::TrackingHandoff {
        let handoff = outcome.reconcile_tracking(&mut self.tracker, &mut self.recovery);
        if handoff.transferred != 0 {
            self.notify_progress();
        }
        handoff
    }

    /// Inspect the oldest uncorrelated result without transferring its charge.
    /// The runtime must supply a known obligation rather than infer one from a
    /// destination alone: independent cursor namespaces may share a destination.
    pub fn next_unresolved_tracking(
        &self,
    ) -> Option<&super::tracking_recovery::UnresolvedRecoveryOutcome> {
        self.recovery.next_unresolved()
    }

    /// Reconcile one retained result against the authoritative tracker, including
    /// after admission closes. A wrong destination, range or progress leaves the
    /// result intact. This never creates an obligation or fabricates progress.
    pub fn reconcile_unresolved_tracking(
        &mut self,
        obligation: DeliveryObligationId,
    ) -> super::tracking_recovery::TrackingRecoveryOutcome {
        let outcome = self
            .recovery
            .reconcile_next_unresolved(&mut self.tracker, obligation);
        if matches!(
            outcome,
            super::tracking_recovery::TrackingRecoveryOutcome::Recovered { .. }
        ) {
            self.notify_progress();
        }
        outcome
    }

    /// Coalesce externally supplied progress until the next coordinator turn.
    /// Failed or unchanged repair attempts must preserve the registered waiter.
    fn notify_progress(&mut self) {
        if let Some(waiter) = self.completion_waiter.take() {
            waiter.wake();
        }
    }

    /// Attach a registry worker to the existing authoritative delivery owners.
    /// Shutdown fencing is enforced by the owned scheduler. Errors return the
    /// worker intact, including queue lease and any pending operation.
    #[allow(clippy::result_large_err)]
    pub fn insert_worker(
        &mut self,
        worker: DestinationWorker,
    ) -> Result<(), (WorkerRegistrationError, DestinationWorker)> {
        self.supervisor.insert_worker(worker)
    }

    /// Release only closed, drained scheduler instances. Call registry reaping
    /// separately; neither operation establishes delivery completion.
    pub fn reap_drained_workers(&mut self) -> usize {
        self.supervisor.reap_drained_workers()
    }

    /// Reap drained ownership and attach at most one unclaimed registry queue.
    /// Repeat on membership change or after a charge is released, not in a busy
    /// loop after capacity failure. Retired queues take precedence over new ones.
    /// A rejected fresh worker has never dequeued or polled an adapter: releasing
    /// its lease leaves every payload and charge in the original registry queue.
    /// Existing workers and their operations are never replaced or dropped here.
    /// Shutdown permits reaping but fences new attachment.
    pub fn sync_registry(
        &mut self,
        registry: &super::router::RouterRegistry,
    ) -> RegistryWorkerSync {
        let workers_reaped = self.reap_drained_workers();
        let retired_reaped = registry.reap_retired();
        let attachment =
            if self.closed {
                Err(RegistryWorkerSyncError::Closed)
            } else {
                match registry.claim_next_worker() {
                    Ok(Some(worker)) => self.insert_worker(worker).map(|()| true).map_err(
                        |(reason, fresh_worker)| {
                            drop(fresh_worker);
                            RegistryWorkerSyncError::Scheduler(reason)
                        },
                    ),
                    Ok(None) => Ok(false),
                    Err(reason) => Err(RegistryWorkerSyncError::Registry(reason)),
                }
            };
        RegistryWorkerSync {
            workers_reaped,
            retired_reaped,
            attachment,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Roll back an existing preparation even after admission closes. This
    /// deliberately grants no access for creating or committing new obligations.
    pub fn abort_prepared(
        &mut self,
        prepared: PreparedObligations,
    ) -> Result<(), PreparedObligationsAbortError> {
        self.tracker.abort_prepared(prepared)
    }

    /// Reporting must explicitly acknowledge or escalate actual terminal records.
    pub fn journal_mut(&mut self) -> &mut FinalizationJournal {
        &mut self.journal
    }

    pub fn snapshot(&self) -> DeliverySupervisionSnapshot {
        self.supervisor
            .snapshot(&self.tracker, &self.recovery, &self.journal)
    }

    /// Does not sleep or loop until success. The enclosing runtime schedules a
    /// turn on worker wakeup, completion, reporting, or a bounded retry timer.
    /// A failed recovery pass alone must not trigger immediate repeated turns.
    /// Recovery precedes finalization so successfully repaired tracking can
    /// release a terminal payload in this same turn. Independent worker work
    /// continues even when some recovery updates cannot yet be applied.
    pub fn drive(&mut self, cx: &mut Context<'_>) -> DeliveryDrive {
        // One replaceable coordinator, bounded independently of obligation count.
        // Arm before polling so completions between turns cannot lose the wake.
        if self
            .completion_waiter
            .as_ref()
            .is_none_or(|waiter| !waiter.will_wake(cx.waker()))
        {
            self.completion_waiter = Some(cx.waker().clone());
        }
        self.journal.arm_capacity_notification(cx.waker());
        let recovery = self.recovery.retry_pass(&mut self.tracker);
        let delivery = self
            .supervisor
            .poll_step(cx, &mut self.tracker, &mut self.journal);
        DeliveryDrive { recovery, delivery }
    }

    /// Reconcile at most one queue, run one delivery turn, then arm membership
    /// observation against the revision captured before either operation. This
    /// closes the race with membership changes during adapter polling.
    ///
    /// Successful attachment, retention and finalization schedule another bounded
    /// turn. Capacity/correlation/reporting failures and failed tracking retries
    /// never self-wake: the runtime must arrange reporting notifications
    /// and a bounded retry timer. Correlated completions wake the last drive waker.
    /// Registered queue payload releases and recovery transfers schedule progress
    /// turns, including releases by external recovery owners after closure.
    /// Worker futures and idle queues register this waker.
    /// This is a cooperative scheduling primitive, not a deadline shutdown path;
    /// close admission and synchronize all queues before closing the driver.
    pub fn drive_registered(
        &mut self,
        registry: &super::router::RouterRegistry,
        cx: &mut Context<'_>,
    ) -> RegisteredDeliveryDrive {
        let revision = registry.revision();
        registry.arm_release_notifications(cx.waker());
        let maintenance = self.sync_registry(registry);
        let delivery = self.drive(cx);
        let membership = registry.poll_membership_change(revision, cx);
        let progressed = matches!(
            delivery.delivery,
            Poll::Ready(
                SupervisionStep::Retained(_)
                    | SupervisionStep::Finalized(_)
                    | SupervisionStep::BestEffort(_)
            )
        );
        if maintenance.attachment == Ok(true)
            || progressed
            || matches!(membership, Poll::Ready(Ok(_)))
        {
            cx.waker().wake_by_ref();
        }
        RegisteredDeliveryDrive {
            registry: maintenance,
            delivery,
            membership,
        }
    }

    /// Run one registered turn and maintain at most one retry timer. Notifications
    /// do not postpone an existing deadline. Expiry never replays adapter calls:
    /// the normal turn retries tracking, reporting handoff and worker maintenance.
    /// The caller must keep using this entry point while retry scheduling is needed.
    ///
    /// A zero interval or timer admission failure is explicit, while independent
    /// delivery progress still runs. The runtime must surface scheduling failure;
    /// it must not immediately repoll it in a busy loop. Closing admission retains
    /// the timer until every owned responsibility and payload charge has drained.
    /// No missed-tick catch-up is performed after a delayed runtime turn.
    /// Early timer readiness returns InvalidInput without self-waking; the
    /// authoritative clock also retires a timer that remains pending when due.
    pub fn drive_registered_with_retry(
        &mut self,
        registry: &super::router::RouterRegistry,
        cx: &mut Context<'_>,
        clock: &dyn crate::live::foundation::clock::MonotonicClock,
        timer: &dyn DeadlineTimer,
        interval: std::time::Duration,
    ) -> (
        RegisteredDeliveryDrive,
        Result<(), crate::live::model::error::ErrorCode>,
    ) {
        use crate::live::model::error::ErrorCode;
        let turn = self.drive_registered(registry, cx);
        if self.closed && self.snapshot().is_drained() {
            self.retry_timer = None;
            return (turn, Ok(()));
        }
        if interval.is_zero() {
            return (turn, Err(ErrorCode::InvalidInput));
        }
        if let Some((deadline, sleep)) = self.retry_timer.as_mut() {
            // Timer readiness is not proof that the authoritative clock reached
            // its boundary. Reject a broken/early adapter without self-waking.
            let ready = sleep.as_mut().poll(cx).is_ready();
            let due = clock.now() >= *deadline;
            if ready || due {
                self.retry_timer = None;
                if !due {
                    return (turn, Err(ErrorCode::InvalidInput));
                }
            }
        }
        if self.retry_timer.is_none() {
            let Some(deadline) = clock.now().checked_add(interval) else {
                return (turn, Err(ErrorCode::Limit));
            };
            let mut sleep = match timer.try_sleep_until(deadline) {
                Ok(sleep) => sleep,
                Err(reason) => return (turn, Err(reason)),
            };
            let ready = sleep.as_mut().poll(cx).is_ready();
            // Check after polling: time may cross the boundary inside the poll.
            if clock.now() >= deadline {
                cx.waker().wake_by_ref();
            } else if ready {
                return (turn, Err(ErrorCode::InvalidInput));
            } else {
                self.retry_timer = Some((deadline, sleep));
            }
        }
        (turn, Ok(()))
    }

    /// Run one timed delivery turn and report at most one terminal record from
    /// the same authoritative journal, including while admission is closed.
    /// Temporary failures retain the record with its existing retry policy;
    /// permanent failures remain escalated for explicit resolution. Only actual
    /// reporting progress schedules another turn, so deferred retries never spin.
    /// Reporters must be bounded, nonblocking and idempotent: this cooperative
    /// owner cannot preempt a synchronous handler or establish its durability.
    #[allow(clippy::too_many_arguments)]
    pub fn drive_registered_reporting<R: super::reporting::FinalizationReporter>(
        &mut self,
        registry: &super::router::RouterRegistry,
        cx: &mut Context<'_>,
        clock: &dyn crate::live::foundation::clock::MonotonicClock,
        timer: &dyn DeadlineTimer,
        interval: std::time::Duration,
        consumer: &super::reporting::FinalizationReportingConsumer<R>,
    ) -> (
        RegisteredDeliveryDrive,
        Result<(), crate::live::model::error::ErrorCode>,
        Result<super::reporting::ReportingStep, super::reporting::ReportingCoordinatorError>,
    ) {
        use super::reporting::ReportingStep;
        let (turn, scheduling) =
            self.drive_registered_with_retry(registry, cx, clock, timer, interval);
        let reporting = consumer.process_one(&mut self.journal, clock);
        // Deferring or escalating one record also advances the eligible scan.
        // Another ready record must not wait for a retry timer (which may have
        // failed admission). The subsequent Idle turn parks without self-waking.
        if matches!(
            reporting,
            Ok(ReportingStep::Reported
                | ReportingStep::RetryScheduled
                | ReportingStep::PermanentlyFailed)
        ) {
            cx.waker().wake_by_ref();
        }
        if self.closed && self.snapshot().is_drained() {
            self.retry_timer = None;
        }
        (turn, scheduling, reporting)
    }

    /// A successfully correlated completion wakes the most recently polled
    /// coordinator once. Rejected completions neither consume nor wake it.
    /// This also applies while draining a closed driver; completion is progress,
    /// not new admission. Further completions coalesce until the next drive.
    pub fn apply_completion(
        &mut self,
        completion: &DestinationCompletion,
    ) -> Result<DeliveryObligationId, PendingDeliveryError> {
        let id = self
            .supervisor
            .apply_completion(&mut self.tracker, completion)?;
        if let Some(waiter) = self.completion_waiter.take() {
            waiter.wake();
        }
        Ok(id)
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.supervisor.close();
        // Shutdown may close immediately after a reporting turn allocated its
        // maintenance timer. A drained owner has no reason to retain that slot.
        if self.snapshot().is_drained() {
            self.retry_timer = None;
        }
    }

    /// Fence worker queues and check all owned delivery responsibilities, not
    /// just worker resources. Expiry performs no recovery update or adapter call.
    /// Keep this owner after expiry; a later deadline can resume draining.
    /// Admission namespace closure remains the enclosing router's responsibility.
    pub fn drive_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        deadline: &mut DeadlineFuture,
    ) -> DeliveryShutdown {
        self.close();
        if self.snapshot().is_drained() {
            return DeliveryShutdown::Drained;
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return DeliveryShutdown::DeadlineReached;
        }
        DeliveryShutdown::Step(self.drive(cx))
    }

    /// Recovery transfer preserves the original worker payload and its charge.
    /// Successful transfer frees a supervision slot and wakes the coordinator,
    /// including after closure. Empty lookups preserve its pending notification.
    pub fn take_blocked_for_recovery(&mut self) -> Option<(PendingDeliveryError, WorkerOutcome)> {
        let outcome = self.supervisor.take_blocked_for_recovery();
        if outcome.is_some() {
            self.notify_progress();
        }
        outcome
    }

    pub fn take_for_recovery(&mut self, id: DeliveryObligationId) -> Option<RetainedDelivery> {
        let retained = self.supervisor.take_for_recovery(id);
        if retained.is_some() {
            self.notify_progress();
        }
        retained
    }

    /// Completion of pre-existing responsibility is allowed after closure.
    /// Restoration never calls the adapter or invents delivery progress.
    #[allow(clippy::result_large_err)]
    pub fn restore_from_recovery(
        &mut self,
        delivery: RetainedDelivery,
    ) -> Result<(), (PendingDeliveryError, RetainedDelivery)> {
        self.supervisor
            .restore_from_recovery(delivery, &self.tracker)?;
        self.notify_progress();
        Ok(())
    }
}
