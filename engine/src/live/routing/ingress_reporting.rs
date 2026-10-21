//! Bounded ownership of producer outcomes between ingress and runtime reporting.
//!
//! Capacity is reserved before ingress can commit. There is no await point
//! between commitment and retaining its complete result. Dequeuing transfers the
//! charge with the result; a slow reporting consumer cannot bypass the bounds.
use std::{
    collections::VecDeque,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

use super::{
    admission::AdmissionContext,
    ingress::{IngressOutcome, IngressRejection, RouterIngress},
};
use crate::live::{
    foundation::resources::{
        ResourceBudget, ResourceBudgetSnapshot, ResourceReservation, ResourceUsage,
    },
    model::{error::ErrorCode, memory::RetainedSize},
    ports::routing::{EventBatch, RoutingAdmission},
};

#[derive(Debug)]
#[must_use = "retain the original batch for retry or recovery"]
pub enum RecordedIngressError {
    Capacity {
        batch: EventBatch,
        reason: ErrorCode,
    },
    Admission(IngressRejection),
}

/// Complete outcome ownership, including any returned recovery reservations.
/// Event bytes remain charged until the reporting consumer drops this record.
/// Per-destination metadata is additionally bounded by the registry's finite
/// destination limit and the journal's outstanding-record limit.
#[derive(Debug)]
#[must_use = "inspect all delivery and recovery outcomes before releasing the record"]
pub struct IngressRecord {
    batch: EventBatch,
    outcome: IngressOutcome,
    _charge: ResourceReservation,
}

impl IngressRecord {
    pub fn batch(&self) -> &EventBatch {
        &self.batch
    }
    pub fn outcome(&self) -> &IngressOutcome {
        &self.outcome
    }

    /// Reconcile or transfer recovery reservations while the record stays charged.
    pub fn outcome_mut(&mut self) -> &mut IngressOutcome {
        &mut self.outcome
    }
}

#[derive(Default)]
struct State {
    records: VecDeque<IngressRecord>,
    waiter: Option<Waker>,
}

/// This journal owns producer results, not delivery obligations or completions.
/// Closure stops new reporting reservations; already reserved admissions may
/// finish and must still be drained. Runtime drives delivery independently.
pub struct IngressOutcomes {
    budget: ResourceBudget,
    state: Mutex<State>,
}
impl IngressOutcomes {
    /// `items` bounds outstanding records (including in-flight admissions and
    /// records held by a consumer); `bytes` bounds their retained event bytes.
    pub fn new(limit: ResourceUsage) -> Result<Self, ErrorCode> {
        Ok(Self {
            budget: ResourceBudget::new(limit)?,
            state: Mutex::new(State::default()),
        })
    }

    pub fn snapshot(&self) -> ResourceBudgetSnapshot {
        self.budget.snapshot()
    }

    pub fn close(&self) {
        self.budget.close();
    }

    /// Ok means the detailed outcome was recorded, even when it describes a
    /// rejected or partial admission. It is not a router admission receipt.
    /// Rejection leaves no journal record and preserves the producer's batch.
    /// Dropping a pending future releases its reporting charge and all ingress
    /// preparations. After commitment this future completes synchronously.
    pub async fn admit(
        &self,
        ingress: &RouterIngress<'_>,
        batch: EventBatch,
        context: AdmissionContext,
    ) -> Result<(), RecordedIngressError> {
        self.admit_receipt(ingress, batch, context)
            .await
            .map(|_| ())
    }

    /// Retain the complete outcome before returning its conservative producer
    /// receipt. Partial or uncertain required delivery never yields Accepted.
    pub async fn admit_receipt(
        &self,
        ingress: &RouterIngress<'_>,
        batch: EventBatch,
        context: AdmissionContext,
    ) -> Result<RoutingAdmission, RecordedIngressError> {
        let amount = batch
            .retained_size_bytes()
            .ok()
            .and_then(|bytes| usize::try_from(bytes).ok());
        let charge = amount
            .ok_or(ErrorCode::Capacity)
            .and_then(|bytes| self.budget.reserve(ResourceUsage { items: 1, bytes }))
            .map_err(|reason| RecordedIngressError::Capacity {
                batch: batch.clone(),
                reason,
            })?;
        let outcome = ingress
            .admit(batch.clone(), context)
            .await
            .map_err(RecordedIngressError::Admission)?;
        let receipt = super::port::receipt(&batch, &outcome);
        let waiter = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.records.push_back(IngressRecord {
                batch,
                outcome,
                _charge: charge,
            });
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Ok(receipt)
    }

    /// One replaceable reporting coordinator; no hidden waiter queue.
    pub fn poll_next(&self, cx: &mut Context<'_>) -> Poll<IngressRecord> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(record) = state.records.pop_front() {
            return Poll::Ready(record);
        }
        if state
            .waiter
            .as_ref()
            .is_none_or(|waiter| !waiter.will_wake(cx.waker()))
        {
            state.waiter = Some(cx.waker().clone());
        }
        Poll::Pending
    }

    /// Includes records already transferred to reporting consumers. A shutdown
    /// coordinator must continue polling `poll_next` while this is pending.
    pub fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.budget.poll_shutdown(cx)
    }

    /// Fence admission and drain records through one shutdown coordinator.
    /// `None` means all reservations have been released, including admissions
    /// still preparing and records held outside the journal. A pending result
    /// registers for both newly recorded outcomes and final charge release.
    /// This shares the single shutdown waiter with `poll_shutdown`; use one
    /// coordinator for both operations, rather than independent tasks.
    pub fn poll_drain(&self, cx: &mut Context<'_>) -> Poll<Option<IngressRecord>> {
        // Register before checking the queue: a cancelled in-flight admission
        // may release the last charge without ever producing a journal record.
        if self.budget.poll_shutdown(cx).is_ready() {
            return Poll::Ready(None);
        }
        self.poll_next(cx).map(Some)
    }
}

/// Runtime handler for a complete ingress outcome. Success means this handler
/// has accounted for every result and transferred any recovery responsibility;
/// it does not mean downstream delivery or durability. On error, mutations must
/// leave unresolved responsibility in `outcome`. Handlers must tolerate retries.
/// Success with tracking reservations or an unresolved mapping is treated as permanent
/// failure. Handlers remain responsible for preserving any ownership they move
/// out of the outcome; the consumer cannot validate an external recovery owner.
pub trait IngressReporter {
    fn report(
        &mut self,
        batch: &EventBatch,
        outcome: &mut IngressOutcome,
    ) -> Result<(), super::reporting::ReportingError>;
}

/// Restore tracking ownership before passing the complete outcome to the
/// runtime's reporting policy. The driver lock is nonwaiting and released before
/// invoking that policy. Recovery transfer is idempotent across reporting retries;
/// foreign or ambiguous reservations keep the record charged for intervention.
pub struct TrackingIngressReporter<'a, R> {
    pub delivery: &'a Mutex<super::driver::DeliveryDriver>,
    pub reporter: R,
}

impl<R: IngressReporter> IngressReporter for TrackingIngressReporter<'_, R> {
    fn report(
        &mut self,
        batch: &EventBatch,
        outcome: &mut IngressOutcome,
    ) -> Result<(), super::reporting::ReportingError> {
        use super::reporting::ReportingError;
        // Inspect even an empty count-mismatch result: it requires explicit
        // intervention despite retaining no reservation in the producer record.
        let handoff = {
            let mut driver = self.delivery.try_lock().map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => ReportingError::TemporarilyUnavailable,
                std::sync::TryLockError::Poisoned(_) => ReportingError::PermanentFailure,
            })?;
            driver.reconcile_ingress_tracking(outcome)
        };
        if handoff.mapping_blocked || handoff.retained != 0 {
            return Err(ReportingError::PermanentFailure);
        }
        self.reporter.report(batch, outcome)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressReportingStep {
    Idle,
    Reported,
    Retained(super::reporting::ReportingError),
}

/// A single reporting coordinator, retaining at most one charged journal record.
/// Failed records stay ahead of later records; neither temporary nor permanent
/// failure silently releases their recovery reservations. The runtime schedules
/// retries and escalation; this bounded turn does not self-wake or spin.
#[derive(Default)]
pub struct IngressReportingConsumer {
    pending: Option<IngressRecord>,
}

impl IngressReportingConsumer {
    pub fn pending(&self) -> Option<&IngressRecord> {
        self.pending.as_ref()
    }

    /// Transfer the charged pending record during runtime replacement/escalation.
    /// The caller must retain it until its unresolved responsibility is handled.
    pub fn take_pending(&mut self) -> Option<IngressRecord> {
        self.pending.take()
    }

    /// Process at most one record, synchronously retaining it on handler failure.
    /// No journal or delivery-driver lock is held while invoking user code.
    pub fn process_one(
        &mut self,
        journal: &IngressOutcomes,
        reporter: &mut impl IngressReporter,
        cx: &mut Context<'_>,
    ) -> IngressReportingStep {
        if self.pending.is_none() {
            match journal.poll_next(cx) {
                Poll::Pending => return IngressReportingStep::Idle,
                Poll::Ready(record) => self.pending = Some(record),
            }
        }
        let record = self.pending.as_mut().expect("record loaded above");
        match reporter.report(&record.batch, &mut record.outcome) {
            Ok(()) if record.outcome.requires_tracking_intervention() => {
                // A handler acknowledgement cannot release repair reservations
                // or an unresolved mapping in this record. Keep the
                // charged record for explicit intervention, without retry spin.
                IngressReportingStep::Retained(super::reporting::ReportingError::PermanentFailure)
            }
            Ok(()) => {
                self.pending.take();
                IngressReportingStep::Reported
            }
            Err(error) => IngressReportingStep::Retained(error),
        }
    }
}

/// Bounded retry policy for the single charged ingress-reporting record.
/// Exhaustion and permanent failure require an explicit runtime intervention.
pub struct ScheduledIngressReporting {
    consumer: IngressReportingConsumer,
    interval: std::time::Duration,
    max_attempts: std::num::NonZeroUsize,
    attempts: usize,
    retry_at: Option<std::time::Instant>,
    timer: Option<super::admission::DeadlineFuture>,
    blocked: bool,
}

/// Read-only scheduler observation. Journal usage includes queued, preparing,
/// pending and externally transferred records; it is not a durability measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressReportingSnapshot {
    pub journal: ResourceBudgetSnapshot,
    pub pending: bool,
    pub attempts: usize,
    pub max_attempts: std::num::NonZeroUsize,
    pub retry_interval: std::time::Duration,
    pub retry_at: Option<std::time::Instant>,
    pub timer_retained: bool,
    pub escalation_required: bool,
}

impl IngressReportingSnapshot {
    /// A closed empty journal proves all charged reports have been released.
    /// Destination delivery and recovery retain their independent obligations.
    pub fn is_drained(&self) -> bool {
        self.journal.closed && self.journal.usage == ResourceUsage::default() && !self.pending
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledIngressStep {
    Idle,
    Reported,
    RetryScheduled,
    EscalationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressReportingShutdownStep {
    Step(ScheduledIngressStep),
    Drained,
    DeadlineReached,
}

impl ScheduledIngressReporting {
    /// Observes existing owners without polling timers, handlers or shutdown.
    /// The scheduler is exclusively borrowed; journal accounting is sampled under
    /// its own lock and does not form a transaction with other router owners.
    pub fn snapshot(&self, journal: &IngressOutcomes) -> IngressReportingSnapshot {
        IngressReportingSnapshot {
            journal: journal.snapshot(),
            pending: self.pending().is_some(),
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            retry_interval: self.interval,
            retry_at: self.retry_at,
            timer_retained: self.timer.is_some(),
            escalation_required: self.blocked,
        }
    }

    /// Fence new admissions and perform at most one reporting turn. Registers
    /// for release of reservations held by preparing admissions or escalated
    /// records as well as new outcomes. One coordinator must own shutdown polling.
    /// Expiry retains the pending record, retry timer and escalation state; resume
    /// with this same owner and a new deadline. Drained concerns reporting only,
    /// never destination completion, recovery completion or durability.
    pub fn poll_shutdown(
        &mut self,
        journal: &IngressOutcomes,
        reporter: &mut impl IngressReporter,
        cx: &mut Context<'_>,
        clock: &dyn crate::live::foundation::clock::MonotonicClock,
        timer: &dyn super::admission::DeadlineTimer,
        deadline: &mut super::admission::DeadlineFuture,
    ) -> Result<Poll<IngressReportingShutdownStep>, ErrorCode> {
        if journal.poll_shutdown(cx).is_ready() && self.pending().is_none() {
            return Ok(Poll::Ready(IngressReportingShutdownStep::Drained));
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return Ok(Poll::Ready(IngressReportingShutdownStep::DeadlineReached));
        }
        match self.poll_step(journal, reporter, cx, clock, timer)? {
            Poll::Ready(ScheduledIngressStep::Idle) => Ok(Poll::Pending),
            step => Ok(step.map(IngressReportingShutdownStep::Step)),
        }
    }

    pub fn new(
        interval: std::time::Duration,
        max_attempts: std::num::NonZeroUsize,
    ) -> Result<Self, ErrorCode> {
        if interval.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            consumer: IngressReportingConsumer::default(),
            interval,
            max_attempts,
            attempts: 0,
            retry_at: None,
            timer: None,
            blocked: false,
        })
    }

    pub fn pending(&self) -> Option<&IngressRecord> {
        self.consumer.pending()
    }

    /// Explicit runtime intervention after escalation or handler unwind. Retain
    /// the original charged record and begin a fresh bounded attempt cycle.
    /// This does not acknowledge the outcome, repair tracking or reopen ingress.
    /// Deferred retries and idle coordinators cannot be accelerated through it.
    /// A successful intervention schedules one turn on the supplied coordinator.
    pub fn retry_escalated(&mut self, cx: &Context<'_>) -> bool {
        if !self.blocked || self.consumer.pending().is_none() {
            return false;
        }
        self.reset();
        cx.waker().wake_by_ref();
        true
    }

    /// Transfer unresolved ownership for explicit escalation; retire its timer.
    pub fn take_pending(&mut self) -> Option<IngressRecord> {
        self.reset();
        self.consumer.take_pending()
    }

    fn reset(&mut self) {
        self.attempts = 0;
        self.retry_at = None;
        self.timer = None;
        self.blocked = false;
    }

    /// One handler call at most. Pending retries reuse one bounded timer.
    /// Timer faults retain the record and deadline; callers must arrange another
    /// poll after a scheduling error. No self-wakes or implicit escalation drops.
    /// Closure does not bypass retry policy or release unresolved records.
    pub fn poll_step(
        &mut self,
        journal: &IngressOutcomes,
        reporter: &mut impl IngressReporter,
        cx: &mut Context<'_>,
        clock: &dyn crate::live::foundation::clock::MonotonicClock,
        timer: &dyn super::admission::DeadlineTimer,
    ) -> Result<Poll<ScheduledIngressStep>, ErrorCode> {
        if self.blocked {
            return Ok(Poll::Ready(ScheduledIngressStep::EscalationRequired));
        }
        if let Some(deadline) = self.retry_at {
            if clock.now() < deadline {
                if self.timer.is_none() {
                    self.timer = Some(timer.try_sleep_until(deadline)?);
                }
                let ready = self
                    .timer
                    .as_mut()
                    .expect("installed above")
                    .as_mut()
                    .poll(cx)
                    .is_ready();
                // The authoritative clock wins even if an adapter stays pending.
                if clock.now() < deadline {
                    if ready {
                        self.timer = None;
                        return Err(ErrorCode::InvalidInput);
                    }
                    return Ok(Poll::Pending);
                }
            }
            self.timer = None;
            self.retry_at = None;
        }
        // Mark intervention required before invoking arbitrary handler code.
        // Unwind therefore preserves ownership without an automatic retry loop.
        self.blocked = true;
        match self.consumer.process_one(journal, reporter, cx) {
            IngressReportingStep::Idle => {
                self.reset();
                Ok(Poll::Ready(ScheduledIngressStep::Idle))
            }
            IngressReportingStep::Reported => {
                self.reset();
                // Outcomes may have queued before this coordinator registered
                // a waiter. Charge release only notifies shutdown at zero usage;
                // it cannot schedule the rest of an existing backlog. Request
                // one more bounded turn, which parks if no outcome remains.
                cx.waker().wake_by_ref();
                Ok(Poll::Ready(ScheduledIngressStep::Reported))
            }
            IngressReportingStep::Retained(error) => {
                self.attempts += 1;
                if error == super::reporting::ReportingError::PermanentFailure
                    || self.attempts >= self.max_attempts.get()
                {
                    return Ok(Poll::Ready(ScheduledIngressStep::EscalationRequired));
                }
                let Some(deadline) = clock.now().checked_add(self.interval) else {
                    return Err(ErrorCode::Limit);
                };
                self.retry_at = Some(deadline);
                self.blocked = false;
                // Arm now so a caller can park immediately after this turn.
                self.timer = Some(timer.try_sleep_until(deadline)?);
                let ready = self
                    .timer
                    .as_mut()
                    .expect("installed above")
                    .as_mut()
                    .poll(cx)
                    .is_ready();
                if clock.now() >= deadline {
                    self.timer = None;
                    cx.waker().wake_by_ref();
                } else if ready {
                    self.timer = None;
                    return Err(ErrorCode::InvalidInput);
                }
                Ok(Poll::Ready(ScheduledIngressStep::RetryScheduled))
            }
        }
    }
}
