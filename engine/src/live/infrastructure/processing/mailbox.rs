use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
};

use thiserror::Error;

/// Maximum admitted events and accounted payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}

impl MailboxLimits {
    pub fn validate(self) -> Result<(), MailboxError> {
        if self.max_events == 0 || self.max_bytes == 0 {
            return Err(MailboxError::InvalidLimits);
        }

        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MailboxStatus {
    pub queued_events: usize,
    pub queued_bytes: usize,
    pub closed: bool,
    pub in_flight_events: usize,
    pub in_flight_bytes: usize,
    pub retained_terminal_events: usize,
    pub retained_terminal_bytes: usize,
    pub completed_events: u64,
    pub indeterminate_events: u64,
    pub indeterminate_bytes: u64,
    pub rejected_events: u64,
    pub abandoned_events: u64,
    pub abandoned_bytes: u64,
}

/// Failure during coordinated mailbox admission.
///
/// The original event is returned on every failure.
#[derive(Debug)]
pub enum AdmissionError<E, N> {
    Mailbox(MailboxPushError<E>),

    Notification { reason: N, event: E },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MailboxError {
    #[error("invalid mailbox capacity limits")]
    InvalidLimits,

    #[error("mailbox is closed")]
    Closed,

    #[error("mailbox capacity exhausted")]
    Capacity,

    #[error("event exceeds the mailbox byte limit")]
    Oversized,

    #[error("mailbox byte accounting overflow")]
    SizeOverflow,

    #[error("standalone admission is disabled on managed mailboxes")]
    Managed,
}

/// Admission failure retains ownership of the rejected event.
///
/// This prevents a failed publication from silently consuming
/// an event that must be retried or handled by an upstream policy.
#[derive(Debug)]
pub struct MailboxPushError<E> {
    pub reason: MailboxError,
    pub event: E,
}

struct Entry<E> {
    event: E,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferStatus {
    pub queued_events: usize,
    pub queued_bytes: usize,
    pub in_flight_events: usize,
    pub in_flight_bytes: usize,
    pub retained_terminal_events: usize,
    pub retained_terminal_bytes: usize,
    pub completed_events: u64,
    pub completed_bytes: u64,
    pub abandoned_events: u64,
    pub abandoned_bytes: u64,
    pub indeterminate_events: u64,
    pub indeterminate_bytes: u64,
    pub rejected_events: u64,
}
/// Aggregate reservation for managed input/output mailboxes.
///
/// Under the budget state lock, queued + in-flight + terminal counts and bytes
/// never exceed the limits (each at most usize::MAX). Only reserve adds a charge:
/// it checks the existing total against max_events before incrementing, and
/// checked-adds bytes before admission. All other transitions transfer an
/// existing charge or subtract it. Therefore the unchecked component sums and
/// transfers cannot overflow, even with usize::MAX limits. Mailbox state always
/// precedes budget state; budget operations never acquire a mailbox lock.
/// These charges do not bound arbitrary processor heap allocations or total RSS.
#[derive(Debug)]
pub struct BufferBudget {
    limits: MailboxLimits,
    state: Mutex<BufferStatus>,
}
impl BufferBudget {
    pub fn new(limits: MailboxLimits) -> Result<Self, MailboxError> {
        limits.validate()?;
        Ok(Self {
            limits,
            state: Mutex::new(BufferStatus::default()),
        })
    }
    fn reserve(&self, bytes: usize) -> bool {
        let mut used = lock(&self.state);
        if used.queued_events + used.in_flight_events + used.retained_terminal_events
            >= self.limits.max_events
            || used
                .queued_bytes
                .checked_add(used.in_flight_bytes)
                .and_then(|n| n.checked_add(used.retained_terminal_bytes))
                .and_then(|n| n.checked_add(bytes))
                .is_none_or(|n| n > self.limits.max_bytes)
        {
            return false;
        }
        used.queued_events += 1;
        used.queued_bytes += bytes;
        true
    }
    fn dequeue(&self, count: usize, bytes: usize) {
        let mut used = lock(&self.state);
        used.queued_events -= count;
        used.queued_bytes -= bytes;
        used.in_flight_events += count;
        used.in_flight_bytes += bytes;
    }
    fn requeue(&self, count: usize, bytes: usize) {
        let mut used = lock(&self.state);
        used.in_flight_events -= count;
        used.in_flight_bytes -= bytes;
        used.queued_events += count;
        used.queued_bytes += bytes;
    }
    fn finish(&self, count: usize, bytes: usize, disposition: u8, retained: bool) {
        let mut used = lock(&self.state);
        if retained {
            used.retained_terminal_events -= count;
            used.retained_terminal_bytes -= bytes;
        } else {
            used.in_flight_events -= count;
            used.in_flight_bytes -= bytes;
        }
        match disposition {
            1 => {
                used.completed_events += count as u64;
                used.completed_bytes += bytes as u64;
            }
            2 => {
                used.abandoned_events += count as u64;
                used.abandoned_bytes += bytes as u64;
            }
            _ => {
                used.indeterminate_events += count as u64;
                used.indeterminate_bytes += bytes as u64;
            }
        }
    }
    fn retain_terminal(&self, count: usize, bytes: usize) {
        let mut used = lock(&self.state);
        used.in_flight_events -= count;
        used.in_flight_bytes -= bytes;
        used.retained_terminal_events += count;
        used.retained_terminal_bytes += bytes;
    }
    fn discard_queued(&self, count: usize, bytes: usize) {
        let mut used = lock(&self.state);
        used.queued_events -= count;
        used.queued_bytes -= bytes;
        used.abandoned_events += count as u64;
        used.abandoned_bytes += bytes as u64;
    }
    fn reject(&self) {
        lock(&self.state).rejected_events += 1;
    }
    pub fn status(&self) -> BufferStatus {
        *lock(&self.state)
    }
    pub fn used(&self) -> (usize, usize) {
        let used = self.status();
        (
            used.queued_events + used.in_flight_events + used.retained_terminal_events,
            used.queued_bytes + used.in_flight_bytes + used.retained_terminal_bytes,
        )
    }
}
struct MailboxState<E> {
    events: VecDeque<Entry<E>>,
    queued_bytes: usize,
    closed: bool,
    managed: bool,
    in_flight_events: usize,
    in_flight_bytes: usize,
    retained_terminal_events: usize,
    retained_terminal_bytes: usize,
    completed_events: u64,
    indeterminate_events: u64,
    indeterminate_bytes: u64,
    rejected_events: u64,
    abandoned_events: u64,
    abandoned_bytes: u64,
    budget: Option<Arc<BufferBudget>>,
}
impl<E> Drop for MailboxState<E> {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            budget.discard_queued(self.events.len(), self.queued_bytes);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Bounded, thread-safe FIFO event mailbox.
///
/// Supports multiple concurrent producers and consumers.
///
/// Correctness does not require a single consumer, but the
/// processing runtime should use one exclusive processor owner
/// to preserve stateful processing semantics.
pub struct ProcessorMailbox<E> {
    limits: MailboxLimits,
    state: Arc<Mutex<MailboxState<E>>>,
    fence: Arc<Mutex<bool>>,
    closed_notify: tokio::sync::Notify,
}

impl<E> ProcessorMailbox<E> {
    pub fn new(limits: MailboxLimits) -> Result<Self, MailboxError> {
        Self::with_fence(limits, Arc::new(Mutex::new(false)))
    }

    pub(crate) fn with_fence(
        limits: MailboxLimits,
        fence: Arc<Mutex<bool>>,
    ) -> Result<Self, MailboxError> {
        limits.validate()?;

        Ok(Self {
            limits,
            fence,
            closed_notify: tokio::sync::Notify::new(),
            state: Arc::new(Mutex::new(MailboxState {
                events: VecDeque::new(),
                queued_bytes: 0,
                closed: false,
                managed: false,
                in_flight_events: 0,
                in_flight_bytes: 0,
                retained_terminal_events: 0,
                retained_terminal_bytes: 0,
                completed_events: 0,
                indeterminate_events: 0,
                indeterminate_bytes: 0,
                rejected_events: 0,
                abandoned_events: 0,
                abandoned_bytes: 0,
                budget: None,
            })),
        })
    }

    /// Atomically admit one complete event.
    ///
    /// `bytes` is the caller-supplied accounted event size.
    ///
    /// The caller must provide a consistent size calculation
    /// for the entire event, including all depth updates.
    pub fn push(&self, event: E, bytes: usize) -> Result<(), MailboxPushError<E>> {
        let mut state = lock(&self.state);
        if state.managed {
            return Err(MailboxPushError {
                reason: MailboxError::Managed,
                event,
            });
        }
        Self::push_locked(&mut state, self.limits, event, bytes)
    }
    pub(crate) fn record_rejection(&self) {
        let mut state = lock(&self.state);
        state.rejected_events += 1;
        if let Some(budget) = &state.budget {
            budget.reject();
        }
    }
    pub(crate) fn push_managed(&self, event: E, bytes: usize) -> Result<(), MailboxPushError<E>> {
        let fence = lock(&self.fence);
        self.push_fenced(&fence, event, bytes)
    }
    /// Caller holds this mailbox's generation fence; never reacquire it.
    pub(crate) fn push_fenced(
        &self,
        fence: &MutexGuard<'_, bool>,
        event: E,
        bytes: usize,
    ) -> Result<(), MailboxPushError<E>> {
        let mut state = lock(&self.state);
        if **fence {
            state.closed = true;
        }
        Self::push_locked(&mut state, self.limits, event, bytes)
    }
    fn push_locked(
        state: &mut MailboxState<E>,
        limits: MailboxLimits,
        event: E,
        bytes: usize,
    ) -> Result<(), MailboxPushError<E>> {
        let reason = if state.closed {
            Some(MailboxError::Closed)
        } else if bytes > limits.max_bytes {
            Some(MailboxError::Oversized)
        } else if state.events.len() + state.in_flight_events + state.retained_terminal_events
            >= limits.max_events
            || state
                .queued_bytes
                .checked_add(state.in_flight_bytes)
                .and_then(|n| n.checked_add(state.retained_terminal_bytes))
                .and_then(|n| n.checked_add(bytes))
                .is_none_or(|n| n > limits.max_bytes)
        {
            Some(MailboxError::Capacity)
        } else if state
            .budget
            .as_ref()
            .is_some_and(|budget| !budget.reserve(bytes))
        {
            Some(MailboxError::Capacity)
        } else {
            None
        };
        if let Some(reason) = reason {
            state.rejected_events += 1;
            if let Some(budget) = &state.budget {
                budget.reject();
            }
            return Err(MailboxPushError { reason, event });
        }
        state.events.push_back(Entry { event, bytes });
        state.queued_bytes += bytes;
        Ok(())
    }
    pub(crate) fn manage(&self, budget: Arc<BufferBudget>) {
        let mut state = lock(&self.state);
        assert!(state.events.is_empty());
        state.managed = true;
        state.budget = Some(budget);
    }
    /// Remove up to `max_events` events in FIFO order.
    ///
    /// Events are moved out without cloning.
    ///
    /// Calling with zero returns an empty batch.
    pub fn take_batch(&self, max_events: usize) -> Vec<E> {
        let mut state = lock(&self.state);

        assert!(!state.managed, "managed processors must use take_work");
        let count = max_events.min(state.events.len());
        let mut batch = Vec::with_capacity(count);

        for _ in 0..count {
            let entry = state
                .events
                .pop_front()
                .expect("batch size was bounded by queue length");

            state.queued_bytes -= entry.bytes;
            batch.push(entry.event);
        }

        batch
    }

    /// Prevent future admissions.
    ///
    /// Already accepted events remain available for draining.
    pub fn close(&self) {
        let mut fence = lock(&self.fence);
        self.close_fenced(&mut fence);
    }

    /// Caller holds this mailbox's generation fence. Accepted work is retained.
    pub(crate) fn close_fenced(&self, fence: &mut MutexGuard<'_, bool>) {
        **fence = true;
        lock(&self.state).closed = true;
        self.closed_notify.notify_waiters();
    }

    pub async fn closed(&self) {
        let notified = self.closed_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.status().closed {
            return;
        }
        notified.await;
    }
    pub(crate) fn publication_fence(&self) -> Arc<Mutex<bool>> {
        self.fence.clone()
    }

    pub fn status(&self) -> MailboxStatus {
        let fence = lock(&self.fence);
        let state = lock(&self.state);

        MailboxStatus {
            queued_events: state.events.len(),
            queued_bytes: state.queued_bytes,
            closed: state.closed || *fence,
            in_flight_events: state.in_flight_events,
            in_flight_bytes: state.in_flight_bytes,
            retained_terminal_events: state.retained_terminal_events,
            retained_terminal_bytes: state.retained_terminal_bytes,
            completed_events: state.completed_events,
            indeterminate_events: state.indeterminate_events,
            indeterminate_bytes: state.indeterminate_bytes,
            rejected_events: state.rejected_events,
            abandoned_events: state.abandoned_events,
            abandoned_bytes: state.abandoned_bytes,
        }
    }

    pub fn take_work(&self, max_events: usize) -> WorkBatch<E> {
        let mut state = lock(&self.state);
        let count = max_events.min(state.events.len());
        let mut events = Vec::with_capacity(count);
        let mut bytes = 0;
        for _ in 0..count {
            let entry = state.events.pop_front().unwrap();
            bytes += entry.bytes;
            events.push(entry.event);
        }
        state.queued_bytes -= bytes;
        state.in_flight_events += count;
        state.in_flight_bytes += bytes;
        if let Some(budget) = &state.budget {
            budget.dequeue(count, bytes);
        }
        WorkBatch {
            events,
            bytes,
            state: self.state.clone(),
            charge: BatchCharge {
                state: self.state.clone(),
                count,
                bytes,
                settled: false,
                disposition: 0,
                retained: false,
            },
        }
    }
    pub(crate) fn return_work(&self, mut batch: WorkBatch<E>) {
        assert!(Arc::ptr_eq(&self.state, &batch.state));
        let mut state = lock(&self.state);
        let events = std::mem::take(&mut batch.events);
        // Only used for a single indivisible publication; preserve its charge.
        assert_eq!(events.len(), 1);
        state.in_flight_events -= 1;
        state.in_flight_bytes -= batch.bytes;
        state.queued_bytes += batch.bytes;
        state.events.push_front(Entry {
            event: events.into_iter().next().unwrap(),
            bytes: batch.bytes,
        });
        if let Some(budget) = &state.budget {
            budget.requeue(1, batch.bytes);
        }
        batch.charge.settled = true;
    }
    pub(crate) fn collect_remaining(&self) -> Vec<E> {
        let mut batch = self.take_work(usize::MAX);
        batch.charge.settle(2);
        std::mem::take(&mut batch.events)
    }
    pub fn is_empty(&self) -> bool {
        lock(&self.state).events.is_empty()
    }

    /// Atomically admit an event and notify its processor.
    ///
    /// The notification callback must be synchronous, nonblocking,
    /// and must not attempt to acquire this mailbox's lock.
    ///
    /// Its contract is:
    /// - Ok means the processor is scheduled or already running.
    /// - Err means the notification was rejected without scheduling.
    ///
    /// The mailbox lock prevents a worker from consuming the event
    /// between notification and admission.
    pub fn push_and_notify<N, F>(
        &self,
        event: E,
        bytes: usize,
        notify: F,
    ) -> Result<(), AdmissionError<E, N>>
    where
        F: FnOnce() -> Result<(), N>,
    {
        let mut state = lock(&self.state);
        if state.managed {
            return Err(AdmissionError::Mailbox(MailboxPushError {
                reason: MailboxError::Managed,
                event,
            }));
        }
        if state.closed {
            return Err(AdmissionError::Mailbox(MailboxPushError {
                reason: MailboxError::Closed,
                event,
            }));
        }

        if bytes > self.limits.max_bytes {
            return Err(AdmissionError::Mailbox(MailboxPushError {
                reason: MailboxError::Oversized,
                event,
            }));
        }

        let Some(next_bytes) = state.queued_bytes.checked_add(bytes) else {
            return Err(AdmissionError::Mailbox(MailboxPushError {
                reason: MailboxError::SizeOverflow,
                event,
            }));
        };

        if state.events.len() >= self.limits.max_events || next_bytes > self.limits.max_bytes {
            return Err(AdmissionError::Mailbox(MailboxPushError {
                reason: MailboxError::Capacity,
                event,
            }));
        }

        // Reserve execution responsibility before admitting the event.
        //
        // ProcessingPool::notify() guarantees that a successful
        // notification is represented by an existing or newly
        // scheduled processor activation.
        if let Err(reason) = notify() {
            return Err(AdmissionError::Notification { reason, event });
        }

        // No fallible admission checks remain.
        //
        // The mailbox lock is still held, so a newly awakened
        // worker cannot consume the event before this commit.
        state.events.push_back(Entry { event, bytes });
        state.queued_bytes = next_bytes;

        Ok(())
    }
}

/// Retains its count/byte reservation while owned. Dropping without complete is
/// indeterminate, including unwinding after partial state mutation. Never replay it.
pub struct WorkBatch<E> {
    // Field order ensures payload destruction precedes releasing the reservation,
    // including when a payload destructor unwinds.
    events: Vec<E>,
    bytes: usize,
    state: Arc<Mutex<MailboxState<E>>>,
    charge: BatchCharge<E>,
}
struct BatchCharge<E> {
    state: Arc<Mutex<MailboxState<E>>>,
    count: usize,
    bytes: usize,
    settled: bool,
    disposition: u8,
    retained: bool,
}
impl<E> std::ops::Deref for WorkBatch<E> {
    type Target = [E];
    fn deref(&self) -> &[E] {
        &self.events
    }
}
impl<E> BatchCharge<E> {
    fn settle(&mut self, disposition: u8) {
        if self.settled {
            return;
        }
        let mut state = lock(&self.state);
        if self.retained {
            state.retained_terminal_events -= self.count;
            state.retained_terminal_bytes -= self.bytes;
        } else {
            state.in_flight_events -= self.count;
            state.in_flight_bytes -= self.bytes;
        }
        match disposition {
            1 => state.completed_events += self.count as u64,
            2 => {
                state.abandoned_events += self.count as u64;
                state.abandoned_bytes += self.bytes as u64;
            }
            _ => {
                state.indeterminate_events += self.count as u64;
                state.indeterminate_bytes += self.bytes as u64;
            }
        }
        if let Some(budget) = &state.budget {
            budget.finish(self.count, self.bytes, disposition, self.retained);
        }
        self.settled = true;
    }
}
impl<E> Drop for BatchCharge<E> {
    fn drop(&mut self) {
        self.settle(self.disposition);
    }
}
impl<E> WorkBatch<E> {
    pub(crate) fn accounted_bytes(&self) -> usize {
        self.bytes
    }
    /// Release canceled work after its payload destructors finish.
    pub fn abandon(mut self) {
        self.charge.disposition = 2;
    }
    /// Acknowledge successful CPU processing. Router custody is independent.
    pub fn complete(mut self) {
        self.charge.disposition = 1;
    }
    pub(crate) fn canceled(mut self) -> Self {
        {
            let mut state = lock(&self.state);
            state.in_flight_events -= self.charge.count;
            state.in_flight_bytes -= self.bytes;
            state.retained_terminal_events += self.charge.count;
            state.retained_terminal_bytes += self.bytes;
            if let Some(budget) = &state.budget {
                budget.retain_terminal(self.charge.count, self.bytes);
            }
        }
        self.charge.retained = true;
        self.charge.disposition = 2;
        self
    }
}
impl<E: Send> super::pool::InputControl for ProcessorMailbox<E> {
    fn close(&self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    fn mailbox() -> ProcessorMailbox<u64> {
        ProcessorMailbox::new(MailboxLimits {
            max_events: 3,
            max_bytes: 100,
        })
        .unwrap()
    }

    #[test]
    fn maximum_budget_bytes_survive_transfers_and_terminal_disposal() {
        let limits = MailboxLimits {
            max_events: usize::MAX,
            max_bytes: usize::MAX,
        };
        let budget = Arc::new(BufferBudget::new(limits).unwrap());
        let mailbox = ProcessorMailbox::new(limits).unwrap();
        mailbox.manage(budget.clone());
        mailbox.push_managed(1_u64, usize::MAX).unwrap();
        assert_eq!(budget.used(), (1, usize::MAX));
        assert_eq!(
            mailbox.push_managed(2, 1).unwrap_err().reason,
            MailboxError::Capacity
        );
        let batch = mailbox.take_work(1);
        assert_eq!(budget.used(), (1, usize::MAX));
        mailbox.return_work(batch);
        let retained = mailbox.take_work(1).canceled();
        assert_eq!(budget.used(), (1, usize::MAX));
        assert_eq!(budget.status().retained_terminal_bytes, usize::MAX);
        retained.abandon();
        assert_eq!(budget.used(), (0, 0));
    }

    #[test]
    fn preserves_fifo_order() {
        let mailbox = mailbox();

        mailbox.push(10, 10).unwrap();
        mailbox.push(20, 10).unwrap();
        mailbox.push(30, 10).unwrap();

        assert_eq!(mailbox.take_batch(2), vec![10, 20]);
        assert_eq!(mailbox.take_batch(2), vec![30]);

        assert!(mailbox.is_empty());
    }

    #[test]
    fn rejects_event_count_overflow_without_consuming_event() {
        let mailbox = mailbox();

        mailbox.push(1, 10).unwrap();
        mailbox.push(2, 10).unwrap();
        mailbox.push(3, 10).unwrap();

        let rejected = mailbox.push(4, 10).unwrap_err();

        assert_eq!(rejected.reason, MailboxError::Capacity);
        assert_eq!(rejected.event, 4);

        assert_eq!(mailbox.take_batch(10), vec![1, 2, 3]);
    }

    #[test]
    fn rejects_byte_overflow_atomically() {
        let mailbox = mailbox();

        mailbox.push(1, 60).unwrap();

        let rejected = mailbox.push(2, 50).unwrap_err();

        assert_eq!(rejected.reason, MailboxError::Capacity);
        assert_eq!(rejected.event, 2);

        assert_eq!(
            mailbox.status(),
            MailboxStatus {
                queued_events: 1,
                queued_bytes: 60,
                rejected_events: 1,
                closed: false,
                ..Default::default()
            }
        );

        assert_eq!(mailbox.take_batch(10), vec![1]);
    }

    #[test]
    fn rejects_oversized_event() {
        let mailbox = mailbox();

        let rejected = mailbox.push(42, 101).unwrap_err();

        assert_eq!(rejected.reason, MailboxError::Oversized);
        assert_eq!(rejected.event, 42);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn batch_processing_releases_capacity() {
        let mailbox = mailbox();

        mailbox.push(1, 40).unwrap();
        mailbox.push(2, 40).unwrap();

        assert_eq!(mailbox.take_batch(1), vec![1]);

        mailbox.push(3, 50).unwrap();

        assert_eq!(mailbox.take_batch(10), vec![2, 3]);
        assert_eq!(mailbox.status().queued_bytes, 0);
    }

    #[test]
    fn closure_preserves_accepted_events() {
        let mailbox = mailbox();

        mailbox.push(1, 10).unwrap();
        mailbox.push(2, 10).unwrap();

        mailbox.close();

        let rejected = mailbox.push(3, 10).unwrap_err();

        assert_eq!(rejected.reason, MailboxError::Closed);
        assert_eq!(rejected.event, 3);

        assert_eq!(mailbox.take_batch(10), vec![1, 2]);
    }

    #[test]
    fn rejects_zero_limits() {
        assert!(matches!(
            ProcessorMailbox::<u64>::new(MailboxLimits {
                max_events: 0,
                max_bytes: 100,
            }),
            Err(MailboxError::InvalidLimits)
        ));

        assert!(matches!(
            ProcessorMailbox::<u64>::new(MailboxLimits {
                max_events: 10,
                max_bytes: 0,
            }),
            Err(MailboxError::InvalidLimits)
        ));
    }

    #[test]
    fn concurrent_admission_respects_capacity() {
        const PRODUCERS: usize = 8;

        let mailbox = Arc::new(
            ProcessorMailbox::new(MailboxLimits {
                max_events: 4,
                max_bytes: 40,
            })
            .unwrap(),
        );

        let barrier = Arc::new(Barrier::new(PRODUCERS));

        let mut handles = Vec::new();

        for id in 0..PRODUCERS {
            let mailbox = Arc::clone(&mailbox);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier.wait();
                mailbox.push(id, 10).is_ok()
            }));
        }

        let admitted = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|&success| success)
            .count();

        assert_eq!(admitted, 4);
        assert_eq!(mailbox.status().queued_events, 4);
        assert_eq!(mailbox.status().queued_bytes, 40);
    }

    #[test]
    fn zero_batch_does_not_consume_events() {
        let mailbox = mailbox();

        mailbox.push(1, 10).unwrap();

        assert!(mailbox.take_batch(0).is_empty());
        assert_eq!(mailbox.status().queued_events, 1);
    }
    #[test]
    fn notification_rejection_preserves_event_and_mailbox() {
        let mailbox = mailbox();

        mailbox.push(10, 20).unwrap();

        let before = mailbox.status();

        let result =
            mailbox.push_and_notify(42, 30, || Err::<(), _>("scheduler capacity exhausted"));

        match result {
            Err(AdmissionError::Notification { reason, event }) => {
                assert_eq!(reason, "scheduler capacity exhausted");
                assert_eq!(event, 42);
            }

            _ => panic!("expected notification rejection"),
        }

        assert_eq!(mailbox.status(), before);
        assert_eq!(mailbox.take_batch(10), vec![10]);
    }
    #[test]
    fn successful_notification_commits_event() {
        let mailbox = mailbox();

        let mut notifications = 0;

        mailbox
            .push_and_notify(42, 20, || {
                notifications += 1;
                Ok::<(), ()>(())
            })
            .unwrap();

        assert_eq!(notifications, 1);
        assert_eq!(mailbox.status().queued_events, 1);
        assert_eq!(mailbox.status().queued_bytes, 20);

        assert_eq!(mailbox.take_batch(10), vec![42]);
    }
    #[test]
    fn mailbox_capacity_rejection_does_not_notify() {
        let mailbox = mailbox();

        mailbox.push(1, 80).unwrap();

        let mut notifications = 0;

        let result = mailbox.push_and_notify(2, 30, || {
            notifications += 1;
            Ok::<(), ()>(())
        });

        match result {
            Err(AdmissionError::Mailbox(error)) => {
                assert_eq!(error.reason, MailboxError::Capacity);
                assert_eq!(error.event, 2);
            }

            _ => panic!("expected mailbox capacity rejection"),
        }

        assert_eq!(notifications, 0);
        assert_eq!(mailbox.take_batch(10), vec![1]);
    }
}
