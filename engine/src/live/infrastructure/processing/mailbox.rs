use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxStatus {
    pub queued_events: usize,
    pub queued_bytes: usize,
    pub closed: bool,
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

struct MailboxState<E> {
    events: VecDeque<Entry<E>>,
    queued_bytes: usize,
    closed: bool,
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
    state: Mutex<MailboxState<E>>,
}

impl<E> ProcessorMailbox<E> {
    pub fn new(limits: MailboxLimits) -> Result<Self, MailboxError> {
        limits.validate()?;

        Ok(Self {
            limits,
            state: Mutex::new(MailboxState {
                events: VecDeque::new(),
                queued_bytes: 0,
                closed: false,
            }),
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

        if state.closed {
            return Err(MailboxPushError {
                reason: MailboxError::Closed,
                event,
            });
        }

        if bytes > self.limits.max_bytes {
            return Err(MailboxPushError {
                reason: MailboxError::Oversized,
                event,
            });
        }

        let Some(next_bytes) = state.queued_bytes.checked_add(bytes) else {
            return Err(MailboxPushError {
                reason: MailboxError::SizeOverflow,
                event,
            });
        };

        if state.events.len() >= self.limits.max_events || next_bytes > self.limits.max_bytes {
            return Err(MailboxPushError {
                reason: MailboxError::Capacity,
                event,
            });
        }

        state.events.push_back(Entry { event, bytes });
        state.queued_bytes = next_bytes;

        Ok(())
    }

    /// Remove up to `max_events` events in FIFO order.
    ///
    /// Events are moved out without cloning.
    ///
    /// Calling with zero returns an empty batch.
    pub fn take_batch(&self, max_events: usize) -> Vec<E> {
        let mut state = lock(&self.state);

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
        let mut state = lock(&self.state);
        state.closed = true;
    }

    pub fn status(&self) -> MailboxStatus {
        let state = lock(&self.state);

        MailboxStatus {
            queued_events: state.events.len(),
            queued_bytes: state.queued_bytes,
            closed: state.closed,
        }
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
                closed: false,
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
