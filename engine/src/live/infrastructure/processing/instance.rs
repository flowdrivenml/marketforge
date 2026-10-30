use std::sync::Arc;

use thiserror::Error;

use super::{
    mailbox::{AdmissionError, MailboxError, MailboxLimits, ProcessorMailbox},
    pool::{PoolError, ProcessingPool, Processor, ProcessorFailure},
    scheduler::{ProcessorToken, SchedulerError},
};

/// Errors encountered while creating or stopping an instance.
#[derive(Debug, Error)]
pub enum InstanceError {
    #[error(transparent)]
    Mailbox(#[from] MailboxError),

    #[error(transparent)]
    Pool(#[from] PoolError),
}

/// Result of stopping a processor generation.
///
/// Unprocessed events are returned to the lifecycle owner.
/// They must not silently disappear.
pub struct StopOutcome<E> {
    pub failure: Option<ProcessorFailure>,
    pub unprocessed: Vec<E>,
    pub unprocessed_bytes: usize,
}

/// A clonable admission handle for one processor generation.
///
/// The handle does not own the processor lifecycle.
pub struct ProcessorIngress<E> {
    token: ProcessorToken,
    mailbox: Arc<ProcessorMailbox<E>>,
    pool: Arc<ProcessingPool>,
}

impl<E> Clone for ProcessorIngress<E> {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            mailbox: Arc::clone(&self.mailbox),
            pool: Arc::clone(&self.pool),
        }
    }
}

impl<E> ProcessorIngress<E> {
    pub fn token(&self) -> ProcessorToken {
        self.token
    }

    /// Atomically admit an event and arrange processing.
    ///
    /// The event is returned to the caller if admission fails.
    pub fn admit(&self, event: E, bytes: usize) -> Result<(), AdmissionError<E, PoolError>> {
        self.mailbox
            .push_and_notify(event, bytes, || self.pool.notify(self.token))
    }
}

/// Owns one processor generation's execution registration
/// and its bounded mailbox.
///
/// The surrounding Live lifecycle controller remains
/// responsible for stream state, publication authority,
/// dependency management, and failure acknowledgment.
pub struct ProcessorInstance<E> {
    token: ProcessorToken,
    mailbox: Arc<ProcessorMailbox<E>>,
    pool: Arc<ProcessingPool>,
    stopped: bool,
}

impl<E: Send + 'static> ProcessorInstance<E> {
    /// Create a mailbox, construct a processor, and register it.
    ///
    /// The factory receives the mailbox so the processor can
    /// consume events during run_turn().
    ///
    /// Registration does not start execution. Execution begins
    /// when an ingress handle admits the first event.
    pub fn start<P, F>(
        pool: Arc<ProcessingPool>,
        token: ProcessorToken,
        limits: MailboxLimits,
        factory: F,
    ) -> Result<Self, InstanceError>
    where
        P: Processor,
        F: FnOnce(Arc<ProcessorMailbox<E>>) -> P,
    {
        let mailbox = Arc::new(ProcessorMailbox::new(limits)?);

        let processor = factory(Arc::clone(&mailbox));

        pool.register(token, processor)?;

        Ok(Self {
            token,
            mailbox,
            pool,
            stopped: false,
        })
    }

    /// Create a handle that can admit events into this
    /// specific processor generation.
    pub fn ingress(&self) -> ProcessorIngress<E> {
        ProcessorIngress {
            token: self.token,
            mailbox: Arc::clone(&self.mailbox),
            pool: Arc::clone(&self.pool),
        }
    }

    pub fn token(&self) -> ProcessorToken {
        self.token
    }

    /// Stop the processor and return any unprocessed events.
    ///
    /// This is a blocking operation.
    ///
    /// Do not call from:
    /// - the processor's own run_turn();
    /// - a Tokio executor thread;
    /// - while holding locks needed by the processor.
    ///
    /// A failure report remains in ProcessingPool until the
    /// lifecycle controller explicitly acknowledges it.
    pub fn stop(&mut self) -> Result<StopOutcome<E>, InstanceError> {
        // Repeated successful stop is safe, but it has no
        // additional events to return.
        if self.stopped {
            return Ok(StopOutcome {
                failure: None,
                unprocessed: Vec::new(),
                unprocessed_bytes: 0,
            });
        }

        // Admission closure is synchronized with any concurrent
        // push_and_notify() operation by the mailbox mutex.
        //
        // Never hold a pool lock while waiting for this.
        self.mailbox.close();

        // Retire the execution registration.
        //
        // If the worker already removed the processor after a
        // panic, its terminal failure must still be observable.
        let failure = match self.pool.retire(self.token) {
            Ok(()) => None,

            Err(PoolError::Scheduler(SchedulerError::UnknownProcessor)) => {
                // Unknown registration is acceptable only when
                // the exact generation has a terminal failure.
                Some(
                    self.pool
                        .failure(self.token.id)
                        .filter(|failure| failure.processor == self.token)
                        .ok_or(PoolError::Scheduler(SchedulerError::UnknownProcessor))?,
                )
            }

            Err(error) => return Err(error.into()),
        };

        // No new admissions are possible, and no worker is
        // executing the processor after successful retirement.
        //
        // Therefore the remaining queued events are stable.
        let status = self.mailbox.status();

        let unprocessed = self.mailbox.take_batch(usize::MAX);

        debug_assert_eq!(unprocessed.len(), status.queued_events,);

        self.stopped = true;

        Ok(StopOutcome {
            failure,
            unprocessed,
            unprocessed_bytes: status.queued_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant},
    };

    use crate::live::config::ProcessingConfig;

    fn pool() -> Arc<ProcessingPool> {
        Arc::new(
            ProcessingPool::new(ProcessingConfig {
                workers: 1,
                ready_queue_capacity: 16,
                max_events_per_turn: 8,
            })
            .unwrap(),
        )
    }

    fn token(generation: u64) -> ProcessorToken {
        ProcessorToken { id: 42, generation }
    }

    fn limits() -> MailboxLimits {
        MailboxLimits {
            max_events: 16,
            max_bytes: 1024,
        }
    }

    struct CountingProcessor {
        mailbox: Arc<ProcessorMailbox<u64>>,
        processed: Arc<AtomicUsize>,
    }

    impl Processor for CountingProcessor {
        fn run_turn(&mut self, max_events: usize) -> bool {
            let batch = self.mailbox.take_batch(max_events);

            self.processed.fetch_add(batch.len(), Ordering::SeqCst);

            !self.mailbox.is_empty()
        }
    }

    #[test]
    fn admitted_events_are_processed() {
        let pool = pool();

        let processed = Arc::new(AtomicUsize::new(0));

        let mut instance = ProcessorInstance::start(Arc::clone(&pool), token(1), limits(), {
            let processed = Arc::clone(&processed);

            move |mailbox| CountingProcessor { mailbox, processed }
        })
        .unwrap();

        let ingress = instance.ingress();

        for event in 0..10 {
            ingress.admit(event, 8).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(2);

        while processed.load(Ordering::SeqCst) != 10 {
            assert!(
                Instant::now() < deadline,
                "processor did not consume all events"
            );

            thread::yield_now();
        }

        let outcome = instance.stop().unwrap();

        assert!(outcome.failure.is_none());
        assert!(outcome.unprocessed.is_empty());
        assert_eq!(outcome.unprocessed_bytes, 0);
    }

    struct BlockingProcessor {
        mailbox: Arc<ProcessorMailbox<u64>>,
        started: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Processor for BlockingProcessor {
        fn run_turn(&mut self, _max_events: usize) -> bool {
            // Consume exactly one event.
            let batch = self.mailbox.take_batch(1);

            if !batch.is_empty() {
                self.started.send(()).unwrap();

                // Simulate an executing CPU turn.
                self.release.recv().unwrap();
            }

            !self.mailbox.is_empty()
        }
    }

    #[test]
    fn stop_closes_ingress_and_returns_unprocessed_events() {
        let pool = pool();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let instance =
            ProcessorInstance::start(Arc::clone(&pool), token(1), limits(), move |mailbox| {
                BlockingProcessor {
                    mailbox,
                    started: started_tx,
                    release: release_rx,
                }
            })
            .unwrap();

        let ingress = instance.ingress();

        ingress.admit(10, 8).unwrap();

        // The worker consumes the first event and blocks.
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // This event must remain pending when retirement starts.
        ingress.admit(20, 8).unwrap();

        let (outcome_tx, outcome_rx) = mpsc::channel();

        let stop_thread = thread::spawn(move || {
            let mut instance = instance;

            outcome_tx.send(instance.stop()).unwrap();
        });

        // Wait until admission is actually closed.
        let deadline = Instant::now() + Duration::from_secs(2);

        while !ingress.mailbox.status().closed {
            assert!(Instant::now() < deadline, "stop did not close admission");

            thread::yield_now();
        }

        // A stale ingress handle cannot admit new events.
        assert!(matches!(
            ingress.admit(30, 8),
            Err(AdmissionError::Mailbox(error))
                if error.reason == MailboxError::Closed
                && error.event == 30
        ));

        // Stop cannot finish while the current turn is running.
        assert!(outcome_rx.recv_timeout(Duration::from_millis(100)).is_err());

        // Finish the current processor turn.
        release_tx.send(()).unwrap();

        let outcome = outcome_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();

        stop_thread.join().unwrap();

        assert!(outcome.failure.is_none());
        assert_eq!(outcome.unprocessed, vec![20]);
        assert_eq!(outcome.unprocessed_bytes, 8);

        assert_eq!(pool.registered_count(), 0);
    }

    #[test]
    fn stopped_generation_cannot_admit_after_replacement() {
        let pool = pool();

        let processed = Arc::new(AtomicUsize::new(0));

        let mut old = ProcessorInstance::start(Arc::clone(&pool), token(1), limits(), {
            let processed = Arc::clone(&processed);

            move |mailbox| CountingProcessor { mailbox, processed }
        })
        .unwrap();

        let stale_ingress = old.ingress();

        old.stop().unwrap();

        let mut replacement =
            ProcessorInstance::start(Arc::clone(&pool), token(2), limits(), |mailbox| {
                CountingProcessor {
                    mailbox,
                    processed: Arc::new(AtomicUsize::new(0)),
                }
            })
            .unwrap();

        let rejected = stale_ingress.admit(99, 8);

        assert!(matches!(
            rejected,
            Err(AdmissionError::Mailbox(error))
                if error.reason == MailboxError::Closed
                && error.event == 99
        ));

        // A new generation has independent admission authority.
        replacement.ingress().admit(100, 8).unwrap();

        let outcome = replacement.stop().unwrap();

        // The new generation may already have processed the
        // event or may return it as unprocessed during stop.
        assert!(outcome.unprocessed.is_empty() || outcome.unprocessed == vec![100]);
    }
}
