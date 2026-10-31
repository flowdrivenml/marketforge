use std::sync::Arc;

use thiserror::Error;

use super::{
    mailbox::{AdmissionError, MailboxError, MailboxLimits, ProcessorMailbox},
    pool::{
        PoolError, ProcessingHandle, ProcessingPool, Processor, ProcessorFailure, Registration,
    },
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
    pub accounting: super::mailbox::MailboxStatus,
}

/// A clonable admission handle for one processor generation.
///
/// The handle does not own the processor lifecycle.
pub struct ProcessorIngress<E> {
    token: ProcessorToken,
    mailbox: Arc<ProcessorMailbox<E>>,
    pool: ProcessingHandle,
    registration: Arc<Registration>,
}

impl<E> Clone for ProcessorIngress<E> {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            mailbox: Arc::clone(&self.mailbox),
            pool: self.pool.clone(),
            registration: self.registration.clone(),
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
        self.pool
            .admit(&self.registration, &self.mailbox, event, bytes)
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
    pool: ProcessingHandle,
    registration: Arc<Registration>,
    stopped: bool,
}

impl<E: Send + 'static> ProcessorInstance<E> {
    /// Create a mailbox, construct a processor, and register it.
    ///
    /// The factory receives the mailbox so the processor can
    /// consume events during run_turn().
    ///
    /// The token supplies a logical ID; its generation is a legacy hint. The
    /// runtime allocates a fresh registration identity, returned by token().
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
        Self::start_with_handle(pool.handle(), token, limits, factory)
    }

    pub fn start_with_handle<P: Processor, F: FnOnce(Arc<ProcessorMailbox<E>>) -> P>(
        pool: ProcessingHandle,
        token: ProcessorToken,
        limits: MailboxLimits,
        factory: F,
    ) -> Result<Self, InstanceError> {
        let mailbox = Arc::new(ProcessorMailbox::new(limits)?);
        mailbox.manage(pool.budget());
        let processor = factory(Arc::clone(&mailbox));
        Self::from_mailbox(pool, token.id, mailbox, processor)
    }
    pub(crate) fn from_mailbox<P: Processor>(
        pool: ProcessingHandle,
        id: u64,
        mailbox: Arc<ProcessorMailbox<E>>,
        processor: P,
    ) -> Result<Self, InstanceError> {
        let token = pool.fresh_token(id);
        let registration = pool.register_managed(token, processor, mailbox.clone())?;

        Ok(Self {
            token,
            mailbox,
            pool,
            registration,
            stopped: false,
        })
    }

    /// Create a handle that can admit events into this
    /// specific processor generation.
    pub fn ingress(&self) -> ProcessorIngress<E> {
        ProcessorIngress {
            token: self.token,
            mailbox: Arc::clone(&self.mailbox),
            pool: self.pool.clone(),
            registration: self.registration.clone(),
        }
    }

    pub fn mailbox(&self) -> &Arc<ProcessorMailbox<E>> {
        &self.mailbox
    }
    pub fn request_stop(&self) {
        let _ = self.pool.request_retire_registration(&self.registration);
    }
    pub fn is_settled(&self) -> bool {
        self.pool.settled_registration(&self.registration)
    }
    #[cfg(test)]
    pub(crate) fn registration_for_test(&self) -> Arc<Registration> {
        self.registration.clone()
    }
    pub fn failure_detail(&self) -> Option<String> {
        self.registration.detail()
    }
    pub fn failure(&self) -> Option<ProcessorFailure> {
        self.registration.failure()
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
                failure: self.registration.failure(),
                unprocessed: Vec::new(),
                unprocessed_bytes: 0,
                accounting: self.mailbox.status(),
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
        let failure = match self.pool.retire_registration(&self.registration) {
            Ok(()) => self.registration.failure(),

            Err(PoolError::Scheduler(SchedulerError::UnknownProcessor)) => {
                // Unknown registration is acceptable only when
                // the exact generation has a terminal failure.
                Some(
                    self.registration
                        .failure()
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

        let unprocessed = self.mailbox.collect_remaining();

        debug_assert_eq!(unprocessed.len(), status.queued_events,);

        self.stopped = true;

        Ok(StopOutcome {
            failure,
            unprocessed,
            unprocessed_bytes: status.queued_bytes,
            accounting: self.mailbox.status(),
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
                ..ProcessingConfig::default()
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
            let batch = self.mailbox.take_work(max_events);

            self.processed.fetch_add(batch.len(), Ordering::SeqCst);
            batch.complete();

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
            let batch = self.mailbox.take_work(1);

            if !batch.is_empty() {
                self.started.send(()).unwrap();

                // Simulate an executing CPU turn.
                self.release.recv_timeout(Duration::from_secs(3)).unwrap();
            }

            batch.complete();
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
    #[test]
    fn dropping_ready_instance_cancels_queued_execution() {
        struct Blocker {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
        }

        impl Processor for Blocker {
            fn run_turn(&mut self, _: usize) -> bool {
                self.started.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(3)).unwrap();
                false
            }
        }

        struct CountRuns(Arc<AtomicUsize>);

        impl Processor for CountRuns {
            fn run_turn(&mut self, _: usize) -> bool {
                self.0.fetch_add(1, Ordering::SeqCst);
                false
            }
        }

        let pool = pool(); // Existing one-worker test helper.

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let blocker_token = ProcessorToken {
            id: 100,
            generation: 1,
        };

        pool.register(
            blocker_token,
            Blocker {
                started: started_tx,
                release: release_rx,
            },
        )
        .unwrap();

        pool.notify(blocker_token).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let runs = Arc::new(AtomicUsize::new(0));

        let instance =
            ProcessorInstance::<u64>::start(Arc::clone(&pool), token(1), limits(), |_mailbox| {
                CountRuns(Arc::clone(&runs))
            })
            .unwrap();

        pool.notify(instance.token()).unwrap();

        assert_eq!(pool.registered_count(), 2);

        // No worker is available to execute this instance.
        // Drop must cancel its queued activation.
        drop(instance);

        assert_eq!(pool.registered_count(), 1);

        release_tx.send(()).unwrap();

        pool.retire(blocker_token).unwrap();

        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(pool.registered_count(), 0);
    }
    #[test]
    fn dropping_running_instance_requests_nonblocking_retirement() {
        struct Blocker {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
        }

        impl Processor for Blocker {
            fn run_turn(&mut self, _: usize) -> bool {
                self.started.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(3)).unwrap();
                false
            }
        }

        let pool = pool();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();

        let instance =
            ProcessorInstance::<u64>::start(Arc::clone(&pool), token(1), limits(), |_mailbox| {
                Blocker {
                    started: started_tx,
                    release: release_rx,
                }
            })
            .unwrap();

        let stale_ingress = instance.ingress();

        pool.notify(instance.token()).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // The CPU worker is blocked inside run_turn().
        //
        // Dropping the instance must not wait for it.
        let drop_thread = thread::spawn(move || {
            drop(instance);
            dropped_tx.send(()).unwrap();
        });

        let dropped = dropped_rx.recv_timeout(Duration::from_secs(2));

        // Always release the worker, including when the
        // nonblocking-drop assertion would fail.
        release_tx.send(()).unwrap();

        drop_thread.join().unwrap();

        assert!(
            dropped.is_ok(),
            "dropping an instance blocked on CPU execution"
        );

        // Retirement must eventually remove the registration.
        let deadline = Instant::now() + Duration::from_secs(2);

        while pool.registered_count() != 0 {
            assert!(
                Instant::now() < deadline,
                "running processor was not retired"
            );

            thread::yield_now();
        }

        // An old ingress handle cannot resume this generation.
        assert!(matches!(
            stale_ingress.admit(42, 8),
            Err(AdmissionError::Mailbox(error))
                if error.reason == MailboxError::Closed
                && error.event == 42
        ));
    }
    #[test]
    fn dropping_stopped_instance_preserves_replacement() {
        struct Noop;

        impl Processor for Noop {
            fn run_turn(&mut self, _: usize) -> bool {
                false
            }
        }

        let pool = pool();

        let mut old =
            ProcessorInstance::<u64>::start(Arc::clone(&pool), token(1), limits(), |_mailbox| Noop)
                .unwrap();

        old.stop().unwrap();

        let mut replacement =
            ProcessorInstance::<u64>::start(Arc::clone(&pool), token(2), limits(), |_mailbox| Noop)
                .unwrap();

        // Drop the old instance after its registration was
        // removed and the new generation was registered.
        drop(old);

        assert_eq!(pool.registered_count(), 1);

        replacement.stop().unwrap();

        assert_eq!(pool.registered_count(), 0);
    }
}

/// Fail-safe cleanup for an abandoned processor instance.
///
/// Explicit stop() remains mandatory for lifecycle-controlled
/// shutdown because only stop() returns unprocessed events.
///
/// Drop must not block waiting for CPU execution.
impl<E> Drop for ProcessorInstance<E> {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }

        // Prevent additional event admission.
        //
        // This serializes with concurrent push_and_notify()
        // operations through the mailbox mutex.
        self.mailbox.close();

        // Request retirement without waiting for a CPU turn.
        //
        // A running processor finishes its current turn,
        // after which the worker removes its registration.
        //
        // The pool may already be shutting down or may have
        // removed this processor following a panic.
        // In those cases, the pool owns final cleanup.
        let _ = self.pool.request_retire_registration(&self.registration);
        let abandoned = self.mailbox.collect_remaining();
        drop(abandoned);
    }
}
