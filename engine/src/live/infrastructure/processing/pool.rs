use super::scheduler::{ProcessingScheduler, ProcessorToken, SchedulerError};
use crate::live::config::general::ProcessingConfig;
use std::collections::{HashMap, HashSet};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::{self, JoinHandle},
};
use thiserror::Error;

/// One independently stateful processor.
///
/// Implementations must process at most `max_events` events
/// during each call.
///
/// Return true when another scheduling turn is needed.
///
/// Implementations must not block indefinitely. Worker shutdown
/// waits for currently executing turns to finish.
pub trait Processor: Send + 'static {
    fn run_turn(&mut self, max_events: usize) -> bool;
}

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("processing runtime is shut down")]
    Shutdown,

    #[error("invalid processing configuration: {0}")]
    InvalidConfig(&'static str),

    #[error(transparent)]
    Scheduler(#[from] SchedulerError),

    #[error("processor has an unacknowledged failure: {0:?}")]
    UnacknowledgedFailure(ProcessorToken),

    #[error("processor is retiring: {0:?}")]
    Retiring(ProcessorToken),
}

/// A terminal failure observed by the processing runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorFailureKind {
    Panicked,
}

/// Identifies the exact processor generation that failed.
///
/// This is not a stream lifecycle state. The owning Live
/// controller decides how to transition the affected stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessorFailure {
    pub processor: ProcessorToken,
    pub kind: ProcessorFailureKind,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub completed_turns: u64,
    pub processor_panics: u64,
}

struct PoolState {
    scheduler: ProcessingScheduler,
    processors: HashMap<ProcessorToken, Option<Box<dyn Processor>>>,
    // Terminal failures awaiting acknowledgment by the
    // owning runtime controller.
    //
    // Keyed by logical processor ID, not generation.
    // A new generation cannot replace an unacknowledged failure.
    failures: HashMap<u64, ProcessorFailure>,
    retiring: HashSet<ProcessorToken>,
    shutdown: bool,
    stats: PoolStats,
}

struct Shared {
    state: Mutex<PoolState>,
    wake: Condvar,
    max_events_per_turn: usize,
}

fn lock(shared: &Shared) -> MutexGuard<'_, PoolState> {
    shared
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Fixed-size CPU worker pool.
///
/// Owns its worker threads and joins them on shutdown.
///
/// The pool does not own stream lifecycle or demand state.
pub struct ProcessingPool {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

impl ProcessingPool {
    pub fn new(config: ProcessingConfig) -> Result<Self, PoolError> {
        if config.workers == 0 {
            return Err(PoolError::InvalidConfig("workers must be positive"));
        }

        if config.ready_queue_capacity == 0 {
            return Err(PoolError::InvalidConfig(
                "ready_queue_capacity must be positive",
            ));
        }

        if config.max_events_per_turn == 0 {
            return Err(PoolError::InvalidConfig(
                "max_events_per_turn must be positive",
            ));
        }

        let scheduler = ProcessingScheduler::new(config.ready_queue_capacity)?;

        let shared = Arc::new(Shared {
            state: Mutex::new(PoolState {
                scheduler,
                processors: HashMap::new(),
                failures: HashMap::new(),
                shutdown: false,
                stats: PoolStats::default(),
                retiring: HashSet::new(),
            }),
            wake: Condvar::new(),
            max_events_per_turn: config.max_events_per_turn,
        });

        let mut workers = Vec::with_capacity(config.workers);

        for index in 0..config.workers {
            let shared = Arc::clone(&shared);

            // Thread creation failure is treated as fatal here.
            // A fallible startup API can be added when this pool
            // becomes part of the Live startup coordinator.
            let worker = thread::Builder::new()
                .name(format!("marketforge-cpu-{index}"))
                .spawn(move || worker_loop(shared))
                .expect("failed to create processing worker");

            workers.push(worker);
        }

        Ok(Self { shared, workers })
    }

    /// Register one processor execution generation.
    ///
    /// Registration alone does not schedule work.
    pub fn register<P>(&self, token: ProcessorToken, processor: P) -> Result<(), PoolError>
    where
        P: Processor,
    {
        let mut state = lock(&self.shared);

        if state.shutdown {
            return Err(PoolError::Shutdown);
        }

        if let Some(failure) = state.failures.get(&token.id) {
            return Err(PoolError::UnacknowledgedFailure(failure.processor));
        }

        state.scheduler.register(token)?;

        state.processors.insert(token, Some(Box::new(processor)));

        Ok(())
    }

    /// Notify the scheduler that a processor has work.
    ///
    /// Notifications are coalesced by ProcessingScheduler.
    pub fn notify(&self, token: ProcessorToken) -> Result<(), PoolError> {
        let mut state = lock(&self.shared);

        if state.retiring.contains(&token) {
            return Err(PoolError::Retiring(token));
        }
        if state.shutdown {
            return Err(PoolError::Shutdown);
        }

        state.scheduler.notify(token)?;

        self.shared.wake.notify_one();

        Ok(())
    }

    /// Unregister a processor that is not executing.
    ///
    /// Running processors return ProcessorBusy through
    /// the underlying scheduler.
    pub fn unregister(&self, token: ProcessorToken) -> Result<(), PoolError> {
        let mut state = lock(&self.shared);

        if state.shutdown {
            return Err(PoolError::Shutdown);
        }
        if state.retiring.contains(&token) {
            return Err(PoolError::Retiring(token));
        }
        state.scheduler.unregister(token)?;
        state.processors.remove(&token);

        Ok(())
    }

    pub fn stats(&self) -> PoolStats {
        lock(&self.shared).stats
    }

    pub fn ready_count(&self) -> usize {
        lock(&self.shared).scheduler.ready_count()
    }

    pub fn registered_count(&self) -> usize {
        lock(&self.shared).scheduler.registered_count()
    }

    /// Stop accepting work and join all workers.
    ///
    /// Queued activations are not executed after shutdown.
    /// Already-running turns are allowed to finish.
    pub fn shutdown(&mut self) {
        {
            let mut state = lock(&self.shared);
            state.shutdown = true;
        }

        self.shared.wake.notify_all();

        for worker in self.workers.drain(..) {
            worker
                .join()
                .expect("processing worker unexpectedly panicked");
        }
    }
    /// Inspect the terminal failure of a logical processor.
    ///
    /// Reading does not remove the failure.
    pub fn failure(&self, processor_id: u64) -> Option<ProcessorFailure> {
        let state = lock(&self.shared);

        state.failures.get(&processor_id).copied()
    }

    /// Return a snapshot of unacknowledged failures.
    ///
    /// Results are sorted by logical processor ID for
    /// deterministic inspection.
    pub fn failures(&self) -> Vec<ProcessorFailure> {
        let state = lock(&self.shared);

        let mut failures: Vec<_> = state.failures.values().copied().collect();

        failures.sort_by_key(|failure| failure.processor.id);

        failures
    }

    /// Acknowledge a terminal failure after the lifecycle owner
    /// has handled it.
    ///
    /// The exact generation must match.
    ///
    /// Returns false when there is no matching failure.
    pub fn acknowledge_failure(&self, token: ProcessorToken) -> bool {
        let mut state = lock(&self.shared);

        match state.failures.get(&token.id) {
            Some(failure) if failure.processor == token => {
                state.failures.remove(&token.id);
                true
            }

            _ => false,
        }
    }
    /// Retire one processor generation.
    ///
    /// Prevents new notifications and waits for an executing
    /// scheduling turn to finish.
    ///
    /// A ready processor is removed without executing.
    /// A running processor finishes its current turn but is
    /// not scheduled again.
    ///
    /// The caller must not hold a lock needed by the processor
    /// and must not call this from the processor's own turn.
    pub fn retire(&self, token: ProcessorToken) -> Result<(), PoolError> {
        let mut state = lock(&self.shared);

        if state.shutdown {
            return Err(PoolError::Shutdown);
        }

        if !state.processors.contains_key(&token) {
            return Err(PoolError::Scheduler(SchedulerError::UnknownProcessor));
        }

        state.retiring.insert(token);

        loop {
            // The worker may have completed retirement while
            // this thread was waiting.
            if !state.processors.contains_key(&token) {
                return Ok(());
            }

            match state.scheduler.unregister(token) {
                Ok(()) => {
                    // The processor was Idle or Ready.
                    // No worker currently owns it.
                    state.processors.remove(&token);
                    state.retiring.remove(&token);

                    self.shared.wake.notify_all();

                    return Ok(());
                }

                Err(SchedulerError::ProcessorBusy) => {
                    // The worker owns the processor.
                    // Condvar::wait releases the pool mutex,
                    // allowing that worker to finish its turn.
                    state = self
                        .shared
                        .wake
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }

                Err(error) => {
                    // Defensive cleanup for unexpected scheduler
                    // errors. Normal states are covered above.
                    state.retiring.remove(&token);
                    return Err(PoolError::Scheduler(error));
                }
            }
        }
    }
}

impl Drop for ProcessingPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_loop(shared: Arc<Shared>) {
    loop {
        // Acquire exclusive ownership of a processor.
        let (lease, mut processor) = {
            let mut state = lock(&shared);

            loop {
                if state.shutdown {
                    return;
                }

                if let Some(lease) = state.scheduler.acquire() {
                    let processor = state
                        .processors
                        .get_mut(&lease.processor)
                        .expect("scheduled processor must be registered")
                        .take()
                        .expect("scheduled processor must be available");

                    break (lease, processor);
                }

                state = shared
                    .wake
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        };

        // CPU computation runs WITHOUT holding the scheduler mutex.
        //
        // Other workers can acquire and process different
        // processor instances concurrently.
        let result = catch_unwind(AssertUnwindSafe(|| {
            processor.run_turn(shared.max_events_per_turn)
        }));

        let mut state = lock(&shared);

        match result {
            Ok(has_more_work) => {
                let retiring = state.retiring.contains(&lease.processor);

                state.processors.insert(lease.processor, Some(processor));

                // A retiring processor must never be reactivated.
                if !state.shutdown && !retiring && has_more_work {
                    state
                        .scheduler
                        .notify(lease.processor)
                        .expect("running processor must remain registered");
                }

                state
                    .scheduler
                    .complete(lease)
                    .expect("valid execution lease must complete");

                state.stats.completed_turns = state.stats.completed_turns.saturating_add(1);

                if retiring {
                    // Any notification that arrived before retirement
                    // may have created a queued activation.
                    //
                    // The processor has finished its current turn,
                    // so it can now be removed safely.
                    state
                        .scheduler
                        .unregister(lease.processor)
                        .expect("retiring processor must be removable");

                    state.processors.remove(&lease.processor);
                    state.retiring.remove(&lease.processor);
                }

                // Wake other workers and any retirement callers.
                shared.wake.notify_all();
            }

            Err(_) => {
                state.stats.processor_panics = state.stats.processor_panics.saturating_add(1);

                // A failed processor must never execute another turn.
                //
                // Release its execution lease and remove its
                // registration while holding the pool state lock.
                state
                    .scheduler
                    .complete(lease)
                    .expect("failed processor lease must complete");

                state
                    .scheduler
                    .unregister(lease.processor)
                    .expect("failed processor must be removable");

                state.processors.remove(&lease.processor);
                state.retiring.remove(&lease.processor);
                // Wake any controller waiting for retirement.
                shared.wake.notify_all();
                // Publish the failure to the pool's lifecycle
                // observation state.
                //
                // A new registration with the same logical ID is
                // blocked until this exact failure is acknowledged.
                state.failures.insert(
                    lease.processor.id,
                    ProcessorFailure {
                        processor: lease.processor,
                        kind: ProcessorFailureKind::Panicked,
                    },
                );

                // The owning Live controller will eventually:
                // - observe this failure;
                // - invalidate the affected derived stream;
                // - reconcile dependent streams;
                // - acknowledge this terminal failure.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    fn config(workers: usize) -> ProcessingConfig {
        ProcessingConfig {
            workers,
            ready_queue_capacity: 16,
            max_events_per_turn: 8,
        }
    }

    fn token(id: u64) -> ProcessorToken {
        ProcessorToken { id, generation: 1 }
    }

    struct CountingProcessor {
        remaining: usize,
        sender: mpsc::Sender<usize>,
    }

    impl Processor for CountingProcessor {
        fn run_turn(&mut self, max_events: usize) -> bool {
            assert_eq!(max_events, 8);

            self.remaining -= 1;
            self.sender.send(self.remaining).unwrap();

            self.remaining > 0
        }
    }

    #[test]
    fn processor_requeues_until_work_is_finished() {
        let mut pool = ProcessingPool::new(config(2)).unwrap();

        let (tx, rx) = mpsc::channel();

        pool.register(
            token(1),
            CountingProcessor {
                remaining: 3,
                sender: tx,
            },
        )
        .unwrap();

        pool.notify(token(1)).unwrap();

        for expected in [2, 1, 0] {
            assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), expected);
        }

        pool.shutdown();
    }

    struct BlockingProcessor {
        started: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Processor for BlockingProcessor {
        fn run_turn(&mut self, _max_events: usize) -> bool {
            self.started.send(()).unwrap();
            self.release.recv().unwrap();
            false
        }
    }

    #[test]
    fn independent_processors_can_execute_concurrently() {
        let mut pool = ProcessingPool::new(config(2)).unwrap();

        let (started_tx, started_rx) = mpsc::channel();

        let mut release_senders = Vec::new();

        for id in 1..=2 {
            let (release_tx, release_rx) = mpsc::channel();

            release_senders.push(release_tx);

            pool.register(
                token(id),
                BlockingProcessor {
                    started: started_tx.clone(),
                    release: release_rx,
                },
            )
            .unwrap();

            pool.notify(token(id)).unwrap();
        }

        let first = started_rx.recv_timeout(Duration::from_secs(2));
        let second = started_rx.recv_timeout(Duration::from_secs(2));

        // Always release the workers, even if an assertion will fail.
        for sender in release_senders {
            let _ = sender.send(());
        }

        assert!(first.is_ok());
        assert!(second.is_ok());

        pool.shutdown();
    }
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NotifiedWhileRunning {
        runs: Arc<AtomicUsize>,
        started: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        completed: mpsc::Sender<usize>,
    }

    impl Processor for NotifiedWhileRunning {
        fn run_turn(&mut self, _max_events: usize) -> bool {
            let run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;

            if run == 1 {
                self.started.send(()).unwrap();
                self.release.recv().unwrap();
            }

            self.completed.send(run).unwrap();

            // Deliberately return false on both turns.
            // The second turn must come from an external notification.
            false
        }
    }

    #[test]
    fn notification_during_execution_schedules_another_turn() {
        let mut pool = ProcessingPool::new(config(2)).unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();

        let runs = Arc::new(AtomicUsize::new(0));

        pool.register(
            token(1),
            NotifiedWhileRunning {
                runs: Arc::clone(&runs),
                started: started_tx,
                release: release_rx,
                completed: completed_tx,
            },
        )
        .unwrap();

        pool.notify(token(1)).unwrap();

        // The processor is now executing its first turn.
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Notify while the first turn is still blocked.
        pool.notify(token(1)).unwrap();
        pool.notify(token(1)).unwrap();

        // The processor must not execute concurrently with itself.
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        release_tx.send(()).unwrap();

        assert_eq!(
            completed_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            1
        );

        assert_eq!(
            completed_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            2
        );

        pool.shutdown();

        // Multiple notifications during execution were coalesced.
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }
    struct PanickingProcessor;

    impl Processor for PanickingProcessor {
        fn run_turn(&mut self, _max_events: usize) -> bool {
            panic!("simulated processor failure");
        }
    }

    struct SignalProcessor {
        sender: mpsc::Sender<()>,
    }

    impl Processor for SignalProcessor {
        fn run_turn(&mut self, _max_events: usize) -> bool {
            self.sender.send(()).unwrap();
            false
        }
    }

    #[test]
    fn processor_panic_does_not_kill_worker_thread() {
        // One worker makes this test stronger:
        // the same worker must survive the panic.
        let mut pool = ProcessingPool::new(config(1)).unwrap();

        let (tx, rx) = mpsc::channel();

        pool.register(token(1), PanickingProcessor).unwrap();

        pool.register(token(2), SignalProcessor { sender: tx })
            .unwrap();

        pool.notify(token(1)).unwrap();
        pool.notify(token(2)).unwrap();

        assert!(
            rx.recv_timeout(Duration::from_secs(2)).is_ok(),
            "worker did not recover from processor panic"
        );

        pool.shutdown();

        let stats = pool.stats();

        assert_eq!(stats.processor_panics, 1);
        assert_eq!(stats.completed_turns, 1);

        // The failed processor must no longer be registered.
        assert_eq!(pool.registered_count(), 1);
    }
    #[test]
    fn shutdown_waits_for_running_turn() {
        let mut pool = ProcessingPool::new(config(1)).unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        pool.register(
            token(1),
            BlockingProcessor {
                started: started_tx,
                release: release_rx,
            },
        )
        .unwrap();

        pool.notify(token(1)).unwrap();

        // Ensure that the processor is actually executing.
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let shutdown_thread = std::thread::spawn(move || {
            pool.shutdown();
            shutdown_tx.send(()).unwrap();
        });

        // Shutdown cannot finish while run_turn is blocked.
        let early_completion = shutdown_rx.recv_timeout(Duration::from_millis(100));

        // Allow the running processor to finish.
        release_tx.send(()).unwrap();

        let completed = shutdown_rx.recv_timeout(Duration::from_secs(2));

        shutdown_thread.join().unwrap();

        assert!(
            early_completion.is_err(),
            "shutdown returned while processor was executing"
        );

        assert!(
            completed.is_ok(),
            "shutdown did not finish after processor completed"
        );
    }
    struct MailboxProcessor {
        mailbox: Arc<super::super::mailbox::ProcessorMailbox<u64>>,
        sender: mpsc::Sender<Vec<u64>>,
    }

    impl Processor for MailboxProcessor {
        fn run_turn(&mut self, max_events: usize) -> bool {
            let batch = self.mailbox.take_batch(max_events);

            if !batch.is_empty() {
                self.sender.send(batch).unwrap();
            }

            !self.mailbox.is_empty()
        }
    }

    #[test]
    fn mailbox_admission_schedules_real_cpu_processing() {
        use super::super::mailbox::{AdmissionError, MailboxLimits, ProcessorMailbox};

        let mut pool = ProcessingPool::new(config(2)).unwrap();

        let mailbox = Arc::new(
            ProcessorMailbox::new(MailboxLimits {
                max_events: 16,
                max_bytes: 1024,
            })
            .unwrap(),
        );

        let (tx, rx) = mpsc::channel();

        pool.register(
            token(1),
            MailboxProcessor {
                mailbox: Arc::clone(&mailbox),
                sender: tx,
            },
        )
        .unwrap();

        for event in 0..10_u64 {
            mailbox
                .push_and_notify(event, 8, || pool.notify(token(1)))
                .unwrap();
        }

        let mut received = Vec::new();

        while received.len() < 10 {
            let batch = rx
                .recv_timeout(Duration::from_secs(2))
                .expect("processor did not receive admitted events");

            received.extend(batch);
        }

        assert_eq!(received, (0..10).collect::<Vec<u64>>());

        // Allow any final scheduling turn to finish before unregistering.
        // Shutdown stops the workers; it does not discard accepted
        // mailbox data that this test has already verified.
        pool.shutdown();

        assert!(mailbox.is_empty());

        // Ensure the error type remains compatible with pool notifications.
        let _: Option<AdmissionError<u64, PoolError>> = None;
    }
    #[test]
    fn processor_failure_requires_acknowledgment() {
        let mut pool = ProcessingPool::new(config(1)).unwrap();

        let failed = token(1);

        pool.register(failed, PanickingProcessor).unwrap();
        pool.notify(failed).unwrap();

        // Wait until the worker records the failure.
        let failure = {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);

            loop {
                if let Some(failure) = pool.failure(failed.id) {
                    break failure;
                }

                assert!(
                    std::time::Instant::now() < deadline,
                    "processor failure was not reported"
                );

                std::thread::sleep(Duration::from_millis(1));
            }
        };

        assert_eq!(
            failure,
            ProcessorFailure {
                processor: failed,
                kind: ProcessorFailureKind::Panicked,
            }
        );

        // Reading does not consume the failure.
        assert_eq!(pool.failure(failed.id), Some(failure));

        let replacement = ProcessorToken {
            id: failed.id,
            generation: failed.generation + 1,
        };

        let (tx, _rx) = mpsc::channel();

        assert!(matches!(
            pool.register(
                replacement,
                SignalProcessor { sender: tx.clone() },
            ),
            Err(PoolError::UnacknowledgedFailure(token))
                if token == failed
        ));

        assert!(pool.acknowledge_failure(failed));

        assert_eq!(pool.failure(failed.id), None);

        pool.register(replacement, SignalProcessor { sender: tx })
            .unwrap();

        pool.shutdown();
    }
    #[test]
    fn stale_generation_cannot_acknowledge_failure() {
        let mut pool = ProcessingPool::new(config(1)).unwrap();

        let failed = token(10);

        pool.register(failed, PanickingProcessor).unwrap();
        pool.notify(failed).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);

        while pool.failure(failed.id).is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "processor failure was not reported"
            );

            std::thread::sleep(Duration::from_millis(1));
        }

        let wrong_generation = ProcessorToken {
            id: failed.id,
            generation: failed.generation + 1,
        };

        assert!(!pool.acknowledge_failure(wrong_generation));

        assert_eq!(pool.failure(failed.id).unwrap().processor, failed);

        assert!(pool.acknowledge_failure(failed));

        pool.shutdown();
    }
    #[test]
    fn failures_are_retained_independently() {
        let mut pool = ProcessingPool::new(config(2)).unwrap();

        pool.register(token(1), PanickingProcessor).unwrap();
        pool.register(token(2), PanickingProcessor).unwrap();

        pool.notify(token(1)).unwrap();
        pool.notify(token(2)).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);

        loop {
            if pool.failures().len() == 2 {
                break;
            }

            assert!(
                std::time::Instant::now() < deadline,
                "not all processor failures were reported"
            );

            std::thread::sleep(Duration::from_millis(1));
        }

        let failures = pool.failures();

        assert_eq!(failures[0].processor, token(1));
        assert_eq!(failures[1].processor, token(2));

        assert!(pool.acknowledge_failure(token(1)));

        assert_eq!(pool.failures().len(), 1);
        assert_eq!(pool.failure(2).unwrap().processor, token(2));

        pool.shutdown();
    }
    #[test]
    fn retirement_waits_for_running_processor() {
        let pool = ProcessingPool::new(config(1)).unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (retired_tx, retired_rx) = mpsc::channel();

        pool.register(
            token(1),
            BlockingProcessor {
                started: started_tx,
                release: release_rx,
            },
        )
        .unwrap();

        pool.notify(token(1)).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let pool = Arc::new(pool);
        let retiring_pool = Arc::clone(&pool);

        let retirement = std::thread::spawn(move || {
            let result = retiring_pool.retire(token(1));
            retired_tx.send(result).unwrap();
        });

        // Wait until retirement has actually been requested.
        // Merely starting the retirement thread is not enough:
        // otherwise the test could pass before retirement begins.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);

        loop {
            let state = lock(&pool.shared);

            if state.retiring.contains(&token(1)) {
                break;
            }

            drop(state);

            assert!(
                std::time::Instant::now() < deadline,
                "retirement did not begin"
            );

            std::thread::yield_now();
        }

        assert!(matches!(
            pool.notify(token(1)),
            Err(PoolError::Retiring(t)) if t == token(1)
        ));

        assert!(
            retired_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "retirement finished before the active turn"
        );

        // Allow the processor to finish.
        release_tx.send(()).unwrap();

        retired_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();

        retirement.join().unwrap();

        assert_eq!(pool.registered_count(), 0);
    }
    #[test]
    fn retirement_suppresses_pending_rescheduling() {
        let pool = ProcessingPool::new(config(1)).unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();

        let runs = Arc::new(AtomicUsize::new(0));

        pool.register(
            token(1),
            NotifiedWhileRunning {
                runs: Arc::clone(&runs),
                started: started_tx,
                release: release_rx,
                completed: completed_tx,
            },
        )
        .unwrap();

        pool.notify(token(1)).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // This notification normally schedules another turn.
        pool.notify(token(1)).unwrap();

        let pool = Arc::new(pool);
        let retiring_pool = Arc::clone(&pool);

        let retirement = std::thread::spawn(move || {
            retiring_pool.retire(token(1)).unwrap();
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(2);

        loop {
            let state = lock(&pool.shared);

            if state.retiring.contains(&token(1)) {
                break;
            }

            drop(state);

            assert!(
                std::time::Instant::now() < deadline,
                "retirement did not begin"
            );

            std::thread::yield_now();
        }

        release_tx.send(()).unwrap();

        retirement.join().unwrap();

        assert_eq!(runs.load(Ordering::SeqCst), 1);

        assert_eq!(completed_rx.try_recv().unwrap(), 1);

        assert!(completed_rx.try_recv().is_err());

        assert_eq!(pool.registered_count(), 0);
    }
    #[test]
    fn ready_processor_can_be_retired_without_execution() {
        let mut pool = ProcessingPool::new(config(1)).unwrap();

        // Occupy the only worker.
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        pool.register(
            token(1),
            BlockingProcessor {
                started: started_tx,
                release: release_rx,
            },
        )
        .unwrap();

        pool.notify(token(1)).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let (tx, rx) = mpsc::channel();

        pool.register(token(2), SignalProcessor { sender: tx })
            .unwrap();

        pool.notify(token(2)).unwrap();

        // Processor 2 is Ready, but cannot execute because
        // the only worker is occupied.
        pool.retire(token(2)).unwrap();

        release_tx.send(()).unwrap();

        pool.shutdown();

        assert!(
            rx.try_recv().is_err(),
            "retired processor unexpectedly executed"
        );
    }
}
