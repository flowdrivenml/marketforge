use super::{
    mailbox::{AdmissionError, BufferBudget, MailboxLimits, ProcessorMailbox},
    scheduler::{ProcessingScheduler, ProcessorToken, SchedulerError},
};
use crate::live::config::general::ProcessingConfig;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Deref,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Instant,
};
use thiserror::Error;

/// Cooperative synchronous state machine. Never retain input outside tracked batches.
pub trait Processor: Send + 'static {
    fn run_turn(&mut self, max_events: usize) -> bool;
    /// Operational errors need not unwind. Existing processors can implement run_turn only.
    fn try_run_turn(&mut self, max_events: usize) -> Result<bool, String> {
        Ok(self.run_turn(max_events))
    }
}
impl Processor for Box<dyn Processor> {
    fn run_turn(&mut self, max_events: usize) -> bool {
        self.as_mut().run_turn(max_events)
    }
    fn try_run_turn(&mut self, max_events: usize) -> Result<bool, String> {
        self.as_mut().try_run_turn(max_events)
    }
}
#[derive(Debug, Error)]
pub enum PoolError {
    #[error("processing runtime is shut down")]
    Shutdown,
    #[error("invalid processing configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("failed to start processing worker: {0}")]
    WorkerStartup(#[source] std::io::Error),
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error("processor has an unacknowledged failure: {0:?}")]
    UnacknowledgedFailure(ProcessorToken),
    #[error("processor is retiring: {0:?}")]
    Retiring(ProcessorToken),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorFailureKind {
    Panicked,
    Error,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessorFailure {
    pub processor: ProcessorToken,
    pub kind: ProcessorFailureKind,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub completed_turns: u64,
    pub processor_panics: u64,
    pub processor_errors: u64,
    pub destructor_panics: u64,
    pub requested_workers: usize,
    pub starting_workers: usize,
    pub retiring_workers: usize,
    pub resize_revision: u64,
    pub resize_pending: bool,
    pub configured_workers: usize,
    pub started_workers: usize,
    pub running_workers: usize,
    pub exited_workers: usize,
    pub active_workers: usize,
    pub executing_processors: usize,
    pub unexpected_worker_exits: usize,
    pub registered: usize,
    pub ready: usize,
    pub retiring: usize,
    pub retained_failures: usize,
    pub shutdown: bool,
    pub owner_dropped: bool,
    pub joined: bool,
}
pub(crate) trait InputControl: Send + Sync {
    fn close(&self);
}
pub(crate) struct Registration {
    pub token: ProcessorToken,
    input: Option<Arc<dyn InputControl>>,
    terminal: Mutex<Option<ProcessorFailure>>,
    detail: Mutex<Option<String>>,
}
impl Registration {
    fn close(&self) {
        if let Some(input) = &self.input {
            input.close();
        }
    }
    pub fn detail(&self) -> Option<String> {
        self.detail
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn failure(&self) -> Option<ProcessorFailure> {
        *self.terminal.lock().unwrap_or_else(|e| e.into_inner())
    }
}
struct Entry {
    processor: Option<Box<dyn Processor>>,
    registration: Arc<Registration>,
}
struct WorkerControl { active: AtomicBool, retire: AtomicBool }
struct PoolState {
    resize_target: Option<usize>,
    resize_error: Option<String>,
    #[cfg(test)]
    spawn_failure_after: Option<usize>,
    scheduler: ProcessingScheduler,
    processors: HashMap<ProcessorToken, Entry>,
    failures: HashMap<u64, ProcessorFailure>,
    retiring: HashSet<ProcessorToken>,
    destroying: HashSet<ProcessorToken>,
    disposals: VecDeque<(ProcessorToken, Entry)>,
    shutdown: bool,
    #[cfg(test)]
    exit_requested: bool,
    stats: PoolStats,
}
struct Shared {
    state: Mutex<PoolState>,
    wake: Condvar,
    supervisor_wake: Condvar,
    updates: tokio::sync::watch::Sender<u64>,
    max_events_per_turn: usize,
    budget: Arc<BufferBudget>,
}
fn lock(shared: &Shared) -> MutexGuard<'_, PoolState> {
    shared.state.lock().unwrap_or_else(|e| e.into_inner())
}
thread_local! { static CURRENT_RUNTIME: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
static NEXT_REGISTRATION: AtomicU64 = AtomicU64::new(1);
/// Lightweight submission capability. Dropping it never joins workers.
#[derive(Clone)]
pub struct ProcessingHandle {
    shared: Arc<Shared>,
}
/// Owns startup and one supervisor which retains and joins every worker.
/// On timeout the owner can retry shutdown; dropping it requests abnormal shutdown,
/// and the supervisor continues to own unfinished workers until they return.
pub struct ProcessingPool {
    handle: ProcessingHandle,
    supervisor: Option<JoinHandle<()>>,
}
impl Deref for ProcessingPool {
    type Target = ProcessingHandle;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownOutcome {
    pub complete: bool,
    pub unfinished_workers: usize,
    pub unfinished_processors: usize,
    pub unexpected_worker_exits: usize,
}
impl ProcessingPool {
    pub fn new(config: ProcessingConfig) -> Result<Self, PoolError> {
        Self::start_with(config, |index, job| {
            thread::Builder::new()
                .name(format!("marketforge-cpu-{index}"))
                .spawn(job)
        })
    }
    fn start_with<F>(config: ProcessingConfig, mut spawn: F) -> Result<Self, PoolError>
    where
        F: FnMut(usize, Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>,
    {
        if config.workers == 0 || config.max_events_per_turn == 0 {
            return Err(PoolError::InvalidConfig(
                "workers and turn budget must be positive",
            ));
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(PoolState {
                resize_target: None, resize_error: None,
                #[cfg(test)]
                spawn_failure_after: None,
                scheduler: ProcessingScheduler::new(config.ready_queue_capacity)?,
                processors: HashMap::new(),
                failures: HashMap::new(),
                retiring: HashSet::new(),
                destroying: HashSet::new(),
                disposals: VecDeque::new(),
                shutdown: false,
                #[cfg(test)]
                exit_requested: false,
                stats: PoolStats {
                    requested_workers: config.workers,
                    configured_workers: config.workers,
                    ..Default::default()
                },
            }),
            wake: Condvar::new(),
            supervisor_wake: Condvar::new(),
            updates: tokio::sync::watch::channel(0).0,
            max_events_per_turn: config.max_events_per_turn,
            budget: Arc::new(
                BufferBudget::new(MailboxLimits {
                    max_events: config.buffered_events,
                    max_bytes: config.buffered_bytes,
                })
                .map_err(|_| PoolError::InvalidConfig("buffer limits must be positive"))?,
            ),
        });
        let mut workers = Vec::new();
        for index in 0..config.workers {
            let control=Arc::new(WorkerControl{active:AtomicBool::new(true),retire:AtomicBool::new(false)});
            let runtime=shared.clone(); let c=control.clone();
            match spawn(index,Box::new(move || worker_entry(runtime,c))) {
                Ok(worker) => {
                    workers.push((worker,control));
                    lock(&shared).stats.started_workers += 1;
                }
                Err(error) => {
                    lock(&shared).shutdown = true;
                    shared.wake.notify_all();
                    for (worker,_) in workers {
                        let _ = worker.join();
                    }
                    return Err(PoolError::WorkerStartup(error));
                }
            }
        }
        // The supervisor owns the JoinHandles even if the facade is dropped.
        // Use a transfer cell so failure to create it can still join all workers.
        let transfer = Arc::new(Mutex::new(Some(workers)));
        let received = transfer.clone();
        let runtime = shared.clone();
        let supervisor = match thread::Builder::new()
            .name("marketforge-cpu-supervisor".into())
            .spawn(move || {
                CURRENT_RUNTIME.with(|current| current.set(Arc::as_ptr(&runtime) as usize));
                let workers = received.lock().unwrap().take().unwrap();
                supervise_workers(&runtime,workers);
                let mut state = lock(&runtime);
                let mut remaining = std::mem::take(&mut state.processors);
                for (token, entry) in state.disposals.drain(..) {
                    remaining.insert(token, entry);
                }
                state.retiring.clear();
                state.scheduler = ProcessingScheduler::new(state.scheduler.capacity())
                    .expect("validated capacity");
                for token in remaining.keys() {
                    state.destroying.insert(*token);
                }
                drop(state);
                // User destructors always run outside the scheduling lock.
                for (token, entry) in remaining {
                    let registration = entry.registration.clone();
                    if catch_unwind(AssertUnwindSafe(|| drop(entry))).is_err() {
                        destructor_failed(&runtime, &registration);
                    }
                    lock(&runtime).destroying.remove(&token);
                    runtime.wake.notify_all();
                }
                lock(&runtime).stats.joined = true;
                runtime.wake.notify_all();
                runtime
                    .updates
                    .send_modify(|version| *version = version.wrapping_add(1));
            }) {
            Ok(supervisor) => supervisor,
            Err(error) => {
                lock(&shared).shutdown = true;
                shared.wake.notify_all();
                for (worker,_) in transfer.lock().unwrap().take().unwrap() {
                    let _ = worker.join();
                }
                return Err(PoolError::WorkerStartup(error));
            }
        };
        Ok(Self {
            handle: ProcessingHandle { shared },
            supervisor: Some(supervisor),
        })
    }
    pub fn handle(&self) -> ProcessingHandle {
        self.handle.clone()
    }
    pub fn shutdown_until(&mut self, deadline: Instant) -> ShutdownOutcome {
        self.handle.begin_shutdown();
        let mut state = lock(&self.handle.shared);
        let on_runtime_thread = CURRENT_RUNTIME
            .with(|current| current.get() == Arc::as_ptr(&self.handle.shared) as usize);
        while !state.stats.joined && !on_runtime_thread {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            state = self
                .handle
                .shared
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        let outcome = ShutdownOutcome {
            complete: state.stats.joined,
            unfinished_workers: if state.stats.joined {
                0
            } else {
                state
                    .stats
                    .started_workers
                    .saturating_sub(state.stats.exited_workers)
            },
            unfinished_processors: state.processors.len() + state.destroying.len(),
            unexpected_worker_exits: state.stats.unexpected_worker_exits,
        };
        drop(state);
        if outcome.complete {
            if let Some(supervisor) = self.supervisor.take() {
                let _ = supervisor.join();
            }
        }
        outcome
    }
    /// Compatibility blocking shutdown. Services should use shutdown_until.
    #[cfg(test)]
    pub(crate) fn shutdown(&mut self) {
        if CURRENT_RUNTIME
            .with(|current| current.get() == Arc::as_ptr(&self.handle.shared) as usize)
        {
            self.handle.begin_shutdown();
            return;
        }
        self.handle.begin_shutdown();
        let mut state = lock(&self.handle.shared);
        while !state.stats.joined {
            state = self
                .handle
                .shared
                .wake
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        drop(state);
        if let Some(supervisor) = self.supervisor.take() {
            let _ = supervisor.join();
        }
    }
}
impl Drop for ProcessingPool {
    fn drop(&mut self) {
        if self.supervisor.is_some() {
            lock(&self.handle.shared).stats.owner_dropped = true;
        }
        self.handle.begin_shutdown();
    }
}
impl ProcessingHandle {
    pub fn budget(&self) -> Arc<BufferBudget> {
        self.shared.budget.clone()
    }
    pub(crate) fn fresh_token(&self, id: u64) -> ProcessorToken {
        ProcessorToken {
            id,
            generation: NEXT_REGISTRATION
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .expect("processing registration identity exhausted"),
        }
    }
    #[cfg(test)]
    pub(crate) fn register<P: Processor>(
        &self,
        token: ProcessorToken,
        processor: P,
    ) -> Result<(), PoolError> {
        self.register_inner(token, processor, None).map(|_| ())
    }
    pub(crate) fn register_managed<P: Processor>(
        &self,
        token: ProcessorToken,
        processor: P,
        input: Arc<dyn InputControl>,
    ) -> Result<Arc<Registration>, PoolError> {
        self.register_inner(token, processor, Some(input))
    }
    fn register_inner<P: Processor>(
        &self,
        token: ProcessorToken,
        processor: P,
        input: Option<Arc<dyn InputControl>>,
    ) -> Result<Arc<Registration>, PoolError> {
        // Box before locking; rejected processor destruction occurs after guard destruction.
        let processor: Box<dyn Processor> = Box::new(processor);
        let mut state = lock(&self.shared);
        if state.shutdown {
            return Err(PoolError::Shutdown);
        }
        if let Some(failure) = state.failures.get(&token.id) {
            return Err(PoolError::UnacknowledgedFailure(failure.processor));
        }
        if state.scheduler.registered_count()
            + state.failures.len()
            + state
                .destroying
                .iter()
                .filter(|token| !state.failures.contains_key(&token.id))
                .count()
            >= state.scheduler.capacity()
        {
            return Err(SchedulerError::Capacity.into());
        }
        if state.destroying.iter().any(|old| old.id == token.id) {
            return Err(PoolError::Retiring(token));
        }
        state.scheduler.register(token)?;
        let registration = Arc::new(Registration {
            token,
            input,
            terminal: Mutex::new(None),
            detail: Mutex::new(None),
        });
        state.processors.insert(
            token,
            Entry {
                processor: Some(processor),
                registration: registration.clone(),
            },
        );
        Ok(registration)
    }
    /// Schedule a bounded deadline activation using the existing reservation.
    pub(crate) fn notify(&self, token: ProcessorToken) -> Result<(), PoolError> {
        let mut state = lock(&self.shared);
        if state.shutdown {
            return Err(PoolError::Shutdown);
        }
        if state.retiring.contains(&token) {
            return Err(PoolError::Retiring(token));
        }
        state.scheduler.notify(token)?;
        self.shared.wake.notify_one();
        Ok(())
    }
    pub(crate) fn admit<E>(
        &self,
        registration: &Arc<Registration>,
        mailbox: &ProcessorMailbox<E>,
        event: E,
        bytes: usize,
    ) -> Result<(), AdmissionError<E, PoolError>> {
        let mut state = lock(&self.shared);
        let token = registration.token;
        let reason = if state.shutdown {
            Some(PoolError::Shutdown)
        } else if state.retiring.contains(&token) {
            Some(PoolError::Retiring(token))
        } else if !state
            .processors
            .get(&token)
            .is_some_and(|entry| Arc::ptr_eq(&entry.registration, registration))
        {
            Some(SchedulerError::UnknownProcessor.into())
        } else {
            None
        };
        if let Some(reason) = reason {
            mailbox.record_rejection();
            if mailbox.status().closed {
                return Err(AdmissionError::Mailbox(super::mailbox::MailboxPushError {
                    reason: super::mailbox::MailboxError::Closed,
                    event,
                }));
            }
            return Err(AdmissionError::Notification { reason, event });
        }
        mailbox
            .push_managed(event, bytes)
            .map_err(AdmissionError::Mailbox)?;
        state
            .scheduler
            .notify(token)
            .expect("registration owns activation reservation");
        self.shared.wake.notify_one();
        Ok(())
    }
    pub(crate) fn begin_shutdown(&self) {
        let mut state = lock(&self.shared);
        state.shutdown = true;
        for entry in state.processors.values() {
            entry.registration.close();
        }
        self.shared.wake.notify_all();
        self.shared
            .updates
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    /// Absolute bounded target. Completion is observed separately; threads remain
    /// owned by the existing supervisor throughout planned retirement.
    pub fn set_workers(&self, target:usize, expected_revision:u64)->Result<u64,PoolError>{
        if !(1..=1024).contains(&target){return Err(PoolError::InvalidConfig("worker target outside 1..=1024"));}
        let mut state=lock(&self.shared);
        if state.shutdown{return Err(PoolError::Shutdown);}
        if state.stats.resize_pending {
            return if target==state.stats.requested_workers {Ok(state.stats.resize_revision)}else{Err(PoolError::InvalidConfig("resize already pending"))};
        }
        if expected_revision!=state.stats.resize_revision{return Err(PoolError::InvalidConfig("pool revision conflict"));}
        state.stats.resize_revision=state.stats.resize_revision.checked_add(1).ok_or(PoolError::InvalidConfig("pool revision exhausted"))?;
        state.stats.requested_workers=target;state.stats.resize_pending=true;state.resize_error=None;state.resize_target=Some(target);
        let revision=state.stats.resize_revision;
        self.shared.wake.notify_all(); Ok(revision)
    }
    pub fn resize_error(&self)->Option<String>{lock(&self.shared).resize_error.clone()}
    pub fn stats(&self) -> PoolStats {
        let state = lock(&self.shared);
        PoolStats {
            starting_workers:state.stats.started_workers.saturating_sub(state.stats.exited_workers+state.stats.running_workers),
            registered: state.processors.len(),
            executing_processors: state
                .processors
                .values()
                .filter(|entry| entry.processor.is_none())
                .count(),
            ready: state.scheduler.ready_count(),
            retiring: state.retiring.len() + state.destroying.len(),
            retained_failures: state.failures.len(),
            shutdown: state.shutdown,
            ..state.stats
        }
    }
    pub fn registered_count(&self) -> usize {
        self.stats().registered
    }
    pub fn ready_count(&self) -> usize {
        self.stats().ready
    }
    pub fn failure(&self, id: u64) -> Option<ProcessorFailure> {
        lock(&self.shared).failures.get(&id).copied()
    }
    pub fn failures(&self) -> Vec<ProcessorFailure> {
        let mut result: Vec<_> = lock(&self.shared).failures.values().copied().collect();
        result.sort_by_key(|f| f.processor.id);
        result
    }
    pub fn acknowledge_failure(&self, token: ProcessorToken) -> bool {
        let mut state = lock(&self.shared);
        if state
            .failures
            .get(&token.id)
            .is_some_and(|f| f.processor == token)
        {
            state.failures.remove(&token.id);
            true
        } else {
            false
        }
    }
    /// Coalesced wakeup; authoritative state is retained in registrations/failures.
    pub fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared.updates.subscribe()
    }
    pub async fn changed(&self) {
        let mut updates = self.subscribe_changes();
        let _ = updates.changed().await;
    }
    #[cfg(test)]
    pub(crate) fn request_retire(&self, token: ProcessorToken) -> Result<(), PoolError> {
        self.request_retire_inner(token, None)
    }
    pub(crate) fn request_retire_registration(
        &self,
        registration: &Arc<Registration>,
    ) -> Result<(), PoolError> {
        self.request_retire_inner(registration.token, Some(registration))
    }
    fn request_retire_inner(
        &self,
        token: ProcessorToken,
        registration: Option<&Arc<Registration>>,
    ) -> Result<(), PoolError> {
        let mut state = lock(&self.shared);
        let entry = state
            .processors
            .get(&token)
            .ok_or(SchedulerError::UnknownProcessor)?;
        if registration.is_some_and(|registration| !Arc::ptr_eq(registration, &entry.registration))
        {
            return Err(SchedulerError::UnknownProcessor.into());
        }
        entry.registration.close();
        state.retiring.insert(token);
        let removed = match state.scheduler.unregister(token) {
            Ok(()) => {
                state.retiring.remove(&token);
                state.processors.remove(&token)
            }
            Err(SchedulerError::ProcessorBusy) => None,
            Err(e) => return Err(e.into()),
        };
        if let Some(entry) = removed {
            state.destroying.insert(token);
            state.disposals.push_back((token, entry));
        }
        drop(state);
        self.shared.wake.notify_all();
        self.shared
            .updates
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn retire(&self, token: ProcessorToken) -> Result<(), PoolError> {
        self.request_retire(token)?;
        let mut state = lock(&self.shared);
        while state.processors.contains_key(&token) || state.destroying.contains(&token) {
            state = self
                .shared
                .wake
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        Ok(())
    }
    pub(crate) fn settled_registration(&self, registration: &Arc<Registration>) -> bool {
        let state = lock(&self.shared);
        !state.destroying.contains(&registration.token)
            && !state
                .processors
                .get(&registration.token)
                .is_some_and(|entry| Arc::ptr_eq(&entry.registration, registration))
    }
    pub(crate) fn retire_registration(
        &self,
        registration: &Arc<Registration>,
    ) -> Result<(), PoolError> {
        if self.settled_registration(registration) {
            return Ok(());
        }
        match self.request_retire_registration(registration) {
            Ok(()) | Err(PoolError::Scheduler(SchedulerError::UnknownProcessor)) => {}
            Err(error) => return Err(error),
        }
        let mut state = lock(&self.shared);
        while state.destroying.contains(&registration.token)
            || state
                .processors
                .get(&registration.token)
                .is_some_and(|entry| Arc::ptr_eq(&entry.registration, registration))
        {
            state = self
                .shared
                .wake
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        Ok(())
    }
}
fn destructor_failed(shared: &Shared, registration: &Arc<Registration>) {
    let mut state = lock(shared);
    state.stats.destructor_panics += 1;
    let mut terminal = registration
        .terminal
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if terminal.is_none() {
        let failure = ProcessorFailure {
            processor: registration.token,
            kind: ProcessorFailureKind::Panicked,
        };
        *terminal = Some(failure);
        *registration
            .detail
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some("processor destructor panicked".into());
        state.failures.insert(registration.token.id, failure);
    }
    shared
        .updates
        .send_modify(|version| *version = version.wrapping_add(1));
}
fn worker_loop(shared: Arc<Shared>, control: Arc<WorkerControl>) {
    loop {
        let (lease, mut processor) = {
            let mut state = lock(&shared);
            loop {
                if control.retire.load(Ordering::Acquire) || state.shutdown {return;}
                if !control.active.load(Ordering::Acquire) {state=shared.wake.wait(state).unwrap_or_else(|e|e.into_inner());continue;}
                #[cfg(test)]
                if state.exit_requested {
                    state.exit_requested = false;
                    panic!("injected unexpected worker exit");
                }
                if let Some((token, entry)) = state.disposals.pop_front() {
                    state.stats.active_workers += 1;
                    drop(state);
                    let registration = entry.registration.clone();
                    if catch_unwind(AssertUnwindSafe(|| drop(entry))).is_err() {
                        destructor_failed(&shared, &registration);
                    }
                    state = lock(&shared);
                    state.stats.active_workers -= 1;
                    state.destroying.remove(&token);
                    shared.wake.notify_all();
                    shared
                        .updates
                        .send_modify(|version| *version = version.wrapping_add(1));
                    continue;
                }
                if state.shutdown {
                    return;
                }
                if let Some(lease) = state.scheduler.acquire() {
                    let processor = state
                        .processors
                        .get_mut(&lease.processor())
                        .unwrap()
                        .processor
                        .take()
                        .unwrap();
                    state.stats.active_workers += 1;
                    break (lease, processor);
                }
                state = shared.wake.wait(state).unwrap_or_else(|e| e.into_inner());
            }
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            processor.try_run_turn(shared.max_events_per_turn)
        }));
        let token = lease.processor();
        let mut state = lock(&shared);
        state.stats.active_workers -= 1;
        let failure = match &result {
            Err(_) => Some(ProcessorFailureKind::Panicked),
            Ok(Err(_)) => Some(ProcessorFailureKind::Error),
            _ => None,
        };
        let retiring = state.shutdown || state.retiring.contains(&token) || failure.is_some();
        if !retiring && matches!(result, Ok(Ok(true))) {
            state.scheduler.notify(token).unwrap();
        }
        state.scheduler.complete(lease).unwrap();
        let removed = if retiring {
            state.scheduler.unregister(token).unwrap();
            state.retiring.remove(&token);
            let entry = state.processors.remove(&token).unwrap();
            entry.registration.close();
            if let Some(kind) = failure {
                let report = ProcessorFailure {
                    processor: token,
                    kind,
                };
                *entry
                    .registration
                    .terminal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(report);
                let detail = match &result {
                    Ok(Err(reason)) => reason.as_str(),
                    Err(payload) => payload
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("processor panicked"),
                    _ => "processor failed",
                };
                *entry
                    .registration
                    .detail
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(detail.chars().take(4096).collect());
                state.failures.insert(token.id, report);
                match kind {
                    ProcessorFailureKind::Panicked => state.stats.processor_panics += 1,
                    ProcessorFailureKind::Error => state.stats.processor_errors += 1,
                }
            }
            state.destroying.insert(token);
            Some(entry)
        } else {
            None
        };
        if failure.is_none() {
            state.stats.completed_turns += 1;
        }
        if !retiring {
            state.processors.get_mut(&token).unwrap().processor = Some(processor);
            drop(state);
        } else {
            drop(state);
            let registration = removed.as_ref().unwrap().registration.clone();
            if catch_unwind(AssertUnwindSafe(|| drop(processor))).is_err() {
                destructor_failed(&shared, &registration);
            }
        }
        drop(removed);
        lock(&shared).destroying.remove(&token);
        shared.wake.notify_all();
        shared
            .updates
            .send_modify(|version| *version = version.wrapping_add(1));
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
            ..ProcessingConfig::default()
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
    fn unexpected_worker_exit_is_supervised() {
        let mut owner = ProcessingPool::new(config(1)).unwrap();
        lock(&owner.shared).exit_requested = true;
        owner.shared.wake.notify_all();
        // Observe the injected failure before initiating planned shutdown. A
        // worker still entering the pool may otherwise correctly retire first.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut state = lock(&owner.shared);
        while state.stats.unexpected_worker_exits == 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "worker failure was not supervised");
            state = owner.shared.wake.wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner()).0;
        }
        drop(state);
        let result = owner.shutdown_until(Instant::now() + Duration::from_secs(2));
        assert!(result.complete);
        assert_eq!(result.unexpected_worker_exits, 1);
        assert_eq!(owner.stats().exited_workers, 1);
    }
    #[test]
    fn worker_cannot_join_itself() {
        struct SelfStop {
            owner: Arc<Mutex<Option<ProcessingPool>>>,
            result: mpsc::Sender<ShutdownOutcome>,
        }
        impl Processor for SelfStop {
            fn run_turn(&mut self, _: usize) -> bool {
                let outcome = self
                    .owner
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .shutdown_until(Instant::now() + Duration::from_secs(2));
                self.result.send(outcome).unwrap();
                false
            }
        }
        let owner = ProcessingPool::new(config(1)).unwrap();
        let handle = owner.handle();
        let slot = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel();
        owner
            .register(
                token(1),
                SelfStop {
                    owner: slot.clone(),
                    result: tx,
                },
            )
            .unwrap();
        *slot.lock().unwrap() = Some(owner);
        handle.notify(token(1)).unwrap();
        let outcome = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!outcome.complete);
        let mut owner = slot.lock().unwrap().take().unwrap();
        assert!(
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(2))
                .complete
        );
    }
    #[test]
    fn partial_worker_startup_joins_started_workers() {
        let (exited, observed) = mpsc::channel();
        let result = ProcessingPool::start_with(config(3), |index, job| {
            if index == 1 {
                return Err(std::io::Error::other("injected creation failure"));
            }
            let exited = exited.clone();
            thread::Builder::new().spawn(move || {
                job();
                exited.send(()).unwrap();
            })
        });
        assert!(matches!(result, Err(PoolError::WorkerStartup(_))));
        observed.recv_timeout(Duration::from_secs(2)).unwrap();
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
            self.release.recv_timeout(Duration::from_secs(3)).unwrap();
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
                self.release.recv_timeout(Duration::from_secs(3)).unwrap();
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

        // Shutdown retires even healthy idle registrations.
        assert_eq!(pool.registered_count(), 0);
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
            let batch = self.mailbox.take_work(max_events);

            if !batch.is_empty() {
                self.sender.send(batch.to_vec()).unwrap();
            }

            batch.complete();
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
        pool.request_retire(token(2)).unwrap();
        assert!(pool.notify(token(2)).is_err());

        release_tx.send(()).unwrap();

        pool.shutdown();

        assert!(
            rx.try_recv().is_err(),
            "retired processor unexpectedly executed"
        );
    }
}

mod resize;
use resize::{worker_entry,supervise_workers};
