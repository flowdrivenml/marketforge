use super::{operations::*, pipeline::*};
pub use crate::live::stream::request::{InputRole, ProcessingInputSpec};
use crate::live::{
    config::general::RuntimeConfig,
    infrastructure::processing::{mailbox::WorkBatch, *},
    stream::{registry::*, router::*},
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc, oneshot};
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct ProcessorContext {
    pub input: Arc<ProcessorMailbox<ProcessingInput>>,
    pub output: ProcessingOutput,
    pub(crate) retained: Arc<Mutex<RetainedProcessingState>>,
}
/// Readiness is processor-specific and may await a signal produced after controlled
/// upstream startup input. A read-only lifecycle handle is returned after readiness;
/// all production event admission remains owned by Router subscriptions.
pub struct PreparedProcessor {
    pub processor: Box<dyn Processor>,
    pub readiness: BoxFuture<'static, Result<(), String>>,
}
impl PreparedProcessor {
    pub fn ready(processor: impl Processor) -> Self {
        Self {
            processor: Box::new(processor),
            readiness: Box::pin(async { Ok(()) }),
        }
    }
}
pub struct StartProcessor {
    pub stream_id: String,
    pub inputs: Vec<ProcessingInputSpec>,
    /// Typed production output or explicitly labelled canonical compatibility.
    pub output_semantics: PublicationSemantics,
    pub input_limits: MailboxLimits,
    pub output_limits: MailboxLimits,
    pub factory: Box<dyn FnOnce(ProcessorContext) -> Result<PreparedProcessor, String> + Send>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingPhase {
    Starting,
    Ready,
    Retiring,
    Terminal,
}
#[derive(Debug, Clone)]
pub struct AssociationStatus {
    pub stream_id: String,
    pub generation: RuntimeGeneration,
    pub processor: Option<ProcessorToken>,
    pub phase: ProcessingPhase,
    pub input: MailboxStatus,
    pub output: MailboxStatus,
    pub router_accepted_outputs: u64,
    pub router_busy_retries: u64,
    pub reason: Option<String>,
    pub inputs: Vec<ProcessingInputStatus>,
    pub derived: DerivedStatus,
}
#[derive(Debug, Clone)]
pub struct ProcessingInputStatus {
    pub spec: ProcessingInputSpec,
    pub generation: Option<RuntimeGeneration>,
    pub ready: bool,
    pub last_delivery_at: Option<Instant>,
}
#[derive(Debug, Clone)]
pub struct ProcessingStatus {
    pub workers: PoolStats,
    pub associations: Vec<AssociationStatus>,
    pub accounted_events: usize,
    pub accounted_bytes: usize,
    pub buffering: crate::live::infrastructure::processing::mailbox::BufferStatus,
    pub pending_starts: usize,
    pub pending_cleanup: usize,
    pub terminal_reports: usize,
    pub shutting_down: bool,
    pub abnormal_owner_drop: bool,
}
/// Lifecycle identity only. All market-data admission belongs to Router.
/// ```compile_fail
/// use marketforge_engine::live::stream::processing::ProcessingStreamHandle;
/// fn inject(stream:ProcessingStreamHandle) { let _ = stream.ingress; }
/// ```
#[derive(Clone)]
pub struct ProcessingStreamHandle {
    pub stream_id: String,
    pub generation: RuntimeGeneration,
    pub processor: ProcessorToken,
    admission_fence: Arc<Mutex<bool>>,
}
impl ProcessingStreamHandle {
    pub fn admission_closed(&self) -> bool {
        *lock(&self.admission_fence)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalDisposition {
    Stopped,
    Failed,
    Superseded,
    Abandoned,
}
/// Explicit terminal accounting for a delivery rejected by managed input.
#[derive(Debug, Clone)]
pub struct RejectedRouterInput {
    pub stream_id: String,
    pub sequence: Option<u64>,
    pub generation: Option<RuntimeGeneration>,
    pub accounted_bytes: usize,
    /// The adapter deliberately discarded this complete delivery after rejection;
    /// continuity is terminal, so execution never continues with a gap.
    pub discarded_complete_delivery: bool,
}
/// Complete queued inputs and outputs remain charged while this report is retained.
/// Input in a failed/dropped batch is indeterminate and is never replayed.
pub struct ProcessingReport {
    pub stream_id: String,
    pub generation: RuntimeGeneration,
    pub processor: Option<ProcessorToken>,
    pub disposition: TerminalDisposition,
    pub reason: Option<String>,
    pub failure: Option<ProcessorFailure>,
    pub input_accounting: MailboxStatus,
    pub output_accounting: MailboxStatus,
    pub unprocessed: WorkBatch<ProcessingInput>,
    pub unpublished: WorkBatch<RouterPublication>,
    pub processor_retained: Vec<RetainedProcessorInput>,
    pub inputs: Vec<ProcessingInputStatus>,
    pub router_accepted_outputs: u64,
    pub rejected_router_input: Option<RejectedRouterInput>,
}
impl ProcessingReport {
    /// Explicitly discard retained canceled work. Counts remain in the report snapshots.
    pub fn discard(self) {
        self.unprocessed.abandon();
        self.unpublished.abandon();
        for retained in self.processor_retained {
            retained.batch.abandon();
        }
    }
}
struct View {
    generation: RuntimeGeneration,
    operation: RuntimeGeneration,
    token: Option<ProcessorToken>,
    phase: ProcessingPhase,
    input: Arc<ProcessorMailbox<ProcessingInput>>,
    output: ProcessingOutput,
    accepted: u64,
    busy_retries: u64,
    reason: Option<String>,
    inputs: Vec<ProcessingInputStatus>,
    retained: Arc<Mutex<RetainedProcessingState>>,
}
struct Observed {
    views: BTreeMap<String, View>,
    reports: BTreeMap<String, ProcessingReport>,
    shutting_down: bool,
    abnormal: bool,
}
struct Shared {
    observed: Mutex<Observed>,
    wake: Arc<Notify>,
    changed: Notify,
    abandoned: AtomicBool,
    pool: ProcessingHandle,
    graph: Arc<StreamGraph>,
    config_derived: crate::live::config::general::DerivedProcessingConfig,
}
#[derive(Clone)]
pub struct ProcessingServiceHandle {
    commands: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}
pub struct ProcessingService {
    handle: ProcessingServiceHandle,
    task: Option<tokio::task::JoinHandle<()>>,
    timeout: Duration,
}
enum Command {
    Start(
        StartProcessor,
        oneshot::Sender<Result<ProcessingStreamHandle, String>>,
        Arc<AtomicBool>,
    ),
    Stop(
        String,
        RuntimeGeneration,
        oneshot::Sender<Result<(), String>>,
    ),
    Shutdown(oneshot::Sender<ShutdownOutcome>),
}
struct Association {
    generation: RuntimeGeneration,
    inputs: Vec<InputBinding>,
    last_inputs: Vec<ProcessingInputStatus>,
    retained: Arc<Mutex<RetainedProcessingState>>,
    retry_at: Option<Instant>,
    deadline_scheduled: Option<Instant>,
    startup_deadline: Instant,
    admission_counter: Arc<Mutex<u64>>,
    readiness_complete: bool,
    operation: RuntimeGeneration,
    canceled: Arc<AtomicBool>,
    instance: Option<ProcessorInstance<ProcessingInput>>,
    input: Arc<ProcessorMailbox<ProcessingInput>>,
    output: ProcessingOutput,

    producer: Option<ProducerHandle>,
    phase: ProcessingPhase,
    factory_pending: bool,
    readiness_pending: bool,
    start_reply: Option<oneshot::Sender<Result<ProcessingStreamHandle, String>>>,
    stop_reply: Option<oneshot::Sender<Result<(), String>>>,
    disposition: TerminalDisposition,
    reason: Option<String>,
    accepted: u64,
    busy_retries: u64,
    rejected_router_input: Option<RejectedRouterInput>,
}
struct InputBinding {
    spec: ProcessingInputSpec,
    generation: Option<RuntimeGeneration>,
    subscription: Arc<tokio::sync::Mutex<SubscriptionHandle>>,
    observer: SubscriptionObserver,
    last_delivery_at: Option<Instant>,
}
enum Completion {
    Factory(
        String,
        RuntimeGeneration,
        Result<
            (
                ProcessorInstance<ProcessingInput>,
                BoxFuture<'static, Result<(), String>>,
            ),
            String,
        >,
    ),
    Ready(String, RuntimeGeneration, Result<(), String>),
}
impl ProcessingService {
    pub fn new(
        config: &RuntimeConfig,
        registry: Arc<Mutex<StreamRuntimeRegistry>>,
        router: Arc<StreamRouter>,
    ) -> Result<Self, String> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err("processing service requires Tokio".into());
        }
        if config.shutdown_timeout_seconds == 0 {
            return Err("shutdown timeout must be positive".into());
        }
        let config_processing = config
            .processing
            .ok_or("processing configuration is required")?;
        config_processing.derived.validate()?;
        let pool = ProcessingPool::new(config_processing).map_err(|e| e.to_string())?;
        let shared = Arc::new(Shared {
            observed: Mutex::new(Observed {
                views: BTreeMap::new(),
                reports: BTreeMap::new(),
                shutting_down: false,
                abnormal: false,
            }),
            wake: Arc::new(Notify::new()),
            changed: Notify::new(),
            abandoned: AtomicBool::new(false),
            pool: pool.handle(),
            graph: router.graph(),
            config_derived: config_processing.derived,
        });
        let (commands, receiver) = mpsc::channel(config_processing.ready_queue_capacity);
        let timeout = Duration::from_secs(config.shutdown_timeout_seconds);
        let task = tokio::spawn(drive(
            pool,
            shared.clone(),
            receiver,
            registry,
            router,
            config_processing.ready_queue_capacity,
            timeout,
            config_processing.derived,
        ));
        Ok(Self {
            handle: ProcessingServiceHandle { commands, shared },
            task: Some(task),
            timeout,
        })
    }
    pub fn handle(&self) -> ProcessingServiceHandle {
        self.handle.clone()
    }
    async fn finish_owner(&mut self) -> Result<ShutdownOutcome, String> {
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| format!("processing service task failed: {error}"))?;
        }
        let stats = self.handle.shared.pool.stats();
        Ok(ShutdownOutcome {
            complete: stats.joined,
            unfinished_workers: stats.started_workers.saturating_sub(stats.exited_workers),
            unfinished_processors: stats.registered,
            unexpected_worker_exits: stats.unexpected_worker_exits,
        })
    }
    pub async fn shutdown(&mut self) -> Result<ShutdownOutcome, String> {
        if self.task.as_ref().is_none_or(|task| task.is_finished()) {
            return self.finish_owner().await;
        }
        let (tx, rx) = oneshot::channel();
        if self
            .handle
            .commands
            .send(Command::Shutdown(tx))
            .await
            .is_err()
        {
            return self.finish_owner().await;
        }
        // A prior timed-out shutdown may finish concurrently with this retry.
        // If it closes the command queue before reading this command, the joined
        // owner remains the authoritative result rather than the reply channel.
        let result = match rx.await {
            Ok(result) => result,
            Err(_) => return self.finish_owner().await,
        };
        if result.complete {
            return self.finish_owner().await;
        }
        Ok(result)
    }
    pub fn shutdown_timeout(&self) -> Duration {
        self.timeout
    }
}
impl Drop for ProcessingService {
    fn drop(&mut self) {
        if self.task.is_some() {
            self.handle.shared.abandoned.store(true, Ordering::Release);
            self.handle.shared.wake.notify_one();
        }
    }
}
struct StartCancellation {
    canceled: Arc<AtomicBool>,
    wake: Arc<Notify>,
    armed: bool,
}
impl Drop for StartCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.canceled.store(true, Ordering::Release);
            self.wake.notify_one();
        }
    }
}
impl ProcessingServiceHandle {
    pub async fn start_derived(&self, id: &str) -> Result<ProcessingStreamHandle, String> {
        let graph = self.shared.graph.clone();
        let id = id.to_owned();
        let config = self.shared.config_derived;
        let request = tokio::task::spawn_blocking(move || resolve_start(&graph, &id, config))
            .await
            .map_err(|e| format!("derived planning failed: {e}"))??;
        self.start(request).await
    }
    pub async fn start(&self, request: StartProcessor) -> Result<ProcessingStreamHandle, String> {
        let canceled = Arc::new(AtomicBool::new(false));
        let mut guard = StartCancellation {
            canceled: canceled.clone(),
            wake: self.shared.wake.clone(),
            armed: true,
        };
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Start(request, tx, canceled))
            .await
            .map_err(|_| "processing service exited")?;
        let result = rx
            .await
            .map_err(|_| "processing startup abandoned by service")?;
        guard.armed = false;
        result
    }
    /// Acceptance requests teardown; wait_terminal observes completion independently.
    pub async fn stop(&self, stream: &ProcessingStreamHandle) -> Result<(), String> {
        self.stop_generation(&stream.stream_id, stream.generation)
            .await
    }
    pub async fn stop_generation(
        &self,
        id: &str,
        generation: RuntimeGeneration,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Stop(id.into(), generation, tx))
            .await
            .map_err(|_| "processing service exited")?;
        rx.await
            .map_err(|_| "processing service exited during stop")?
    }
    pub fn status(&self) -> ProcessingStatus {
        let observed = lock(&self.shared.observed);
        let used = self.shared.pool.budget().used();
        let associations = observed
            .views
            .iter()
            .map(|(id, v)| AssociationStatus {
                stream_id: id.clone(),
                generation: v.generation,
                processor: v.token,
                phase: v.phase,
                input: v.input.status(),
                output: v.output.status(),
                router_accepted_outputs: v.accepted,
                router_busy_retries: v.busy_retries,
                reason: v.reason.clone(),
                inputs: v.inputs.clone(),
                derived: lock(&v.retained).status.clone(),
            })
            .collect::<Vec<_>>();
        ProcessingStatus {
            buffering: self.shared.pool.budget().status(),
            workers: self.shared.pool.stats(),
            pending_starts: associations
                .iter()
                .filter(|v| v.phase == ProcessingPhase::Starting)
                .count(),
            pending_cleanup: associations
                .iter()
                .filter(|v| v.phase == ProcessingPhase::Retiring)
                .count(),
            terminal_reports: observed.reports.len(),
            associations,
            accounted_events: used.0,
            accounted_bytes: used.1,
            shutting_down: observed.shutting_down,
            abnormal_owner_drop: observed.abnormal,
        }
    }
    pub async fn wait_terminal(
        &self,
        id: &str,
        generation: RuntimeGeneration,
    ) -> Result<AssociationStatus, String> {
        self.wait_phase(id, generation, ProcessingPhase::Terminal)
            .await
    }
    pub async fn wait_phase(
        &self,
        id: &str,
        generation: RuntimeGeneration,
        phase: ProcessingPhase,
    ) -> Result<AssociationStatus, String> {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let status = self.status();
            if let Some(value) = status.associations.into_iter().find(|v| v.stream_id == id) {
                if value.generation != generation {
                    return Err("generation superseded".into());
                }
                if value.phase == phase {
                    return Ok(value);
                }
            } else {
                return Err("association absent".into());
            }
            changed.await;
        }
    }
    /// Taking a terminal report frees its association slot. Retained batches remain
    /// accounted until the recipient explicitly discards/processes or drops them.
    pub fn take_report(&self, id: &str, generation: RuntimeGeneration) -> Option<ProcessingReport> {
        let mut observed = lock(&self.shared.observed);
        if !observed
            .reports
            .get(id)
            .is_some_and(|r| r.generation == generation)
        {
            return None;
        }
        let report = observed.reports.remove(id);
        let view = observed.views.remove(id);
        drop(observed);
        drop(view);
        self.shared.wake.notify_one();
        report
    }
}
fn input_statuses(a: &Association) -> Vec<ProcessingInputStatus> {
    if a.inputs.is_empty() {
        return a.last_inputs.clone();
    }
    a.inputs
        .iter()
        .map(|b| ProcessingInputStatus {
            spec: b.spec.clone(),
            generation: b.generation,
            ready: b.observer.ready_for_processing(),
            last_delivery_at: b.last_delivery_at,
        })
        .collect()
}
fn record(shared: &Shared, id: &str, a: &Association) {
    let mut observed = lock(&shared.observed);
    let token = a
        .instance
        .as_ref()
        .map(ProcessorInstance::token)
        .or_else(|| observed.views.get(id).and_then(|view| view.token));
    observed.views.insert(
        id.into(),
        View {
            generation: a.generation,
            operation: a.operation,
            token,
            phase: a.phase,
            input: a.input.clone(),
            output: a.output.clone(),
            accepted: a.accepted,
            busy_retries: a.busy_retries,
            reason: a.reason.clone(),
            retained: a.retained.clone(),
            inputs: input_statuses(a),
        },
    );
    drop(observed);
    shared.changed.notify_waiters();
}
fn bounded_reason(reason: String) -> String {
    reason.chars().take(4096).collect()
}
fn fence(
    registry: &Mutex<StreamRuntimeRegistry>,
    a: &mut Association,
    id: &str,
    disposition: TerminalDisposition,
    reason: Option<String>,
) {
    let reason = reason.map(bounded_reason);
    // Registry revocation precedes input fencing and potentially slow destruction.
    let mut runtime = lock(registry);
    if disposition == TerminalDisposition::Stopped {
        if runtime
            .status(id)
            .is_some_and(|status| status.generation == a.generation)
        {
            match runtime.begin_stop(id) {
                Ok(g) => a.operation = g,
                Err(e) => {
                    a.reason = Some(e.to_string());
                    a.disposition = TerminalDisposition::Failed;
                }
            }
        } else {
            a.disposition = TerminalDisposition::Superseded;
        }
    } else {
        let _ = runtime.mark_failed(
            id,
            a.operation,
            reason.clone().unwrap_or_else(|| "processing failed".into()),
        );
        a.disposition = disposition;
    }
    drop(runtime);
    a.reason = reason.or_else(|| a.reason.take());
    a.output.fence();
    a.input.close();
    a.last_inputs = input_statuses(a);
    a.inputs.clear();
    a.producer.take();
    a.phase = ProcessingPhase::Retiring;
    if let Some(instance) = &a.instance {
        instance.request_stop();
    }
}
fn confirm_ready(
    registry: &Mutex<StreamRuntimeRegistry>,
    shared: &Shared,
    id: &str,
    a: &mut Association,
) {
    if a.phase != ProcessingPhase::Starting
        || !a.readiness_complete
        || !a.inputs.iter().all(|b| b.observer.ready_for_processing())
    {
        return;
    }
    let runtime = lock(registry);
    if !a.inputs.iter().all(|b| {
        runtime.status(&b.spec.stream_id).is_some_and(|s| {
            s.lifecycle == StreamLifecycleState::Running
                && b.generation.is_none_or(|g| g == s.generation)
        })
    }) {
        return;
    }
    for b in &mut a.inputs {
        if b.generation.is_none() {
            b.generation = runtime.status(&b.spec.stream_id).map(|s| s.generation);
        }
    }
    let expected = a
        .inputs
        .iter()
        .map(|b| (b.spec.stream_id.clone(), b.generation.unwrap()))
        .collect::<Vec<_>>();
    drop(runtime);
    if a.retry_at.is_some_and(|d| Instant::now() < d) {
        return;
    }
    a.retry_at = None;
    match a.producer.as_ref().unwrap().freeze_dependencies(&expected) {
        Ok(()) => {}
        Err(PublishError::Busy) => {
            a.retry_at =
                Some(Instant::now() + Duration::from_millis(shared.config_derived.retry_delay_ms));
            return;
        }
        Err(error) => {
            fence(
                registry,
                a,
                id,
                TerminalDisposition::Failed,
                Some(format!("dependency binding lost before readiness: {error}")),
            );
            return;
        }
    }
    let mut runtime = lock(registry);
    if !a.inputs.iter().all(|b| {
        runtime.status(&b.spec.stream_id).is_some_and(|s| {
            s.lifecycle == StreamLifecycleState::Running && Some(s.generation) == b.generation
        })
    }) {
        drop(runtime);
        fence(
            registry,
            a,
            id,
            TerminalDisposition::Failed,
            Some("dependency changed during readiness confirmation".into()),
        );
        return;
    }
    let gate = a.input.publication_fence();
    let guard = lock(&gate);
    if *guard {
        drop(guard);
        drop(runtime);
        fence(
            registry,
            a,
            id,
            TerminalDisposition::Failed,
            Some("processor admission closed before readiness confirmation".into()),
        );
        return;
    }
    let running = runtime.mark_running(id, a.generation);
    drop(guard);
    drop(runtime);
    if matches!(running, Ok(true)) {
        a.phase = ProcessingPhase::Ready;
        let instance = a.instance.as_ref().unwrap();
        let handle = ProcessingStreamHandle {
            stream_id: id.into(),
            generation: a.generation,
            processor: instance.token(),
            admission_fence: a.input.publication_fence(),
        };
        if a.start_reply.take().unwrap().send(Ok(handle)).is_err() {
            fence(
                registry,
                a,
                id,
                TerminalDisposition::Abandoned,
                Some("startup caller canceled".into()),
            );
        }
    } else {
        fence(
            registry,
            a,
            id,
            TerminalDisposition::Superseded,
            Some("startup generation superseded".into()),
        );
    }
    record(shared, id, a);
}
struct ServiceTaskGuard {
    shared: Arc<Shared>,
    registry: Arc<Mutex<StreamRuntimeRegistry>>,
}
impl Drop for ServiceTaskGuard {
    fn drop(&mut self) {
        let resources = {
            let mut observed = lock(&self.shared.observed);
            let unfinished = observed
                .views
                .values()
                .any(|v| v.phase != ProcessingPhase::Terminal);
            if !unfinished {
                return;
            }
            observed.abnormal = true;
            observed.shutting_down = true;
            observed
                .views
                .iter_mut()
                .filter(|(_, v)| v.phase != ProcessingPhase::Terminal)
                .map(|(id, v)| {
                    v.phase = ProcessingPhase::Retiring;
                    v.reason =
                        Some("processing service task destroyed before cleanup completed".into());
                    (id.clone(), v.operation, v.input.clone(), v.output.clone())
                })
                .collect::<Vec<_>>()
        };
        for (id, generation, input, output) in resources {
            let _ = lock(&self.registry).mark_failed(
                &id,
                generation,
                "processing service task destroyed before cleanup completed",
            );
            output.fence();
            input.close();
        }
        self.shared.changed.notify_waiters();
    }
}
async fn drive(
    mut owner: ProcessingPool,
    shared: Arc<Shared>,
    mut commands: mpsc::Receiver<Command>,
    registry: Arc<Mutex<StreamRuntimeRegistry>>,
    router: Arc<StreamRouter>,
    capacity: usize,
    timeout: Duration,
    config_derived: crate::live::config::general::DerivedProcessingConfig,
) {
    let _task_guard = ServiceTaskGuard {
        shared: shared.clone(),
        registry: registry.clone(),
    };
    let mut associations: BTreeMap<String, Association> = BTreeMap::new();
    let mut pending: FuturesUnordered<BoxFuture<'static, Completion>> = FuturesUnordered::new();
    let mut pool_updates = shared.pool.subscribe_changes();
    let mut registry_updates = lock(&registry).subscribe_changes();
    let mut commands_open = true;
    let mut next_id = 0_u64;
    let mut poll_cursor = 0usize;
    let mut shutdown = false;
    let mut shutdown_waiter: Option<(Instant, oneshot::Sender<ShutdownOutcome>)> = None;
    loop {
        // Register wakeups before inspecting retained state; terminal reports are
        // authoritative, so coalescing notifications cannot erase a failure.
        let pool_changed = pool_updates.changed();
        tokio::pin!(pool_changed);
        // changed() uses notify_one permits: an event before polling is retained.
        let wake = shared.wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let abandoned = shared.abandoned.load(Ordering::Acquire);
        if abandoned && !lock(&shared.observed).abnormal {
            shutdown = true;
            let mut view = lock(&shared.observed);
            view.shutting_down = true;
            view.abnormal = true;
            drop(view);
            for (id, a) in &mut associations {
                if a.disposition == TerminalDisposition::Failed {
                    continue;
                }
                fence(
                    &registry,
                    a,
                    id,
                    TerminalDisposition::Abandoned,
                    Some("processing service owner dropped unexpectedly".into()),
                );
            }
            shared.pool.begin_shutdown();
        }
        if shared.pool.stats().unexpected_worker_exits > 0 {
            shutdown = true;
            lock(&shared.observed).shutting_down = true;
            for (id, a) in &mut associations {
                if a.phase != ProcessingPhase::Retiring {
                    fence(
                        &registry,
                        a,
                        id,
                        TerminalDisposition::Failed,
                        Some("unexpected CPU worker exit".into()),
                    );
                }
            }
        }
        let mut terminal = Vec::new();
        for (id, a) in &mut associations {
            if a.phase == ProcessingPhase::Starting && Instant::now() >= a.startup_deadline {
                fence(
                    &registry,
                    a,
                    id,
                    TerminalDisposition::Failed,
                    Some("required input readiness deadline expired".into()),
                );
            }
            if a.phase != ProcessingPhase::Retiring {
                let lost = {
                    let runtime = lock(&registry);
                    a.inputs.iter().find_map(|b|runtime.status(&b.spec.stream_id).filter(|s|b.generation.is_some_and(|g|s.generation!=g) || (b.generation.is_some() && matches!(s.lifecycle,StreamLifecycleState::Failed|StreamLifecycleState::Stopping|StreamLifecycleState::Stopped)) || (a.phase==ProcessingPhase::Ready && s.lifecycle!=StreamLifecycleState::Running)).map(|s|format!("required input {} lost authority: {:?}, generation {}, reason {:?}",b.spec.stream_id,s.lifecycle,s.generation.get(),s.last_error)))
                };
                if let Some(reason) = lost {
                    fence(&registry, a, id, TerminalDisposition::Failed, Some(reason));
                }
            }
            if a.phase != ProcessingPhase::Retiring {
                if a.canceled.load(Ordering::Acquire) {
                    fence(
                        &registry,
                        a,
                        id,
                        TerminalDisposition::Abandoned,
                        Some("startup caller canceled".into()),
                    );
                } else if let Some(failure) =
                    a.instance.as_ref().and_then(ProcessorInstance::failure)
                {
                    let detail = a
                        .instance
                        .as_ref()
                        .and_then(ProcessorInstance::failure_detail)
                        .unwrap_or_else(|| format!("{failure:?}"));
                    fence(&registry, a, id, TerminalDisposition::Failed, Some(detail));
                } else if let Some(reason) = a.output.failure_reason() {
                    fence(
                        &registry,
                        a,
                        id,
                        TerminalDisposition::Failed,
                        Some(reason.into()),
                    );
                } else if !lock(&registry)
                    .status(id)
                    .is_some_and(|status| status.generation == a.generation)
                {
                    fence(
                        &registry,
                        a,
                        id,
                        TerminalDisposition::Superseded,
                        Some("stream generation superseded".into()),
                    );
                }
            }
            confirm_ready(&registry, &shared, id, a);
            let worker_deadline = lock(&a.retained).deadline;
            if a.phase != ProcessingPhase::Retiring
                && worker_deadline != a.deadline_scheduled
                && worker_deadline.is_some_and(|d| Instant::now() >= d)
            {
                a.deadline_scheduled = worker_deadline;
                if let Some(instance) = &a.instance {
                    let _ = shared.pool.notify(instance.token());
                }
            }
            if a.phase == ProcessingPhase::Ready && a.retry_at.is_none_or(|d| Instant::now() >= d) {
                a.retry_at = None;
                // One cooperative output turn per association, preserving fairness.
                for _ in 0..64 {
                    match a.output.publish_one(a.producer.as_ref().unwrap()) {
                        Ok(true) => a.accepted += 1,
                        Ok(false) => break,
                        Err(PublishError::Busy) => {
                            a.busy_retries = a.busy_retries.saturating_add(1);
                            a.retry_at = Some(
                                Instant::now()
                                    + Duration::from_millis(config_derived.retry_delay_ms),
                            );
                            break;
                        }
                        Err(error) => {
                            fence(
                                &registry,
                                a,
                                id,
                                TerminalDisposition::Failed,
                                Some(format!(
                                    "Router rejected retained processor output: {error}"
                                )),
                            );
                            break;
                        }
                    }
                }
                if !a.output.mailbox.is_empty()
                    && a.phase == ProcessingPhase::Ready
                    && a.retry_at.is_none()
                {
                    shared.wake.notify_one();
                }
            }
            if a.phase == ProcessingPhase::Retiring
                && !a.factory_pending
                && !a.readiness_pending
                && a.instance
                    .as_ref()
                    .is_none_or(ProcessorInstance::is_settled)
            {
                // An executing turn may fail after stop was requested.
                let failure = a.instance.as_ref().and_then(ProcessorInstance::failure);
                if failure.is_some() {
                    a.disposition = TerminalDisposition::Failed;
                    let detail = a
                        .instance
                        .as_ref()
                        .and_then(ProcessorInstance::failure_detail)
                        .unwrap_or_else(|| {
                            format!("processor failed during retirement: {failure:?}")
                        });
                    a.reason = Some(bounded_reason(match a.reason.take() {
                        Some(previous) if previous != detail => {
                            format!("{previous}; processor retirement: {detail}")
                        }
                        _ => detail,
                    }));
                }
                // A concurrent stop must not hide continuity failure when a
                // processor ignored its output rejection and returned success.
                if let Some(reason) = a.output.failure_reason() {
                    a.disposition = TerminalDisposition::Failed;
                    a.reason = Some(reason.into());
                }
                let applied = if a.disposition == TerminalDisposition::Stopped {
                    lock(&registry).mark_stopped(id, a.operation)
                } else {
                    lock(&registry).mark_failed(
                        id,
                        a.operation,
                        a.reason
                            .clone()
                            .unwrap_or_else(|| "processing failed".into()),
                    )
                };
                if matches!(applied, Ok(false)) {
                    a.disposition = TerminalDisposition::Superseded;
                }
                if let Err(error) = applied {
                    a.disposition = TerminalDisposition::Failed;
                    a.reason = Some(error.to_string());
                }
                let input_accounting = a.input.status();
                let output_accounting = a.output.status();
                let unprocessed = a.input.take_work(usize::MAX).canceled();
                let unpublished = a.output.retained().canceled();
                let report = ProcessingReport {
                    stream_id: id.clone(),
                    generation: a.generation,
                    processor: a.instance.as_ref().map(ProcessorInstance::token),
                    disposition: a.disposition,
                    reason: a.reason.clone(),
                    failure,
                    input_accounting,
                    output_accounting,
                    unprocessed,
                    unpublished,
                    processor_retained: lock(&a.retained).terminal(),
                    inputs: input_statuses(a),
                    router_accepted_outputs: a.accepted,
                    rejected_router_input: a.rejected_router_input.clone(),
                };
                if let Some(instance) = a.instance.take() {
                    shared.pool.acknowledge_failure(instance.token());
                    drop(instance);
                }
                a.phase = ProcessingPhase::Terminal;
                lock(&shared.observed).reports.insert(id.clone(), report);
                record(&shared, id, a);
                if let Some(reply) = a.start_reply.take() {
                    let _ = reply.send(Err(a
                        .reason
                        .clone()
                        .unwrap_or_else(|| "startup canceled".into())));
                }
                if let Some(reply) = a.stop_reply.take() {
                    let _ = reply.send(if a.disposition == TerminalDisposition::Stopped {
                        Ok(())
                    } else {
                        Err(a
                            .reason
                            .clone()
                            .unwrap_or_else(|| "processor did not stop cleanly".into()))
                    });
                }
                terminal.push(id.clone());
                shared.changed.notify_waiters();
            } else {
                record(&shared, id, a);
            }
        }
        for id in terminal {
            associations.remove(&id);
        }
        if shutdown && associations.is_empty() && pending.is_empty() {
            // Joining occurs on Tokio's bounded blocking executor, never a Tokio worker.
            let deadline = shutdown_waiter
                .as_ref()
                .map(|(d, _)| *d)
                .unwrap_or_else(|| Instant::now() + timeout);
            let joined = tokio::task::spawn_blocking(move || {
                let result = owner.shutdown_until(deadline);
                (owner, result)
            })
            .await;
            match joined {
                Ok((returned, result)) => {
                    owner = returned;
                    if let Some((_, reply)) = shutdown_waiter.take() {
                        let _ = reply.send(result);
                    }
                    if result.complete {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let mut deliveries: FuturesUnordered<
            BoxFuture<
                '_,
                (
                    String,
                    RuntimeGeneration,
                    usize,
                    usize,
                    Option<RouterDelivery>,
                ),
            >,
        > = FuturesUnordered::new();
        let bindings = associations
            .iter()
            .flat_map(|(id, a)| a.inputs.iter().map(move |b| (id, a, b)))
            .collect::<Vec<_>>();
        for offset in 0..bindings.len() {
            let poll_index = (poll_cursor + offset) % bindings.len();
            let (id, a, b) = bindings[poll_index];
            let id = id.clone();
            let generation = a.generation;
            let position = b.spec.position;
            if a.instance.is_none() {
                let observer = b.observer.clone();
                deliveries.push(Box::pin(async move {
                    let state = observer.terminated().await;
                    let reason = match state {
                        SubscriptionState::Failed(reason) => reason,
                        _ => ContinuityFailure::ProducerRevoked,
                    };
                    (
                        id,
                        generation,
                        position,
                        poll_index,
                        Some(RouterDelivery::ContinuityLost(reason)),
                    )
                }));
            } else {
                let subscription = b.subscription.clone();
                deliveries.push(Box::pin(async move {
                    (
                        id,
                        generation,
                        position,
                        poll_index,
                        subscription.lock().await.recv().await,
                    )
                }));
            }
        }
        let deadline = shutdown_waiter
            .as_ref()
            .map(|(d, _)| *d)
            .into_iter()
            .chain(
                associations
                    .values()
                    .filter(|a| a.phase != ProcessingPhase::Retiring)
                    .flat_map(|a| {
                        [
                            a.retry_at,
                            lock(&a.retained)
                                .deadline
                                .filter(|d| Some(*d) != a.deadline_scheduled),
                            (a.phase == ProcessingPhase::Starting).then_some(a.startup_deadline),
                        ]
                        .into_iter()
                        .flatten()
                    }),
            )
            .min();
        tokio::select! {
            _=&mut pool_changed=>{}, _=&mut wake=>{}, _=registry_updates.changed()=>{},
            _=async {if let Some(deadline)=deadline {tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;} else {std::future::pending::<()>().await;}}=> {
                if shutdown_waiter.as_ref().is_some_and(|(d,_)|Instant::now()>=*d) {let (_,reply)=shutdown_waiter.take().unwrap();let stats=shared.pool.stats();let _=reply.send(ShutdownOutcome{complete:false,unfinished_workers:stats.started_workers.saturating_sub(stats.exited_workers),unfinished_processors:associations.len(),unexpected_worker_exits:stats.unexpected_worker_exits});}
            }
            Some(completion)=pending.next(), if !pending.is_empty()=> {
                match completion {
                    Completion::Factory(id,generation,result)=> {
                        if let Some(a)=associations.get_mut(&id).filter(|a|a.generation==generation) {
                            a.factory_pending=false;
                            match result {
                                Ok((instance,readiness))=> {a.instance=Some(instance);if a.phase==ProcessingPhase::Retiring {a.instance.as_ref().unwrap().request_stop();} else {a.readiness_pending=true; let input = a.input.clone(); pending.push(Box::pin(async move {
                                    let result=tokio::select! {
                                        result=AssertUnwindSafe(readiness).catch_unwind()=> result.map_err(|_|"readiness panicked".to_string()).and_then(|r|r),
                                        _=input.closed()=>Err("readiness canceled by closure".into()),
                                    };
                                    Completion::Ready(id,generation,result.map_err(bounded_reason))
                                }));}}
                                Err(reason)=> {if a.phase!=ProcessingPhase::Retiring || reason!="processing runtime is shut down" {fence(&registry,a,&id,TerminalDisposition::Failed,Some(reason));}},
                            }
                        }
                    }
                    Completion::Ready(id,generation,result)=> {
                        if let Some(a)=associations.get_mut(&id).filter(|a|a.generation==generation) {
                            a.readiness_pending=false;
                            if a.phase!=ProcessingPhase::Retiring {
                                match result {
                                    Ok(())=> {a.readiness_complete=true;}
                                    Err(reason)=>fence(&registry,a,&id,TerminalDisposition::Failed,Some(reason)),
                                }
                            }
                        }
                    }
                }
            }
            Some((id,generation,position,poll_index,delivery))=deliveries.next(), if !deliveries.is_empty()=> {
                poll_cursor=poll_index.wrapping_add(1);
                if let Some(a)=associations.get_mut(&id).filter(|a|a.generation==generation && a.phase!=ProcessingPhase::Retiring) {
                    if a.instance.is_none(){fence(&registry,a,&id,TerminalDisposition::Failed,Some("upstream continuity lost during processor construction".into()));continue;}
                    match delivery {
                        Some(delivery)=> {
                            let binding=&mut a.inputs[position];
                            binding.last_delivery_at=Some(Instant::now());
                            if let RouterDelivery::Publication {runtime_generation,..}=&delivery {
                                if binding.generation.is_some_and(|g| g!=*runtime_generation) {
                                    fence(&registry,a,&id,TerminalDisposition::Failed,Some("upstream generation changed".into()));continue;
                                }
                                binding.generation=Some(*runtime_generation);
                            }
                            let continuity=match &delivery{RouterDelivery::ContinuityLost(reason)=>Some(*reason),_=>None};
                            if let Err(rejected)=forward_router_input(&binding.spec,&a.admission_counter,delivery,&a.instance.as_ref().unwrap().ingress(),a.phase==ProcessingPhase::Starting) {
                                a.rejected_router_input=Some(RejectedRouterInput{stream_id:rejected.input.stream_id.clone(),generation:rejected.input.generation(),sequence:match &rejected.input.delivery{RouterDelivery::Publication{sequence,..}=>Some(*sequence),_=>None},accounted_bytes:rejected.input.retained_bytes().unwrap_or(usize::MAX),discarded_complete_delivery:true});
                                fence(&registry,a,&id,TerminalDisposition::Failed,Some(format!("input {} at rank {}: {} ({continuity:?})",rejected.input.stream_id,rejected.input.input_position,rejected.reason)));
                                // The rejected whole event is explicitly abandoned after
                                // fencing, outside every registry/pool/mailbox lock.
                                drop(rejected);
                            }
                        }
                        None=>fence(&registry,a,&id,TerminalDisposition::Failed,Some("upstream continuity ended".into())),
                    }
                }
            }
            command=commands.recv(), if commands_open=> {
                match command {
                    Some(Command::Start(request,reply,canceled))=> {
                        if canceled.load(Ordering::Acquire) {let _=reply.send(Err("startup caller canceled".into()));continue;}
                        if shutdown || shared.pool.stats().shutdown || associations.len()+lock(&shared.observed).reports.len()>=capacity || associations.contains_key(&request.stream_id) || lock(&shared.observed).reports.contains_key(&request.stream_id) {let _=reply.send(Err("processing association capacity exhausted, shutting down, or previous cleanup unacknowledged".into()));continue;}
                        let id=request.stream_id;
                        let expected=match router.processing_inputs(&id) {Ok(v)=>v,Err(e)=>{let _=reply.send(Err(e.to_string()));continue;}};
                        if request.inputs!=expected || expected.len()>config_derived.max_inputs {let _=reply.send(Err("invalid processing dependency bindings or fan-in limit".into()));continue;}

                        let generation=match lock(&registry).begin_start(&id){Ok(g)=>g,Err(e)=>{let _=reply.send(Err(e.to_string()));continue;}};
                        let resources=(|| {
                            let input=Arc::new(ProcessorMailbox::new(request.input_limits).map_err(|e|e.to_string())?);input.manage(shared.pool.budget());
                            let output=ProcessingOutput::new(request.output_limits,shared.pool.budget(),&input,shared.wake.clone()).map_err(|e|e.to_string())?;
                            let authority=lock(&registry).publication_authority(&id).ok_or("startup authority revoked")?;
                            let producer=match request.output_semantics {
                                PublicationSemantics::Derived=>router.attach_processing_producer(authority,&request.inputs),
                                PublicationSemantics::CanonicalCompatibility if request.inputs.len()==1=>router.attach_derived_producer(authority,&request.inputs[0].stream_id),
                                _=>return Err("invalid processing publication semantics".into()),
                            }.map_err(|e|e.to_string())?;
                            Ok::<_,String>((input,output,producer))
                        })();
                        let (input,output,producer)=match resources {Ok(value)=>value,Err(reason)=>{let _=lock(&registry).mark_failed(&id,generation,reason.clone());let _=reply.send(Err(reason));continue;}};
                        let attached=async {
                            let mut bindings=Vec::new();
                            for spec in request.inputs {
                                let subscription=router.subscribe(SubscribeRequest{stream_id:spec.stream_id.clone(),owner:DemandOwner::Direct{consumer_id:"processing-runtime".into(),subscription_id:format!("{id}:{}:{}",generation.get(),spec.position)}}).await.map_err(|e|e.to_string())?;
                                let observer=subscription.observer();
                                let upstream=lock(&registry).status(&spec.stream_id).filter(|s|s.lifecycle.occupies_runtime()).map(|s|s.generation);
                                bindings.push(InputBinding{spec,generation:upstream,subscription:Arc::new(tokio::sync::Mutex::new(subscription)),observer,last_delivery_at:None});
                            }
                            Ok::<_,String>(bindings)
                        }.await;
                        let inputs=match attached {Ok(v)=>v,Err(reason)=>{output.fence();let _=lock(&registry).mark_failed(&id,generation,reason.clone());let _=reply.send(Err(reason));continue;}};
                        let retained=Arc::new(Mutex::new(RetainedProcessingState::default()));
                        let a=Association{generation,inputs,last_inputs:Vec::new(),retained:retained.clone(),retry_at:None,deadline_scheduled:None,startup_deadline:Instant::now()+Duration::from_millis(config_derived.stall_timeout_ms),admission_counter:Arc::new(Mutex::new(0)),readiness_complete:false,operation:generation,canceled,instance:None,input:input.clone(),output:output.clone(),producer:Some(producer),phase:ProcessingPhase::Starting,factory_pending:true,readiness_pending:false,start_reply:Some(reply),stop_reply:None,disposition:TerminalDisposition::Stopped,reason:None,accepted:0,busy_retries:0,rejected_router_input:None};
                        record(&shared,&id,&a);associations.insert(id.clone(),a);
                        next_id=next_id.checked_add(1).expect("processing association identity exhausted");
                        let logical_id=next_id;
                        let pool=shared.pool.clone();let factory=request.factory;
                        pending.push(Box::pin(async move {
                            let result=tokio::task::spawn_blocking(move || {
                                if input.status().closed {return Err("startup canceled before construction".into());}
                                let built=catch_unwind(AssertUnwindSafe(||factory(ProcessorContext{input:input.clone(),output,retained}))).map_err(|_|"processor factory panicked".to_string()).and_then(|result|result.map_err(bounded_reason))?;
                                let instance=ProcessorInstance::from_mailbox(pool, logical_id,input,built.processor).map_err(|e|e.to_string())?;
                                Ok((instance,built.readiness))
                            }).await.map_err(|e|format!("processor factory task failed: {e}")).and_then(|r|r);
                            Completion::Factory(id,generation,result)
                        }));
                    }
                    Some(Command::Stop(id,generation,reply))=> {
                        if let Some(a)=associations.get_mut(&id).filter(|a|a.generation==generation) {
                            if a.phase!=ProcessingPhase::Retiring {fence(&registry,a,&id,TerminalDisposition::Stopped,None);}
                            if a.stop_reply.is_some(){let _=reply.send(Err("stop already pending".into()));} else {a.stop_reply=Some(reply);}
                        } else {let _=reply.send(Err("stale or absent processing generation".into()));}
                    }
                    Some(Command::Shutdown(reply))=> {
                        shutdown=true;lock(&shared.observed).shutting_down=true;
                        if let Some((_,previous))=shutdown_waiter.replace((Instant::now()+timeout,reply)){let stats=shared.pool.stats();let _=previous.send(ShutdownOutcome{complete:false,unfinished_workers:stats.started_workers.saturating_sub(stats.exited_workers),unfinished_processors:associations.len(),unexpected_worker_exits:stats.unexpected_worker_exits});}
                        for(id,a)in &mut associations {if a.phase!=ProcessingPhase::Retiring {fence(&registry,a,id,TerminalDisposition::Stopped,None);}}
                        shared.pool.begin_shutdown();
                    }
                    None=> {commands_open=false;shared.abandoned.store(true,Ordering::Release);}
                }
            }
        }
    }
    shared.changed.notify_waiters();
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use crate::live::{config::ProcessingConfig, stream::persistence::*};
    const WAIT: Duration = Duration::from_secs(5);
    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(WAIT, future)
            .await
            .expect("processing retry deadline")
    }
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }
    struct GatedOutput {
        context: ProcessorContext,
        entered: Option<oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl Processor for GatedOutput {
        fn run_turn(&mut self, max: usize) -> bool {
            let batch = self.context.input.take_work(max);
            if batch.is_empty() {
                batch.complete();
                return false;
            }
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            self.release.recv_timeout(WAIT).unwrap();
            let input = &batch[0];
            let event = DerivedEvent::from_delivery(
                &input.delivery,
                input.input_position,
                input.admission_ordinal,
                0,
            )
            .unwrap();
            assert!(
                self.context
                    .output
                    .try_emit(RouterPublication::Derived(vec![event].into()))
                    .is_ok()
            );
            batch.complete();
            false
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn router_busy_retries_by_timer_without_reexecuting_processor() {
        let mut definitions = Vec::new();
        for id in ["a", "b"] {
            definitions.push(PersistedStreamDefinition{stream_id:id.into(),recording_stream_key:Some(15),definition:StreamDefinition::Source(serde_json::from_value(serde_json::json!({"instrument_id":15,"exchange":"binance","symbol":"BTCUSDT","market_category":"spot","instrument_type":"spot","base_asset":"BTC","quote_asset":"USDT","quantity_type":"base","channel":"trades"})).unwrap()),desired:Default::default(),revision:1});
        }
        definitions.push(PersistedStreamDefinition{stream_id:"m".into(),recording_stream_key:None,definition:StreamDefinition::Derived(serde_json::from_value(serde_json::json!({"name":"m","operation":"merge","inputs":["a","b"],"ordering":"arrival_order"})).unwrap()),desired:Default::default(),revision:1});
        let graph = Arc::new(
            StreamGraph::restore(
                definitions,
                ["a", "b"]
                    .iter()
                    .enumerate()
                    .map(|(i, id)| StreamDependency {
                        derived_stream_id: "m".into(),
                        input_stream_id: id.to_string(),
                        input_position: i as i32,
                    })
                    .collect(),
            )
            .unwrap(),
        );
        let mut runtime = StreamRuntimeRegistry::new(&graph);
        for id in ["a", "b"] {
            let g = runtime.begin_start(id).unwrap();
            runtime.mark_running(id, g).unwrap();
        }
        let router = Arc::new(
            StreamRouter::new(
                RouterConfig::default(),
                RouterContext {
                    graph,
                    demand: Arc::new(Mutex::new(DemandRegistry::new())),
                },
            )
            .unwrap(),
        );
        let producer = router
            .attach_producer(AttachProducerRequest {
                authority: runtime.publication_authority("a").unwrap(),
            })
            .unwrap();
        let _required_producer = router
            .attach_producer(AttachProducerRequest {
                authority: runtime.publication_authority("b").unwrap(),
            })
            .unwrap();
        let registry = Arc::new(Mutex::new(runtime));
        let mut service = ProcessingService::new(
            &RuntimeConfig {
                shutdown_timeout_seconds: 1,
                processing: Some(ProcessingConfig::default()),
            },
            registry.clone(),
            router.clone(),
        )
        .unwrap();
        let handle = service.handle();
        let mut sub = router
            .subscribe(SubscribeRequest {
                stream_id: "m".into(),
                owner: DemandOwner::Manual {
                    session_id: "busy-test".into(),
                },
            })
            .await
            .unwrap();
        let (entered, started) = oneshot::channel();
        let (resume_cpu, cpu_gate) = std::sync::mpsc::channel();
        let mut cpu_release = Release(Some(resume_cpu));
        let stream = bounded(handle.start(StartProcessor {
            output_semantics: PublicationSemantics::Derived,
            stream_id: "m".into(),
            inputs: router.processing_inputs("m").unwrap(),
            input_limits: MailboxLimits {
                max_events: 4,
                max_bytes: 1 << 20,
            },
            output_limits: MailboxLimits {
                max_events: 4,
                max_bytes: 1 << 20,
            },
            factory: Box::new(move |context| {
                Ok(PreparedProcessor::ready(GatedOutput {
                    context,
                    entered: Some(entered),
                    release: cpu_gate,
                }))
            }),
        }))
        .await
        .unwrap();
        let trade=serde_json::from_value(serde_json::json!({"envelope":{"event_timestamp_ns":100,"exchange":"binance","instrument_id":15,"symbol":"BTCUSDT","stream_id":"a"},"sequence":1,"side":"Buy","price":"2","quantity_base":"3"})).unwrap();
        let publication = RouterPublication::trades(vec![trade]);
        bounded(async {
            loop {
                match producer.try_publish(publication.clone()) {
                    Ok(_) => break,
                    Err(PublishError::Busy) => tokio::task::yield_now().await,
                    other => panic!("{other:?}"),
                }
            }
        })
        .await;
        // Establish Router input delivery before forcing output contention;
        // holding authority first could block the dispatcher's revocation check.
        bounded(started).await.unwrap();
        let authority = lock(&registry).publication_authority("m").unwrap();
        let (held, ready) = oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let mut guard = Release(Some(release));
        let thread = std::thread::spawn(move || {
            let _guard = authority.guard();
            let _ = held.send(());
            gate.recv_timeout(WAIT).unwrap();
        });
        bounded(ready).await.unwrap();
        cpu_release.0.take().unwrap().send(()).unwrap();
        bounded(async {
            loop {
                let s = handle.status();
                if s.associations[0].router_busy_retries > 0 && s.workers.active_workers == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let before = handle.status();
        assert_eq!(before.associations[0].output.queued_events, 1);
        assert_eq!(before.associations[0].router_accepted_outputs, 0);
        guard.0.take().unwrap().send(()).unwrap();
        thread.join().unwrap();
        assert!(
            matches!(bounded(sub.recv()).await,Some(RouterDelivery::Publication{publication,..}) if matches!(publication.as_ref(),RouterPublication::Derived(events) if events.len()==1))
        );
        assert_eq!(
            handle.status().workers.completed_turns,
            before.workers.completed_turns
        );
        bounded(handle.stop(&stream)).await.unwrap();
        let report = handle.take_report("m", stream.generation).unwrap();
        assert_eq!(report.router_accepted_outputs, 1);
        report.discard();
        assert_eq!(handle.status().accounted_events, 0);
        assert!(bounded(service.shutdown()).await.unwrap().complete);
        router.shutdown().await.unwrap();
    }
}
