use super::pipeline::*;
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
}
/// Readiness is processor-specific and may await a signal produced after controlled
/// upstream startup input. Public ingress is returned only after this future succeeds.
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
    /// v1 identity-preserving canonical adapter: one declared Source dependency.
    pub input_stream_id: String,
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
    pub reason: Option<String>,
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
#[derive(Clone)]
pub struct ProcessingStreamHandle {
    pub stream_id: String,
    pub generation: RuntimeGeneration,
    pub processor: ProcessorToken,
    pub ingress: ProcessorIngress<ProcessingInput>,
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
    pub router_accepted_outputs: u64,
    pub rejected_router_input: Option<RejectedRouterInput>,
}
impl ProcessingReport {
    /// Explicitly discard retained canceled work. Counts remain in the report snapshots.
    pub fn discard(self) {
        self.unprocessed.abandon();
        self.unpublished.abandon();
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
    reason: Option<String>,
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
    input_stream_id: String,
    operation: RuntimeGeneration,
    canceled: Arc<AtomicBool>,
    instance: Option<ProcessorInstance<ProcessingInput>>,
    input: Arc<ProcessorMailbox<ProcessingInput>>,
    output: ProcessingOutput,
    subscription: Option<Arc<tokio::sync::Mutex<SubscriptionHandle>>>,
    observer: Option<SubscriptionObserver>,
    producer: Option<ProducerHandle>,
    phase: ProcessingPhase,
    factory_pending: bool,
    readiness_pending: bool,
    start_reply: Option<oneshot::Sender<Result<ProcessingStreamHandle, String>>>,
    stop_reply: Option<oneshot::Sender<Result<(), String>>>,
    disposition: TerminalDisposition,
    reason: Option<String>,
    accepted: u64,
    rejected_router_input: Option<RejectedRouterInput>,
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
                reason: v.reason.clone(),
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
            reason: a.reason.clone(),
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
    a.subscription.take();
    a.observer.take();
    a.producer.take();
    a.phase = ProcessingPhase::Retiring;
    if let Some(instance) = &a.instance {
        instance.request_stop();
    }
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
) {
    let _task_guard = ServiceTaskGuard {
        shared: shared.clone(),
        registry: registry.clone(),
    };
    let mut associations: BTreeMap<String, Association> = BTreeMap::new();
    let mut pending: FuturesUnordered<BoxFuture<'static, Completion>> = FuturesUnordered::new();
    let mut pool_updates = shared.pool.subscribe_changes();
    let mut commands_open = true;
    let mut next_id = 0_u64;
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
            if a.phase == ProcessingPhase::Ready {
                // One cooperative output turn per association, preserving fairness.
                for _ in 0..64 {
                    match a.output.publish_one(a.producer.as_ref().unwrap()) {
                        Ok(true) => a.accepted += 1,
                        Ok(false) => break,
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
                if !a.output.mailbox.is_empty() && a.phase == ProcessingPhase::Ready {
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
                    a.reason = a
                        .instance
                        .as_ref()
                        .and_then(ProcessorInstance::failure_detail)
                        .or_else(|| {
                            Some(format!("processor failed during retirement: {failure:?}"))
                        });
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
            BoxFuture<'_, (String, RuntimeGeneration, Option<RouterDelivery>)>,
        > = FuturesUnordered::new();
        for (id, a) in &associations {
            if let Some(subscription) = &a.subscription {
                if a.instance.is_none() {
                    let observer = a.observer.as_ref().unwrap().clone();
                    let id = id.clone();
                    let generation = a.generation;
                    deliveries.push(Box::pin(async move {
                        let state = observer.terminated().await;
                        let reason = match state {
                            SubscriptionState::Failed(reason) => reason,
                            _ => ContinuityFailure::ProducerRevoked,
                        };
                        (id, generation, Some(RouterDelivery::ContinuityLost(reason)))
                    }));
                    continue;
                }
                let subscription = subscription.clone();
                let id = id.clone();
                let generation = a.generation;
                deliveries.push(Box::pin(async move {
                    (id, generation, subscription.lock().await.recv().await)
                }));
            }
        }
        let deadline = shutdown_waiter.as_ref().map(|(d, _)| *d);
        tokio::select! {
            _=&mut pool_changed=>{}, _=&mut wake=>{},
            _=async {if let Some(deadline)=deadline {tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;} else {std::future::pending::<()>().await;}}=> {
                if let Some((_,reply))=shutdown_waiter.take(){let stats=shared.pool.stats();let _=reply.send(ShutdownOutcome{complete:false,unfinished_workers:stats.started_workers.saturating_sub(stats.exited_workers),unfinished_processors:associations.len(),unexpected_worker_exits:stats.unexpected_worker_exits});}
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
                                    Ok(())=> { if !a.observer.as_ref().is_some_and(|observer|observer.ready_for_processing()) {fence(&registry,a,&id,TerminalDisposition::Failed,Some("upstream continuity lost before readiness confirmation".into()));continue;}
                                        // Registry precedes the admission fence whenever both
                                        // are needed. This commit invokes no processor code.
                                        let mut runtime=lock(&registry);
                                        let gate=a.input.publication_fence();
                                        let guard=lock(&gate);
                                        if *guard {drop(guard);drop(runtime);fence(&registry,a,&id,TerminalDisposition::Failed,Some("processor admission closed before readiness confirmation".into()));continue;}
                                        let running=runtime.mark_running(&id,generation);
                                        drop(guard);drop(runtime);if matches!(running,Ok(true)) {
                                        a.phase=ProcessingPhase::Ready;
                                        let instance=a.instance.as_ref().unwrap();
                                        let handle=ProcessingStreamHandle{stream_id:id.clone(),generation,processor:instance.token(),ingress:instance.ingress()};
                                        if a.start_reply.take().unwrap().send(Ok(handle)).is_err(){fence(&registry,a,&id,TerminalDisposition::Abandoned,Some("startup caller canceled".into()));}
                                    } else {fence(&registry,a,&id,TerminalDisposition::Superseded,Some("startup generation superseded".into()));} }
                                    Err(reason)=>fence(&registry,a,&id,TerminalDisposition::Failed,Some(reason)),
                                }
                            }
                        }
                    }
                }
            }
            Some((id,generation,delivery))=deliveries.next(), if !deliveries.is_empty()=> {
                if let Some(a)=associations.get_mut(&id).filter(|a|a.generation==generation && a.phase!=ProcessingPhase::Retiring) {
                    if a.instance.is_none(){fence(&registry,a,&id,TerminalDisposition::Failed,Some("upstream continuity lost during processor construction".into()));continue;}
                    match delivery {
                        Some(delivery)=> {
                            if let Err(rejected)=forward_router_input(a.input_stream_id.clone(),delivery,&a.instance.as_ref().unwrap().ingress()) {
                                a.rejected_router_input=Some(RejectedRouterInput{stream_id:rejected.input.stream_id.clone(),generation:rejected.input.generation(),sequence:match &rejected.input.delivery{RouterDelivery::Publication{sequence,..}=>Some(*sequence),_=>None},accounted_bytes:rejected.input.retained_bytes().unwrap_or(usize::MAX),discarded_complete_delivery:true});
                                fence(&registry,a,&id,TerminalDisposition::Failed,Some(rejected.reason.into()));
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
                        let generation=match lock(&registry).begin_start(&id){Ok(g)=>g,Err(e)=>{let _=reply.send(Err(e.to_string()));continue;}};
                        let resources=(|| {
                            let input=Arc::new(ProcessorMailbox::new(request.input_limits).map_err(|e|e.to_string())?);input.manage(shared.pool.budget());
                            let output=ProcessingOutput::new(request.output_limits,shared.pool.budget(),&input,shared.wake.clone()).map_err(|e|e.to_string())?;
                            let authority=lock(&registry).publication_authority(&id).ok_or("startup authority revoked")?;
                            let producer=router.attach_derived_producer(authority,&request.input_stream_id).map_err(|e|e.to_string())?;
                            Ok::<_,String>((input,output,producer))
                        })();
                        let (input,output,producer)=match resources {Ok(value)=>value,Err(reason)=>{let _=lock(&registry).mark_failed(&id,generation,reason.clone());let _=reply.send(Err(reason));continue;}};
                        let (subscription,observer)=match router.subscribe(SubscribeRequest{stream_id:request.input_stream_id.clone(),owner:DemandOwner::Direct{consumer_id:"processing-runtime".into(),subscription_id:format!("{id}:{}",generation.get())}}).await {Ok(value)=>{let observer=value.observer();(Some(Arc::new(tokio::sync::Mutex::new(value))),Some(observer))},Err(e)=>{let reason=e.to_string();output.fence();let _=lock(&registry).mark_failed(&id,generation,reason.clone());let _=reply.send(Err(reason));continue;}};
                        let a=Association{generation,input_stream_id:request.input_stream_id,operation:generation,canceled,instance:None,input:input.clone(),output:output.clone(),subscription,observer,producer:Some(producer),phase:ProcessingPhase::Starting,factory_pending:true,readiness_pending:false,start_reply:Some(reply),stop_reply:None,disposition:TerminalDisposition::Stopped,reason:None,accepted:0,rejected_router_input:None};
                        record(&shared,&id,&a);associations.insert(id.clone(),a);
                        next_id=next_id.checked_add(1).expect("processing association identity exhausted");
                        let logical_id=next_id;
                        let pool=shared.pool.clone();let factory=request.factory;
                        pending.push(Box::pin(async move {
                            let result=tokio::task::spawn_blocking(move || {
                                if input.status().closed {return Err("startup canceled before construction".into());}
                                let built=catch_unwind(AssertUnwindSafe(||factory(ProcessorContext{input:input.clone(),output}))).map_err(|_|"processor factory panicked".to_string()).and_then(|result|result.map_err(bounded_reason))?;
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
