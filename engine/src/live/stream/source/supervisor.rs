use super::SourceReadiness;
use super::{
    SourceExecutorError, SourceExecutorExit, SourceStreamExecutor, SourceTradePublication,
};
use crate::live::stream::registry::StreamLifecycleState;
use crate::live::{
    integrity::DepthPublicationEvent,
    stream::registry::{RuntimeGeneration, StreamRuntimeError, StreamRuntimeRegistry},
};
use std::collections::BTreeMap;
use thiserror::Error;
use tokio::sync::watch;
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};

#[derive(Debug, Error)]
pub enum SourceSupervisorError {
    #[error(transparent)]
    Runtime(#[from] StreamRuntimeError),

    #[error(transparent)]
    Router(#[from] crate::live::stream::router::RouterError),

    #[error("executor belongs to another stream: {0}")]
    WrongStream(String),

    #[error("stream already has an executor: {0}")]
    AlreadyActive(String),

    #[error("stream has no managed executor: {0}")]
    NotActive(String),

    #[error("source executor task failed: {0}")]
    Join(#[from] JoinError),

    #[error(transparent)]
    Executor(#[from] SourceExecutorError),

    #[error("source stopped unexpectedly: {0}")]
    UnexpectedExit(String),
}

struct ManagedSourceTask {
    generation: RuntimeGeneration,
    shutdown: Option<oneshot::Sender<()>>,
    started_at: std::time::Instant,
    ever_ready: bool,
    task: JoinHandle<Result<SourceExecutorExit, SourceExecutorError>>,
    readiness: watch::Receiver<SourceReadiness>,
}

/// Serial owner of source executor tasks and runtime lifecycle.
///
/// Source mutations use the same shared registry as ProcessingService.
///
/// The registry generation fences late completion and failure
/// notifications from obsolete executor tasks.
///
/// This supervisor does not own physical WebSocket connections.
/// Each executor owns its logical attachment.
pub struct SourceRuntimeSupervisor {
    registry: std::sync::Arc<std::sync::Mutex<StreamRuntimeRegistry>>,
    tasks: BTreeMap<String, ManagedSourceTask>,
    cleanup: Vec<JoinHandle<()>>,
}

impl SourceRuntimeSupervisor {
    pub fn new(registry: StreamRuntimeRegistry) -> Self {
        Self::shared(std::sync::Arc::new(std::sync::Mutex::new(registry)))
    }

    pub fn shared(registry: std::sync::Arc<std::sync::Mutex<StreamRuntimeRegistry>>) -> Self {
        Self { registry, tasks: BTreeMap::new(), cleanup:Vec::new() }
    }

    fn registry_lock(&self) -> std::sync::MutexGuard<'_, StreamRuntimeRegistry> {
        self.registry.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn fail_preparation(&mut self,id:&str,reason:String) {
        if !self.tasks.contains_key(id) {let mut registry=self.registry_lock();if let Ok(g)=registry.begin_start(id){let _=registry.mark_failed(id,g,reason);}}
    }
    pub fn registry(&self)->SourceRegistryRead {SourceRegistryRead(self.registry.clone())}
    /// Deadline changes authority first, then requests ordinary executor cleanup.
    pub fn expire_starting(&mut self,deadline:std::time::Duration){
        let expired=self.tasks.iter().filter(|(_,t)|!t.ever_ready && t.started_at.elapsed()>=deadline).filter_map(|(id,t)|self.registry_lock().status(id).filter(|s|s.generation==t.generation && matches!(s.lifecycle,StreamLifecycleState::Starting|StreamLifecycleState::Recovering|StreamLifecycleState::Degraded)).map(|_|(id.clone(),t.generation))).collect::<Vec<_>>();
        for(id,generation)in expired {let _=self.registry_lock().mark_failed(&id,generation,"Source readiness deadline expired");if let Some(task)=self.tasks.get_mut(&id){if let Some(shutdown)=task.shutdown.take(){let _=shutdown.send(());}}}
    }
    pub fn readiness_statuses(&self)->Vec<SourceReadinessStatus>{self.tasks.iter().map(|(id,task)|SourceReadinessStatus{stream_id:id.clone(),generation:task.generation,readiness:*task.readiness.borrow(),initial_readiness_pending:!task.ever_ready}).collect()}
    pub fn active_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_active(&self, stream_id: &str) -> bool {
        self.tasks.contains_key(stream_id)
    }

    /// Register and spawn one fully constructed source executor.
    ///
    /// The caller must prepare its integrity handle, acquisition
    /// binder and WebSocket attachment before calling start().
    ///
    /// This method does NOT mark the stream Running.
    pub fn start<D, T>(
        &mut self,
        stream_id: &str,
        executor: SourceStreamExecutor,
        publish_depth: D,
        publish_trades: T,
    ) -> Result<RuntimeGeneration, SourceSupervisorError>
    where
        D: for<'a> FnMut(&[DepthPublicationEvent<'a>]) -> Result<(), String> + Send + 'static,
        T: for<'a> FnMut(SourceTradePublication<'a>) -> Result<(), String> + Send + 'static,
    {
        if executor.stream_id() != stream_id {
            return Err(SourceSupervisorError::WrongStream(stream_id.to_owned()));
        }
        if self.tasks.contains_key(stream_id) {
            return Err(SourceSupervisorError::AlreadyActive(stream_id.to_owned()));
        }

        let generation = self.registry_lock().begin_start(stream_id)?;

        self.spawn_registered(
            stream_id,
            executor,
            generation,
            publish_depth,
            publish_trades,
        )
    }

    /// Start a Source with runtime-authorized Router publication callbacks.
    /// Normalization, acquisition fencing and integrity acknowledgment remain unchanged.
    pub fn start_routed(
        &mut self,
        stream_id: &str,
        mut executor: SourceStreamExecutor,
        router: &crate::live::stream::router::StreamRouter,
    ) -> Result<RuntimeGeneration, SourceSupervisorError> {
        if executor.stream_id() != stream_id {
            return Err(SourceSupervisorError::WrongStream(stream_id.into()));
        }
        if self.tasks.contains_key(stream_id) {
            return Err(SourceSupervisorError::AlreadyActive(stream_id.into()));
        }
        let generation = self.registry_lock().begin_start(stream_id)?;
        let authority = self
            .registry_lock()
            .publication_authority(stream_id)
            .expect("new runtime authority");
        let producer = match router
            .attach_producer(crate::live::stream::router::AttachProducerRequest { authority })
        {
            Ok(producer) => std::sync::Arc::new(producer),
            Err(error) => {
                self.registry_lock()
                    .mark_failed(stream_id, generation, error.to_string())?;
                return Err(error.into());
            }
        };
        producer.install_source_metrics(executor.metrics_handle());
        executor.observe_depth(producer.clone());
        let depth = producer.clone();
        self.spawn_registered(
            stream_id,
            executor,
            generation,
            move |events| depth.publish_source_depth(events),
            move |trades| producer.publish_source_trades(trades),
        )
    }

    fn spawn_registered<D, T>(
        &mut self,
        stream_id: &str,
        executor: SourceStreamExecutor,
        generation: RuntimeGeneration,
        publish_depth: D,
        publish_trades: T,
    ) -> Result<RuntimeGeneration, SourceSupervisorError>
    where
        D: for<'a> FnMut(&[DepthPublicationEvent<'a>]) -> Result<(), String> + Send + 'static,
        T: for<'a> FnMut(SourceTradePublication<'a>) -> Result<(), String> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let (readiness_tx, readiness_rx) = watch::channel(SourceReadiness::AwaitingData);

        let task = tokio::spawn(async move {
            executor
                .run(shutdown_rx, readiness_tx, publish_depth, publish_trades)
                .await
        });

        self.tasks.insert(
            stream_id.to_owned(),
            ManagedSourceTask {
                generation,
                shutdown: Some(shutdown_tx),
                started_at: std::time::Instant::now(),
                ever_ready: false,
                task,
                readiness: readiness_rx,
            },
        );

        Ok(generation)
    }

    /// The source has entered an integrity recovery operation.
    ///
    /// Connection establishment alone does not satisfy
    /// stream readiness.
    pub fn mark_recovering(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, SourceSupervisorError> {
        if !self.is_current_task(stream_id, generation) {
            return Ok(false);
        }

        Ok(self.registry_lock().mark_recovering(stream_id, generation)?)
    }

    /// A running source has lost readiness but its executor
    /// remains available for recovery.
    pub fn mark_degraded(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, SourceSupervisorError> {
        if !self.is_current_task(stream_id, generation) {
            return Ok(false);
        }

        Ok(self.registry_lock().mark_degraded(stream_id, generation)?)
    }

    /// A trusted readiness evaluator confirms that all
    /// stream-specific conditions have been satisfied.
    ///
    /// For depth streams, this must follow an acknowledged
    /// synchronized-book publication.
    ///
    /// Do not invoke this merely upon StreamInput::Connected.
    pub fn mark_running(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, SourceSupervisorError> {
        if !self.is_current_task(stream_id, generation) {
            return Ok(false);
        }

        Ok(self.registry_lock().mark_running(stream_id, generation)?)
    }

    /// Stop one executor without aborting its Tokio task.
    ///
    /// Graceful executor shutdown is important because
    /// SourceStreamExecutor::run() performs explicit
    /// WebSocket detachment.
    pub async fn stop(&mut self,stream_id:&str)->Result<(),SourceSupervisorError>{self.request_stop(stream_id)?.finish().await}
    /// Transfers the wait to an owned cleanup task. Dropping the ticket does not
    /// cancel WebSocket detachment, joining, or the generation-fenced completion.
    pub fn request_stop(&mut self,stream_id:&str)->Result<SourceStopOperation,SourceSupervisorError>{
        if !self.tasks.contains_key(stream_id){return Err(SourceSupervisorError::NotActive(stream_id.into()));}
        let runtime=tokio::runtime::Handle::try_current().map_err(|_|SourceSupervisorError::UnexpectedExit("Source cleanup requires an async runtime".into()))?;
        self.cleanup.retain(|task|!task.is_finished());
        let stop_generation=self.registry_lock().begin_stop(stream_id)?;
        let mut managed=self.tasks.remove(stream_id).unwrap();
        if let Some(shutdown)=managed.shutdown.take(){let _=shutdown.send(());}
        let registry=self.registry.clone();let id=stream_id.to_owned();let(reply,result)=oneshot::channel();
        self.cleanup.push(runtime.spawn(async move {let stream_id=id.as_str();let outcome=async {
        match managed.task.await {
            Ok(Ok(SourceExecutorExit::ShutdownRequested)) => {
                registry.lock().unwrap_or_else(|p|p.into_inner()).mark_stopped(stream_id, stop_generation)?;

                Ok(())
            }

            Ok(Ok(SourceExecutorExit::TransportClosed)) => {
                let reason = "transport closed before coordinated shutdown";

                registry.lock().unwrap_or_else(|p|p.into_inner())
                    .mark_failed(stream_id, stop_generation, reason)?;

                Err(SourceSupervisorError::UnexpectedExit(reason.into()))
            }

            Ok(Err(error)) => {
                registry.lock().unwrap_or_else(|p|p.into_inner())
                    .mark_failed(stream_id, stop_generation, error.to_string())?;

                Err(SourceSupervisorError::Executor(error))
            }

            Err(error) => {
                registry.lock().unwrap_or_else(|p|p.into_inner())
                    .mark_failed(stream_id, stop_generation, error.to_string())?;

                Err(SourceSupervisorError::Join(error))
            }
        }

        }.await;let _=reply.send(outcome);}));
        Ok(SourceStopOperation{result})
    }

    /// Collect executors that terminated independently of
    /// an explicit stop request.
    ///
    /// A terminated source is marked Failed, preventing an
    /// automatic restart loop during reconciliation.
    ///
    /// Invoke periodically from the stream-management loop.
    pub async fn reap_finished(&mut self) -> Result<Vec<String>, SourceSupervisorError> {
        self.sync_readiness()?;
        let finished = self
            .tasks
            .iter()
            .filter(|(_, task)| task.task.is_finished())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();

        let mut retired = Vec::with_capacity(finished.len());

        for stream_id in finished {
            let managed = self
                .tasks
                .remove(&stream_id)
                .expect("finished task was present");

            let reason = match managed.task.await {
                Ok(Ok(SourceExecutorExit::TransportClosed)) => {
                    "source transport closed unexpectedly".into()
                }

                Ok(Ok(SourceExecutorExit::ShutdownRequested)) => {
                    "source exited without supervisor shutdown".into()
                }

                Ok(Err(error)) => error.to_string(),

                Err(error) => error.to_string(),
            };

            let previous=self.registry_lock().status(&stream_id).cloned();
            let reason=previous.filter(|s|s.generation==managed.generation && s.lifecycle==StreamLifecycleState::Failed).and_then(|s|s.last_error).unwrap_or(reason);
            self.registry_lock().mark_failed(&stream_id, managed.generation, reason)?;

            retired.push(stream_id);
        }

        Ok(retired)
    }

    /// Stop all currently managed source executors.
    ///
    /// Continue stopping remaining streams after one failure.
    pub async fn shutdown_all(&mut self) -> Result<(), SourceSupervisorError> {
        let ids = self.tasks.keys().cloned().collect::<Vec<_>>();

        let mut first_error = None;

        for stream_id in ids {
            if let Err(error) = self.stop(&stream_id).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        while let Some(task)=self.cleanup.pop(){if let Err(error)=task.await{if first_error.is_none(){first_error=Some(SourceSupervisorError::Join(error));}}}
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn is_current_task(&self, stream_id: &str, generation: RuntimeGeneration) -> bool {
        let Some(task) = self.tasks.get(stream_id) else {
            return false;
        };

        task.generation == generation && !task.task.is_finished()
    }

    /// Apply the latest readiness state from every active executor.
    ///
    /// The stream registry remains the authoritative lifecycle owner.
    ///
    /// Completed tasks cannot publish readiness transitions.
    /// Their terminal result is handled by reap_finished().
    pub fn sync_readiness(&mut self) -> Result<usize, SourceSupervisorError> {
        let updates = self
            .tasks
            .iter()
            .filter(|(_, task)| !task.task.is_finished())
            .map(|(stream_id, task)| (stream_id.clone(), task.generation, *task.readiness.borrow()))
            .collect::<Vec<_>>();

        let mut transitions = 0;

        for (stream_id, generation, readiness) in updates {
            if !self.is_current_task(&stream_id, generation) {
                continue;
            }

            let Some(status) = self.registry_lock().status(&stream_id).cloned() else {
                continue;
            };

            // Never resurrect a terminal or stopping generation.
            if status.generation != generation {
                continue;
            }

            let current = status.lifecycle;

            if !matches!(
                current,
                StreamLifecycleState::Starting
                    | StreamLifecycleState::Running
                    | StreamLifecycleState::Recovering
                    | StreamLifecycleState::Degraded
            ) {
                continue;
            }

            if readiness==SourceReadiness::Ready {if let Some(task)=self.tasks.get_mut(&stream_id){task.ever_ready=true;}}
            let changed = match readiness {
                SourceReadiness::Ready => {
                    if current == StreamLifecycleState::Running {
                        false
                    } else {
                        self.registry_lock().mark_running(&stream_id, generation)?
                    }
                }

                SourceReadiness::Recovering => {
                    if current == StreamLifecycleState::Recovering {
                        false
                    } else {
                        self.registry_lock().mark_recovering(&stream_id, generation)?
                    }
                }

                SourceReadiness::AwaitingData => {
                    // Starting is already a non-ready state.
                    //
                    // A previously Running stream must lose
                    // readiness when its accepted publication
                    // evidence is revoked.
                    if current == StreamLifecycleState::Starting
                        || current == StreamLifecycleState::Recovering
                    {
                        false
                    } else {
                        self.registry_lock().mark_recovering(&stream_id, generation)?
                    }
                }

                SourceReadiness::Degraded => {
                    if current == StreamLifecycleState::Degraded {
                        false
                    } else {
                        self.registry_lock().mark_degraded(&stream_id, generation)?
                    }
                }
            };

            if changed {
                transitions += 1;
            }
        }

        Ok(transitions)
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;

    #[test]
    fn ready_does_not_mean_connected_only() {
        assert_ne!(SourceReadiness::AwaitingData, SourceReadiness::Ready);

        assert_ne!(SourceReadiness::Recovering, SourceReadiness::Ready);

        assert_ne!(SourceReadiness::Degraded, SourceReadiness::Ready);
    }

    #[test]
    fn watch_channel_keeps_latest_readiness() {
        let (tx, rx) = watch::channel(SourceReadiness::AwaitingData);

        tx.send_replace(SourceReadiness::Recovering);
        tx.send_replace(SourceReadiness::Ready);

        assert_eq!(*rx.borrow(), SourceReadiness::Ready);
    }

    #[test]
    fn disconnect_replaces_previous_ready_notification() {
        let (tx, rx) = watch::channel(SourceReadiness::AwaitingData);

        tx.send_replace(SourceReadiness::Ready);
        tx.send_replace(SourceReadiness::Recovering);

        assert_eq!(*rx.borrow(), SourceReadiness::Recovering);
    }

    #[test]
    fn publication_failure_replaces_ready_notification() {
        let (tx, rx) = watch::channel(SourceReadiness::Ready);

        tx.send_replace(SourceReadiness::Degraded);

        assert_eq!(*rx.borrow(), SourceReadiness::Degraded);
    }
}

/// Immutable compatibility view; a read never lends out a registry lock.
pub struct SourceRegistryRead(std::sync::Arc<std::sync::Mutex<StreamRuntimeRegistry>>);
impl SourceRegistryRead {pub fn publication_authority(&self,id:&str)->Option<crate::live::stream::registry::RuntimePublicationAuthority>{self.0.lock().unwrap_or_else(|p|p.into_inner()).publication_authority(id)}pub fn status(&self,id:&str)->Option<crate::live::stream::registry::StreamRuntimeStatus>{self.0.lock().unwrap_or_else(|p|p.into_inner()).status(id).cloned()}}

/// A wait handle only; cleanup ownership remains in the supervisor-issued task.
pub struct SourceStopOperation {result:oneshot::Receiver<Result<(),SourceSupervisorError>>}
impl SourceStopOperation {pub async fn finish(self)->Result<(),SourceSupervisorError>{self.result.await.map_err(|_|SourceSupervisorError::UnexpectedExit("Source cleanup task lost".into()))?}}

#[derive(Debug,Clone)]
pub struct SourceReadinessStatus {pub stream_id:String,pub generation:RuntimeGeneration,pub readiness:SourceReadiness,pub initial_readiness_pending:bool}
