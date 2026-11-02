use crate::live::{
    config::{ExchangeConfigStore, FormatConfigStore, general::GeneralConfig},
    infrastructure::{limiter::factory::ExchangeLimiters, rest::*, websocket::*},
    metrics::*,
    recovery::*,
    stream::{persistence::*, processing::*, registry::*, router::*, source::*},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

/// Already-resolved immutable definitions are the embedding/test boundary.
pub struct LiveApplicationOptions {
    pub config: GeneralConfig,
    pub definitions: Vec<PersistedStreamDefinition>,
    pub dependencies: Vec<StreamDependency>,
    pub exchanges: Arc<ExchangeConfigStore>,
    pub formats: Arc<FormatConfigStore>,
    pub limiters: ExchangeLimiters,
    pub websocket: WebSocketManagerRuntime,
}
#[derive(Debug, Clone)]
pub struct LiveStatus {
    pub shutdown_report: Option<ShutdownReport>,
    pub sources: Vec<SourceReadinessStatus>,
    pub prometheus_address: Option<std::net::SocketAddr>,
    pub serving: bool,
    pub shutting_down: bool,
    pub streams: Vec<StreamRuntimeStatus>,
    pub reconciliation_total: u64,
    pub lifecycle_failures: u64,
}
#[derive(Debug, Clone)]
pub struct ShutdownReport {
    pub complete: bool,
    pub cleanup_continues: bool,
    pub errors: Vec<String>,
    /// Canceled retained publications explicitly disposed, not flushed as finite data.
    pub discarded_publications: Option<u64>,
}
#[derive(Debug, Clone, thiserror::Error)]
#[error("live application: {0}")]
pub struct ApplicationError(pub String);
impl From<String> for ApplicationError {
    fn from(s: String) -> Self {
        Self(s)
    }
}
enum Command {
    Graph { graph: Arc<StreamGraph>, expected: u64, affected: BTreeSet<String>, reply: oneshot::Sender<Result<(), ApplicationError>> },
    Shutdown(oneshot::Sender<()>),
    Demand {
        stream: String,
        owner: DemandOwner,
        acquire: bool,
        reply: oneshot::Sender<Result<(), ApplicationError>>,
    },
    Retry {
        stream: String,
        reply: oneshot::Sender<Result<(), ApplicationError>>,
    },
}
#[derive(Clone)]
pub struct LiveApplicationHandle {
    pub(crate) router: Arc<StreamRouter>,
    pub(crate) registry: Arc<Mutex<StreamRuntimeRegistry>>,
    pub(crate) demand_registry: Arc<Mutex<DemandRegistry>>,
    pub(crate) processing: ProcessingServiceHandle,
    processing_config: Option<crate::live::config::general::ProcessingConfig>,
    builder:Arc<SourceRuntimeBuilder>,
    exchanges:Arc<ExchangeConfigStore>,
    configuration:Arc<GeneralConfig>,
    commands: mpsc::Sender<Command>,
    status: watch::Receiver<LiveStatus>,
    collector: Arc<MetricsSnapshotCollector>,
    runtime: Arc<RuntimeMetrics>,
}
impl LiveApplicationHandle {
    pub(crate) fn validate_candidate(&self,graph:&StreamGraph)->Result<(),String>{
        if graph.stream_ids().count()>self.configuration.runtime.application.max_definitions{return Err("definition capacity".into());}
        router_limits(&self.configuration,graph).map_err(|e|e.to_string())?;
        if let Some(p)=self.processing_config {crate::live::stream::processing::preflight(graph,p.derived)?;if graph.stream_ids().filter(|id|matches!(graph.get(id).unwrap().definition,StreamDefinition::Derived(_))).count()>p.ready_queue_capacity{return Err("processing association capacity".into());}}
        for id in graph.stream_ids(){let d=graph.get(id).unwrap();if d.desired.enabled&&matches!(d.definition,StreamDefinition::Source(_)){let mut candidate=d.clone();if candidate.recording_stream_key==Some(-1){candidate.recording_stream_key=Some(1);}self.builder.prepare(candidate).map_err(|e|e.to_string())?.validate_subscription(&self.exchanges).map_err(|e|e.to_string())?;}}
        Ok(())
    }
    pub(crate) fn shutdown_budget(&self)->Duration{Duration::from_secs(self.configuration.runtime.shutdown_timeout_seconds)}
    pub(crate) fn processing_config(&self)->Option<crate::live::config::general::ProcessingConfig>{self.processing_config}
    pub(crate) async fn replace_graph(&self, expected:u64, graph:Arc<StreamGraph>, affected:BTreeSet<String>)->Result<(),ApplicationError> {
        let (reply,rx)=oneshot::channel();
        self.commands.try_send(Command::Graph{expected,graph,affected,reply}).map_err(|_|ApplicationError("application command capacity".into()))?;
        rx.await.map_err(|_|ApplicationError("graph operation owner closed".into()))?
    }
    pub async fn request_shutdown(&self) -> Result<(), ApplicationError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Shutdown(reply))
            .map_err(|_| ApplicationError("command capacity or closed application".into()))?;
        rx.await
            .map_err(|_| ApplicationError("application closed".into()))
    }
    pub async fn stream_action(
        &self,
        request: crate::live::control::StreamActionRequest,
    ) -> Result<(), ApplicationError> {
        match request.action {
            crate::live::control::StreamAction::Restart => {
                self.retry_stream(&request.stream_id).await
            }
            _ => Err(ApplicationError(
                "operation unsupported in v1; only Restart is implemented".into(),
            )),
        }
    }
    pub fn status(&self) -> LiveStatus {
        self.status.borrow().clone()
    }
    pub fn latest_metrics(&self) -> Option<Arc<MetricsSnapshot>> {
        self.collector.latest()
    }
    /// Completion means demand ownership changed, not that acquisition is ready.
    pub async fn demand(
        &self,
        stream: &str,
        owner: DemandOwner,
        acquire: bool,
    ) -> Result<(), ApplicationError> {
        if stream.len() > 4096
            || !matches!(&owner,DemandOwner::Manual{session_id} if session_id.len()<=256 && !session_id.is_empty())
        {
            return Err(ApplicationError(
                "only bounded Manual application demand is supported".into(),
            ));
        }
        let (reply, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Demand {
                stream: stream.into(),
                owner,
                acquire,
                reply,
            })
            .map_err(|_| ApplicationError("command capacity or closed application".into()))?;
        rx.await
            .map_err(|_| ApplicationError("application closed".into()))?
    }
    /// Admission of explicit retirement/retry; status reports subsequent readiness.
    pub async fn retry_stream(&self, stream: &str) -> Result<(), ApplicationError> {
        if stream.len() > 4096 {
            return Err(ApplicationError("stream ID too long".into()));
        }
        let (reply, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Retry {
                stream: stream.into(),
                reply,
            })
            .map_err(|_| ApplicationError("command capacity or closed application".into()))?;
        rx.await
            .map_err(|_| ApplicationError("application closed".into()))?
    }
}
pub struct LiveApplication {
    handle: LiveApplicationHandle,
    stop: Arc<Notify>,
    task: Option<JoinHandle<ShutdownReport>>,
    deadline: Duration,
    report: Option<ShutdownReport>,
}
impl LiveApplication {
    pub fn validate(options: &LiveApplicationOptions) -> Result<(), ApplicationError> {
        options.config.validate()?;
        if !options
            .limiters
            .manager()
            .shares_state_with(&options.websocket.shared_limiter)
        {
            return Err(ApplicationError(
                "REST and WebSocket must share one limiter manager".into(),
            ));
        }
        if options.definitions.iter().any(|d| d.stream_id.len() > 4096) {
            return Err(ApplicationError(
                "stream identity exceeds 4096-byte limit".into(),
            ));
        }
        if options.config.recording.enabled {
            return Err(ApplicationError("automatic market recording is unsupported: no faithful Router recorder adapter and durable identity allocator; definition restoration still requires PostgreSQL".into()));
        }
        if options.config.metrics.prometheus.enabled && !options.config.metrics.enabled {
            return Err(ApplicationError(
                "Prometheus requires metrics.enabled".into(),
            ));
        }
        if options.definitions.len() > options.config.runtime.application.max_definitions {
            return Err(ApplicationError(
                "definition count exceeds runtime.application.max_definitions".into(),
            ));
        }
        let graph = StreamGraph::restore(options.definitions.clone(), options.dependencies.clone())
            .map_err(|e| ApplicationError(e.to_string()))?;
        if options.config.runtime.processing.is_none()
            && graph.stream_ids().any(|id| {
                matches!(
                    graph.get(id).unwrap().definition,
                    StreamDefinition::Derived(_)
                )
            })
        {
            return Err(ApplicationError(
                "Derived definitions require runtime.processing".into(),
            ));
        }
        router_limits(&options.config, &graph)?;
        if let Some(processing) = options.config.runtime.processing {
            crate::live::stream::processing::preflight(&graph, processing.derived)?;
            let derived = graph
                .stream_ids()
                .filter(|id| {
                    matches!(
                        graph.get(id).unwrap().definition,
                        StreamDefinition::Derived(_)
                    )
                })
                .count();
            if derived > processing.ready_queue_capacity {
                return Err(ApplicationError(
                    "processing association capacity cannot prepare the declared graph".into(),
                ));
            }
        }
        let builder = SourceRuntimeBuilder::new(
            options.exchanges.clone(),
            options.formats.clone(),
            Default::default(),
        )
        .map_err(|e| ApplicationError(e.to_string()))?;
        for id in graph.stream_ids() {
            let d = graph.get(id).unwrap();
            if d.desired.enabled && matches!(d.definition, StreamDefinition::Source(_)) {
                builder
                    .prepare(d.clone())
                    .map_err(|e| ApplicationError(e.to_string()))?
                    .validate_subscription(&options.exchanges)
                    .map_err(|e| ApplicationError(e.to_string()))?;
            }
        }
        Ok(())
    }
    pub async fn start(options: LiveApplicationOptions) -> Result<Self, ApplicationError> {
        Self::validate(&options)?;
        let LiveApplicationOptions {
            config,
            definitions,
            dependencies,
            exchanges,
            formats,
            limiters,
            websocket,
        } = options;
        let feedback = exchanges
            .names()
            .map(|p| {
                Ok((
                    p.to_string(),
                    RestFactoryFeedbackPolicy::new(vec![], Duration::from_millis(100))?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        // Build owned dependencies before spawning; the task borrows its own
        // configurations ordinarily, never a leaked singleton.
        let rest = ExchangeRestBootstrap::build(
            &exchanges,
            &limiters,
            feedback.clone(),
            RestClientConfig {
                max_concurrent_requests: 8,
                max_response_bytes: 4 << 20,
            },
        )?;
        let rest_client = rest.client().clone();
        drop(rest);
        let graph = Arc::new(
            StreamGraph::restore(definitions, dependencies)
                .map_err(|e| ApplicationError(e.to_string()))?,
        );
        let registry = Arc::new(Mutex::new(StreamRuntimeRegistry::new(&graph)));
        let demand = Arc::new(Mutex::new(DemandRegistry::new()));
        let router_limits = router_limits(&config, &graph)?;
        let router = Arc::new(
            StreamRouter::new(
                router_limits,
                RouterContext {
                    graph: graph.clone(),
                    demand: demand.clone(),
                },
            )
            .map_err(|e| ApplicationError(e.to_string()))?,
        );
        registry
            .lock()
            .unwrap()
            .install_metrics(router.runtime_metrics());
        let manager = Arc::new(
            WebSocketManager::from_runtime(websocket)
                .map_err(|e| ApplicationError(e.to_string()))?,
        );
        let builder = Arc::new(
            SourceRuntimeBuilder::new(exchanges.clone(), formats, Default::default())
                .map_err(|e| ApplicationError(e.to_string()))?,
        );
        let supervisor = Arc::new(tokio::sync::Mutex::new(SourceRuntimeSupervisor::shared(
            registry.clone(),
        )));
        let mut runtime_config = config.runtime.clone();
        if runtime_config.processing.is_none() {
            runtime_config.processing = Some(Default::default());
        }
        // Initial partial-start rollback may join staged CPU threads too. Keep
        // that work off executor threads, just like normal pool retirement.
        let pool_registry=registry.clone();
        let pool_router=router.clone();
        let pool_config=runtime_config.clone();
        let mut processing=tokio::task::spawn_blocking(move || ProcessingService::new(&pool_config,pool_registry,pool_router))
            .await.map_err(|_|ApplicationError("processing initialization task failed".into()))??;
        let server = match crate::live::transport::unix::UnixStreamingServer::bind(
            router.clone(),
            config.publication.clone(),
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                let _ = processing.shutdown().await;
                let _ = router.shutdown().await;
                return Err(ApplicationError(e.to_string()));
            }
        };
        let (coordinator, recovery) = RecoveryCoordinator::new(RecoveryCoordinatorConfig {
            command_capacity: 128,
            scheduler: RecoverySchedulerLimits {
                max_requests: 128,
                max_jobs: 64,
                max_concurrent_jobs: 8,
            },
        })
        .map_err(|e| ApplicationError(e.to_string()))?;
        let limiter_metrics = limiters.manager();
        let managed_exchanges=exchanges.clone();
        let recovery_task = tokio::spawn(async move {
            let service = ExchangeRestService::new(&exchanges, rest_client);
            let executor = RecoveryRestExecutor::new(&service);
            coordinator
                .run(|job| {
                    let executor = &executor;
                    async move { executor.execute(&job).await }
                })
                .await;
        });
        let collector = Arc::new(MetricsSnapshotCollector::new());
        let sampling = Arc::new(crate::live::metrics::prometheus::SamplingState::default());
        let prometheus = if config.metrics.prometheus.enabled {
            match crate::live::metrics::prometheus::PrometheusServer::bind(
                config.metrics.prometheus.clone(),
                collector.clone(),
                sampling.clone(),
            )
            .await
            {
                Ok(s) => Some(s),
                Err(e) => {
                    let _ = server.shutdown().await;
                    let _ = processing.shutdown().await;
                    let _ = recovery.shutdown().await;
                    let _ = recovery_task.await;
                    let _ = manager.shutdown().await;
                    let _ = router.shutdown().await;
                    return Err(ApplicationError(e));
                }
            }
        } else {
            None
        };
        let prometheus_address = prometheus.as_ref().map(|s| s.address);
        let initial = LiveStatus {
            shutdown_report: None,
            sources: vec![],
            prometheus_address,
            serving: true,
            shutting_down: false,
            streams: registry.lock().unwrap().statuses().cloned().collect(),
            reconciliation_total: 0,
            lifecycle_failures: 0,
        };
        let (status_tx, status) = watch::channel(initial);
        let (commands, rx) = mpsc::channel(config.runtime.application.command_capacity);
        let stop = Arc::new(Notify::new());
        let deadline = Duration::from_secs(config.runtime.shutdown_timeout_seconds);
        let router_metrics = router.runtime_metrics();
        let managed_configuration=Arc::new(config.clone());let managed_builder=builder.clone();
        let managed_processing_config=runtime_config.processing;
        let managed_router=router.clone(); let managed_registry=registry.clone(); let managed_demand=demand.clone(); let managed_processing=processing.handle();
        let task = tokio::spawn(run(
            config,
            graph,
            registry,
            demand,
            router,
            manager,
            builder,
            supervisor,
            processing,
            server,
            recovery,
            recovery_task,
            collector.clone(),
            limiter_metrics,
            sampling,
            prometheus,
            status_tx,
            rx,
            stop.clone(),
        ));
        Ok(Self {
            handle: LiveApplicationHandle {
                configuration:managed_configuration,builder:managed_builder,exchanges:managed_exchanges,processing_config:managed_processing_config, router:managed_router, registry:managed_registry, demand_registry:managed_demand, processing:managed_processing,
                commands,
                status,
                collector,
                runtime: router_metrics,
            },
            stop,
            task: Some(task),
            deadline,
            report: None,
        })
    }
    pub fn handle(&self) -> LiveApplicationHandle {
        self.handle.clone()
    }
    pub async fn shutdown(&mut self) -> ShutdownReport {
        if let Some(report) = &self.report {
            return report.clone();
        }
        self.stop.notify_one();
        let Some(task) = self.task.as_mut() else {
            return ShutdownReport {
                complete: true,
                cleanup_continues: false,
                errors: vec![],
                discarded_publications: Some(0),
            };
        };
        match tokio::time::timeout(self.deadline, task).await {
            Ok(result) => {
                self.task.take();
                let report = result.unwrap_or_else(|e| ShutdownReport {
                    complete: false,
                    cleanup_continues: false,
                    errors: vec![format!(
                        "application task failed; cleanup outcome unknown: {e}"
                    )],
                    discarded_publications: None,
                });
                self.report = Some(report.clone());
                report
            }
            Err(_) => {
                self.handle.runtime.record_incomplete_shutdown_total();
                ShutdownReport {
                    complete: false,
                    cleanup_continues: true,
                    errors: vec!["application shutdown deadline; cleanup remains owned".into()],
                    discarded_publications: None,
                }
            }
        }
    }
}
impl Drop for LiveApplication {
    fn drop(&mut self) {
        self.stop.notify_one();
    }
}

async fn run(
    config: GeneralConfig,
    mut graph: Arc<StreamGraph>,
    registry: Arc<Mutex<StreamRuntimeRegistry>>,
    demand: Arc<Mutex<DemandRegistry>>,
    router: Arc<StreamRouter>,
    manager: Arc<WebSocketManager>,
    builder: Arc<SourceRuntimeBuilder>,
    supervisor: Arc<tokio::sync::Mutex<SourceRuntimeSupervisor>>,
    mut processing: ProcessingService,
    server: crate::live::transport::unix::UnixStreamingServer,
    recovery: RecoveryHandle,
    recovery_task: JoinHandle<()>,
    collector: Arc<MetricsSnapshotCollector>,
    limiter_metrics: crate::live::infrastructure::limiter::RateLimitManager,
    sampling: Arc<crate::live::metrics::prometheus::SamplingState>,
    prometheus: Option<crate::live::metrics::prometheus::PrometheusServer>,
    status: watch::Sender<LiveStatus>,
    mut commands: mpsc::Receiver<Command>,
    stop: Arc<Notify>,
) -> ShutdownReport {
    let handle = processing.handle();
    let mut operations: JoinSet<(String, Result<(), String>)> = JoinSet::new();
    let mut busy = BTreeSet::new();
    let mut starting = BTreeSet::new();
    let mut deferred_graph:Option<Command>=None;
    let mut deferred_retry:Option<(Command,BTreeSet<String>)>=None;
    let mut graph_change: Option<(Arc<StreamGraph>,u64,oneshot::Sender<Result<(),ApplicationError>>,BTreeSet<String>)>=None;
    let mut retry_scopes: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut fatal = None;
    let mut tick = tokio::time::interval(Duration::from_millis(
        config.runtime.application.reconcile_interval_ms,
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut process_sampler = crate::live::metrics::process::ProcessSampler::default();
    let runtime = router.runtime_metrics();
    let publication = router.publication_metrics();
    let recording = RecordingMetrics::new();
    let mut sample =
        tokio::time::interval(Duration::from_millis(config.metrics.snapshot_interval_ms));
    sample.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let lifecycle_capacity_available=operations.len()<config.runtime.application.max_lifecycle_operations;
        tokio::select! {
            _=stop.notified()=>break,
            command=async {if deferred_graph.as_ref().is_some_and(|c|matches!(c,Command::Graph{affected,..} if !affected.iter().any(|id|busy.contains(id)))){deferred_graph.take()}else if deferred_retry.as_ref().is_some_and(|(_,scope)|!scope.iter().any(|id|busy.contains(id)) && lifecycle_capacity_available){deferred_retry.take().map(|(command,_)|command)}else{commands.recv().await}}=> match command {
                None=>break,
                Some(Command::Graph{graph:new_graph,expected,affected,reply})=>{
                    if affected.iter().any(|id|busy.contains(id)) {if deferred_graph.is_none(){deferred_graph=Some(Command::Graph{graph:new_graph,expected,affected,reply});}else{let _=reply.send(Err(ApplicationError("graph transition already queued".into())));}continue;}

                    let validation:Result<(),String>=(||{
                        if graph_change.is_some() {return Err("lifecycle transition already pending".into());}
                        if expected!=router.graph_revision(){return Err("graph revision conflict".into());}
                        if new_graph.stream_ids().count()>config.runtime.application.max_definitions {return Err("definition capacity".into());}
                        for id in new_graph.stream_ids(){if new_graph.get(id).unwrap().desired.enabled && matches!(new_graph.get(id).unwrap().definition,StreamDefinition::Source(_)) {builder.prepare(new_graph.get(id).unwrap().clone()).map_err(|e|e.to_string())?;}}
                        if let Some(p)=config.runtime.processing{crate::live::stream::processing::preflight(&new_graph,p.derived)?;}

                        Ok(())
                    })();
                    if let Err(e)=validation {let _=reply.send(Err(ApplicationError(e)));continue;}
                    let order=LifecyclePlanner::retirement_order(&graph,&affected).map_err(|e|e.to_string());
                    match order {Err(e)=>{let _=reply.send(Err(ApplicationError(e)));},Ok(ids)=>{
                        busy.extend(affected.iter().cloned());
                        graph_change=Some((new_graph,expected,reply,affected));
                        let sup=supervisor.clone();let h=handle.clone();let r=registry.clone();let old=graph.clone();
                        operations.spawn(async move{let result=async {for id in ids {retire_one(&id,matches!(old.get(&id).unwrap().definition,StreamDefinition::Source(_)),&sup,&h,&r).await?;} Ok(())}.await;(String::from("__graph"),result)});
                    }}
                },
                Some(Command::Shutdown(reply))=>{let _=reply.send(());break;},
                Some(Command::Demand{stream,owner,acquire,reply})=>{let result=router.change_manual_demand(&stream,owner,acquire).map_err(ApplicationError);let _=reply.send(result);},
                Some(Command::Retry{stream,reply})=>{
                    let mut affected=BTreeSet::from([stream.clone()]);let mut pending=vec![stream.clone()];
                    while let Some(id)=pending.pop(){if let Some(dependents)=graph.dependents(&id){for next in dependents {if affected.insert(next.clone()){pending.push(next.clone());}}}}
                    let required=demand.lock().unwrap().required_streams(&graph);
                    let result=if graph.get(&stream).is_none(){Err(ApplicationError("unknown stream".into()))}else if !required.is_ok_and(|ids|ids.contains(&stream)){Err(ApplicationError("retry requires current demand".into()))}else if affected.iter().any(|id|busy.contains(id)) || operations.len()>=config.runtime.application.max_lifecycle_operations {if deferred_retry.is_none(){deferred_retry=Some((Command::Retry{stream,reply},affected));continue;}Err(ApplicationError("lifecycle retry already queued".into()))}else{
                        let ids=LifecyclePlanner::retirement_order(&graph,&affected).map_err(|e|ApplicationError(e.to_string()));
                        match ids {Err(e)=>Err(e),Ok(ids)=>{busy.extend(affected.iter().cloned());retry_scopes.insert(stream.clone(),affected.into_iter().collect());let sup=supervisor.clone();let h=handle.clone();let r=registry.clone();let g=graph.clone();let root=stream.clone();operations.spawn(async move{let mut result=Ok(());for id in ids {if let Err(e)=retire_one(&id,matches!(g.get(&id).unwrap().definition,StreamDefinition::Source(_)),&sup,&h,&r).await{result=Err(e);}}(root,result)});Ok(())}}
                    };let _=reply.send(result);
                }
            },
            result=operations.join_next(),if !operations.is_empty()=>{if let Some(Err(error))=&result {fatal=Some(format!("lifecycle owner task failed: {error}"));break;}if let Some(Ok((id,result)))=result {if id=="__graph" {if let Some((new_graph,expected,reply,affected))=graph_change.take(){let result=result.and_then(|_|router.install_graph(expected,new_graph.clone(),&registry));if result.is_ok(){graph=new_graph;}for id in affected{busy.remove(&id);}let _=reply.send(result.map_err(ApplicationError));}continue;}busy.remove(&id);let was_starting=starting.remove(&id);if was_starting {if let Err(reason)=&result {let mut r=registry.lock().unwrap();if r.status(&id).is_some_and(|s|s.lifecycle==StreamLifecycleState::Stopped){if let Ok(g)=r.begin_start(&id){let _=r.mark_failed(&id,g,format!("start was not admitted: {reason}"));}}}}if let Some(scope)=retry_scopes.remove(&id){for member in scope{busy.remove(&member);}}if result.is_err(){status.send_modify(|s|s.lifecycle_failures+=1);}}},
            _=sample.tick(),if config.metrics.enabled=>{let started=std::time::Instant::now();process_sampler.sample(&runtime);runtime.set_websocket(tokio::time::timeout(Duration::from_millis(50),async{(manager.slot_count().await as u64,manager.stream_count().await as u64)}).await.ok());runtime.set_router(&router.status());runtime.set_limiter(limiter_metrics.pending_count() as u64,limiter_metrics.budget_count() as u64);let result={let (sources,derived)=router.metric_handles();let processing_status=handle.status();for (id,_,metrics) in &derived {if let Some(a)=processing_status.associations.iter().find(|a|&a.stream_id==id){metrics.set_retained(a.derived.retained_events as u64,a.derived.retained_bytes as u64);let _=metrics.set_input_health(a.inputs.len() as u64,a.inputs.iter().filter(|input|input.ready).count() as u64);let _=metrics.set_input_queue_depth(a.input.queued_events as u64);}}let source_refs=sources.iter().map(|(id,g,m)|SourceMetricsRef{stream_id:id,generation:*g,metrics:m}).collect::<Vec<_>>();let derived_refs=derived.iter().map(|(id,g,m)|DerivedMetricsRef{stream_id:id,generation:*g,metrics:m}).collect::<Vec<_>>();collector.collect(&runtime,&publication,&recording,&source_refs,&derived_refs)};if result.is_ok(){sampling.success(started.elapsed());}else{sampling.failures.fetch_add(1,std::sync::atomic::Ordering::Relaxed);}},
            _=tick.tick()=>{
                if let Ok(mut s)=supervisor.try_lock(){let _=s.reap_finished().await;s.expire_starting(Duration::from_millis(config.runtime.application.source_readiness_timeout_ms));status.send_modify(|status|status.sources=s.readiness_statuses());}
                let states=registry.lock().unwrap().statuses().cloned().collect::<Vec<_>>();
                let previous=status.borrow().streams.clone();for(old,current)in previous.iter().zip(&states){if old!=current{tracing::info!(event="stream_lifecycle",stream_id=%current.stream_id,generation=current.generation.get(),lifecycle=?current.lifecycle,reason=?current.last_error,error_truncated=current.error_truncated);}}
                for s in &states {if s.lifecycle==StreamLifecycleState::Failed {if let Some(report)=handle.take_report(&s.stream_id,s.generation){report.discard();}}}
                runtime.set_running_stream_counts(states.iter().filter(|s|s.is_ready() && matches!(graph.get(&s.stream_id).unwrap().definition,StreamDefinition::Source(_))).count() as u64,states.iter().filter(|s|s.is_ready() && matches!(graph.get(&s.stream_id).unwrap().definition,StreamDefinition::Derived(_))).count() as u64);
                runtime.set_unhealthy_stream_counts(states.iter().filter(|s|s.lifecycle==StreamLifecycleState::Degraded).count() as u64,states.iter().filter(|s|s.lifecycle==StreamLifecycleState::Failed).count() as u64);
                status.send_modify(|s|{s.streams=states;s.reconciliation_total+=1;});
                runtime.record_reconciliations_total();
                let plan={let r=registry.lock().unwrap();let d=demand.lock().unwrap();r.plan(&graph,&d)};
                let Ok(plan)=plan else{continue};
                if let Some(id)=plan.stop.first().cloned() {
                    if !busy.contains(&id) {
                        busy.insert(id.clone());let sup=supervisor.clone();let h=handle.clone();let r=registry.clone();let source=matches!(graph.get(&id).unwrap().definition,StreamDefinition::Source(_));
                        operations.spawn(async move{let result=retire_one(&id,source,&sup,&h,&r).await;(id,result)});
                    }
                    continue;
                }
                // Prepare all Derived consumers before activating a cold Source.
                for id in plan.start.iter().rev().filter(|id|matches!(graph.get(id).unwrap().definition,StreamDefinition::Derived(_))) {if busy.contains(id)||router.transition_pending(id)||operations.len()>=config.runtime.application.max_lifecycle_operations{continue;}busy.insert(id.clone());starting.insert(id.clone());let id=id.clone();let h=handle.clone();operations.spawn(async move{let result=h.start_derived(&id).await.map(|_|());(id,result)});}
                let prepared=plan.target.iter().filter(|id|matches!(graph.get(id).unwrap().definition,StreamDefinition::Derived(_))).all(|id|registry.lock().unwrap().status(id).is_some_and(|s|s.lifecycle==StreamLifecycleState::Failed)||handle.status().associations.iter().any(|s|&s.stream_id==id));
                if !prepared {continue;}
                for id in plan.start.into_iter().filter(|id|matches!(graph.get(id).unwrap().definition,StreamDefinition::Source(_))) {if busy.iter().filter(|id|matches!(graph.get(id).unwrap().definition,StreamDefinition::Source(_))).count()>=config.runtime.application.source_start_concurrency {break;}if busy.contains(&id)||router.transition_pending(&id)||operations.len()>=config.runtime.application.max_lifecycle_operations{continue;}busy.insert(id.clone());starting.insert(id.clone());let sup=supervisor.clone();let b=builder.clone();let m=manager.clone();let rec=recovery.clone();let r=router.clone();let def=graph.get(&id).unwrap().clone();operations.spawn(async move{let result:Result<(),String>=async {let prepared=b.prepare(def).map_err(|e|e.to_string())?;let executor=prepared.attach(m,rec).await.map_err(|e|e.to_string())?;sup.lock().await.start_routed(&id,executor,&r).map_err(|e|e.to_string())?;Ok(())}.await;if let Err(reason)=&result {sup.lock().await.fail_preparation(&id,reason.clone());}(id,result)});}
            }
        }
    }
    commands.close();
    while let Ok(command) = commands.try_recv() {
        match command {
            Command::Shutdown(reply) => {
                let _ = reply.send(());
            }
            Command::Demand { reply, .. } | Command::Retry { reply, .. } | Command::Graph {reply,..} => {
                let _ = reply.send(Err(ApplicationError(
                    "application shutting down before command acceptance".into(),
                )));
            }
        }
    }

    status.send_modify(|s| {
        s.shutting_down = true;
        s.serving = false;
    });
    runtime.set_running_stream_counts(0, 0);
    let mut errors = fatal.into_iter().collect::<Vec<_>>();
    if let Err(e) = server.shutdown().await {
        errors.push(e.to_string());
    }
    let initial_processing_shutdown = processing.shutdown().await;
    while operations.join_next().await.is_some() {}
    let _ = initial_processing_shutdown;
    loop {
        match processing.shutdown().await {
            Ok(outcome) if outcome.complete => break,
            Ok(_) => {
                tokio::time::sleep(Duration::from_millis(
                    config.runtime.application.reconcile_interval_ms,
                ))
                .await
            }
            Err(e) => {
                errors.push(e);
                break;
            }
        }
    }
    if let Err(e) = supervisor.lock().await.shutdown_all().await {
        errors.push(e.to_string());
    }
    if let Err(e) = router.shutdown().await {
        errors.push(e.to_string());
    }
    if let Err(e) = manager.shutdown().await {
        errors.push(e.to_string());
    }
    let _ = recovery.shutdown().await;
    let _ = recovery_task.await;
    demand.lock().unwrap().clear();
    runtime.set_router(&router.status());
    runtime.set_websocket(Some((0, 0)));
    runtime.set_limiter(
        limiter_metrics.pending_count() as u64,
        limiter_metrics.budget_count() as u64,
    );
    runtime.set_running_stream_counts(0, 0);
    if config.metrics.enabled {
        let started = std::time::Instant::now();
        let result = {
            let (sources, derived) = router.metric_handles();
            let processing_status = handle.status();
            for (id, _, metrics) in &derived {
                if let Some(a) = processing_status
                    .associations
                    .iter()
                    .find(|a| &a.stream_id == id)
                {
                    metrics.set_retained(
                        a.derived.retained_events as u64,
                        a.derived.retained_bytes as u64,
                    );
                    let _ = metrics.set_input_queue_depth(a.input.queued_events as u64);
                }
            }
            let source_refs = sources
                .iter()
                .map(|(id, g, m)| SourceMetricsRef {
                    stream_id: id,
                    generation: *g,
                    metrics: m,
                })
                .collect::<Vec<_>>();
            let derived_refs = derived
                .iter()
                .map(|(id, g, m)| DerivedMetricsRef {
                    stream_id: id,
                    generation: *g,
                    metrics: m,
                })
                .collect::<Vec<_>>();
            collector.collect(
                &runtime,
                &publication,
                &recording,
                &source_refs,
                &derived_refs,
            )
        };
        if result.is_ok() {
            sampling.success(started.elapsed());
        } else {
            sampling
                .failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let mut discarded = 0u64;
    for report in handle.take_reports(config.runtime.application.max_definitions) {
        discarded = discarded
            .saturating_add(report.unprocessed.len() as u64)
            .saturating_add(report.unpublished.len() as u64)
            .saturating_add(
                report
                    .processor_retained
                    .iter()
                    .map(|r| r.batch.len() as u64)
                    .sum::<u64>(),
            );
        report.discard();
    }
    if let Some(prometheus) = prometheus {
        prometheus.shutdown().await;
    }
    errors.truncate(64);
    for e in &mut errors {
        e.truncate(e.floor_char_boundary(512));
    }
    let report = ShutdownReport {
        complete: errors.is_empty(),
        cleanup_continues: false,
        errors,
        discarded_publications: Some(discarded),
    };
    let streams = registry.lock().unwrap().statuses().cloned().collect();
    status.send_modify(|s| {
        s.streams = streams;
        s.sources.clear();
        s.shutdown_report = Some(report.clone());
    });
    report
}

async fn retire_one(
    id: &str,
    source: bool,
    supervisor: &Arc<tokio::sync::Mutex<SourceRuntimeSupervisor>>,
    processing: &ProcessingServiceHandle,
    registry: &Arc<Mutex<StreamRuntimeRegistry>>,
) -> Result<(), String> {
    let generation = registry
        .lock()
        .unwrap()
        .status(id)
        .ok_or("unknown stream")?
        .generation;
    let result = if source {
        let operation = {
            let mut s = supervisor.lock().await;
            if s.is_active(id) {
                Some(s.request_stop(id).map_err(|e| e.to_string())?)
            } else {
                None
            }
        };
        if let Some(operation) = operation {
            operation.finish().await.map_err(|e| e.to_string())
        } else {
            Ok(())
        }
    } else {
        if processing
            .status()
            .associations
            .iter()
            .any(|s| s.stream_id == id)
        {
            processing.stop_generation(id, generation).await
        } else {
            Ok(())
        }
    };
    if let Some(report) = processing.take_report(id, generation) {
        report.discard();
    }
    // Services have finished relinquishing their owners. Only terminal state
    // with no owner needs the application's final retirement acknowledgment.
    let mut r = registry.lock().unwrap();
    if r.status(id)
        .is_some_and(|s| s.lifecycle == StreamLifecycleState::Failed)
    {
        let g = r.begin_stop(id).map_err(|e| e.to_string())?;
        r.mark_stopped(id, g).map_err(|e| e.to_string())?;
    }
    result
}

fn router_limits(
    config: &GeneralConfig,
    graph: &StreamGraph,
) -> Result<RouterConfig, ApplicationError> {
    let external = config
        .publication
        .max_connections
        .checked_mul(config.publication.streaming.max_subscriptions)
        .ok_or_else(|| ApplicationError("publication subscription count overflow".into()))?;
    let internal = graph
        .stream_ids()
        .map(|id| match &graph.get(id).unwrap().definition {
            StreamDefinition::Derived(r) => r.input_specs().len(),
            _ => 0,
        })
        .sum::<usize>();
    let _total = external
        .checked_add(internal)
        .filter(|n| *n <= 16384)
        .ok_or_else(|| {
            ApplicationError(
                "combined internal/external Router subscription limit exceeds 16384".into(),
            )
        })?;
    Ok(RouterConfig {
        max_subscriptions: 16384,
        max_producers: config.runtime.application.max_definitions,
        ..Default::default()
    })
}
