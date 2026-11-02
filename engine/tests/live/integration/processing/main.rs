use marketforge_engine::live::{
    config::{ProcessingConfig, RuntimeConfig},
    infrastructure::processing::*,
    stream::{persistence::*, processing::*, registry::*, request::*, router::*},
};
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
#[path = "../router/harness.rs"]
mod fixture;
const WAIT: Duration = Duration::from_secs(6);
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("bounded processing test deadline")
}
struct Rig {
    router: Arc<StreamRouter>,
    registry: Arc<Mutex<StreamRuntimeRegistry>>,
    service: ProcessingService,
    source: ProducerHandle,
}
impl Rig {
    fn new(channel: SourceChannel, router_config: RouterConfig) -> Self {
        let source = fixture::definition("binance_spot", channel, "test");
        let derived = PersistedStreamDefinition {
            stream_id: "synthetic".into(),
            recording_stream_key: None,
            definition: StreamDefinition::Derived(CreateDerivedStreamRequest {
                name: "synthetic".into(),
                operation: DerivedOperation::Convert,
                inputs: vec!["test".into()],
                ordering: None,
                max_lateness_ns: None,
                reference_inputs: Vec::new(),
                conversion: None,
                target_quote_asset: Some("USDT".into()),
            }),
            desired: DesiredStreamState::default(),
            revision: 1,
        };
        let graph = Arc::new(
            StreamGraph::restore(
                vec![source, derived],
                vec![StreamDependency {
                    derived_stream_id: "synthetic".into(),
                    input_stream_id: "test".into(),
                    input_position: 0,
                }],
            )
            .unwrap(),
        );
        let mut registry = StreamRuntimeRegistry::new(&graph);
        let generation = registry.begin_start("test").unwrap();
        registry.mark_running("test", generation).unwrap();
        let router = Arc::new(
            StreamRouter::new(
                router_config,
                RouterContext {
                    graph,
                    demand: Arc::new(Mutex::new(DemandRegistry::new())),
                },
            )
            .unwrap(),
        );
        let source = router
            .attach_producer(AttachProducerRequest {
                authority: registry.publication_authority("test").unwrap(),
            })
            .unwrap();
        let registry = Arc::new(Mutex::new(registry));
        let service = ProcessingService::new(
            &RuntimeConfig {
                application: Default::default(),
                shutdown_timeout_seconds: 1,
                processing: Some(ProcessingConfig {
                    workers: 2,
                    ready_queue_capacity: 8,
                    derived: Default::default(),
                    buffered_events: 128,
                    buffered_bytes: 1 << 20,
                    ..Default::default()
                }),
            },
            registry.clone(),
            router.clone(),
        )
        .unwrap();
        Self {
            router,
            registry,
            service,
            source,
        }
    }
    fn handle(&self) -> ProcessingServiceHandle {
        self.service.handle()
    }
    async fn subscribe(&self) -> SubscriptionHandle {
        self.router
            .subscribe(SubscribeRequest {
                stream_id: "synthetic".into(),
                owner: fixture::owner("synthetic-output"),
            })
            .await
            .unwrap()
    }
    async fn close(mut self) {
        assert!(bounded(self.service.shutdown()).await.unwrap().complete);
        self.router.shutdown().await.unwrap();
    }
}
fn request(
    factory: impl FnOnce(ProcessorContext) -> Result<PreparedProcessor, String> + Send + 'static,
) -> StartProcessor {
    StartProcessor {
        output_semantics: PublicationSemantics::CanonicalCompatibility,
        stream_id: "synthetic".into(),
        inputs: vec![ProcessingInputSpec {
            stream_id: "test".into(),
            position: 0,
            role: InputRole::Data,
        }],
        input_limits: MailboxLimits {
            max_events: 32,
            max_bytes: 1 << 18,
        },
        output_limits: MailboxLimits {
            max_events: 32,
            max_bytes: 1 << 18,
        },
        factory: Box::new(factory),
    }
}
struct Echo {
    context: ProcessorContext,
    count: i64,
    threads: Option<mpsc::Sender<String>>,
    ready: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Processor for Echo {
    fn run_turn(&mut self, _: usize) -> bool {
        false
    }
    fn try_run_turn(&mut self, max: usize) -> Result<bool, String> {
        if let Some(sender) = self.threads.take() {
            sender
                .send(std::thread::current().name().unwrap().to_owned())
                .unwrap();
        }
        let batch = self.context.input.take_work(max);
        for event in batch.iter() {
            assert_eq!(event.stream_id, "test");
            let RouterDelivery::Publication {
                publication,
                synchronized: true,
                ..
            } = &event.delivery
            else {
                return Err("upstream unsynchronized".into());
            };
            let mut publication = publication.as_ref().clone();
            self.count += 1;
            if let RouterPublication::Trades { records, .. } = &mut publication {
                for record in Arc::make_mut(records) {
                    record.envelope.system_timestamp_ns = Some(self.count);
                }
            }
            self.context
                .output
                .try_emit(publication)
                .map_err(|error| error.reason.to_owned())?;
            if let Some(ready) = self.ready.take() {
                let _ = ready.send(());
            }
        }
        batch.complete();
        Ok(!self.context.input.is_empty())
    }
}
fn echo(context: ProcessorContext) -> Result<PreparedProcessor, String> {
    Ok(PreparedProcessor::ready(Echo {
        context,
        count: 0,
        threads: None,
        ready: None,
    }))
}
async fn publish(source: &ProducerHandle, publication: RouterPublication) {
    // Source producers may retry Busy before committing state; no accepted event
    // is replayed. This is a bounded admission retry driven by Tokio scheduling.
    bounded(async {
        loop {
            match source.try_publish(publication.clone()) {
                Ok(_) => break,
                Err(PublishError::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("source rejected: {error}"),
            }
        }
    })
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_router_stateful_cpu_pipeline_preserves_identity_and_order() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let mut subscriber = rig.subscribe().await;
    let (threads, observed) = mpsc::channel();
    let stream = bounded(handle.start(request(move |context| {
        Ok(PreparedProcessor::ready(Echo {
            context,
            count: 0,
            threads: Some(threads),
            ready: None,
        }))
    })))
    .await
    .unwrap();
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Running
    );
    for id in 1..=8 {
        publish(&rig.source, fixture::publication(id)).await;
    }
    for id in 1..=8 {
        match bounded(subscriber.recv()).await.unwrap() {
            RouterDelivery::Publication {
                sequence,
                runtime_generation,
                publication,
                synchronized: true,
                event_valid: true,
                admission,
            } => {
                assert_eq!(
                    admission.semantics(),
                    PublicationSemantics::CanonicalCompatibility
                );
                assert_eq!(sequence, id);
                assert_eq!(runtime_generation, stream.generation);
                let RouterPublication::Trades { records, .. } = publication.as_ref() else {
                    panic!("wrong type")
                };
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].sequence, Some(id));
                assert_eq!(records[0].envelope.stream_id, "test");
                assert_eq!(records[0].envelope.system_timestamp_ns, Some(id as i64));
            }
            other => {
                let terminal = tokio::time::timeout(
                    WAIT,
                    handle.wait_terminal("synthetic", stream.generation),
                )
                .await;
                panic!(
                    "unexpected {other:?}; processing terminal: {terminal:?}; status: {:?}",
                    handle.status()
                );
            }
        }
    }
    assert!(
        observed
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .starts_with("marketforge-cpu-")
    );
    bounded(handle.stop(&stream)).await.unwrap();
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", stream.generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Stopped);
    assert_eq!(report.router_accepted_outputs, 8);
    assert_eq!(report.input_accounting.completed_events, 8);
    assert!(report.unprocessed.is_empty());
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    assert_eq!(handle.status().workers.registered, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controlled_startup_input_establishes_readiness_before_public_ingress() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (constructed, signal) = tokio::sync::oneshot::channel();
    let (ready, readiness) = tokio::sync::oneshot::channel();
    let starter = handle.clone();
    let start = tokio::spawn(async move {
        starter
            .start(request(move |context| {
                let _ = constructed.send(());
                Ok(PreparedProcessor {
                    processor: Box::new(Echo {
                        context,
                        count: 0,
                        threads: None,
                        ready: Some(ready),
                    }),
                    readiness: Box::pin(async move { readiness.await.map_err(|e| e.to_string()) }),
                })
            }))
            .await
    });
    bounded(signal).await.unwrap();
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Starting
    );
    publish(&rig.source, fixture::publication(1)).await;
    let stream = bounded(start).await.unwrap().unwrap();
    assert_eq!(
        handle.status().associations[0].phase,
        ProcessingPhase::Ready
    );
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("synthetic", stream.generation)
        .unwrap()
        .discard();
    rig.close().await;
}
struct Release(Option<mpsc::Sender<()>>);
impl Release {
    fn release(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        self.release();
    }
}
struct Block {
    context: ProcessorContext,
    started: Option<tokio::sync::oneshot::Sender<()>>,
    release: mpsc::Receiver<()>,
    panic: bool,
    emit: bool,
}
impl Processor for Block {
    fn run_turn(&mut self, _: usize) -> bool {
        false
    }
    fn try_run_turn(&mut self, _: usize) -> Result<bool, String> {
        let batch = self.context.input.take_work(1);
        if batch.is_empty() {
            batch.complete();
            return Ok(false);
        }
        if let Some(tx) = self.started.take() {
            let _ = tx.send(());
        }
        self.release.recv_timeout(Duration::from_secs(4)).unwrap();
        if self.panic {
            panic!("synthetic partial mutation");
        }
        if self.emit {
            let RouterDelivery::Publication { publication, .. } = &batch[0].delivery else {
                panic!()
            };
            self.context
                .output
                .try_emit(publication.as_ref().clone())
                .map_err(|e| e.reason.to_string())?;
        }
        batch.complete();
        Ok(false)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_stop_keeps_ownership_and_fences_before_executing_turn_finishes() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let mut subscriber = rig.subscribe().await;
    let (tx, started) = tokio::sync::oneshot::channel();
    let (release, rx) = mpsc::channel();
    let mut guard = Release(Some(release));
    let stream = bounded(handle.start(request(move |context| {
        Ok(PreparedProcessor::ready(Block {
            context,
            started: Some(tx),
            release: rx,
            panic: false,
            emit: true,
        }))
    })))
    .await
    .unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    bounded(started).await.unwrap();
    let stopping = handle.clone();
    let old = stream.clone();
    let stop = tokio::spawn(async move { stopping.stop(&old).await });
    bounded(handle.wait_phase("synthetic", stream.generation, ProcessingPhase::Retiring))
        .await
        .unwrap();
    stop.abort();
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Stopping
    );
    assert!(
        rig.registry
            .lock()
            .unwrap()
            .publication_authority("synthetic")
            .is_none()
    );
    assert!(stream.admission_closed());
    assert_eq!(handle.status().associations[0].input.in_flight_events, 1);
    guard.release();
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", stream.generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Failed);
    assert!(report.failure.is_some());
    assert_eq!(report.input_accounting.indeterminate_events, 1);
    assert_eq!(report.router_accepted_outputs, 0);
    report.discard();
    assert!(matches!(
        bounded(subscriber.recv()).await,
        Some(RouterDelivery::ContinuityLost(_))
    ));
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_panic_cleanup_marks_registry_failed_and_retains_inflight_accounting() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, started) = tokio::sync::oneshot::channel();
    let (release, rx) = mpsc::channel();
    let mut guard = Release(Some(release));
    let stream = bounded(handle.start(request(move |context| {
        Ok(PreparedProcessor::ready(Block {
            context,
            started: Some(tx),
            release: rx,
            panic: true,
            emit: false,
        }))
    })))
    .await
    .unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    bounded(started).await.unwrap();
    guard.release();
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    let status = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .clone();
    assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
    assert!(
        status
            .last_error
            .unwrap()
            .contains("synthetic partial mutation")
    );
    let report = handle.take_report("synthetic", stream.generation).unwrap();
    assert_eq!(report.input_accounting.indeterminate_events, 1);
    assert_eq!(report.input_accounting.completed_events, 0);
    assert_eq!(report.failure.unwrap().kind, ProcessorFailureKind::Panicked);
    report.discard();
    assert_eq!(handle.status().workers.retained_failures, 0);
    assert_eq!(handle.status().workers.registered, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_readiness_errors_and_panics_cleanup_without_false_running() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    for mode in 0..4 {
        let result = bounded(handle.start(request(move |context| match mode {
            0 => Err("factory returned error".into()),
            1 => panic!("factory unwind"),
            2 => Ok(PreparedProcessor {
                processor: Box::new(Echo {
                    context,
                    count: 0,
                    threads: None,
                    ready: None,
                }),
                readiness: Box::pin(async { Err("not ready".into()) }),
            }),
            _ => Ok(PreparedProcessor {
                processor: Box::new(Echo {
                    context,
                    count: 0,
                    threads: None,
                    ready: None,
                }),
                readiness: Box::pin(async { panic!("readiness unwind") }),
            }),
        })))
        .await;
        assert!(result.is_err());
        let status = rig
            .registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .clone();
        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
        bounded(handle.wait_terminal("synthetic", status.generation))
            .await
            .unwrap();
        handle
            .take_report("synthetic", status.generation)
            .unwrap()
            .discard();
        assert_eq!(handle.status().workers.registered, 0);
        assert_eq!(handle.status().accounted_events, 0);
    }
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_start_and_late_stop_do_not_change_new_registry_generation() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, constructed) = tokio::sync::oneshot::channel();
    let (release, rx) = mpsc::channel();
    let mut guard = Release(Some(release));
    let starter = handle.clone();
    let start = tokio::spawn(async move {
        starter
            .start(request(move |context| {
                let _ = tx.send(());
                rx.recv_timeout(Duration::from_secs(4)).unwrap();
                echo(context)
            }))
            .await
    });
    bounded(constructed).await.unwrap();
    let (old, new) = {
        let mut registry = rig.registry.lock().unwrap();
        let old = registry.status("synthetic").unwrap().generation;
        let stopping = registry.begin_stop("synthetic").unwrap();
        registry.mark_stopped("synthetic", stopping).unwrap();
        let new = registry.begin_start("synthetic").unwrap();
        registry.mark_running("synthetic", new).unwrap();
        (old, new)
    };
    guard.release();
    assert!(bounded(start).await.unwrap().is_err());
    bounded(handle.wait_terminal("synthetic", old))
        .await
        .unwrap();
    assert!(
        bounded(handle.stop_generation("synthetic", old))
            .await
            .is_err()
    );
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .generation,
        new
    );
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Running
    );
    assert_eq!(
        handle.take_report("synthetic", old).unwrap().disposition,
        TerminalDisposition::Superseded
    );
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_start_and_owner_drop_remain_owned_and_observably_abnormal() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, constructed) = tokio::sync::oneshot::channel();
    let starter = handle.clone();
    let start = tokio::spawn(async move {
        starter
            .start(request(move |context| {
                let _ = tx.send(());
                Ok(PreparedProcessor {
                    processor: Box::new(Echo {
                        context,
                        count: 0,
                        threads: None,
                        ready: None,
                    }),
                    readiness: Box::pin(std::future::pending()),
                })
            }))
            .await
    });
    bounded(constructed).await.unwrap();
    start.abort();
    let generation = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .generation;
    let Rig {
        service,
        router,
        registry,
        source,
    } = rig;
    drop(service);
    bounded(handle.wait_terminal("synthetic", generation))
        .await
        .unwrap();
    assert!(handle.status().abnormal_owner_drop);
    assert_eq!(
        registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Failed
    );
    handle
        .take_report("synthetic", generation)
        .unwrap()
        .discard();
    drop(source);
    router.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn input_saturation_fails_continuity_and_accounts_rejected_delivery() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, started) = tokio::sync::oneshot::channel();
    let (release, rx) = mpsc::channel();
    let mut guard = Release(Some(release));
    let mut spec = request(move |context| {
        Ok(PreparedProcessor::ready(Block {
            context,
            started: Some(tx),
            release: rx,
            panic: false,
            emit: false,
        }))
    });
    spec.input_limits.max_events = 1;
    let stream = bounded(handle.start(spec)).await.unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    bounded(started).await.unwrap();
    publish(&rig.source, fixture::publication(2)).await;
    bounded(handle.wait_phase("synthetic", stream.generation, ProcessingPhase::Retiring))
        .await
        .unwrap();
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Failed
    );
    guard.release();
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", stream.generation).unwrap();
    let rejected = report.rejected_router_input.as_ref().unwrap();
    assert_eq!(rejected.sequence, Some(2));
    assert!(rejected.discarded_complete_delivery);
    assert!(rejected.accounted_bytes > 0);
    assert_eq!(report.input_accounting.completed_events, 1);
    assert_eq!(report.input_accounting.rejected_events, 1);
    report.discard();
    rig.close().await;
}
struct Burst {
    context: ProcessorContext,
    expand: bool,
}
impl Processor for Burst {
    fn run_turn(&mut self, _: usize) -> bool {
        false
    }
    fn try_run_turn(&mut self, _: usize) -> Result<bool, String> {
        let batch = self.context.input.take_work(1);
        if batch.is_empty() {
            batch.complete();
            return Ok(false);
        }
        let RouterDelivery::Publication { publication, .. } = &batch[0].delivery else {
            panic!()
        };
        let mut value = publication.as_ref().clone();
        if self.expand {
            if let RouterPublication::Trades { records, .. } = &mut value {
                *records = std::iter::repeat_n(records[0].clone(), 100)
                    .collect::<Vec<_>>()
                    .into();
            }
        }
        self.context
            .output
            .try_emit(value.clone())
            .map_err(|e| e.reason.to_owned())?;
        if !self.expand {
            let error = self
                .context
                .output
                .try_emit(value)
                .err()
                .expect("startup output bound must reject second publication");
            assert!(
                matches!(&error.publication,RouterPublication::Trades{records,..} if records.len()==1)
            );
            return Err(error.reason.to_owned());
        }
        batch.complete();
        Ok(false)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_router_saturation_retains_original_without_reexecuting_processor() {
    let mut config = RouterConfig::default();
    config.ingress.bytes = 4096;
    let rig = Rig::new(SourceChannel::Trades, config);
    let handle = rig.handle();
    let stream = bounded(handle.start(request(|context| {
        Ok(PreparedProcessor::ready(Burst {
            context,
            expand: true,
        }))
    })))
    .await
    .unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", stream.generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Failed);
    assert!(report.reason.as_ref().unwrap().contains("capacity"));
    assert_eq!(report.input_accounting.completed_events, 1);
    assert_eq!(report.router_accepted_outputs, 0);
    assert_eq!(report.unpublished.len(), 1);
    assert!(
        matches!(&report.unpublished[0],RouterPublication::Trades{records,..} if records.len()==100)
    );
    assert!(handle.status().accounted_bytes > 4096);
    report.discard();
    assert_eq!(handle.status().accounted_bytes, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_output_limit_reports_operational_error_and_preserves_prior_output() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, constructed) = tokio::sync::oneshot::channel();
    let mut spec = request(move |context| {
        let _ = tx.send(());
        Ok(PreparedProcessor {
            processor: Box::new(Burst {
                context,
                expand: false,
            }),
            readiness: Box::pin(std::future::pending()),
        })
    });
    spec.output_limits.max_events = 1;
    let starter = handle.clone();
    let start = tokio::spawn(async move { starter.start(spec).await });
    bounded(constructed).await.unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    assert!(bounded(start).await.unwrap().is_err());
    let generation = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .generation;
    bounded(handle.wait_terminal("synthetic", generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", generation).unwrap();
    assert_eq!(report.failure.unwrap().kind, ProcessorFailureKind::Error);
    assert_eq!(report.input_accounting.indeterminate_events, 1);
    assert_eq!(report.output_accounting.rejected_events, 1);
    assert_eq!(report.unpublished.len(), 1);
    assert_eq!(report.router_accepted_outputs, 0);
    report.discard();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_cycles_release_registrations_subscriptions_and_bounded_reports() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let mut prior: Option<ProcessingStreamHandle> = None;
    for cycle in 0..20 {
        let stream = bounded(handle.start(request(echo))).await.unwrap();
        if let Some(old) = prior.take() {
            assert_ne!(stream.processor, old.processor);
            assert!(bounded(handle.stop(&old)).await.is_err());
            assert!(old.admission_closed());
        }
        bounded(handle.stop(&stream)).await.unwrap();
        bounded(handle.wait_terminal("synthetic", stream.generation))
            .await
            .unwrap();
        assert!(
            bounded(handle.start(request(echo))).await.is_err(),
            "terminal association must retain its reservation until report removal"
        );
        handle
            .take_report("synthetic", stream.generation)
            .unwrap()
            .discard();
        assert_eq!(handle.status().workers.registered, 0);
        assert_eq!(handle.status().workers.retained_failures, 0);
        assert_eq!(handle.status().accounted_events, 0);
        assert_eq!(rig.router.status().subscriptions, 0, "cycle {cycle}");
        prior = Some(stream);
    }
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_shutdown_timeout_is_observable_and_cleanup_remains_owned() {
    let mut rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, started) = tokio::sync::oneshot::channel();
    let (release, rx) = mpsc::channel();
    let mut guard = Release(Some(release));
    let stream = bounded(handle.start(request(move |context| {
        Ok(PreparedProcessor::ready(Block {
            context,
            started: Some(tx),
            release: rx,
            panic: false,
            emit: false,
        }))
    })))
    .await
    .unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    bounded(started).await.unwrap();
    let timed = bounded(rig.service.shutdown()).await.unwrap();
    assert!(!timed.complete);
    assert_eq!(timed.unfinished_processors, 1);
    assert!(timed.unfinished_workers > 0);
    assert_eq!(handle.status().pending_cleanup, 1);
    guard.release();
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    handle
        .take_report("synthetic", stream.generation)
        .unwrap()
        .discard();
    assert!(bounded(rig.service.shutdown()).await.unwrap().complete);
    rig.router.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_cancellation_alone_triggers_owned_cleanup() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, constructed) = tokio::sync::oneshot::channel();
    let starter = handle.clone();
    let start = tokio::spawn(async move {
        starter
            .start(request(move |context| {
                let _ = tx.send(());
                Ok(PreparedProcessor {
                    processor: Box::new(Echo {
                        context,
                        count: 0,
                        threads: None,
                        ready: None,
                    }),
                    readiness: Box::pin(std::future::pending()),
                })
            }))
            .await
    });
    bounded(constructed).await.unwrap();
    let generation = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .generation;
    start.abort();
    let _ = start.await;
    bounded(handle.wait_terminal("synthetic", generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Abandoned);
    assert!(report.reason.as_ref().unwrap().contains("canceled"));
    report.discard();
    assert_eq!(handle.status().workers.registered, 0);
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_depth_snapshot_and_delta_keep_boundaries_then_invalidation_fails() {
    use marketforge_engine::{
        live::integrity::{
            DepthAcceptedTimestamp, DepthSequenceBaseline, DepthTimestampComposition,
        },
        shared::domain::{book::BookLevel, canonical::BookSide},
    };
    let rig = Rig::new(SourceChannel::Depth, RouterConfig::default());
    let handle = rig.handle();
    let mut subscriber = rig.subscribe().await;
    let starting = handle.clone();
    let start = tokio::spawn(async move { starting.start(request(echo)).await });
    bounded(async {
        while handle.status().associations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let levels = (1..=128)
        .map(|price| {
            (
                BookSide::Bid,
                price.into(),
                BookLevel {
                    quantity_base: Some(2.into()),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: Some(1),
                },
            )
        })
        .collect::<Vec<_>>();
    let baseline = DepthSequenceBaseline {
        last_sequence: 100,
        cross_sequence: Some(120),
        synchronized: true,
    };
    let timestamp = DepthAcceptedTimestamp {
        latest_source: None,
        composition: DepthTimestampComposition::SingleMessage,
    };
    let events = vec![
        RouterDepthEvent::Snapshot {
            stream_generation: 7,
            book_generation: 3,
            baseline,
            levels: levels.clone().into(),
            timestamp,
        },
        RouterDepthEvent::Delta {
            stream_generation: 7,
            book_generation: 3,
            baseline,
            changes: levels[..3].to_vec().into(),
            timestamp,
        },
    ];
    publish(&rig.source, RouterPublication::Depth(events.into())).await;
    let stream = bounded(start).await.unwrap().unwrap();
    let RouterDelivery::Publication {
        sequence: 1,
        runtime_generation,
        publication,
        synchronized: true,
        event_valid: true,
        admission,
    } = bounded(subscriber.recv()).await.unwrap()
    else {
        panic!("expected atomic depth output")
    };
    assert_eq!(runtime_generation, stream.generation);
    assert_eq!(
        admission.semantics(),
        PublicationSemantics::CanonicalCompatibility
    );
    let RouterPublication::Depth(events) = publication.as_ref() else {
        panic!("wrong output type")
    };
    assert_eq!(events.len(), 2);
    assert!(
        matches!(&events[0],RouterDepthEvent::Snapshot{stream_generation:7,book_generation:3,levels:got,..} if got.as_ref()==levels.as_slice())
    );
    assert!(
        matches!(&events[1],RouterDepthEvent::Delta{changes,..} if changes.as_ref()==&levels[..3])
    );
    publish(
        &rig.source,
        RouterPublication::Depth(
            vec![RouterDepthEvent::Invalidated {
                stream_generation: 7,
                book_generation: 3,
            }]
            .into(),
        ),
    )
    .await;
    bounded(handle.wait_terminal("synthetic", stream.generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", stream.generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Failed);
    assert!(report.reason.as_ref().unwrap().contains("continuity"));
    assert_eq!(report.input_accounting.completed_events, 1);
    report.discard();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_old_output_capability_cannot_publish_in_replacement_generation() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let stream = bounded(handle.start(request(move |context| {
        let _ = tx.send(context.output.clone());
        echo(context)
    })))
    .await
    .unwrap();
    let old_output = bounded(rx).await.unwrap();
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("synthetic", stream.generation)
        .unwrap()
        .discard();
    let replacement = bounded(handle.start(request(echo))).await.unwrap();
    let rejected = old_output.try_emit(fixture::publication(9)).err().unwrap();
    assert!(
        matches!(rejected.publication,RouterPublication::Trades{records,..} if records[0].sequence==Some(9))
    );
    assert_eq!(
        rig.registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .generation,
        replacement.generation
    );
    assert_eq!(handle.status().accounted_events, 0);
    bounded(handle.stop(&replacement)).await.unwrap();
    handle
        .take_report("synthetic", replacement.generation)
        .unwrap()
        .discard();
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_derived_origin_preserves_source_identity_and_rechecks_authority() {
    // Nonblocking admission may encounter dispatch/revocation contention. Only
    // Busy is retryable; identity and authority assertions remain exact.
    async fn settled(
        producer: &ProducerHandle,
        publication: RouterPublication,
    ) -> Result<RouterAcceptance, PublishError> {
        bounded(async {
            loop {
                match producer.try_publish(publication.clone()) {
                    Err(PublishError::Busy) => tokio::task::yield_now().await,
                    result => break result,
                }
            }
        })
        .await
    }
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let generation = rig
        .registry
        .lock()
        .unwrap()
        .begin_start("synthetic")
        .unwrap();
    let authority = rig
        .registry
        .lock()
        .unwrap()
        .publication_authority("synthetic")
        .unwrap();
    assert!(matches!(
        rig.router.attach_producer(AttachProducerRequest {
            authority: authority.clone()
        }),
        Err(RouterError::UnknownStream)
    ));
    assert!(matches!(
        rig.router
            .attach_derived_producer(authority.clone(), "unrelated"),
        Err(RouterError::UnknownStream)
    ));
    let derived = rig
        .router
        .attach_derived_producer(authority, "test")
        .unwrap();
    let mut wrong = fixture::trade(1);
    wrong.envelope.stream_id = "synthetic".into();
    assert_eq!(
        settled(&derived, RouterPublication::trades(vec![wrong])).await,
        Err(PublishError::InvalidPublication)
    );
    publish(&derived, fixture::publication(1)).await;
    let stopped = rig
        .registry
        .lock()
        .unwrap()
        .begin_stop("synthetic")
        .unwrap();
    assert_eq!(
        settled(&derived, fixture::publication(2)).await,
        Err(PublishError::Revoked)
    );
    assert_eq!(rig.router.status().accepted, 1);
    rig.registry
        .lock()
        .unwrap()
        .mark_stopped("synthetic", stopped)
        .unwrap();
    assert_ne!(generation, stopped);
    publish(&rig.source, fixture::publication(3)).await;
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_output_rejection_still_fences_the_stream_automatically() {
    struct Ignore {
        context: ProcessorContext,
    }
    impl Processor for Ignore {
        fn run_turn(&mut self, _: usize) -> bool {
            let batch = self.context.input.take_work(1);
            if let Some(input) = batch.first() {
                let RouterDelivery::Publication { publication, .. } = &input.delivery else {
                    panic!()
                };
                assert!(
                    self.context
                        .output
                        .try_emit(publication.as_ref().clone())
                        .is_ok()
                );
                let rejected = self
                    .context
                    .output
                    .try_emit(publication.as_ref().clone())
                    .err()
                    .unwrap();
                assert_eq!(rejected.kind, OutputRejectionKind::Capacity);
                // Deliberately return success: the adapter must retain its own failure.
                drop(rejected);
            }
            batch.complete();
            false
        }
    }
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, constructed) = tokio::sync::oneshot::channel();
    let mut spec = request(move |context| {
        let _ = tx.send(());
        Ok(PreparedProcessor {
            processor: Box::new(Ignore { context }),
            readiness: Box::pin(std::future::pending()),
        })
    });
    spec.output_limits.max_events = 1;
    let starter = handle.clone();
    let start = tokio::spawn(async move { starter.start(spec).await });
    bounded(constructed).await.unwrap();
    publish(&rig.source, fixture::publication(1)).await;
    assert!(bounded(start).await.unwrap().is_err());
    let generation = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .generation;
    bounded(handle.wait_terminal("synthetic", generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Failed);
    assert_eq!(report.unpublished.len(), 1);
    assert_eq!(report.output_accounting.rejected_events, 1);
    assert_eq!(report.router_accepted_outputs, 0);
    report.discard();
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dependency_failure_during_factory_construction_fences_before_readiness() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let (tx, constructed) = tokio::sync::oneshot::channel();
    let (release, rx) = mpsc::channel();
    let mut guard = Release(Some(release));
    let starter = handle.clone();
    let start = tokio::spawn(async move {
        starter
            .start(request(move |context| {
                let _ = tx.send(());
                rx.recv_timeout(Duration::from_secs(4)).unwrap();
                echo(context)
            }))
            .await
    });
    bounded(constructed).await.unwrap();
    let generation = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .generation;
    let Rig {
        service,
        router,
        registry,
        source,
    } = rig;
    drop(source);
    bounded(handle.wait_phase("synthetic", generation, ProcessingPhase::Retiring))
        .await
        .unwrap();
    assert_eq!(
        registry
            .lock()
            .unwrap()
            .status("synthetic")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Failed
    );
    assert!(
        registry
            .lock()
            .unwrap()
            .publication_authority("synthetic")
            .is_none()
    );
    guard.release();
    assert!(bounded(start).await.unwrap().is_err());
    bounded(handle.wait_terminal("synthetic", generation))
        .await
        .unwrap();
    let report = handle.take_report("synthetic", generation).unwrap();
    assert_eq!(report.disposition, TerminalDisposition::Failed);
    assert!(report.reason.as_ref().unwrap().contains("continuity"));
    report.discard();
    let mut service = service;
    assert!(bounded(service.shutdown()).await.unwrap().complete);
    router.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_cannot_expose_ingress_after_its_registration_has_closed() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let handle = rig.handle();
    let result = bounded(handle.start(request(|context| {
        let input = context.input.clone();
        Ok(PreparedProcessor {
            processor: Box::new(Echo {
                context,
                count: 0,
                threads: None,
                ready: None,
            }),
            readiness: Box::pin(async move {
                input.close();
                Ok(())
            }),
        })
    })))
    .await;
    assert!(result.is_err());
    let status = rig
        .registry
        .lock()
        .unwrap()
        .status("synthetic")
        .unwrap()
        .clone();
    assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
    bounded(handle.wait_terminal("synthetic", status.generation))
        .await
        .unwrap();
    handle
        .take_report("synthetic", status.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().workers.registered, 0);
    rig.close().await;
}
