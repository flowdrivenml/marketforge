// Compiled as a Router-private unit module so tests can hold the actual admission
// locks without adding production accessors or exposing locks in the public API.
// The pipeline itself is the production WS manager / Source executor / Router.
use super::*;
use crate::live::{
    infrastructure::websocket::WebSocketManager,
    recovery::RecoveryHandle,
    stream::{
        registry::{StreamLifecycleState, StreamRuntimeRegistry},
        request::SourceChannel,
        source::*,
    },
};
use serde_json::{Value, json};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
mod rest_support {
    use crate as marketforge_engine;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/live/integration/rest/support.rs"
    ));
}
mod websocket_server {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/live/support/websocket_server.rs"
    ));
}
mod source_harness {
    use crate as marketforge_engine;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/live/integration/source/harness.rs"
    ));
}
mod harness {
    use crate as marketforge_engine;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/live/integration/router/harness.rs"
    ));
}
use source_harness::{DEADLINE, LocalSocket};

struct Pipeline {
    router: Arc<StreamRouter>,
    supervisor: SourceRuntimeSupervisor,
    socket: LocalSocket,
    consumers: Vec<SubscriptionHandle>,
    manager: Arc<WebSocketManager>,
    recovery: RecoveryHandle,
    recovery_task: JoinHandle<()>,
    waits: std::sync::mpsc::Receiver<&'static str>,
    channel: SourceChannel,
}
impl Pipeline {
    async fn start(channel: SourceChannel, lock_timeout: Duration) -> Self {
        Self::start_mode(channel, lock_timeout, false).await
    }
    async fn start_mode(channel: SourceChannel, lock_timeout: Duration, immediate: bool) -> Self {
        let profile = if channel == SourceChannel::Trades {
            "binance_spot"
        } else {
            "bybit_spot"
        };
        let mut socket = LocalSocket::start(profile).await;
        let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
        let limiters = rest_support::build_limiters(&configs, profile);
        let manager = source_harness::manager(&configs, profile, &limiters, &socket.url);
        let definition = harness::definition(profile, channel, "contended");
        let graph = Arc::new(StreamGraph::restore(vec![definition.clone()], vec![]).unwrap());
        let mut config = RouterConfig::default();
        config.source_lock_timeout = lock_timeout;
        let router = Arc::new(
            StreamRouter::new(
                config,
                RouterContext {
                    graph: graph.clone(),
                    demand: Arc::new(Mutex::new(DemandRegistry::new())),
                },
            )
            .unwrap(),
        );
        let (wait_tx, waits) = std::sync::mpsc::sync_channel(8);
        *lock(&router.shared.source_wait) = Some(wait_tx);
        let mut consumers = vec![];
        for name in ["a", "b"] {
            consumers.push(
                router
                    .subscribe(SubscribeRequest {
                        stream_id: "contended".into(),
                        owner: harness::owner(name),
                    })
                    .await
                    .unwrap(),
            );
        }
        let prepared =
            source_harness::builder(configs.clone(), SourceInitializationLimits::default())
                .prepare(definition)
                .unwrap();
        let (recovery, recovery_task) = source_harness::recovery(configs, profile, limiters);
        let executor = prepared
            .attach(manager.clone(), recovery.clone())
            .await
            .unwrap();
        let mut supervisor = SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph));
        if immediate {
            // Reproduce the original adapter's immediate try_publish -> String
            // error conversion through the production executor/supervisor.
            let slot = Arc::new(std::sync::OnceLock::<ProducerHandle>::new());
            let publisher = slot.clone();
            supervisor
                .start(
                    "contended",
                    executor,
                    |_| Ok(()),
                    move |payload| {
                        use crate::live::integrity::{
                            BoundTradeIngressOutcome, IntegrityOutcome, TradeControllerOutcome,
                        };
                        let SourceTradePublication::Integrity(IntegrityOutcome::Trades(
                            BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(
                                batch,
                            )),
                        )) = payload
                        else {
                            return Ok(());
                        };
                        if batch.trades.is_empty() {
                            return Ok(());
                        }
                        let producer = publisher.get().unwrap();
                        let result =
                            producer.try_publish(RouterPublication::trades(batch.trades.clone()));
                        if result == Err(PublishError::Busy) {
                            producer.shared.note_source_wait("immediate-busy");
                        }
                        result.map(|_| ()).map_err(|e| e.to_string())
                    },
                )
                .unwrap();
            let producer = router
                .attach_producer(AttachProducerRequest {
                    authority: supervisor
                        .registry()
                        .publication_authority("contended")
                        .unwrap(),
                })
                .unwrap();
            assert!(slot.set(producer).is_ok());
        } else {
            supervisor
                .start_routed("contended", executor, &router)
                .unwrap();
        }
        socket.ack().await;
        Self {
            router,
            supervisor,
            socket,
            consumers,
            manager,
            recovery,
            recovery_task,
            waits,
            channel,
        }
    }
    fn message(&self, index: u64) -> Value {
        if self.channel == SourceChannel::Trades {
            let mut native = serde_json::from_str::<Value>(include_str!(
                "../../support/data/source/trades.json"
            ))
            .unwrap()["binance_spot"]
                .clone();
            native["t"] = json!(native["t"].as_u64().unwrap() + index);
            native
        } else {
            json!({"topic":"orderbook.200.BTCUSDT","type":if index==0 {"snapshot"}else{"delta"},"ts":1720000000000_u64+index,"cts":1720000000000_u64+index,"data":{"s":"BTCUSDT","u":100+index,"seq":200+index,"b":[["80000","2"]],"a":[["100000","3"]]}})
        }
    }
    async fn published(&mut self, sequence: u64) {
        let expected_id = self.message(sequence - 1)["t"]
            .as_u64()
            .map(|id| id.to_string());
        for consumer in &mut self.consumers {
            match harness::receive(consumer).await {
                RouterDelivery::Publication {
                    sequence: actual,
                    publication,
                    synchronized,
                    ..
                } => {
                    assert_eq!(actual, sequence);
                    assert!(synchronized);
                    match publication.as_ref() {
                        RouterPublication::Trades { records, .. } => {
                            assert_eq!(records.len(), 1);
                            assert_eq!(records[0].trade_id, expected_id);
                            assert_eq!(records[0].envelope.stream_id, "contended");
                        }
                        RouterPublication::Depth(events) => {
                            assert_eq!(events.len(), 1);
                            if sequence == 1 {
                                assert!(matches!(events[0], RouterDepthEvent::Snapshot { .. }));
                            } else {
                                assert!(matches!(events[0], RouterDepthEvent::Delta { .. }));
                            }
                        }
                    }
                }
                other => panic!("{other:?}"),
            }
        }
        source_harness::wait_running(&mut self.supervisor, "contended").await;
        assert!(self.supervisor.is_active("contended"));
    }
    async fn finish(mut self, failed: bool) {
        if !failed {
            timeout(DEADLINE, self.supervisor.stop("contended"))
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(self.manager.stream_count().await, 0);
        self.router.shutdown().await.unwrap();
        assert_eq!(lock(&self.router.shared.context.demand).total_owners(), 0);
        assert_eq!(self.router.status().ingress_bytes, 0);
        assert_eq!(self.router.status().consumer_bytes, 0);
        self.manager.shutdown().await.unwrap();
        self.recovery.shutdown().await.unwrap();
        timeout(DEADLINE, self.recovery_task)
            .await
            .unwrap()
            .unwrap();
    }
}

// Owner thread holds an actual lock until the Source reports that its immediate
// acquisition failed. No sleeps, task scheduling guesses, or mock publisher.
struct HeldLock {
    release: std::sync::mpsc::SyncSender<()>,
    thread: std::thread::JoinHandle<()>,
}
impl HeldLock {
    async fn acquire(pipeline: &Pipeline, kind: &'static str) -> Self {
        let router = pipeline.router.clone();
        let authority = pipeline
            .supervisor
            .registry()
            .publication_authority("contended")
            .unwrap();
        let (ready, ready_rx) = oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            if kind == "state" {
                let _guard = router.shared.state.lock();
                ready.send(()).unwrap();
                release_rx.recv_timeout(DEADLINE).unwrap();
            } else {
                let _guard = authority.guard();
                ready.send(()).unwrap();
                release_rx.recv_timeout(DEADLINE).unwrap();
            }
        });
        timeout(DEADLINE, ready_rx).await.unwrap().unwrap();
        Self { release, thread }
    }
    fn release(self) {
        self.release.send(()).unwrap();
        self.thread.join().unwrap();
    }
}
async fn wait_for(receiver: &std::sync::mpsc::Receiver<&'static str>, expected: &'static str) {
    // Callback waits are synchronous and bounded; a separate OS thread receives
    // the observation, leaving Tokio workers free to execute the real pipeline.
    tokio::task::block_in_place(|| {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let actual = receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            if actual == expected {
                break;
            }
            // A normal dispatch/status critical section may briefly precede the
            // deliberately held authority lock. Still require the authority wait.
            assert!(
                expected == "authority" && actual == "state",
                "expected {expected}, got {actual}"
            );
        }
    });
}

async fn transient_contention(channel: SourceChannel, kind: &'static str) {
    let mut pipeline = Pipeline::start(channel, Duration::from_secs(1)).await;
    let held = HeldLock::acquire(&pipeline, kind).await;
    pipeline
        .socket
        .send
        .send(pipeline.message(0))
        .await
        .unwrap();
    wait_for(&pipeline.waits, kind).await;
    held.release();
    pipeline.published(1).await;
    // Another unique event establishes order; duplicate trade replay must not
    // publish again or consume another admission sequence.
    if channel == SourceChannel::Trades {
        pipeline
            .socket
            .send
            .send(pipeline.message(0))
            .await
            .unwrap();
    }
    pipeline
        .socket
        .send
        .send(pipeline.message(1))
        .await
        .unwrap();
    pipeline.published(2).await;
    assert_eq!(pipeline.router.status().accepted, 2);
    println!(
        "[{channel:?}/{kind}] actual contention witnessed, Source stayed Running, two ordered exact fanouts, cleanup verified"
    );
    pipeline.finish(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_trades_survive_state_lock_contention() {
    transient_contention(SourceChannel::Trades, "state").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_trades_survive_authority_lock_contention() {
    transient_contention(SourceChannel::Trades, "authority").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_depth_survives_state_lock_contention() {
    transient_contention(SourceChannel::Depth, "state").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_depth_survives_authority_lock_contention() {
    transient_contention(SourceChannel::Depth, "authority").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_contention_timeout_is_bounded_and_never_acknowledges_publication() {
    for kind in ["state", "authority"] {
        let mut pipeline = Pipeline::start(SourceChannel::Trades, Duration::from_millis(20)).await;
        let held = HeldLock::acquire(&pipeline, kind).await;
        pipeline
            .socket
            .send
            .send(pipeline.message(0))
            .await
            .unwrap();
        wait_for(&pipeline.waits, kind).await;
        // Keep the guard until the actual deadline result, not an arbitrary sleep.
        wait_for(&pipeline.waits, "timeout").await;
        held.release();
        timeout(DEADLINE, async {
            while pipeline.supervisor.is_active("contended") {
                pipeline.supervisor.reap_finished().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let status = pipeline.supervisor.registry().status("contended").unwrap();
        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
        assert!(
            status
                .last_error
                .as_ref()
                .unwrap()
                .contains("lock deadline exceeded")
        );
        assert_eq!(pipeline.router.status().accepted, 0);
        for consumer in &mut pipeline.consumers {
            assert!(matches!(
                harness::receive(consumer).await,
                RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
            ));
        }
        pipeline.finish(true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_busy_publication_reproduces_original_source_failure() {
    for kind in ["state", "authority"] {
        let mut pipeline =
            Pipeline::start_mode(SourceChannel::Trades, Duration::from_secs(1), true).await;
        let held = HeldLock::acquire(&pipeline, kind).await;
        pipeline
            .socket
            .send
            .send(pipeline.message(0))
            .await
            .unwrap();
        wait_for(&pipeline.waits, "immediate-busy").await;
        held.release();
        timeout(DEADLINE, async {
            while pipeline.supervisor.is_active("contended") {
                pipeline.supervisor.reap_finished().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let status = pipeline.supervisor.registry().status("contended").unwrap();
        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
        assert!(
            status
                .last_error
                .as_ref()
                .unwrap()
                .contains("admission is busy")
        );
        assert_eq!(pipeline.router.status().accepted, 0);
        println!(
            "[{kind}] original immediate Busy -> handoff failure -> executor cleanup -> Failed reproduced"
        );
        pipeline.finish(true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_survives_real_subscription_demand_cleanup_contention() {
    let mut pipeline = Pipeline::start(SourceChannel::Trades, Duration::from_secs(1)).await;
    let demand = pipeline.router.shared.context.demand.clone();
    let (held, held_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let owner = std::thread::spawn(move || {
        let _guard = lock(&demand);
        held.send(()).unwrap();
        release_rx.recv_timeout(DEADLINE).unwrap();
    });
    timeout(DEADLINE, held_rx).await.unwrap().unwrap();
    let closing = pipeline.consumers.pop().unwrap();
    let runtime = tokio::runtime::Handle::current();
    let closer = std::thread::spawn(move || runtime.block_on(closing.close()));
    wait_for(&pipeline.waits, "demand-release").await;
    // remove() has closed this queue and is now waiting for the demand mutex
    // while still owning Router state. This is ordinary production cleanup.
    pipeline
        .socket
        .send
        .send(pipeline.message(0))
        .await
        .unwrap();
    wait_for(&pipeline.waits, "state").await;
    release.send(()).unwrap();
    owner.join().unwrap();
    closer.join().unwrap().unwrap();
    pipeline.published(1).await;
    assert_eq!(
        lock(&pipeline.router.shared.context.demand).owner_count("contended"),
        1
    );
    assert_eq!(pipeline.router.status().accepted, 1);
    pipeline.finish(false).await;
}
