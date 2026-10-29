use super::rest_support;
use futures_util::{SinkExt, StreamExt};
use marketforge_engine::live::{
    config::{ExchangeConfigStore, FormatConfigStore},
    infrastructure::{limiter::factory::ExchangeLimiters, websocket::*},
    recovery::*,
    stream::{
        persistence::PersistedStreamDefinition,
        registry::{StreamGraph, StreamLifecycleState, StreamRuntimeRegistry},
        source::*,
    },
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};
pub const DEADLINE: Duration = Duration::from_secs(8);

pub struct LocalSocket {
    pub url: String,
    pub send: mpsc::Sender<Value>,
    pub acknowledged: mpsc::Receiver<Value>,
    task: JoinHandle<()>,
}
impl LocalSocket {
    pub async fn start(profile: &str) -> Self {
        let listener = super::websocket_server::listen().await;
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (send, mut messages) = mpsc::channel::<Value>(32);
        let (acknowledged_tx, acknowledged) = mpsc::channel(16);
        let profile = profile.to_owned();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                loop {
                    tokio::select! {
                        frame = socket.next() => {
                            match frame {
                                Some(Ok(Message::Text(text))) => {
                                    let request: Value = serde_json::from_str(text.as_str()).unwrap();
                                    let ack = if profile.starts_with("binance") { json!({"result":null,"id":request["id"]}) }
                                    else if profile.starts_with("bybit") { json!({"success":true,"op":request["op"],"req_id":request["req_id"]}) }
                                    else if profile.starts_with("gateio") { json!({"event":request["event"],"channel":request["channel"],"id":request["id"],"result":{"status":"success"}}) }
                                    else { json!({"event":request["op"],"arg":request["args"][0],"id":request["id"]}) };
                                    socket.send(Message::Text(ack.to_string().into())).await.unwrap();
                                    if acknowledged_tx.send(request).await.is_err() { return; }
                                }
                                Some(Ok(Message::Ping(payload))) => { let _ = socket.send(Message::Pong(payload)).await; }
                                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                        message = messages.recv() => {
                            let Some(message) = message else { return; };
                            if message == Value::Null {
                                let _ = socket.close(None).await;
                                break;
                            }
                            if socket.send(Message::Text(message.to_string().into())).await.is_err() { break; }
                        }
                    }
                }
            }
        });
        Self {
            url,
            send,
            acknowledged,
            task,
        }
    }
    pub async fn ack(&mut self) -> Value {
        timeout(DEADLINE, self.acknowledged.recv())
            .await
            .expect("subscription deadline")
            .expect("server stopped")
    }
}
impl Drop for LocalSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn builder(
    configs: Arc<ExchangeConfigStore>,
    limits: SourceInitializationLimits,
) -> SourceRuntimeBuilder {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");
    SourceRuntimeBuilder::new(
        configs,
        Arc::new(FormatConfigStore::load_dir(root).unwrap()),
        limits,
    )
    .unwrap()
}

pub fn manager(
    configs: &ExchangeConfigStore,
    profile: &str,
    limiters: &ExchangeLimiters,
    url: &str,
) -> Arc<WebSocketManager> {
    let exchange = configs.get_shared(profile).unwrap();
    let mut ack_bindings = BTreeMap::new();
    if profile.starts_with("okx") || profile.starts_with("bitget") {
        let fields = if profile.starts_with("okx") {
            vec!["channel", "instId"]
        } else {
            vec!["instType", "topic", "symbol"]
        };
        let binding = CompiledSubscriptionAckBinding::new(
            fields
                .iter()
                .map(|key| SubscriptionAckFieldBinding::object(0, *key, format!("arg.{key}")))
                .collect(),
        )
        .unwrap();
        ack_bindings.insert("trades".into(), binding.clone());
        ack_bindings.insert("depth".into(), binding);
    }
    let mut urls = BTreeMap::from([(None, url.to_owned())]);
    for route in exchange.websocket.routes.keys() {
        urls.insert(Some(route.clone()), url.to_owned());
    }
    for channel in exchange.channels.values() {
        if let Some(route) = &channel.route {
            urls.insert(Some(route.clone()), url.to_owned());
        }
    }
    Arc::new(
        WebSocketManager::from_runtime(WebSocketManagerRuntime {
            runtime_id: format!("source-test-{profile}"),
            profiles: vec![WebSocketProfileRuntime {
                heartbeat_plan: HeartbeatPlanCompiler::compile(&exchange).unwrap(),
                exchange,
                urls,
                compiled_limiters: limiters.profile(profile).unwrap().websocket.clone(),
                ack_bindings,
            }],
            shared_limiter: limiters.manager(),
            reconnect_policy: ReconnectPolicy::new(
                Duration::from_millis(5),
                Duration::from_millis(20),
                Duration::from_secs(10),
                Duration::from_secs(2),
                None,
            )
            .unwrap(),
            bootstrap_limits: ConnectionBootstrapLimits {
                maximum_pending: 16,
                acknowledgement_timeout: DEADLINE,
            },
            session_limits: ConnectionSessionLimits {
                inbound_capacity: 64,
                outbound_capacity: 32,
                command_capacity: 16,
                event_capacity: 32,
                dispatch_timeout: Duration::from_secs(2),
                shutdown_timeout: Duration::from_secs(2),
                driver: SubscriptionDriverLimits {
                    queue_timeout: Duration::from_secs(2),
                    receipt_timeout: Duration::from_secs(2),
                    maximum_early_frames: 64,
                },
            },
            event_capacity: 128,
            maximum_early_frames: 64,
        })
        .unwrap(),
    )
}

pub fn recovery(
    configs: Arc<ExchangeConfigStore>,
    profile: &str,
    limiters: ExchangeLimiters,
) -> (RecoveryHandle, JoinHandle<()>) {
    let (coordinator, handle) = RecoveryCoordinator::new(RecoveryCoordinatorConfig {
        command_capacity: 32,
        scheduler: RecoverySchedulerLimits {
            max_requests: 32,
            max_jobs: 16,
            max_concurrent_jobs: 4,
        },
    })
    .unwrap();
    let profile = profile.to_owned();
    let task = tokio::spawn(async move {
        let service = rest_support::build_service(&configs, &limiters, &profile);
        let executor = RecoveryRestExecutor::new(&service);
        coordinator
            .run(|job| {
                let executor = &executor;
                async move { executor.execute(&job).await }
            })
            .await;
    });
    (handle, task)
}

pub fn supervisor(definition: PersistedStreamDefinition) -> SourceRuntimeSupervisor {
    let graph = StreamGraph::restore(vec![definition], vec![]).unwrap();
    SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph))
}

pub async fn wait_running(supervisor: &mut SourceRuntimeSupervisor, id: &str) {
    timeout(DEADLINE, async {
        loop {
            supervisor.reap_finished().await.unwrap();
            let status = supervisor.registry().status(id).unwrap();
            assert_ne!(
                status.lifecycle,
                StreamLifecycleState::Failed,
                "{:?}",
                status.last_error
            );
            if status.lifecycle == StreamLifecycleState::Running {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source readiness deadline");
}
