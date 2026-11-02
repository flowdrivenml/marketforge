#![cfg(unix)]
#[path = "live/integration/source/harness.rs"]
mod acquisition;
#[path = "live/integration/derived/harness.rs"]
mod definitions;
#[path = "live/integration/rest/support.rs"]
mod rest_support;
#[path = "live/support/websocket_server.rs"]
mod websocket_server;
use marketforge_engine::{
    client::streaming::*,
    live::{
        application::*,
        config::*,
        stream::{persistence::*, registry::*, request::*},
    },
    stream_protocol::market::NativeEvent,
};
use serde_json::json;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(8), f)
        .await
        .expect("application test deadline")
}
fn source(id: &str, symbol: &str, base: &str, quote: &str, key: i64) -> PersistedStreamDefinition {
    let mut d = definitions::source::definition("binance_spot", SourceChannel::Trades, id);
    d.recording_stream_key = Some(key);
    let StreamDefinition::Source(r) = &mut d.definition else {
        unreachable!()
    };
    r.symbol = symbol.into();
    r.base_asset = Some(base.into());
    r.quote_asset = Some(quote.into());
    d
}
fn edges(defs: &[PersistedStreamDefinition]) -> Vec<StreamDependency> {
    defs.iter()
        .flat_map(|d| match &d.definition {
            StreamDefinition::Derived(r) => r
                .input_specs()
                .into_iter()
                .map(|s| StreamDependency {
                    derived_stream_id: d.stream_id.clone(),
                    input_stream_id: s.stream_id,
                    input_position: s.position as i32,
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}
fn options(
    dir: &tempfile::TempDir,
    url: &str,
    defs: Vec<PersistedStreamDefinition>,
    prometheus: bool,
) -> LiveApplicationOptions {
    options_profile(dir, url, defs, prometheus, "binance_spot")
}
fn options_profile(
    dir: &tempfile::TempDir,
    url: &str,
    defs: Vec<PersistedStreamDefinition>,
    prometheus: bool,
    profile: &str,
) -> LiveApplicationOptions {
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut config = general::GeneralConfig::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/general.toml"
    ))
    .unwrap();
    config.publication.socket_path = dir.path().join("data.sock").display().to_string();
    config.metrics.snapshot_interval_ms = 10;
    config.metrics.prometheus.enabled = prometheus;
    config.metrics.prometheus.cache_interval_ms = 10;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    config.metrics.prometheus.listen_address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let exchanges = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&exchanges, profile);
    let websocket = compile_websocket(
        &exchanges,
        &limiters,
        &config.defaults.recovery,
        &BTreeMap::from([(profile.into(), url.into())]),
    )
    .unwrap();
    LiveApplicationOptions {
        config,
        dependencies: edges(&defs),
        definitions: defs,
        exchanges,
        formats: Arc::new(
            FormatConfigStore::load_dir(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../data/.config/live/formats"
            ))
            .unwrap(),
        ),
        limiters,
        websocket,
    }
}
fn trade(symbol: &str, id: u64, time: u64, price: &str) -> serde_json::Value {
    json!({"e":"trade","E":time,"s":symbol,"t":id,"p":price,"q":"2","T":time,"m":false})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn application_drives_sources_merge_convert_metrics_and_shutdown() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let defs = vec![
        source("a", "ETHBTC", "ETH", "BTC", 1),
        source("b", "BTCUSDT", "BTC", "USDT", 2),
        source("c", "LTCBTC", "LTC", "BTC", 3),
        definitions::derived(
            "m",
            DerivedOperation::Merge,
            &["a", "c"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
        definitions::derived("v", DerivedOperation::Convert, &["m"], &["b"], None),
    ];
    let opts = options(&dir, &exchange.url, defs, true);
    let path = opts.config.publication.socket_path.clone();
    let mut app = LiveApplication::start(opts).await.unwrap();
    let handle = app.handle();
    // Incomplete HTTP headers retain bounded exporter connections while the
    // independent Source/Derived/Unix pipeline below continues to progress.
    use tokio::io::AsyncWriteExt;
    let mut slow_scrapes = Vec::new();
    for _ in 0..2 {
        let mut socket =
            tokio::net::TcpStream::connect(handle.status().prometheus_address.unwrap())
                .await
                .unwrap();
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        slow_scrapes.push(socket);
    }
    assert!(
        handle
            .status()
            .streams
            .iter()
            .all(|s| s.lifecycle == StreamLifecycleState::Stopped)
    );
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("v").await.unwrap();
    bounded(sub.recv()).await.unwrap();
    for _ in 0..3 {
        bounded(exchange.ack()).await;
    }
    bounded(async {
        while handle.status().sources.len() != 3
            || !handle.status().sources.iter().all(|source| {
                source.readiness
                    == marketforge_engine::live::stream::source::SourceReadiness::AwaitingData
            })
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    exchange.send.send(trade("BTCUSDT", 1, 90, "10")).unwrap();
    exchange.send.send(trade("ETHBTC", 1, 100, "3")).unwrap();
    exchange.send.send(trade("LTCBTC", 1, 100, "4")).unwrap();
    exchange.send.send(trade("BTCUSDT", 2, 200, "20")).unwrap();
    exchange.send.send(trade("ETHBTC", 2, 300, "5")).unwrap();
    exchange.send.send(trade("LTCBTC", 2, 300, "6")).unwrap();
    // Both event-time stages require strictly later input progress. These are
    // genuine native observations, not completion/flush controls.
    exchange.send.send(trade("ETHBTC", 3, 400, "7")).unwrap();
    exchange.send.send(trade("LTCBTC", 3, 400, "8")).unwrap();
    let publication = {
        use futures_util::FutureExt;
        match std::panic::AssertUnwindSafe(next_publication(&mut sub))
            .catch_unwind()
            .await
        {
            Ok(p) => p,
            Err(_) => {
                let _ = tokio::time::timeout(Duration::from_secs(1), async {
                    while !handle
                        .status()
                        .streams
                        .iter()
                        .any(|s| s.lifecycle == StreamLifecycleState::Failed)
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
                panic!("pipeline terminated: {:?}", handle.status());
            }
        }
    };
    assert!(matches!(
        &publication.records[0].native,
        NativeEvent::Trade(_)
    ));
    assert!(publication.records[0].valuation.is_some());
    assert!(publication.records[0].integrity.is_some());
    bounded(async {
        loop {
            if handle.latest_metrics().is_some_and(|m| {
                m.runtime.source_events_total >= 8 && m.runtime.derived_events_total > 0
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let snapshot = handle.latest_metrics().unwrap();
    assert_eq!(snapshot.runtime.source_events_total, 8);
    assert_eq!(snapshot.sources["a"].metrics.events_produced_total, 3);
    let address = handle.status().prometheus_address.unwrap();
    let http = reqwest::Client::new();
    let body = bounded(http.get(format!("http://{address}/metrics")).send())
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("marketforge_source_events_total 8"));
    assert!(body.contains("# TYPE marketforge_publication_queued_bytes gauge"));
    let old = snapshot.sources["a"].metrics.events_produced_total;
    for _ in 0..3 {
        let _ = http
            .get(format!("http://{address}/metrics"))
            .send()
            .await
            .unwrap();
    }
    assert_eq!(
        handle.latest_metrics().unwrap().sources["a"]
            .metrics
            .events_produced_total,
        old
    );
    let independent = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut reference_tail = independent.subscribe("b").await.unwrap();
    bounded(reference_tail.recv()).await.unwrap();
    let reference_generation = h_generation(&handle, "b");
    sub.close().await.unwrap();
    client.close().await.unwrap();
    bounded(async {
        while !handle.status().streams.iter().all(|s| {
            if s.stream_id == "b" {
                s.is_ready()
            } else {
                s.lifecycle == StreamLifecycleState::Stopped
            }
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(h_generation(&handle, "b"), reference_generation);
    exchange.send.send(trade("BTCUSDT", 3, 300, "20")).unwrap();
    let p = next_publication(&mut reference_tail).await;
    assert_eq!(p.router_sequence, 3);
    reference_tail.close().await.unwrap();
    independent.close().await.unwrap();
    bounded(async {
        while handle
            .status()
            .streams
            .iter()
            .any(|s| s.lifecycle.occupies_runtime())
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(app.shutdown().await.complete);
    assert!(!std::path::Path::new(&path).exists());
}
#[tokio::test]
async fn unsupported_recording_and_binding_failure_are_preflighted() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(
        &dir,
        "ws://127.0.0.1:1",
        vec![source("a", "BTCUSDT", "BTC", "USDT", 1)],
        false,
    );
    opts.config.recording.enabled = true;
    assert!(
        LiveApplication::validate(&opts)
            .unwrap_err()
            .to_string()
            .contains("durable identity")
    );
    assert!(!std::path::Path::new(&opts.config.publication.socket_path).exists());
    opts.config.recording.enabled = false;
    std::fs::write(&opts.config.publication.socket_path, "preserve").unwrap();
    assert!(LiveApplication::start(opts).await.is_err());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("data.sock")).unwrap(),
        "preserve"
    );
}
#[test]
fn executable_validation_is_offline_and_rejects_invalid_configuration() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_marketforge-live"))
        .args([
            "--validate-only",
            "--config-dir",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"),
            "--profiles",
            "binance_spot",
            "--rest-weight",
            "binance_spot=6000/60",
        ])
        .env_remove("MARKETFORGE_DATABASE_URL")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_marketforge-live"))
        .args(["--validate-only", "--config-dir", "/missing/config"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

struct LocalExchange {
    on_subscribe: Arc<std::sync::Mutex<std::collections::BTreeMap<String, serde_json::Value>>>,
    url: String,
    send: tokio::sync::broadcast::Sender<serde_json::Value>,
    acks: tokio::sync::mpsc::Receiver<()>,
    task: tokio::task::JoinHandle<()>,
}
impl LocalExchange {
    async fn start() -> Self {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (send, _) = tokio::sync::broadcast::channel::<serde_json::Value>(128);
        let broadcast = send.clone();
        let on_subscribe = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::<
            String,
            serde_json::Value,
        >::new()));
        let scripts = on_subscribe.clone();
        let (tx, acks) = tokio::sync::mpsc::channel(128);
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {accepted=listener.accept()=>{let(stream,_)=accepted.unwrap();let mut messages=broadcast.subscribe();let tx=tx.clone();let scripts=scripts.clone();tasks.spawn(async move{let Ok(mut socket)=tokio_tungstenite::accept_async(stream).await else{return};let mut topics=std::collections::BTreeSet::<String>::new();loop{tokio::select!{frame=socket.next()=>match frame {Some(Ok(Message::Text(text)))=>{let request:serde_json::Value=serde_json::from_str(&text).unwrap();let action=request["method"].as_str().or(request["op"].as_str()).unwrap_or("");if let Some(args)=request["params"].as_array().or(request["args"].as_array()){for topic in args.iter().filter_map(|v|v.as_str()){if action.eq_ignore_ascii_case("subscribe"){topics.insert(topic.to_string());}else if action.eq_ignore_ascii_case("unsubscribe"){topics.remove(topic);}}}if socket.send(Message::Text((if request.get("op").is_some(){json!({"success":true,"op":request["op"],"req_id":request["req_id"]})}else{json!({"result":null,"id":request["id"]})}).to_string().into())).await.is_err(){break;}if request.get("method").and_then(|v|v.as_str())==Some("SUBSCRIBE") || request.get("op").and_then(|v|v.as_str())==Some("subscribe"){let payloads={let mut scripts=scripts.lock().unwrap();topics.iter().filter_map(|topic|scripts.remove(topic)).collect::<Vec<_>>()};for payload in payloads {if socket.send(Message::Text(payload.to_string().into())).await.is_err(){return;}}let _=tx.send(()).await;}},Some(Ok(Message::Ping(b)))=>{let _=socket.send(Message::Pong(b)).await;},Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,_=>()},message=messages.recv()=>{let Ok(message)=message else{break};let wanted=if let Some(topic)=message["topic"].as_str(){topics.contains(topic)}else if let Some(symbol)=message["s"].as_str(){let prefix=format!("{}@",symbol.to_lowercase());topics.iter().any(|t|t.starts_with(&prefix))}else{false};if !wanted {continue;}if socket.send(Message::Text(message.to_string().into())).await.is_err(){break;}}}}});},_=tasks.join_next(),if !tasks.is_empty()=>()}
            }
        });
        Self {
            on_subscribe,
            url,
            send,
            acks,
            task,
        }
    }
    async fn ack(&mut self) {
        bounded(self.acks.recv()).await.unwrap();
    }
}
impl Drop for LocalExchange {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn recorder_counts_confirmed_commit_and_failed_attempt_at_writer_boundary() {
    use marketforge_engine::{live::recording::*, shared::domain::canonical::CanonicalEvent};
    struct Writer {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        fail: bool,
    }
    impl RecordingBatchWriter for Writer {
        fn write_batch<'a>(
            &'a self,
            events: &'a [RecordingEvent],
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<BatchWriteResult, RecordingError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                if self.fail {
                    Err(RecordingError::InvalidConfiguration(
                        "controlled ambiguous write failure".into(),
                    ))
                } else {
                    Ok(BatchWriteResult {
                        records_written: events.len(),
                    })
                }
            })
        }
    }
    for fail in [false, true] {
        let writer = Arc::new(Writer {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            fail,
        });
        let (control, task) = PostgresRecorder::start_with_writer(
            writer.clone(),
            general::RecordingConfig {
                enabled: true,
                batch_max_records: 1,
                batch_max_delay_ms: 100,
            },
            2,
        )
        .unwrap();
        let metrics = control.metrics();
        control
            .submit(RecordingEvent {
                stream_key: 1,
                log_epoch: 77,
                event_offset: 1,
                event: CanonicalEvent::Trade(definitions::source::trade(1)),
            })
            .await
            .unwrap();
        bounded(writer.entered.notified()).await;
        assert_eq!(metrics.pending_records(), 1);
        assert_eq!(metrics.records_written_total(), 0);
        writer.release.notify_one();
        if !fail {
            control.shutdown().await.unwrap();
            assert!(bounded(task).await.unwrap().is_ok());
            assert_eq!(metrics.records_written_total(), 1);
        } else {
            assert!(bounded(task).await.unwrap().is_err());
            assert_eq!(metrics.records_written_total(), 0);
        }
        assert_eq!(metrics.pending_records(), 0);
        assert_eq!(metrics.write_failures_total(), u64::from(fail));
    }
}
#[tokio::test]
async fn exporter_bounds_http_and_reads_only_completed_snapshots() {
    use marketforge_engine::live::metrics::{prometheus::*, *};
    let collector = Arc::new(MetricsSnapshotCollector::new());
    let sampling = Arc::new(SamplingState::default());
    let config = general::PrometheusConfig {
        enabled: true,
        listen_address: "127.0.0.1:0".into(),
        path: "/metrics".into(),
        max_concurrent_requests: 2,
        max_response_bytes: 128,
        cache_interval_ms: 10,
    };
    let server = PrometheusServer::bind(config, collector.clone(), sampling.clone())
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let url = format!("http://{}/metrics", server.address);
    assert_eq!(bounded(http.get(&url).send()).await.unwrap().status(), 503);
    let snapshot = collector
        .collect(
            &marketforge_engine::live::metrics::RuntimeMetrics::new(),
            &PublicationMetrics::new(),
            &RecordingMetrics::new(),
            &[],
            &[],
        )
        .unwrap();
    sampling.success(Duration::ZERO);
    for _ in 0..3 {
        let response = bounded(http.get(&url).send()).await.unwrap();
        assert_eq!(response.status(), 503);
        assert!(response.text().await.unwrap().contains("exceeds"));
    }
    assert_eq!(collector.latest().unwrap().revision, snapshot.revision);
    assert_eq!(bounded(http.post(&url).send()).await.unwrap().status(), 404);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn always_on_manual_demand_retry_and_generation_metrics_are_isolated() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut a = source("a", "ETHBTC", "ETH", "BTC", 1);
    a.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let opts = options(
        &dir,
        &exchange.url,
        vec![a, source("b", "BTCUSDT", "BTC", "USDT", 2)],
        false,
    );
    let mut app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    exchange.ack().await;
    assert!(!h.status().streams.iter().any(StreamRuntimeStatus::is_ready));
    let owner = DemandOwner::Manual {
        session_id: "other-root".into(),
    };
    h.demand("b", owner.clone(), true).await.unwrap();
    exchange.ack().await;
    bounded(async {
        while h.status().sources.len() != 2
            || !h.status().sources.iter().all(|s| {
                s.readiness
                    == marketforge_engine::live::stream::source::SourceReadiness::AwaitingData
            })
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    exchange.send.send(trade("ETHBTC", 1, 100, "3")).unwrap();
    exchange.send.send(trade("BTCUSDT", 1, 100, "10")).unwrap();
    bounded(async {
        while !h.status().streams.iter().all(StreamRuntimeStatus::is_ready) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let old = h
        .status()
        .streams
        .iter()
        .find(|s| s.stream_id == "a")
        .unwrap()
        .generation;
    h.demand("b", owner, false).await.unwrap();
    bounded(async {
        while h
            .status()
            .streams
            .iter()
            .find(|s| s.stream_id == "b")
            .unwrap()
            .lifecycle
            != StreamLifecycleState::Stopped
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        h.status()
            .streams
            .iter()
            .find(|s| s.stream_id == "a")
            .unwrap()
            .is_ready()
    );
    h.retry_stream("a").await.unwrap();
    wait_new_source(&h, "a", old).await;
    exchange.send.send(trade("ETHBTC", 1, 200, "4")).unwrap();
    bounded(async {
        while !h
            .status()
            .streams
            .iter()
            .any(|a| a.stream_id == "a" && a.generation.get() > old.get() && a.is_ready())
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    bounded(async {
        loop {
            if h.latest_metrics().is_some_and(|m| {
                m.sources.get("a").is_some_and(|a| {
                    a.generation > old.get() && a.metrics.events_produced_total == 1
                }) && m.runtime.source_events_total == 3
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(app.shutdown().await.complete);
}
#[tokio::test]
async fn metrics_disabled_and_quiet_source_shutdown_are_explicit() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut a = source("a", "BTCUSDT", "BTC", "USDT", 1);
    a.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let mut opts = options(&dir, &exchange.url, vec![a], false);
    opts.config.metrics.enabled = false;
    opts.config.runtime.processing = None;
    let mut app = LiveApplication::start(opts).await.unwrap();
    exchange.ack().await;
    assert!(app.handle().latest_metrics().is_none());
    assert!(
        !app.handle()
            .status()
            .streams
            .iter()
            .any(StreamRuntimeStatus::is_ready)
    );
    assert!(app.shutdown().await.complete);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn originating_failure_and_retry_do_not_restart_independent_source() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut a = source("a", "ETHBTC", "ETH", "BTC", 1);
    a.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let mut b = source("b", "LTCBTC", "LTC", "BTC", 2);
    b.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let mut m = definitions::derived(
        "m",
        DerivedOperation::Merge,
        &["a", "b"],
        &[],
        Some(MergeOrdering::ArrivalOrder),
    );
    m.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let opts = options(&dir, &exchange.url, vec![a, b, m], false);
    let mut app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    for _ in 0..2 {
        exchange.ack().await;
    }
    bounded(async {
        while h.status().sources.len() != 2
            || !h.status().sources.iter().all(|s| {
                s.readiness
                    == marketforge_engine::live::stream::source::SourceReadiness::AwaitingData
            })
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    exchange.send.send(trade("LTCBTC", 1, 100, "10")).unwrap();
    exchange
        .send
        .send(trade("ETHBTC", 1, 100, "invalid-price"))
        .unwrap();
    bounded(async {
        while !h
            .status()
            .streams
            .iter()
            .any(|s| s.stream_id == "a" && s.lifecycle == StreamLifecycleState::Failed)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let previous = h.status();
    let old = previous
        .streams
        .iter()
        .find(|s| s.stream_id == "a")
        .unwrap()
        .generation;
    let independent = previous
        .streams
        .iter()
        .find(|s| s.stream_id == "b")
        .unwrap()
        .generation;
    assert!(
        previous
            .streams
            .iter()
            .find(|s| s.stream_id == "a")
            .unwrap()
            .last_error
            .as_ref()
            .unwrap()
            .contains("normalization")
    );
    h.retry_stream("a").await.unwrap();
    wait_new_source(&h, "a", old).await;
    exchange.send.send(trade("ETHBTC", 1, 200, "4")).unwrap();
    bounded(async {
        while !h.status().streams.iter().all(StreamRuntimeStatus::is_ready) {
            let current = h.status();
            for stream in &current.streams {
                if stream.lifecycle == StreamLifecycleState::Failed
                    && stream.generation.get()
                        > previous
                            .streams
                            .iter()
                            .find(|s| s.stream_id == stream.stream_id)
                            .unwrap()
                            .generation
                            .get()
                {
                    panic!("replacement failed: {current:?}");
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let current = h.status();
    assert!(
        current
            .streams
            .iter()
            .find(|s| s.stream_id == "a")
            .unwrap()
            .generation
            .get()
            > old.get()
    );
    assert_eq!(
        current
            .streams
            .iter()
            .find(|s| s.stream_id == "b")
            .unwrap()
            .generation,
        independent
    );
    bounded(async {
        while !h
            .latest_metrics()
            .is_some_and(|s| s.runtime.source_normalization_errors_total == 1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(app.shutdown().await.complete);
}
#[tokio::test]
async fn quiet_readiness_deadline_is_a_failure_and_shutdown_commands_are_owned() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut a = source("a", "BTCUSDT", "BTC", "USDT", 1);
    a.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let mut opts = options(&dir, &exchange.url, vec![a], false);
    opts.config.runtime.application.source_readiness_timeout_ms = 100;
    let mut app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    exchange.ack().await;
    bounded(async {
        while !h
            .status()
            .streams
            .iter()
            .any(|s| s.lifecycle == StreamLifecycleState::Failed)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        h.status().streams[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("readiness deadline")
    );
    h.request_shutdown().await.unwrap();
    let first = app.shutdown().await;
    assert!(first.complete, "{first:?}");
    assert!(app.shutdown().await.complete);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in small local application sampling overhead assessment"]
async fn application_sampling_measurement() {
    for enabled in [false, true] {
        let mut exchange = LocalExchange::start().await;
        let dir = tempfile::tempdir().unwrap();
        let opts = options(
            &dir,
            &exchange.url,
            vec![source("a", "BTCUSDT", "BTC", "USDT", 1)],
            false,
        );
        let mut opts = opts;
        opts.config.metrics.enabled = enabled;
        let path = opts.config.publication.socket_path.clone();
        let mut app = LiveApplication::start(opts).await.unwrap();
        let client = MarketForgeStreamClient::connect_unix(path, ClientOptions::default())
            .await
            .unwrap();
        let mut sub = client.subscribe("a").await.unwrap();
        bounded(sub.recv()).await.unwrap();
        exchange.ack().await;
        let start = std::time::Instant::now();
        let mut times = vec![];
        for id in 1..=100 {
            let now = std::time::Instant::now();
            exchange.send.send(trade("BTCUSDT", id, 100, "10")).unwrap();
            loop {
                match bounded(sub.recv()).await.unwrap() {
                    Some(StreamMessage::Publication(_)) => break,
                    Some(StreamMessage::End(end)) => panic!("unexpected terminal: {end:?}"),
                    None => panic!("unexpected EOF"),
                    _ => tokio::task::yield_now().await,
                }
            }
            times.push(now.elapsed().as_nanos());
        }
        times.sort_unstable();
        println!(
            "sampling={enabled} publications/s={:.0} records/s={:.0} latency_ns median={} p95={} p99={}",
            100. / start.elapsed().as_secs_f64(),
            100. / start.elapsed().as_secs_f64(),
            times[50],
            times[95],
            times[99]
        );
        sub.close().await.unwrap();
        client.close().await.unwrap();
        assert!(app.shutdown().await.complete);
    }
}

fn h_generation(h: &LiveApplicationHandle, id: &str) -> RuntimeGeneration {
    h.status()
        .streams
        .iter()
        .find(|s| s.stream_id == id)
        .unwrap()
        .generation
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_timeout_retains_cleanup_ownership_until_socket_can_progress() {
    use marketforge_engine::stream_protocol::{self as p, codec::*};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let source = definitions::source::definition("bybit_spot", SourceChannel::Trades, "a");
    let mut opts = options_profile(&dir, &exchange.url, vec![source], false, "bybit_spot");
    opts.config.runtime.shutdown_timeout_seconds = 1;
    opts.config.publication.streaming.write_timeout_ms = 5000;
    opts.config.publication.streaming.shutdown_timeout_ms = 3000;
    let path = opts.config.publication.socket_path.clone();
    let mut app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    let mut socket = tokio::net::UnixStream::connect(&path).await.unwrap();
    write_frame(
        &mut socket,
        &p::Envelope::control(
            1,
            p::Message::Hello {
                versions: vec![1],
                schemas: vec![1],
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    read_frame(&mut socket, 1048576).await.unwrap().unwrap();
    write_frame(
        &mut socket,
        &p::Envelope::control(
            2,
            p::Message::Subscribe {
                stream_id: "a".into(),
                subscription_id: 1,
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    read_frame(&mut socket, 1048576).await.unwrap().unwrap();
    read_frame(&mut socket, 1048576).await.unwrap().unwrap();
    exchange.ack().await;
    exchange.send.send(json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1720000000000_u64,"data":(0..400).map(|i|json!({"T":1720000000000_u64,"s":"BTCUSDT","S":"Buy","v":"2","p":"80000","i":format!("{}-{i}","x".repeat(110)),"seq":i+100,"BT":false})).collect::<Vec<_>>()})).unwrap();
    read_frame(&mut socket, 1048576).await.unwrap().unwrap();
    let mut prefix = [0; 4];
    bounded(socket.read_exact(&mut prefix)).await.unwrap();
    assert!(u32::from_be_bytes(prefix) > 200000);
    let first = app.shutdown().await;
    assert!(!first.complete && first.cleanup_continues, "{first:?}");
    assert!(first.discarded_publications.is_none());
    let mut remaining = vec![];
    bounded((&mut socket).take(2 * 1048576).read_to_end(&mut remaining))
        .await
        .unwrap();
    let _ = socket.shutdown().await;
    let final_report = app.shutdown().await;
    assert!(final_report.complete, "{final_report:?}");
    assert!(!final_report.cleanup_continues);
    assert!(!std::path::Path::new(&path).exists());
    assert_eq!(
        h.latest_metrics()
            .unwrap()
            .runtime
            .incomplete_shutdown_total,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_and_late_depth_consumers_get_current_authoritative_baselines() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut depth = definitions::source::definition("bybit_spot", SourceChannel::Depth, "d");
    depth.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let mut trades = definitions::source::definition("bybit_spot", SourceChannel::Trades, "t");
    trades.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let defs = vec![
        depth,
        trades,
        definitions::derived(
            "cold",
            DerivedOperation::Merge,
            &["d", "t"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ),
        definitions::derived(
            "late",
            DerivedOperation::Merge,
            &["d", "t"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ),
    ];
    let opts = options_profile(&dir, &exchange.url, defs, false, "bybit_spot");
    let path = opts.config.publication.socket_path.clone();
    let mut app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut cold = client.subscribe("cold").await.unwrap();
    bounded(cold.recv()).await.unwrap();
    for _ in 0..2 {
        exchange.ack().await;
    }
    exchange.send.send(json!({"topic":"orderbook.200.BTCUSDT","type":"snapshot","ts":1720000000000_u64,"cts":1720000000000_u64,"data":{"s":"BTCUSDT","u":100,"seq":200,"b":[["80000","2"]],"a":[["100000","3"]]}})).unwrap();
    exchange.send.send(json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1720000000000_u64,"data":[{"T":1720000000000_u64,"s":"BTCUSDT","S":"Buy","v":"2","p":"80000","i":"one","seq":1,"BT":false}]})).unwrap();
    bounded(async {
        while !h
            .status()
            .streams
            .iter()
            .filter(|s| s.stream_id != "late")
            .all(StreamRuntimeStatus::is_ready)
        {
            assert!(
                h.status()
                    .streams
                    .iter()
                    .all(|s| s.lifecycle != StreamLifecycleState::Failed),
                "{:?}",
                h.status()
            );
            tokio::task::yield_now().await;
        }
    })
    .await;
    let old_generation = h_generation(&h, "d");
    let mut late = client.subscribe("late").await.unwrap();
    bounded(late.recv()).await.unwrap();
    let baseline=bounded(async{loop{let p=next_publication(&mut late).await;if p.records.iter().any(|r|matches!(&r.native,NativeEvent::Depth(d) if d.kind=="snapshot_omitted" && d.operations.is_empty())){break p;}}}).await;
    assert!(baseline.records.iter().all(|r| r.valuation.is_none()));
    bounded(async {
        while !h
            .status()
            .streams
            .iter()
            .find(|s| s.stream_id == "late")
            .unwrap()
            .is_ready()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(h_generation(&h, "d"), old_generation);
    bounded(async {
        while !h.latest_metrics().is_some_and(|s| {
            s.sources
                .get("d")
                .is_some_and(|d| d.metrics.depth_events_accepted_total == 1)
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(
        h.latest_metrics().unwrap().sources["d"]
            .metrics
            .depth_levels_accepted_total,
        2
    );
    late.close().await.unwrap();
    cold.close().await.unwrap();
    client.close().await.unwrap();
    assert!(app.shutdown().await.complete);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_native_batch_counts_ten_source_events_and_thirty_delivery_admissions() {
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let source = definitions::source::definition("bybit_spot", SourceChannel::Trades, "a");
    let opts = options_profile(&dir, &exchange.url, vec![source], false, "bybit_spot");
    let path = opts.config.publication.socket_path.clone();
    let mut app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    let mut consumers = vec![];
    for _ in 0..3 {
        let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
            .await
            .unwrap();
        let mut sub = client.subscribe("a").await.unwrap();
        bounded(sub.recv()).await.unwrap();
        consumers.push((client, sub));
    }
    exchange.ack().await;
    exchange.send.send(json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1720000000000_u64,"data":(0..10).map(|i|json!({"T":1720000000000_u64,"s":"BTCUSDT","S":"Buy","v":"2","p":"80000","i":format!("trade-{i}"),"seq":i+100,"BT":false})).collect::<Vec<_>>()})).unwrap();
    for (_, sub) in &mut consumers {
        let p = next_publication(sub).await;
        assert_eq!(p.records.len(), 10);
    }
    bounded(async {
        while !h
            .latest_metrics()
            .is_some_and(|s| s.publication.events_queued_total == 30)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let snapshot = h.latest_metrics().unwrap();
    assert_eq!(snapshot.runtime.source_events_total, 10);
    assert_eq!(snapshot.sources["a"].metrics.events_produced_total, 10);
    assert_eq!(snapshot.sources["a"].metrics.messages_received_total, 1);
    assert!(snapshot.publication.frames_written_total >= 6);
    assert!(snapshot.publication.bytes_written_total > 0);
    for (client, mut sub) in consumers {
        sub.close().await.unwrap();
        client.close().await.unwrap();
    }
    assert!(app.shutdown().await.complete);
    let final_metrics = h.latest_metrics().unwrap();
    assert_eq!(final_metrics.publication.connections_active, 0);
    assert_eq!(final_metrics.publication.subscriptions_active, 0);
    assert_eq!(final_metrics.publication.queued_bytes_total, 0);
    assert_eq!(final_metrics.runtime.source_events_total, 10);
}
#[tokio::test]
async fn executable_signal_shutdown_owns_actual_services_without_database() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let opts = options(&dir, "ws://127.0.0.1:1", vec![], false);
    let general = dir.path().join("general.toml");
    std::fs::write(&general, toml::to_string(&opts.config).unwrap()).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_marketforge-live"))
        .args([
            "--empty-definitions",
            "--config-dir",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"),
            "--general",
            general.to_str().unwrap(),
            "--profiles",
            "binance_spot",
            "--rest-weight",
            "binance_spot=6000/60",
        ])
        .env_remove("MARKETFORGE_DATABASE_URL")
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    bounded(async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .expect("child exited before serving");
            if line.contains("application_serving") {
                break;
            }
        }
    })
    .await;
    assert!(std::path::Path::new(&opts.config.publication.socket_path).exists());
    assert!(
        tokio::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(bounded(child.wait()).await.unwrap().success());
    assert!(!std::path::Path::new(&opts.config.publication.socket_path).exists());
}

#[tokio::test]
async fn dropping_application_requests_owned_cleanup_and_rejects_unsupported_actions() {
    use marketforge_engine::live::control::*;
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut source = source("a", "BTCUSDT", "BTC", "USDT", 1);
    source.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let opts = options(&dir, &exchange.url, vec![source], false);
    let path = opts.config.publication.socket_path.clone();
    let app = LiveApplication::start(opts).await.unwrap();
    let h = app.handle();
    exchange.ack().await;
    assert!(
        h.stream_action(StreamActionRequest {
            stream_id: "a".into(),
            action: StreamAction::Reconnect
        })
        .await
        .is_err()
    );
    drop(app);
    bounded(async {
        while h.status().shutdown_report.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(h.status().shutdown_report.unwrap().complete);
    assert!(!std::path::Path::new(&path).exists());
    assert_eq!(
        h.latest_metrics().unwrap().publication.connections_active,
        0
    );
}

#[tokio::test]
async fn cancelled_retry_caller_does_not_abandon_accepted_lifecycle_work() {
    use std::{future::Future, task::Poll};
    let mut exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut definition = source("a", "ETHBTC", "ETH", "BTC", 1);
    definition.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let mut app = LiveApplication::start(options(&dir, &exchange.url, vec![definition], false))
        .await
        .unwrap();
    let h = app.handle();
    exchange.ack().await;
    bounded(async {
        while !h.status().sources.iter().any(|s| {
            s.readiness == marketforge_engine::live::stream::source::SourceReadiness::AwaitingData
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    exchange.send.send(trade("ETHBTC", 1, 100, "3")).unwrap();
    bounded(async {
        while !h.status().streams[0].is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let old = h_generation(&h, "a");
    let mut operation = Box::pin(h.retry_stream("a"));
    std::future::poll_fn(|cx| {
        assert!(matches!(operation.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    drop(operation);
    // FIFO acknowledgment proves the cancelled caller's earlier command was admitted.
    h.demand(
        "a",
        DemandOwner::Manual {
            session_id: "admission-barrier".into(),
        },
        true,
    )
    .await
    .unwrap();
    wait_new_source(&h, "a", old).await;
    exchange.send.send(trade("ETHBTC", 1, 200, "4")).unwrap();
    bounded(async {
        while !h
            .status()
            .streams
            .iter()
            .any(|s| s.stream_id == "a" && s.generation.get() > old.get() && s.is_ready())
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(app.shutdown().await.complete);
}

async fn wait_new_source(h: &LiveApplicationHandle, id: &str, old: RuntimeGeneration) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !h.status().sources.iter().any(|s| {
            s.stream_id == id
                && s.generation.get() > old.get()
                && s.readiness
                    == marketforge_engine::live::stream::source::SourceReadiness::AwaitingData
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "replacement acquisition did not reach data boundary: {:?}",
            h.status()
        )
    });
}

async fn next_publication(
    sub: &mut Subscription,
) -> marketforge_engine::stream_protocol::market::StreamPublication {
    bounded(async {
        loop {
            match sub.recv().await.expect("stream receive failed") {
                Some(StreamMessage::Publication(p)) => return p,
                Some(StreamMessage::End(end)) => {
                    panic!("unexpected subscription terminal: {end:?}")
                }
                None => panic!("unexpected subscription EOF"),
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await
}
