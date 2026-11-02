#![allow(dead_code, unused_imports)]
#[path = "../live/integration/source/harness.rs"]
mod acquisition;
#[path = "../live/integration/derived/harness.rs"]
pub(crate) mod definitions;
#[path = "../live/integration/rest/support.rs"]
mod rest_support;
#[path = "../live/support/websocket_server.rs"]
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
pub(crate) async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(8), f)
        .await
        .expect("application test deadline")
}
pub(crate) fn source(
    id: &str,
    symbol: &str,
    base: &str,
    quote: &str,
    key: i64,
) -> PersistedStreamDefinition {
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
pub(crate) fn options(
    dir: &tempfile::TempDir,
    url: &str,
    defs: Vec<PersistedStreamDefinition>,
    prometheus: bool,
) -> LiveApplicationOptions {
    options_profile(dir, url, defs, prometheus, "binance_spot")
}
pub(crate) fn options_profile(
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
    config
        .runtime
        .processing
        .get_or_insert_with(Default::default);
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
pub(crate) fn trade(symbol: &str, id: u64, time: u64, price: &str) -> serde_json::Value {
    json!({"e":"trade","E":time,"s":symbol,"t":id,"p":price,"q":"2","T":time,"m":false})
}
pub(crate) struct LocalExchange {
    pub(crate) on_subscribe:
        Arc<std::sync::Mutex<std::collections::BTreeMap<String, serde_json::Value>>>,
    pub(crate) url: String,
    pub(crate) send: tokio::sync::broadcast::Sender<serde_json::Value>,
    acks: tokio::sync::mpsc::Receiver<()>,
    task: tokio::task::JoinHandle<()>,
}
impl LocalExchange {
    pub(crate) async fn start() -> Self {
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
    pub(crate) async fn ack(&mut self) {
        bounded(self.acks.recv()).await.unwrap();
    }
}
impl Drop for LocalExchange {
    fn drop(&mut self) {
        self.task.abort();
    }
}
