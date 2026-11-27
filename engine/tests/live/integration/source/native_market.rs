use std::{collections::BTreeMap, path::Path, sync::Arc};

use marketforge_engine::live::{
    application::compile_websocket,
    config::{FormatConfigStore, general::RecoveryDefaultsConfig},
    infrastructure::websocket::{StreamInput, WebSocketManager},
    stream::{
        request::{CreateSourceStreamRequest, SourceChannel},
        source::SourceInitializationLimits,
    },
};
use serde_json::{Value, json};
use tokio::{sync::mpsc, time::timeout};

use super::{
    harness::{DEADLINE, LocalSocket, builder},
    profiles, rest_support,
};

fn instrument(
    profile: &str,
    symbol: &str,
    asset: &str,
    channel: SourceChannel,
) -> CreateSourceStreamRequest {
    let mut source = profiles::requests()
        .into_iter()
        .find(|source| {
            format!(
                "{}_{}",
                source.exchange.as_str(),
                source.market_category.as_str()
            ) == profile
        })
        .unwrap();
    // Alternate settlement variants are synthetic routing inputs. Their financial
    // contract values and acquisition readiness are outside these tests.
    if source.symbol != symbol {
        source.instrument_id += 10_000;
    }
    source.symbol = symbol.into();
    source.quote_asset = Some(asset.to_ascii_uppercase());
    source.settlement_asset = Some(asset.into());
    source.channel = channel;
    source
}

fn formats() -> FormatConfigStore {
    FormatConfigStore::load_dir(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats"),
    )
    .unwrap()
}

async fn connected(
    receiver: &mut mpsc::Receiver<StreamInput>,
) -> marketforge_engine::live::infrastructure::websocket::StreamAcquisition {
    timeout(DEADLINE, async {
        loop {
            if let StreamInput::Connected(acquisition) = receiver.recv().await.unwrap() {
                return acquisition;
            }
        }
    })
    .await
    .unwrap()
}

async fn matching_ack(socket: &mut LocalSocket, predicate: impl Fn(&Value) -> bool) -> Value {
    timeout(DEADLINE, async {
        loop {
            let request = socket.ack().await;
            if predicate(&request) {
                return request;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn gateio_linear_routes_drive_local_slot_allocation_and_reconnect() {
    let profile = "gateio_linear";
    let mut usdt = LocalSocket::start(profile).await;
    let mut usd1 = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let mut runtime = compile_websocket(
        &configs,
        &limiters,
        &RecoveryDefaultsConfig {
            initial_backoff_ms: 5,
            max_backoff_seconds: 1,
            jitter: false,
        },
        &BTreeMap::new(),
    )
    .unwrap();
    runtime.profiles[0].urls = BTreeMap::from([
        (Some("usdt".into()), usdt.url.clone()),
        (Some("usd1".into()), usd1.url.clone()),
    ]);
    let manager = WebSocketManager::from_runtime(runtime).unwrap();
    let builder = builder(configs.clone(), SourceInitializationLimits::default());
    let formats = formats();
    let mut receivers = Vec::new();
    for (symbol, asset, channel, wire_channel) in [
        ("BTC_USDT", "USDT", SourceChannel::Trades, "futures.trades"),
        (
            "ETH_USDT",
            "USDT",
            SourceChannel::Depth,
            "futures.order_book_update",
        ),
        ("BTC_USD1", "Usd1", SourceChannel::Trades, "futures.trades"),
        (
            "BTC_USD1",
            "Usd1",
            SourceChannel::Depth,
            "futures.order_book_update",
        ),
    ] {
        let id = format!("{symbol}:{}", channel.as_str());
        let prepared = builder
            .prepare(profiles::definition(
                instrument(profile, symbol, asset, channel),
                &id,
            ))
            .unwrap();
        prepared.validate_subscription(&configs).unwrap();
        assert_eq!(prepared.stream_request().stream_id(), Some(id.as_str()));
        let (tx, mut rx) = mpsc::channel(32);
        manager
            .attach_request_with_formats(prepared.stream_request().clone(), tx, &formats)
            .await
            .unwrap();
        let server = if asset == "USDT" {
            &mut usdt
        } else {
            &mut usd1
        };
        let request = matching_ack(server, |request| {
            request["channel"] == wire_channel && request["payload"][0] == symbol
        })
        .await;
        assert_eq!(request["event"], "subscribe");
        if channel == SourceChannel::Depth {
            assert_eq!(request["payload"], json!([symbol, "100ms", "100"]));
        } else {
            assert_eq!(request["payload"], json!([symbol]));
        }
        let acquisition = connected(&mut rx).await;
        receivers.push((rx, acquisition));
    }
    assert_eq!(manager.stream_count().await, 4);
    assert_eq!(manager.slot_count().await, 2);

    // Close only the USD1 connection after both subscriptions are confirmed.
    while usd1.acknowledged.try_recv().is_ok() {}
    usd1.send.send(Value::Null).await.unwrap();
    let mut restored = BTreeMap::new();
    for _ in 0..2 {
        let request = usd1.ack().await;
        restored.insert(
            request["channel"].as_str().unwrap().to_owned(),
            request["payload"].clone(),
        );
    }
    assert_eq!(restored["futures.trades"], json!(["BTC_USD1"]));
    assert_eq!(
        restored["futures.order_book_update"],
        json!(["BTC_USD1", "100ms", "100"])
    );
    let (receiver, previous) = &mut receivers[3];
    let current = connected(receiver).await;
    assert_ne!(current.connection, previous.connection);
    assert!(current.generation > previous.generation);
    assert_eq!(manager.slot_count().await, 2);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn bitget_linear_preparation_and_replay_use_native_product_type() {
    let profile = "bitget_linear";
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let builder = builder(configs.clone(), SourceInitializationLimits::default());
    let formats = formats();
    let mut socket = LocalSocket::start(profile).await;
    let manager = super::harness::manager(&configs, profile, &limiters, &socket.url);
    let mut attached = Vec::new();
    let mut bindings = Vec::new();
    for (symbol, asset, inst_type) in [
        ("BTCUSDT", "USDT", "usdt-futures"),
        ("BTCUSDC", "USDC", "usdc-futures"),
    ] {
        for (channel, topic) in [
            (SourceChannel::Trades, "publicTrade"),
            (SourceChannel::Depth, "books"),
        ] {
            let id = format!("{symbol}:{}", channel.as_str());
            let definition = profiles::definition(instrument(profile, symbol, asset, channel), &id);
            let prepared = builder.prepare(definition.clone()).unwrap();
            prepared.validate_subscription(&configs).unwrap();
            assert_eq!(
                prepared.stream_request().parameters()["native_ws_inst_type"],
                inst_type
            );
            assert!(
                builder
                    .prepare(definition.clone())
                    .unwrap()
                    .with_subscription_parameter("native_ws_inst_type", inst_type)
                    .is_ok()
            );
            assert!(
                builder
                    .prepare(definition)
                    .unwrap()
                    .with_subscription_parameter("native_ws_inst_type", "coin-futures")
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("conflicts")
            );

            let (tx, mut rx) = mpsc::channel(32);
            let binding = manager
                .attach_request_with_formats(prepared.stream_request().clone(), tx, &formats)
                .await
                .unwrap();
            let expected = json!({"instType":inst_type,"topic":topic,"symbol":symbol});
            assert_eq!(socket.ack().await["args"], json!([expected]));
            let previous = connected(&mut rx).await;

            if channel == SourceChannel::Trades {
                let mut message = super::pipeline::native(profile);
                message["arg"]["symbol"] = symbol.into();
                message["arg"]["instType"] = inst_type.into();
                message["data"][0]["p"] = "456".into();
                socket.send.send(message).await.unwrap();
                let price = timeout(DEADLINE, async {
                    loop {
                        if let StreamInput::Data { message, .. } = rx.recv().await.unwrap() {
                            let message: Value =
                                serde_json::from_str(message.to_text().unwrap()).unwrap();
                            return message["data"][0]["p"].clone();
                        }
                    }
                })
                .await
                .unwrap();
                assert_eq!(price, "456");
            }

            if let Some((_, _, first)) = attached.first() {
                assert_eq!(&previous.connection, first);
            }
            attached.push((expected, (rx, previous.generation), previous.connection));
            bindings.push(binding);
        }
    }
    assert_eq!(manager.slot_count().await, 1);
    socket.send.send(Value::Null).await.unwrap();
    let mut restored = Vec::new();
    for _ in &attached {
        restored.push(socket.ack().await["args"][0].clone());
    }
    for (expected, (rx, prior_generation), prior_stamp) in &mut attached {
        assert!(restored.contains(expected));
        let current = connected(rx).await;
        assert_ne!(&current.connection, prior_stamp);
        assert!(current.generation > *prior_generation);
        *prior_stamp = current.connection;
    }
    let (removed, (mut retired, _), _) = attached.pop().unwrap();
    manager.detach(bindings.pop().unwrap()).await.unwrap();
    let request = socket.ack().await;
    assert_eq!(request["op"], "unsubscribe");
    assert_eq!(request["args"], json!([removed]));
    assert!(retired.recv().await.is_none());
    assert_eq!(manager.slot_count().await, 1);
    manager.shutdown().await.unwrap();
}

#[test]
fn missing_settlement_fails_persisted_source_preparation() {
    for (profile, symbol) in [("gateio_linear", "BTC_USDT"), ("bitget_linear", "BTCUSDT")] {
        let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
        let builder = builder(configs, SourceInitializationLimits::default());
        let mut source = instrument(profile, symbol, "USDT", SourceChannel::Trades);
        source.settlement_asset = None;
        let error = builder
            .prepare(profiles::definition(source, "missing-settlement"))
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains(profile)
                && error.contains(symbol)
                && error.contains("settlement_asset is required"),
            "{error}"
        );
    }
}
