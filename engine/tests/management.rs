#![cfg(unix)]
#[path = "management/support.rs"]
mod support;
use marketforge_engine::{
    client::{
        management::{ClientOptions as ManagementOptions, MarketForgeManagementClient},
        streaming::{ClientOptions as StreamOptions, MarketForgeStreamClient},
    },
    live::{application::*, control::http::ManagementHttpServer, management::*},
    management_protocol::{self as p, definitions::*},
    stream_protocol::{StreamMessage, market::NativeEvent},
};
use std::{sync::Arc, time::Duration};
use support::*;
const OPERATOR: &str = "operator-token-for-local-test-only";
const ADMIN: &str = "administrator-token-local-test-only";
const VIEWER: &str = "viewer-token-for-local-test-only";
struct Rig {
    app: LiveApplication,
    management: ManagementService,
    http: ManagementHttpServer,
    client: MarketForgeManagementClient,
    store: MemoryDefinitionStore,
    dir: tempfile::TempDir,
    exchange: LocalExchange,
    path: String,
}
impl Rig {
    async fn new(defs: Vec<PersistedStreamDefinition>) -> Self {
        Self::configured(defs, |_| {}).await
    }
    async fn configured(
        defs: Vec<PersistedStreamDefinition>,
        configure: impl FnOnce(&mut marketforge_engine::live::config::general::GeneralConfig),
    ) -> Self {
        let exchange = LocalExchange::start().await;
        let dir = tempfile::tempdir().unwrap();
        let mut options = options(&dir, &exchange.url, defs.clone(), false);
        options.config.control.management.overview_interval_ms = 100;
        configure(&mut options.config);
        let limits = options.config.control.management.clone();
        let config_path = dir.path().join("general.toml");
        std::fs::write(
            &config_path,
            format!(
                "# preserve this operator comment\n{}",
                toml::to_string(&options.config).unwrap()
            ),
        )
        .unwrap();
        let path = options.config.publication.socket_path.clone();
        let app = LiveApplication::start(options).await.unwrap();
        let store = MemoryDefinitionStore::new(defs);
        let management = ManagementService::start_with_config(
            app.handle(),
            Arc::new(store.clone()),
            limits,
            config_path,
        )
        .unwrap();
        let credentials=Credentials::from_json(&serde_json::json!([{"identity":"operator","role":"operator","token":OPERATOR},{"identity":"admin","role":"administrator","token":ADMIN},{"identity":"viewer","role":"viewer","token":VIEWER}]).to_string()).unwrap();
        let http = ManagementHttpServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            management.handle(),
            credentials,
        )
        .await
        .unwrap();
        let client = MarketForgeManagementClient::new(
            &format!("http://{}", http.address),
            OPERATOR.into(),
            ManagementOptions::default(),
        )
        .unwrap();
        Self {
            app,
            management,
            http,
            client,
            store,
            dir,
            exchange,
            path,
        }
    }
    async fn close(mut self) {
        self.client.close().await;
        self.http.shutdown().await.unwrap();
        self.management.shutdown().await;
        assert!(self.app.shutdown().await.complete);
    }
    fn peer(&self, token: &str) -> MarketForgeManagementClient {
        MarketForgeManagementClient::new(
            &format!("http://{}", self.http.address),
            token.into(),
            ManagementOptions::default(),
        )
        .unwrap()
    }
}
async fn apply(client: &MarketForgeManagementClient, action: p::Action, key: &str) -> p::Operation {
    let plan = client.preview(action).await.unwrap();
    let operation = client.apply(&plan.id, key).await.unwrap();
    let completed = bounded(client.wait_operation(&operation.id, Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(completed.state, "completed", "{completed:?}");
    completed
}
async fn next_publication(
    sub: &mut marketforge_engine::client::streaming::Subscription,
) -> marketforge_engine::stream_protocol::market::StreamPublication {
    bounded(async {
        loop {
            match sub.recv().await.unwrap().unwrap() {
                StreamMessage::Publication(p) => return p,
                StreamMessage::End(e) => panic!("unexpected boundary: {e:?}"),
                _ => (),
            }
        }
    })
    .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_creates_actual_source_merge_convert_resizes_and_restarts_with_unix_boundary() {
    let mut rig = Rig::new(vec![]).await;
    for (id, symbol, base, quote, key) in [
        ("a", "ETHBTC", "ETH", "BTC", 1),
        ("b", "BTCUSDT", "BTC", "USDT", 2),
        ("c", "LTCBTC", "LTC", "BTC", 3),
    ] {
        let StreamDefinition::Source(request) = source(id, symbol, base, quote, key).definition
        else {
            unreachable!()
        };
        apply(
            &rig.client,
            p::Action::CreateSource {
                stream_id: id.into(),
                request,
                desired: DesiredStreamState::default(),
            },
            id,
        )
        .await;
    }
    for definition in [
        definitions::derived(
            "m",
            DerivedOperation::Merge,
            &["a", "c"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
        definitions::derived("v", DerivedOperation::Convert, &["m"], &["b"], None),
    ] {
        let StreamDefinition::Derived(request) = definition.definition else {
            unreachable!()
        };
        apply(
            &rig.client,
            p::Action::CreateDerived {
                request,
                desired: DesiredStreamState::default(),
            },
            &definition.stream_id,
        )
        .await;
    }
    assert_eq!(
        rig.client
            .streams(p::PageRequest::default())
            .await
            .unwrap()
            .items
            .len(),
        5
    );
    assert!(
        rig.app
            .handle()
            .status()
            .streams
            .iter()
            .all(|s| !s.is_ready())
    );
    let stream = MarketForgeStreamClient::connect_unix(&rig.path, StreamOptions::default())
        .await
        .unwrap();
    let mut subscription = stream.subscribe("v").await.unwrap();
    assert!(matches!(
        bounded(subscription.recv()).await.unwrap(),
        Some(StreamMessage::IntegritySnapshot(_))
    ));
    for _ in 0..3 {
        rig.exchange.ack().await;
    }
    bounded(async {
        while rig.app.handle().status().sources.len() != 3
            || !rig
                .app
                .handle()
                .status()
                .sources
                .iter()
                .all(|s| format!("{:?}", s.readiness) == "AwaitingData")
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    for (symbol, id, time, price) in [
        ("BTCUSDT", 1, 90, "10"),
        ("ETHBTC", 1, 100, "3"),
        ("LTCBTC", 1, 100, "4"),
        ("BTCUSDT", 2, 200, "20"),
        ("ETHBTC", 2, 300, "5"),
        ("LTCBTC", 2, 300, "6"),
        ("ETHBTC", 3, 400, "7"),
        ("LTCBTC", 3, 400, "8"),
    ] {
        rig.exchange
            .send
            .send(trade(symbol, id, time, price))
            .unwrap();
    }
    let data = next_publication(&mut subscription).await;
    assert!(
        data.records
            .iter()
            .any(|r| matches!(&r.native, NativeEvent::Trade(_))
                && r.origin.is_some()
                && r.valuation.is_some()
                && r.integrity.is_some())
    );
    let before = rig
        .client
        .overview()
        .await
        .unwrap()
        .streams
        .into_iter()
        .map(|s| (s.stream_id, s.generation))
        .collect::<std::collections::BTreeMap<_, _>>();
    let StreamDefinition::Source(request) = source("d", "XRPUSDT", "XRP", "USDT", 4).definition
    else {
        unreachable!()
    };
    apply(
        &rig.client,
        p::Action::CreateSource {
            stream_id: "d".into(),
            request,
            desired: DesiredStreamState::default(),
        },
        "add-unrelated",
    )
    .await;
    assert!(
        stream
            .list_streams(marketforge_engine::stream_protocol::PageRequest {
                after: None,
                limit: 128
            })
            .await
            .unwrap()
            .streams
            .iter()
            .any(|d| d.stream_id == "d")
    );
    for status in rig
        .client
        .overview()
        .await
        .unwrap()
        .streams
        .iter()
        .filter(|s| s.stream_id != "d")
    {
        assert_eq!(before[&status.stream_id], status.generation);
    }
    let admin = rig.peer(ADMIN);
    apply(
        &admin,
        p::Action::Delete {
            stream_id: "d".into(),
            expected_revision: "1".into(),
        },
        "remove-unrelated",
    )
    .await;
    for (n, target) in [4, 8, 4, 2].into_iter().enumerate() {
        apply(
            &rig.client,
            p::Action::ResizeWorkers {
                workers: target,
                save: false,
            },
            &format!("resize-{n}"),
        )
        .await;
        let view = rig.client.overview().await.unwrap();
        assert_eq!(view.pool.live_workers, target);
        assert_eq!(view.pool.requested_target, target);
        assert_eq!(
            (view.pool.starting_workers, view.pool.retiring_workers),
            (0, 0)
        );
        assert!(!view.pool.resizing);
        for s in view.streams {
            assert_eq!(before[&s.stream_id], s.generation);
        }
        let event_id = 4 + 3 * n as u64;
        let timestamp = 1000 + 1000 * n as u64;
        rig.exchange
            .send
            .send(trade("BTCUSDT", 3 + 2 * n as u64, timestamp - 10, "20"))
            .unwrap();
        for symbol in ["ETHBTC", "LTCBTC"] {
            rig.exchange
                .send
                .send(trade(symbol, event_id, timestamp, "5"))
                .unwrap();
            rig.exchange
                .send
                .send(trade(symbol, event_id + 1, timestamp + 100, "6"))
                .unwrap();
        }
        rig.exchange
            .send
            .send(trade("BTCUSDT", 4 + 2 * n as u64, timestamp + 200, "25"))
            .unwrap();
        for symbol in ["ETHBTC", "LTCBTC"] {
            rig.exchange
                .send
                .send(trade(symbol, event_id + 2, timestamp + 300, "7"))
                .unwrap();
        }
        bounded(async {
            loop {
                let data = next_publication(&mut subscription).await;
                if let Some(record) = data.records.iter().find(
                    |r| matches!(&r.native,NativeEvent::Trade(t) if t.trade_id.as_deref()==Some(event_id.to_string().as_str())),
                ) {
                    assert!(matches!(&record.native,NativeEvent::Trade(t) if t.price.coefficient=="5"));
                    assert!(record.valuation.is_some());
                    assert!(record.integrity.is_some());
                    break;
                }
            }
        })
        .await;
    }
    let current = rig.client.stream("v").await.unwrap();
    apply(
        &rig.client,
        p::Action::SetDesired {
            stream_id: "v".into(),
            expected_revision: current.definition.revision.to_string(),
            desired: DesiredStreamState {
                enabled: true,
                activation_policy: ActivationPolicy::AlwaysOn,
            },
            cascade: false,
        },
        "keep-v-running",
    )
    .await;
    let admin = rig.peer(ADMIN);
    let plan = admin
        .preview(p::Action::Restart {
            stream_id: "m".into(),
            cascade: true,
        })
        .await
        .unwrap();
    assert!(plan.continuity_impact.contains("terminate"));
    let op = admin.apply(&plan.id, "restart-v").await.unwrap();
    bounded(async {
        loop {
            if matches!(
                subscription.recv().await.unwrap(),
                Some(StreamMessage::End(_))
            ) {
                break;
            }
        }
    })
    .await;
    bounded(async {
        loop {
            let overview = admin.overview().await.unwrap();
            if overview.pool.registered_processors == 2
                && overview
                    .streams
                    .iter()
                    .find(|s| s.stream_id == "v")
                    .is_some_and(|s| s.generation != before["v"] && s.runtime == "Running")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut resumed = stream.subscribe("v").await.unwrap();
    bounded(resumed.recv()).await.unwrap();
    rig.exchange
        .send
        .send(trade("BTCUSDT", 11, 4990, "20"))
        .unwrap();
    for symbol in ["ETHBTC", "LTCBTC"] {
        rig.exchange
            .send
            .send(trade(symbol, 16, 5000, "5"))
            .unwrap();
        rig.exchange
            .send
            .send(trade(symbol, 17, 5100, "6"))
            .unwrap();
    }
    rig.exchange
        .send
        .send(trade("BTCUSDT", 12, 5200, "25"))
        .unwrap();
    for symbol in ["ETHBTC", "LTCBTC"] {
        rig.exchange
            .send
            .send(trade(symbol, 18, 5300, "7"))
            .unwrap();
    }
    assert!(
        next_publication(&mut resumed)
            .await
            .records
            .iter()
            .any(|r| r.valuation.is_some())
    );
    let completed = bounded(admin.wait_operation(&op.id, Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(completed.state, "completed");
    assert_eq!(completed.runtime_ready, Some(true));
    let m = rig.client.stream("m").await.unwrap();
    let before_edit = m.generation.clone();
    assert_ne!(before_edit, before["m"]);
    let StreamDefinition::Derived(mut request) = m.definition.definition else {
        unreachable!()
    };
    request.max_lateness_ns = Some(20);
    assert!(
        rig.client
            .preview(p::Action::EditDerived {
                stream_id: "m".into(),
                expected_revision: m.definition.revision.to_string(),
                request: request.clone(),
                cascade: false
            })
            .await
            .is_err()
    );
    let plan = rig
        .client
        .preview(p::Action::EditDerived {
            stream_id: "m".into(),
            expected_revision: m.definition.revision.to_string(),
            request,
            cascade: true,
        })
        .await
        .unwrap();
    assert!(plan.affected_streams.contains(&"v".into()));
    let edited = rig
        .client
        .apply(&plan.id, "edit-running-merge")
        .await
        .unwrap();
    bounded(async {
        loop {
            if matches!(resumed.recv().await.unwrap(), Some(StreamMessage::End(_))) {
                break;
            }
        }
    })
    .await;
    let edited = bounded(
        rig.client
            .wait_operation(&edited.id, Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert_eq!(edited.state, "completed");
    assert!(edited.saved);
    assert_eq!(edited.runtime_ready, Some(true));
    for id in ["a", "b", "c"] {
        assert_eq!(rig.client.stream(id).await.unwrap().generation, before[id]);
    }
    assert_ne!(
        rig.client.stream("m").await.unwrap().generation,
        before_edit
    );
    resumed.close().await.unwrap();
    let fresh = rig.peer(OPERATOR);
    assert!(fresh.overview().await.unwrap().serving);
    fresh.close().await;
    stream.close().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authorization_idempotency_persistence_failure_and_dynamic_demand_are_authoritative() {
    let rig = Rig::new(vec![source("a", "ETHBTC", "ETH", "BTC", 1)]).await;
    let viewer = rig.peer(VIEWER);
    let admin = rig.peer(ADMIN);
    assert!(viewer.overview().await.is_ok());
    assert!(
        viewer
            .preview(p::Action::ResizeWorkers {
                workers: 2,
                save: false
            })
            .await
            .is_err()
    );
    assert!(rig.client.preview(p::Action::Shutdown).await.is_err());
    assert!(
        admin
            .preview(p::Action::Delete {
                stream_id: "a".into(),
                expected_revision: "999".into()
            })
            .await
            .is_err()
    );
    let plan = rig
        .client
        .preview(p::Action::ResizeWorkers {
            workers: 2,
            save: false,
        })
        .await
        .unwrap();
    let one = rig.client.apply(&plan.id, "same-key").await.unwrap();
    let two = rig.client.apply(&plan.id, "same-key").await.unwrap();
    assert_eq!(one.id, two.id);
    bounded(rig.client.wait_operation(&one.id, Duration::from_secs(5)))
        .await
        .unwrap();
    let conflict = rig
        .client
        .preview(p::Action::ResizeWorkers {
            workers: 3,
            save: false,
        })
        .await
        .unwrap();
    assert!(rig.client.apply(&conflict.id, "same-key").await.is_err());
    let deletion = admin
        .preview(p::Action::Delete {
            stream_id: "a".into(),
            expected_revision: "1".into(),
        })
        .await
        .unwrap();
    let stream = MarketForgeStreamClient::connect_unix(&rig.path, StreamOptions::default())
        .await
        .unwrap();
    let mut sub = stream.subscribe("a").await.unwrap();
    bounded(sub.recv()).await.unwrap();
    assert!(
        matches!(admin.apply(&deletion.id,"delete-with-new-demand").await,Err(marketforge_engine::client::management::ClientError::Api(e)) if e.code=="stale_plan")
    );
    assert!(rig.client.stream("a").await.is_ok());
    sub.close().await.unwrap();
    stream.close().await.unwrap();
    rig.store
        .fail_next
        .store(true, std::sync::atomic::Ordering::Release);
    let StreamDefinition::Source(request) = source("b", "BTCUSDT", "BTC", "USDT", 2).definition
    else {
        unreachable!()
    };
    let plan = rig
        .client
        .preview(p::Action::CreateSource {
            stream_id: "b".into(),
            request,
            desired: DesiredStreamState::default(),
        })
        .await
        .unwrap();
    let op = rig.client.apply(&plan.id, "storage-failure").await.unwrap();
    let op = bounded(rig.client.wait_operation(&op.id, Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(op.state, "failed");
    assert!(!op.saved);
    assert!(rig.client.stream("b").await.is_err());
    assert_eq!(rig.store.snapshot().len(), 1);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viewing_does_not_acquire_demand_and_leases_are_owned_renewable_and_expiring() {
    let rig = Rig::new(vec![source("a", "ETHBTC", "ETH", "BTC", 1)]).await;
    for _ in 0..3 {
        rig.client.overview().await.unwrap();
        rig.client.stream("a").await.unwrap();
    }
    assert!(
        rig.client
            .stream("a")
            .await
            .unwrap()
            .direct_owners
            .is_empty()
    );
    let lease = rig
        .client
        .acquire_lease(p::LeaseRequest {
            stream_id: "a".into(),
            seconds: 1,
            idempotency_key: "lease-key".into(),
        })
        .await
        .unwrap();
    let duplicate = rig
        .client
        .acquire_lease(p::LeaseRequest {
            stream_id: "a".into(),
            seconds: 1,
            idempotency_key: "lease-key".into(),
        })
        .await
        .unwrap();
    assert_eq!(lease.id, duplicate.id);
    let other = Principal::trusted("other", p::Role::Operator).unwrap();
    assert!(
        rig.management
            .handle()
            .renew_lease(
                &other,
                &lease.id,
                p::RenewLease {
                    seconds: 1,
                    idempotency_key: "other-renew".into()
                }
            )
            .is_err()
    );
    assert!(
        rig.management
            .handle()
            .release_lease(&other, &lease.id, false)
            .await
            .is_err()
    );
    rig.client
        .renew_lease_with_key(&lease.id, 1, "same-renew")
        .await
        .unwrap();
    rig.client
        .renew_lease_with_key(&lease.id, 1, "same-renew")
        .await
        .unwrap();
    assert!(
        rig.client
            .renew_lease_with_key(&lease.id, 2, "same-renew")
            .await
            .is_err()
    );
    bounded(async {
        loop {
            if rig
                .client
                .stream("a")
                .await
                .unwrap()
                .direct_owners
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_initial_cut_cursor_expiry_and_malformed_requests_are_explicit() {
    use futures_util::StreamExt;
    let rig = Rig::new(vec![]).await;
    let mut notifications = rig.client.notifications();
    assert!(matches!(
        bounded(notifications.recv()).await.unwrap().unwrap(),
        marketforge_engine::client::management::Notification::Snapshot(_)
    ));
    apply(
        &rig.client,
        p::Action::ResizeWorkers {
            workers: 2,
            save: false,
        },
        "event-op",
    )
    .await;
    let http = reqwest::Client::new();
    let url = format!("http://{}", rig.http.address);
    assert_eq!(
        http.get(format!("{url}/api/v1/overview"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let bad = http
        .post(format!("{url}/api/v1/change-plans"))
        .bearer_auth(OPERATOR)
        .header("content-type", "application/json")
        .body("{bad")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    assert_eq!(
        bad.json::<p::ManagementError>().await.unwrap().code,
        "invalid_json"
    );
    let large = http
        .post(format!("{url}/api/v1/change-plans"))
        .bearer_auth(OPERATOR)
        .body(vec![b'x'; 70000])
        .send()
        .await
        .unwrap();
    assert_eq!(large.status(), 413);
    let response = http
        .get(format!(
            "{url}/api/v1/events?instance=previous-daemon&after=0"
        ))
        .bearer_auth(OPERATOR)
        .send()
        .await
        .unwrap();
    let mut frames = response.bytes_stream();
    let frame = bounded(frames.next()).await.unwrap().unwrap();
    assert!(
        std::str::from_utf8(&frame)
            .unwrap()
            .contains("resync_required")
    );
    notifications.close().await;
    rig.close().await;
}
#[cfg(feature = "management-tui")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tui_intent_uses_real_http_client_and_quit_leaves_daemon_running() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use marketforge_engine::tui::management::{App, HttpBackend, Intent, Update};
    let rig = Rig::new(vec![]).await;
    let mut backend = HttpBackend::start(rig.client.clone());
    let mut ui = App::default();
    bounded(async {
        while !ui.connected {
            ui.update(backend.recv().await.unwrap());
        }
    })
    .await;
    backend
        .send(Intent::Preview(p::Action::ResizeWorkers {
            workers: 3,
            save: false,
        }))
        .unwrap();
    let plan = bounded(async {
        loop {
            match backend.recv().await.unwrap() {
                Update::Plan(p) => break p,
                update => ui.update(update),
            }
        }
    })
    .await;
    ui.update(Update::Plan(plan));
    let intent = ui
        .input(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
        .unwrap();
    backend.send(intent).unwrap();
    let op = bounded(async {
        loop {
            match backend.recv().await.unwrap() {
                Update::Operation(op) => break op,
                update => ui.update(update),
            }
        }
    })
    .await;
    bounded(rig.client.wait_operation(&op.id, Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(rig.client.overview().await.unwrap().pool.live_workers, 3);
    ui.input(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));
    assert!(ui.quit);
    backend.close().await;
    assert!(rig.app.handle().status().serving);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conflicting_edits_and_graph_validation_do_not_replace_runtime() {
    let rig = Rig::new(vec![
        source("a", "ETHBTC", "ETH", "BTC", 1),
        source("b", "BTCUSDT", "BTC", "USDT", 2),
    ])
    .await;
    let mut request = match definitions::derived(
        "m",
        DerivedOperation::Merge,
        &["a", "b"],
        &[],
        Some(MergeOrdering::ArrivalOrder),
    )
    .definition
    {
        StreamDefinition::Derived(r) => r,
        _ => unreachable!(),
    };
    request.inputs = vec!["m".into(), "a".into()];
    assert!(
        rig.client
            .preview(p::Action::CreateDerived {
                request: request.clone(),
                desired: DesiredStreamState::default()
            })
            .await
            .is_err()
    );
    request.inputs = vec!["a".into(), "missing".into()];
    assert!(
        rig.client
            .preview(p::Action::CreateDerived {
                request,
                desired: DesiredStreamState::default()
            })
            .await
            .is_err()
    );
    rig.exchange
        .on_subscribe
        .lock()
        .unwrap()
        .insert("ethbtc@trade".into(), trade("ETHBTC", 1, 100, "3"));
    let action = |policy| p::Action::SetDesired {
        stream_id: "a".into(),
        expected_revision: "1".into(),
        desired: DesiredStreamState {
            enabled: true,
            activation_policy: policy,
        },
        cascade: false,
    };
    let one = rig
        .client
        .preview(action(ActivationPolicy::AlwaysOn))
        .await
        .unwrap();
    let two = rig
        .client
        .preview(action(ActivationPolicy::OnDemand))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        rig.client.apply(&one.id, "edit-one"),
        rig.client.apply(&two.id, "edit-two")
    );
    let mut completed = 0;
    for result in [a, b] {
        if let Ok(op) = result {
            let op = bounded(rig.client.wait_operation(&op.id, Duration::from_secs(5)))
                .await
                .unwrap();
            if op.state == "completed" {
                completed += 1;
            }
        }
    }
    assert_eq!(completed, 1);
    assert_eq!(rig.client.stream("a").await.unwrap().definition.revision, 2);
    assert_eq!(
        rig.store
            .snapshot()
            .iter()
            .find(|d| d.stream_id == "b")
            .unwrap()
            .revision,
        1
    );
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_configuration_save_preserves_other_settings_and_detects_external_edits() {
    let rig = Rig::new(vec![]).await;
    let path = rig.dir.path().join("general.toml");
    let before = marketforge_engine::live::config::general::GeneralConfig::load(&path).unwrap();
    let operation = apply(
        &rig.client,
        p::Action::ResizeWorkers {
            workers: 3,
            save: true,
        },
        "save-workers",
    )
    .await;
    assert!(operation.saved);
    assert_eq!(
        rig.client
            .overview()
            .await
            .unwrap()
            .pool
            .saved_startup_workers,
        Some(3)
    );
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("# preserve this operator comment"));
    let after = marketforge_engine::live::config::general::GeneralConfig::load(&path).unwrap();
    assert_eq!(
        after.runtime.processing.unwrap().buffered_bytes,
        before.runtime.processing.unwrap().buffered_bytes
    );
    assert_eq!(after.publication, before.publication);
    assert_eq!(after.runtime.processing.unwrap().workers, 3);
    std::fs::write(&path, format!("{text}\n# concurrent external edit\n")).unwrap();
    let plan = rig
        .client
        .preview(p::Action::ResizeWorkers {
            workers: 4,
            save: true,
        })
        .await
        .unwrap();
    let op = rig.client.apply(&plan.id, "save-conflict").await.unwrap();
    let op = bounded(rig.client.wait_operation(&op.id, Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(op.state, "failed");
    assert!(!op.saved);
    assert_eq!(rig.client.overview().await.unwrap().pool.live_workers, 3);
    assert!(
        std::fs::read_to_string(path)
            .unwrap()
            .contains("concurrent external edit")
    );
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_expiry_history_resync_and_operation_retention_are_bounded() {
    let rig = Rig::configured(vec![], |c| {
        c.control.management.plan_seconds = 1;
        c.control.management.event_history = 2;
        c.control.management.operation_history = 2;
        c.control.management.active_operations = 1;
    })
    .await;
    let expired = rig
        .client
        .preview(p::Action::ResizeWorkers {
            workers: 2,
            save: false,
        })
        .await
        .unwrap();
    bounded(async {
        loop {
            if rig.client.plan(&expired.id).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(rig.client.apply(&expired.id, "expired").await.is_err());
    let mut ids = vec![];
    for n in 0..4 {
        let op = apply(
            &rig.client,
            p::Action::ResizeWorkers {
                workers: 1 + n % 2,
                save: false,
            },
            &format!("bounded-{n}"),
        )
        .await;
        ids.push(op.id);
    }
    assert!(rig.client.operation(&ids[0]).await.is_err());
    assert_eq!(
        rig.client
            .operations(p::PageRequest::default())
            .await
            .unwrap()
            .items
            .len(),
        2
    );
    let principal = Principal::trusted("operator", p::Role::Operator).unwrap();
    let events = rig
        .management
        .handle()
        .events(&principal, Some(rig.management.handle().instance()), 0)
        .unwrap();
    assert!(events.resync_required);
    rig.close().await;
}

#[derive(Clone)]
struct GatedStore {
    inner: MemoryDefinitionStore,
    entered: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}
impl DefinitionStore for GatedStore {
    fn durable(&self) -> bool {
        false
    }
    fn save(
        &self,
        previous: Option<PersistedStreamDefinition>,
        next: Option<PersistedStreamDefinition>,
    ) -> StoreFuture<Option<PersistedStreamDefinition>> {
        let this = self.clone();
        Box::pin(async move {
            this.entered.notify_one();
            let permit = this.gate.acquire().await.unwrap();
            permit.forget();
            this.inner.save(previous, next).await
        })
    }
}
struct ReleaseGate(Arc<tokio::sync::Semaphore>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.add_permits(64);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_apply_response_resolves_same_owned_operation_without_duplicate_storage() {
    use tokio::io::AsyncWriteExt;
    let exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut app = LiveApplication::start(options(&dir, &exchange.url, vec![], false))
        .await
        .unwrap();
    let store = GatedStore {
        inner: MemoryDefinitionStore::new(vec![]),
        entered: Arc::new(tokio::sync::Notify::new()),
        gate: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let _release = ReleaseGate(store.gate.clone());
    let mut service =
        ManagementService::start(app.handle(), Arc::new(store.clone()), Default::default())
            .unwrap();
    let credentials = Credentials::from_json(
        &serde_json::json!([{"identity":"operator","role":"operator","token":OPERATOR}])
            .to_string(),
    )
    .unwrap();
    let mut http = ManagementHttpServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        service.handle(),
        credentials,
    )
    .await
    .unwrap();
    let client = MarketForgeManagementClient::new(
        &format!("http://{}", http.address),
        OPERATOR.into(),
        ManagementOptions::default(),
    )
    .unwrap();
    let StreamDefinition::Source(request) = source("a", "ETHBTC", "ETH", "BTC", 1).definition
    else {
        unreachable!()
    };
    let plan = client
        .preview(p::Action::CreateSource {
            stream_id: "a".into(),
            request,
            desired: DesiredStreamState::default(),
        })
        .await
        .unwrap();
    let body = serde_json::json!({"idempotency_key":"lost-response"}).to_string();
    let mut raw = tokio::net::TcpStream::connect(http.address).await.unwrap();
    raw.write_all(format!("POST /api/v1/change-plans/{}/apply HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {OPERATOR}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",plan.id,body.len()).as_bytes()).await.unwrap();
    bounded(store.entered.notified()).await;
    drop(raw);
    let resolved = client.apply(&plan.id, "lost-response").await.unwrap();
    assert!(!resolved.cleanup_complete);
    assert_eq!(store.inner.snapshot().len(), 0);
    store.gate.add_permits(1);
    let complete = bounded(client.wait_operation(&resolved.id, Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(complete.state, "completed");
    assert!(complete.saved);
    assert!(complete.cleanup_complete);
    assert_eq!(store.inner.snapshot().len(), 1);
    assert_eq!(
        client.apply(&plan.id, "lost-response").await.unwrap().id,
        resolved.id
    );
    http.shutdown().await.unwrap();
    assert!(service.shutdown().await);
    assert!(app.shutdown().await.complete);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_storage_commit_after_shutdown_remains_saved_and_restores_without_old_leases() {
    let exchange = LocalExchange::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut app = LiveApplication::start(options(&dir, &exchange.url, vec![], false))
        .await
        .unwrap();
    let store = GatedStore {
        inner: MemoryDefinitionStore::new(vec![]),
        entered: Arc::new(tokio::sync::Notify::new()),
        gate: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let _release = ReleaseGate(store.gate.clone());
    let mut limits = marketforge_engine::live::config::general::ManagementConfig::default();
    limits.active_operations = 1;
    limits.operation_history = 2;
    limits.command_capacity = 1;
    limits.request_timeout_ms = 100;
    let mut service =
        ManagementService::start(app.handle(), Arc::new(store.clone()), limits).unwrap();
    let handle = service.handle();
    let principal = Principal::trusted("operator", p::Role::Operator).unwrap();
    let admin = Principal::trusted("admin", p::Role::Administrator).unwrap();
    let StreamDefinition::Source(request) = source("a", "ETHBTC", "ETH", "BTC", 1).definition
    else {
        unreachable!()
    };
    let plan = handle
        .preview(
            &principal,
            p::PreviewRequest {
                action: p::Action::CreateSource {
                    stream_id: "a".into(),
                    request,
                    desired: DesiredStreamState::default(),
                },
            },
        )
        .unwrap();
    let accepted = handle
        .apply(
            &principal,
            &plan.id,
            p::ApplyRequest {
                idempotency_key: "accepted-storage".into(),
            },
        )
        .unwrap();
    bounded(store.entered.notified()).await;
    let shutdown = handle
        .preview(
            &admin,
            p::PreviewRequest {
                action: p::Action::Shutdown,
            },
        )
        .unwrap();
    let shutdown = handle
        .apply(
            &admin,
            &shutdown.id,
            p::ApplyRequest {
                idempotency_key: "reserved-shutdown".into(),
            },
        )
        .unwrap();
    assert!(!service.shutdown().await);
    assert!(
        !handle
            .operation(&principal, &accepted.id)
            .unwrap()
            .cleanup_complete
    );
    assert!(app.shutdown().await.complete);
    store.gate.add_permits(1);
    bounded(async {
        while !handle
            .operation(&principal, &accepted.id)
            .unwrap()
            .cleanup_complete
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let operation = handle.operation(&principal, &accepted.id).unwrap();
    assert!(operation.saved);
    assert_eq!(operation.state, "saved_runtime_failed");
    assert!(
        handle
            .operation(&admin, &shutdown.id)
            .unwrap()
            .cleanup_complete
    );
    assert!(service.shutdown().await);
    let next_dir = tempfile::tempdir().unwrap();
    let mut restored = LiveApplication::start(options(
        &next_dir,
        &exchange.url,
        store.inner.snapshot(),
        false,
    ))
    .await
    .unwrap();
    assert_eq!(restored.handle().status().streams.len(), 1);
    assert_eq!(restored.handle().status().streams[0].generation.get(), 0);
    assert!(restored.shutdown().await.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_headless_binary_authenticates_resizes_saves_and_shuts_down_without_tui() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(&dir, "ws://127.0.0.1:1", vec![], false);
    opts.config.control.listen_address = "127.0.0.1:0".into();
    let path = dir.path().join("general.toml");
    std::fs::write(&path, toml::to_string(&opts.config).unwrap()).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_marketforge-live"))
        .args([
            "--empty-definitions",
            "--config-dir",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"),
            "--config",
            path.to_str().unwrap(),
            "--profiles",
            "binance_spot",
            "--rest-weight",
            "binance_spot=6000/60",
        ])
        .env_remove("MARKETFORGE_DATABASE_URL")
        .env(
            "MARKETFORGE_MANAGEMENT_CREDENTIALS",
            serde_json::json!([{"identity":"admin","role":"administrator","token":ADMIN}])
                .to_string(),
        )
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    let address = bounded(async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .expect("daemon exited before management listener");
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                if value["fields"]["event"] == "management_serving" {
                    break value["fields"]["address"].as_str().unwrap().to_string();
                }
            }
        }
    })
    .await;
    let client = MarketForgeManagementClient::new(
        &format!("http://{address}"),
        ADMIN.into(),
        Default::default(),
    )
    .unwrap();
    assert!(client.overview().await.unwrap().serving);
    let operation = apply(
        &client,
        p::Action::ResizeWorkers {
            workers: 3,
            save: true,
        },
        "daemon-resize",
    )
    .await;
    assert!(operation.saved);
    assert_eq!(client.overview().await.unwrap().pool.live_workers, 3);
    assert_eq!(
        marketforge_engine::live::config::general::GeneralConfig::load(&path)
            .unwrap()
            .runtime
            .processing
            .unwrap()
            .workers,
        3
    );
    let plan = client.preview(p::Action::Shutdown).await.unwrap();
    client.apply(&plan.id, "daemon-shutdown").await.unwrap();
    assert!(bounded(child.wait()).await.unwrap().success());
    assert!(!std::path::Path::new(&opts.config.publication.socket_path).exists());
}
#[tokio::test]
async fn unavailable_definition_persistence_is_not_replaced_by_an_empty_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options(&dir, "ws://127.0.0.1:1", vec![], false);
    let path = dir.path().join("general.toml");
    std::fs::write(&path, toml::to_string(&opts.config).unwrap()).unwrap();
    let result = bounded(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_marketforge-live"))
            .args([
                "--config-dir",
                concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"),
                "--general",
                path.to_str().unwrap(),
                "--profiles",
                "binance_spot",
            ])
            .env_remove("MARKETFORGE_DATABASE_URL")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap();
    assert!(!result.status.success());
    let reason = String::from_utf8(result.stderr).unwrap();
    assert!(reason.contains("MARKETFORGE_DATABASE_URL is required"));
    assert!(!std::path::Path::new(&opts.config.publication.socket_path).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonreading_sse_observer_is_bounded_and_an_independent_client_progresses() {
    use std::os::fd::AsRawFd;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let definitions = (0..200)
        .map(|n| {
            source(
                &format!("source-{n:03}-{}", "x".repeat(3500)),
                "ETHBTC",
                "ETH",
                "BTC",
                n + 1,
            )
        })
        .collect();
    let rig = Rig::configured(definitions, |c| {
        c.control.management.sse_clients = 1;
        c.control.management.request_timeout_ms = 1000;
        c.control.management.max_response_bytes = 16 * 1024 * 1024;
    })
    .await;
    let bytes = serde_json::to_vec(&rig.client.overview().await.unwrap()).unwrap();
    assert!(bytes.len() > 1024 * 1024 && bytes.len() < 16 * 1024 * 1024);
    let mut raw = tokio::net::TcpStream::connect(rig.http.address)
        .await
        .unwrap();
    let receive: libc::c_int = 4096;
    assert_eq!(
        unsafe {
            libc::setsockopt(
                raw.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &receive as *const _ as *const libc::c_void,
                std::mem::size_of_val(&receive) as libc::socklen_t,
            )
        },
        0
    );
    raw.write_all(format!("GET /api/v1/events HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {OPERATOR}\r\n\r\n").as_bytes()).await.unwrap();
    let mut headers = Vec::new();
    bounded(async {
        while !headers.ends_with(b"\r\n\r\n") {
            let byte = raw.read_u8().await.unwrap();
            headers.push(byte);
            assert!(headers.len() < 8192);
        }
    })
    .await;
    assert!(String::from_utf8_lossy(&headers).contains("200 OK"));
    bounded(async {
        loop {
            let clients = rig.client.clients().await.unwrap();
            if clients["sse_observers"] == 0
                && clients["management_http_connections"]
                    .as_u64()
                    .is_some_and(|n| n <= 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        rig.client
            .streams(p::PageRequest {
                limit: Some(1),
                ..Default::default()
            })
            .await
            .unwrap()
            .next
            .is_some()
    );
    drop(raw);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_one_hold_preserves_another_owners_actual_running_generation() {
    let mut rig = Rig::new(vec![source("a", "ETHBTC", "ETH", "BTC", 1)]).await;
    let first = rig
        .client
        .acquire_lease(p::LeaseRequest {
            stream_id: "a".into(),
            seconds: 30,
            idempotency_key: "first-owner".into(),
        })
        .await
        .unwrap();
    rig.exchange.ack().await;
    bounded(async {
        while !rig
            .app
            .handle()
            .status()
            .sources
            .iter()
            .any(|s| format!("{:?}", s.readiness) == "AwaitingData")
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    rig.exchange
        .send
        .send(trade("ETHBTC", 1, 100, "3"))
        .unwrap();
    bounded(async {
        while rig.client.stream("a").await.unwrap().runtime != "Running" {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = rig.client.stream("a").await.unwrap().generation;
    let other = rig.peer(ADMIN);
    let second = other
        .acquire_lease(p::LeaseRequest {
            stream_id: "a".into(),
            seconds: 30,
            idempotency_key: "second-owner".into(),
        })
        .await
        .unwrap();
    let released = rig.client.release_lease(&first.id).await.unwrap();
    assert!(released.stream_still_required);
    assert!(
        rig.client
            .release_lease(&first.id)
            .await
            .unwrap()
            .stream_still_required
    );
    assert_eq!(rig.client.stream("a").await.unwrap().generation, before);
    assert_eq!(rig.client.stream("a").await.unwrap().runtime, "Running");
    assert!(rig.client.release_lease(&second.id).await.is_err());
    other.release_lease(&second.id).await.unwrap();
    let third = rig
        .client
        .acquire_lease(p::LeaseRequest {
            stream_id: "a".into(),
            seconds: 30,
            idempotency_key: "forced-owner".into(),
        })
        .await
        .unwrap();
    assert!(
        rig.client
            .release_lease_with_override(&third.id, true)
            .await
            .is_err()
    );
    other
        .release_lease_with_override(&third.id, true)
        .await
        .unwrap();
    let admin = Principal::trusted("admin", p::Role::Administrator).unwrap();
    let events = rig.management.handle().events(&admin, None, 0).unwrap();
    assert!(
        events
            .events
            .iter()
            .any(|event| event.kind == "demand_force_released")
    );
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_api_metric_readers_do_not_consume_or_reset_sampling_windows() {
    let rig = Rig::configured(vec![], |c| c.metrics.snapshot_interval_ms = 60000).await;
    bounded(async {
        while rig.app.handle().latest_metrics().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = rig.app.handle().latest_metrics().unwrap();
    let viewer = rig.peer(VIEWER);
    let (one, two) = tokio::join!(rig.client.overview(), viewer.overview());
    let one = one.unwrap();
    let two = two.unwrap();
    assert_eq!(
        one.metrics.as_ref().unwrap()["revision"],
        before.revision.to_string()
    );
    assert_eq!(one.metrics, two.metrics);
    assert_eq!(
        rig.app.handle().latest_metrics().unwrap().revision,
        before.revision
    );
    rig.close().await;
}
