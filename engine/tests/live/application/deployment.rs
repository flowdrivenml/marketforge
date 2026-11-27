use super::*;
use std::{path::Path, process::Command};

const CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live");
const GENERAL: &str = live_daemon::GENERAL;

pub(super) fn general(root: &Path) -> std::path::PathBuf {
    let general = root.join("general.toml");
    std::fs::write(&general, GENERAL.replace("127.0.0.1:8080", "127.0.0.1:0")).unwrap();
    general
}

fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::create_dir(root.path().join("data")).unwrap();
    std::fs::create_dir(root.path().join("data/.live")).unwrap();
    std::fs::set_permissions(
        root.path().join("data/.live"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    std::fs::create_dir(root.path().join(".secrets")).unwrap();
    let general = general(root.path());
    (root, general)
}

fn secret(root: &Path, name: &str, text: &str) {
    let file = root.join(".secrets").join(name);
    std::fs::write(&file, text).unwrap();
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn daemon(root: &Path, general: &Path) -> Command {
    let mut command = live_daemon::command(root);
    command
        .args(["--config-dir", CONFIG_DIR, "--general"])
        .arg(general);
    command
}

#[test]
fn workspace_postgres_credentials_fail_safely_before_database_io() {
    let (root, general) = fixture();
    let output = daemon(root.path(), &general).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(".secrets/postgres.env"));

    const INVALID: &str = "synthetic-invalid-value-fixture";
    secret(
        root.path(),
        "postgres.env",
        &format!("PRODUCTION_DATABASE_KEY='{INVALID}'\n"),
    );
    let output = daemon(root.path(), &general).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PRODUCTION_DATABASE_KEY in .secrets/postgres.env"));
    assert!(!stderr.contains(INVALID));

    let output = daemon(root.path(), &general)
        .env("MARKETFORGE_DATABASE_URL", "invalid-explicit-value-fixture")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("MARKETFORGE_DATABASE_URL"));
    assert!(!stderr.contains("invalid-explicit-value-fixture"));
}

#[test]
fn deployment_quota_loading_and_profile_compatibility_are_offline() {
    let (root, general) = fixture();
    let run = |profile, quota: Option<&str>| {
        let mut command = daemon(root.path(), &general);
        command.args(["--validate-only", "--profiles", profile]);
        if let Some(quota) = quota {
            command.args(["--rest-weight", quota]);
        }
        command.output().unwrap()
    };
    let missing = run("binance_spot", None);
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("shared-weight capacity/window required")
    );
    assert!(run("bybit_spot", None).status.success());

    std::fs::write(
        &general,
        format!(
            r#"{GENERAL}
[deployment]
egress_id = "test-egress"
[deployment.rest_weight.binance_spot]
capacity = 6000
window_seconds = 60
"#
        ),
    )
    .unwrap();
    let configured = run("binance_spot", None);
    assert!(
        configured.status.success(),
        "{}",
        String::from_utf8_lossy(&configured.stderr)
    );
    let overridden = run("binance_spot", Some("binance_spot=4500/30"));
    assert!(
        overridden.status.success(),
        "{}",
        String::from_utf8_lossy(&overridden.stderr)
    );
    let unknown = run("binance_spot", Some("absent_spot=1/1"));
    assert!(!unknown.status.success());
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("missing exchange profile absent_spot")
    );
    let unknown_profile = run("absent_spot", None);
    assert!(!unknown_profile.status.success());
    assert!(
        String::from_utf8_lossy(&unknown_profile.stderr)
            .contains("missing exchange profile absent_spot")
    );

    let unsupported = run("bybit_spot", None);
    assert!(
        String::from_utf8_lossy(&unsupported.stderr)
            .contains("automatic REST recovery is unsupported")
    );
}

#[cfg(feature = "management-api")]
#[tokio::test]
async fn startup_prepares_unused_profiles_with_workspace_management_credentials() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let (root, general) = fixture();
    const TOKEN: &str = "synthetic-workspace-admin-token";
    secret(
        root.path(),
        "live-management.json",
        &format!(r#"[{{"identity":"fixture","role":"administrator","token":"{TOKEN}"}}]"#),
    );

    let mut child = tokio::process::Command::from(daemon(root.path(), &general));
    let mut child = child
        .arg("--empty-definitions")
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    let mut unavailable = false;
    let mut recent = Vec::new();
    bounded(async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("daemon exited before serving: {recent:?}"));
            assert!(!line.contains(TOKEN));
            unavailable |= line.contains("binance_spot")
                && line.contains("shared-weight capacity/window required");
            if line.contains("application_serving") {
                break;
            }
            recent.push(line);
        }
    })
    .await;
    assert!(unavailable);
    assert!(
        root.path()
            .join("data/.live/marketforge-data.sock")
            .exists()
    );
    assert!(
        tokio::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(bounded(child.wait()).await.unwrap().success());
}

#[cfg(feature = "management-api")]
#[tokio::test]
async fn startup_prepared_profile_admits_new_source_without_restarting_an_existing_source() {
    use marketforge_engine::live::management::{
        client::{ClientOptions, MarketForgeManagementClient},
        protocol::Action,
        service::{Credentials, MemoryDefinitionStore},
    };
    use std::collections::BTreeSet;

    let (root, general) = fixture();
    let config = general::GeneralConfig::load(general).unwrap();
    let store = ExchangeConfigStore::load_dir(Path::new(CONFIG_DIR).join("exchanges")).unwrap();
    let exchanges = Arc::new(
        store
            .selected(&BTreeSet::from([
                "binance_spot".into(),
                "bybit_spot".into(),
                "binance_linear".into(),
            ]))
            .unwrap()
            .without_unavailable(BTreeMap::from([(
                "binance_linear".into(),
                "verified REST shared-weight capacity/window required".into(),
            )])),
    );
    let limiters = compile_limiters(
        &exchanges,
        "fixture",
        &BTreeMap::from([("binance_spot".into(), (6000, 60))]),
    )
    .unwrap();
    let mut existing_exchange = LocalExchange::start().await;
    let mut new_exchange = LocalExchange::start().await;
    let websocket = compile_websocket(
        &exchanges,
        &limiters,
        &config.defaults.recovery,
        &BTreeMap::from([
            ("binance_spot".into(), existing_exchange.url.clone()),
            ("bybit_spot".into(), new_exchange.url.clone()),
        ]),
    )
    .unwrap();
    let mut config = config;
    config.publication.socket_path = root
        .path()
        .join("data/.live/test.sock")
        .display()
        .to_string();
    let mut existing = source("existing", "BTCUSDT", "BTC", "USDT", 1);
    existing.desired.activation_policy = ActivationPolicy::AlwaysOn;
    let formats =
        Arc::new(FormatConfigStore::load_dir(Path::new(CONFIG_DIR).join("formats")).unwrap());
    let options = LiveApplicationOptions {
        config,
        definitions: vec![existing.clone()],
        dependencies: vec![],
        exchanges,
        formats,
        limiters,
        websocket,
    };
    let store = Arc::new(MemoryDefinitionStore::new(vec![existing]));
    const TOKEN: &str = "synthetic-dynamic-admission-token";
    let credentials = Credentials::from_json(&format!(
        r#"[{{"identity":"fixture","role":"administrator","token":"{TOKEN}"}}]"#
    ))
    .unwrap();
    let mut app = ManagedLiveApplication::start(options, store.clone(), credentials, None)
        .await
        .unwrap();
    existing_exchange.ack().await;
    existing_exchange
        .send
        .send(trade("BTCUSDT", 1, 1720000000000, "80000"))
        .unwrap();
    let handle = app.handle();
    let generation = bounded(async {
        loop {
            if let Some(status) = handle
                .status()
                .streams
                .iter()
                .find(|s| s.stream_id == "existing" && s.is_ready())
            {
                break status.generation;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(new_exchange.acks.try_recv().is_err());
    let client = MarketForgeManagementClient::new(
        &format!("http://{}", app.management_address()),
        TOKEN.into(),
        ClientOptions::default(),
    )
    .unwrap();
    let StreamDefinition::Source(unavailable) =
        definitions::source::definition("binance_linear", SourceChannel::Trades, "unavailable")
            .definition
    else {
        unreachable!()
    };
    let error = client
        .preview(Action::CreateSource {
            stream_id: "unavailable".into(),
            request: unavailable,
            desired: DesiredStreamState::default(),
        })
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("binance_linear")
            && error.to_string().contains("capacity/window required")
    );
    assert_eq!(store.snapshot().len(), 1);

    let StreamDefinition::Source(request) =
        definitions::source::definition("bybit_spot", SourceChannel::Depth, "new").definition
    else {
        unreachable!()
    };
    let plan = client
        .preview(Action::CreateSource {
            stream_id: "new".into(),
            request,
            desired: DesiredStreamState {
                enabled: true,
                activation_policy: ActivationPolicy::AlwaysOn,
            },
        })
        .await
        .unwrap();
    let operation = client
        .apply(&plan.id, "admit-prepared-profile")
        .await
        .unwrap();
    new_exchange.ack().await;
    new_exchange.send.send(json!({
        "topic": "orderbook.200.BTCUSDT", "type": "snapshot",
        "ts": 1720000000000_u64, "cts": 1720000000000_u64,
        "data": {"s": "BTCUSDT", "u": 100, "seq": 200, "b": [["80000", "2"]], "a": [["100000", "3"]]}
    })).unwrap();
    let operation = client
        .wait_operation(&operation.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(operation.state, "completed", "{operation:?}");
    assert_eq!(store.snapshot().len(), 2);
    let status = handle.status();
    assert!(
        status
            .streams
            .iter()
            .any(|s| s.stream_id == "existing" && s.generation == generation && s.is_ready())
    );
    assert!(existing_exchange.acks.try_recv().is_err());
    assert!(app.shutdown().await.complete);
}
