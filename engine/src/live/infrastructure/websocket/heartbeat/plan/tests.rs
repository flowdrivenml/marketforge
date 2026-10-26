use std::path::PathBuf;

use crate::live::{
    config::exchange::ExchangeConfig, infrastructure::websocket::HeartbeatPlanCompiler,
};

fn load(profile: &str) -> ExchangeConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/exchanges")
        .join(format!("{profile}.toml"));

    ExchangeConfig::load(&path).unwrap_or_else(|error| {
        panic!("{profile}: {error}");
    })
}

#[test]
fn rejects_ambiguous_heartbeat_configuration() {
    let mut config = load("okx_spot");

    let heartbeat = config.websocket.heartbeat.as_mut().unwrap();

    // Inactivity-based scheduling must not silently
    // become periodic scheduling.
    heartbeat.interval_seconds = Some(20);

    assert!(HeartbeatPlanCompiler::compile(&config).is_err());
}

#[test]
fn rejects_invalid_server_control_payload_policy() {
    let mut config = load("binance_spot");

    let heartbeat = config.websocket.heartbeat.as_mut().unwrap();

    heartbeat.echo_ping_payload = Some(false);

    assert!(HeartbeatPlanCompiler::compile(&config).is_err());
}
