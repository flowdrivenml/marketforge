use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ApplicationHeartbeatSchedule, CompiledHeartbeatPlan, HeartbeatExecutionMode,
        HeartbeatPlanCompiler,
    },
};

const EXCHANGES: [&str; 5] = ["binance", "bybit", "bitget", "okx", "gateio"];

const MARKETS: [&str; 3] = ["spot", "linear", "inverse"];

fn load(profile: &str) -> ExchangeConfig {
    let path = crate::support::data(".config/live/exchanges").join(format!("{profile}.toml"));

    ExchangeConfig::load(&path).unwrap_or_else(|error| {
        panic!("{profile}: {error}");
    })
}

#[test]
fn compiles_all_fifteen_real_heartbeat_profiles() {
    let mut counts = [0_usize; 4];

    for exchange in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange}_{market}");

            let config = load(&profile);

            let plan = HeartbeatPlanCompiler::compile(&config).unwrap_or_else(|error| {
                panic!("{profile}: {error}");
            });

            let mode = plan.mode();

            match mode {
                HeartbeatExecutionMode::ServerControl => {
                    counts[0] += 1;
                }

                HeartbeatExecutionMode::ClientControl => {
                    counts[1] += 1;
                }

                HeartbeatExecutionMode::PeriodicApplication => {
                    counts[2] += 1;
                }

                HeartbeatExecutionMode::InactivityApplication => {
                    counts[3] += 1;
                }
            }

            println!("{profile}: {mode:?}");
        }
    }

    println!("Server control:        {}", counts[0]);
    println!("Client control:        {}", counts[1]);
    println!("Periodic application:  {}", counts[2]);
    println!("Inactivity application:{}", counts[3]);

    assert_eq!(
        counts,
        [6, 0, 6, 3],
        "unexpected heartbeat coverage distribution"
    );
}

#[test]
fn server_control_does_not_invent_client_ping() {
    for profile in [
        "binance_spot",
        "binance_linear",
        "binance_inverse",
        "gateio_spot",
        "gateio_linear",
        "gateio_inverse",
    ] {
        let config = load(profile);

        let plan = HeartbeatPlanCompiler::compile(&config).unwrap();

        let CompiledHeartbeatPlan::ServerControl(server) = plan else {
            panic!("{profile}: expected server control");
        };

        assert!(server.respond_to_ping);
        assert!(server.echo_ping_payload);
    }
}

#[test]
fn inactivity_heartbeat_uses_incoming_traffic() {
    for profile in ["okx_spot", "okx_linear", "okx_inverse"] {
        let config = load(profile);

        let plan = HeartbeatPlanCompiler::compile(&config).unwrap();

        let CompiledHeartbeatPlan::Application(application) = plan else {
            panic!("{profile}: expected application heartbeat");
        };

        match application.schedule {
            ApplicationHeartbeatSchedule::ReceiveInactivity { threshold } => {
                assert_eq!(threshold.as_secs(), 25);
            }

            other => {
                panic!("{profile}: incorrect heartbeat schedule: {other:?}");
            }
        }

        assert_eq!(application.response_timeout.as_secs(), 5);

        assert_eq!(application.protocol.request_text(), "ping");

        assert!(application.protocol.matches_response("pong"));
    }
}

#[test]
fn periodic_heartbeats_preserve_native_messages() {
    for profile in [
        "bybit_spot",
        "bybit_linear",
        "bybit_inverse",
        "bitget_spot",
        "bitget_linear",
        "bitget_inverse",
    ] {
        let config = load(profile);

        let plan = HeartbeatPlanCompiler::compile(&config).unwrap();

        let CompiledHeartbeatPlan::Application(application) = plan else {
            panic!("{profile}: expected application heartbeat");
        };

        let ApplicationHeartbeatSchedule::Periodic { interval } = application.schedule else {
            panic!("{profile}: expected periodic heartbeat");
        };

        assert!(interval.as_secs() > 0);

        assert!(application.response_timeout.as_secs() > 0);

        assert!(!application.protocol.request_text().is_empty());
    }
}
