use std::path::PathBuf;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig, infrastructure::websocket::HeartbeatCoverageAuditor,
};

const EXCHANGES: [&str; 5] = ["binance", "bybit", "bitget", "okx", "gateio"];

const MARKETS: [&str; 3] = ["spot", "linear", "inverse"];

fn exchange_config(profile: &str) -> ExchangeConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/exchanges")
        .join(format!("{profile}.toml"));

    ExchangeConfig::load(&path).unwrap_or_else(|error| {
        panic!(
            "cannot load exchange configuration {}: {error}",
            path.display()
        )
    })
}

#[test]
fn w24_audit_all_fifteen_heartbeat_profiles() {
    let mut passed = 0_usize;
    let mut blocked = Vec::new();

    println!();
    println!("MARKETFORGE WEBSOCKET HEARTBEAT COVERAGE");
    println!("=======================================");

    for exchange in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange}_{market}");

            let config = exchange_config(&profile);

            let result = HeartbeatCoverageAuditor::audit(&config);

            let status = if result.executable {
                passed += 1;
                "SUPPORTED"
            } else {
                blocked.push(profile.clone());
                "BLOCKED"
            };

            println!();
            println!("[{status}] {profile}");

            for kind in &result.kinds {
                println!("  strategy: {kind:?}");
            }

            for diagnostic in &result.diagnostics {
                println!("  diagnostic: {diagnostic}");
            }
        }
    }

    println!();
    println!("=======================================");
    println!("Supported: {passed}/15");
    println!("Blocked:   {}/15", blocked.len());
    println!("=======================================");

    assert_eq!(
        passed + blocked.len(),
        15,
        "not all exchange profiles were audited",
    );

    assert!(
        blocked.is_empty(),
        "heartbeat support remains incomplete for: {}",
        blocked.join(", ")
    );
}
