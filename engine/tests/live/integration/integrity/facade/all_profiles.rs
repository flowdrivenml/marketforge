use std::path::{Path, PathBuf};

use marketforge_engine::{
    canonical::Exchange,
    live::{
        config::{
            exchange::ExchangeConfig,
            format::{DepthFormatConfig, TradeFormatConfig},
        },
        integrity::{
            CreateDepthIntegrity, CreateTradeIntegrity, DepthBufferLimits, DepthContinuityCompiler,
            DepthResourceLimits, DepthSnapshotSource, DepthSourceBinding, DepthSourceEpoch,
            IntegrityAdmissionState, IntegrityChannel, IntegrityFacadeFactory,
            IntegrityLifecycleEvent, RestDepthEndpointConfig, TradeIdentityLimits,
            TradeIntegrityCompiler, TradeSourceBinding, TradeSourceEpoch,
        },
    },
};

// ============================================================
// ALL SUPPORTED EXCHANGE PROFILES
// ============================================================

const PROFILES: [(&str, &str); 15] = [
    ("binance", "spot"),
    ("binance", "linear"),
    ("binance", "inverse"),
    ("bybit", "spot"),
    ("bybit", "linear"),
    ("bybit", "inverse"),
    ("bitget", "spot"),
    ("bitget", "linear"),
    ("bitget", "inverse"),
    ("gateio", "spot"),
    ("gateio", "linear"),
    ("gateio", "inverse"),
    ("okx", "spot"),
    ("okx", "linear"),
    ("okx", "inverse"),
];

// ============================================================
// REAL CONFIGURATION PATHS
// ============================================================

fn config_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live")
}

fn exchange_path(profile: &str) -> PathBuf {
    config_root()
        .join("exchanges")
        .join(format!("{profile}.toml"))
}

fn depth_path(profile: &str) -> PathBuf {
    config_root()
        .join("formats/depth")
        .join(format!("{profile}.toml"))
}

fn trade_path(profile: &str) -> PathBuf {
    config_root()
        .join("formats/trades")
        .join(format!("{profile}.toml"))
}

fn load_exchange(path: &Path) -> ExchangeConfig {
    ExchangeConfig::load(path).unwrap_or_else(|error| {
        panic!("cannot load exchange config {}: {error:?}", path.display(),)
    })
}

fn load_depth(path: &Path) -> DepthFormatConfig {
    DepthFormatConfig::load(path)
        .unwrap_or_else(|error| panic!("cannot load depth config {}: {error:?}", path.display(),))
}

fn load_trades(path: &Path) -> TradeFormatConfig {
    TradeFormatConfig::load(path)
        .unwrap_or_else(|error| panic!("cannot load trade config {}: {error:?}", path.display(),))
}

// ============================================================
// CANONICAL PROFILE IDENTITY
// ============================================================

fn exchange_identity(name: &str) -> Exchange {
    match name {
        "binance" => Exchange::Binance,
        "bybit" => Exchange::Bybit,
        "bitget" => Exchange::Bitget,
        "gateio" => Exchange::GateIo,
        "okx" => Exchange::Okx,

        other => panic!("unknown exchange: {other}"),
    }
}

fn depth_epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

fn trade_epoch() -> TradeSourceEpoch {
    TradeSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

// ============================================================
// DEPTH FACTORY — ALL PROFILES
// ============================================================

#[test]
fn all_profiles_construct_depth_integrity_handles() {
    let mut verified = 0usize;

    for (exchange_name, market) in PROFILES {
        let profile = format!("{exchange_name}_{market}");

        let exchange_config = load_exchange(&exchange_path(&profile));

        let depth_config = load_depth(&depth_path(&profile));

        let plan = DepthContinuityCompiler::compile(&depth_config)
            .unwrap_or_else(|error| panic!("{profile}: depth compilation failed: {error:?}"));

        let requires_rest = plan.snapshot_source == DepthSnapshotSource::Rest;

        let binding = DepthSourceBinding::new(
            format!("{profile}:depth"),
            exchange_identity(exchange_name),
            market,
            42,
            "BTCUSDT",
            None,
            depth_epoch(),
        )
        .unwrap();

        let rest_endpoint = if requires_rest {
            Some(RestDepthEndpointConfig {
                exchange: &exchange_config,
                format: &depth_config,
            })
        } else {
            None
        };

        let mut handle = IntegrityFacadeFactory::depth(CreateDepthIntegrity {
            binding,
            plan,

            buffer_limits: DepthBufferLimits {
                max_messages: 100,
                max_bytes: 1_000_000,
            },

            resource_limits: DepthResourceLimits::default(),

            rest_endpoint,
        })
        .unwrap_or_else(|error| panic!("{profile}: depth factory failed: {error:?}"));

        assert_eq!(handle.channel(), IntegrityChannel::Depth, "{profile}",);

        assert_eq!(handle.stream_generation(), 10, "{profile}",);

        assert_eq!(
            handle.admission_state(),
            IntegrityAdmissionState::Active,
            "{profile}",
        );

        assert_eq!(handle.status().depth_synchronized, Some(false), "{profile}",);

        assert!(handle.book().is_none(), "{profile}",);

        handle
            .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
            .unwrap();

        assert_eq!(
            handle.admission_state(),
            IntegrityAdmissionState::Stopped,
            "{profile}",
        );

        verified += 1;
    }

    assert_eq!(verified, 15);
}

// ============================================================
// TRADES FACTORY — ALL PROFILES
// ============================================================

#[test]
fn all_profiles_construct_trade_integrity_handles() {
    let mut verified = 0usize;

    for (exchange_name, market) in PROFILES {
        let profile = format!("{exchange_name}_{market}");

        let trade_config = load_trades(&trade_path(&profile));

        let plan = TradeIntegrityCompiler::compile(&trade_config)
            .unwrap_or_else(|error| panic!("{profile}: trade compilation failed: {error:?}"));

        let binding = TradeSourceBinding::new(
            format!("{profile}:trades"),
            exchange_identity(exchange_name),
            market,
            42,
            "BTCUSDT",
            trade_epoch(),
        )
        .unwrap();

        let mut handle = IntegrityFacadeFactory::trades(CreateTradeIntegrity {
            binding,
            plan,

            identity_limits: TradeIdentityLimits {
                max_ids: 10_000,
                max_id_bytes: 256,
                max_batch_records: 1_000,
            },
        })
        .unwrap_or_else(|error| panic!("{profile}: trade factory failed: {error:?}"));

        assert_eq!(handle.channel(), IntegrityChannel::Trades, "{profile}",);

        assert_eq!(handle.stream_generation(), 10, "{profile}",);

        assert_eq!(
            handle.admission_state(),
            IntegrityAdmissionState::Active,
            "{profile}",
        );

        assert_eq!(handle.status().trades_admissible, Some(true), "{profile}",);

        assert!(handle.book().is_none(), "{profile}",);

        handle
            .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
            .unwrap();

        assert_eq!(
            handle.admission_state(),
            IntegrityAdmissionState::Stopped,
            "{profile}",
        );

        verified += 1;
    }

    assert_eq!(verified, 15);
}
