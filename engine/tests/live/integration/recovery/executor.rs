use std::path::PathBuf;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::rest::{HistoricalTradeCursor, HistoricalTradePage, StreamRestCapabilities},
    recovery::{RecoveryExecutionError, RecoveryOperation, RecoveryRestExecutor},
};

// ============================================================
// REAL CONFIGURATION
// ============================================================

fn config_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/exchanges")
}

fn capabilities(profile: &str) -> StreamRestCapabilities {
    let config = ExchangeConfig::load(config_root().join(format!("{profile}.toml"))).unwrap();

    StreamRestCapabilities::compile(&config)
}

// ============================================================
// TESTS
// ============================================================

#[test]
fn capability_validation_uses_real_exchange_configuration() {
    let profiles = [
        "binance_spot",
        "binance_linear",
        "binance_inverse",
        "bybit_spot",
        "bybit_linear",
        "bybit_inverse",
        "bitget_spot",
        "bitget_linear",
        "bitget_inverse",
        "gateio_spot",
        "gateio_linear",
        "gateio_inverse",
        "okx_spot",
        "okx_linear",
        "okx_inverse",
    ];

    let mut checked = 0;

    for profile in profiles {
        let capabilities = capabilities(profile);

        let operations = [
            RecoveryOperation::DepthSnapshot { depth: None },
            RecoveryOperation::RecentTrades { limit: None },
            RecoveryOperation::HistoricalTrades {
                page: HistoricalTradePage::latest(),
            },
        ];

        for operation in &operations {
            let result = RecoveryRestExecutor::validate_capability(operation, &capabilities);

            assert_eq!(
                result.is_ok(),
                operation.is_supported(&capabilities),
                "{profile}: inconsistent operation admission",
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 45);
}

#[test]
fn historical_recovery_rejects_unsupported_profile() {
    let capabilities = capabilities("bybit_spot");

    let operation = RecoveryOperation::HistoricalTrades {
        page: HistoricalTradePage::latest(),
    };

    assert!(matches!(
        RecoveryRestExecutor::validate_capability(&operation, &capabilities,),
        Err(RecoveryExecutionError::UnsupportedOperation),
    ));
}

#[test]
fn binance_spot_supports_trade_id_history() {
    let capabilities = capabilities("binance_spot");

    let operation = RecoveryOperation::HistoricalTrades {
        page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::TradeId { from_id: 1000 }),
    };

    RecoveryRestExecutor::validate_capability(&operation, &capabilities).unwrap();
}

#[test]
fn capability_validation_does_not_validate_native_cursors() {
    let capabilities = capabilities("binance_spot");

    let operation = RecoveryOperation::HistoricalTrades {
        page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::Okx {
            before: Some(1000),
            after: None,
        }),
    };

    // The endpoint supports historical trades.
    //
    // Capability validation alone must not claim that
    // an arbitrary native cursor is valid.
    //
    // The REST Facade validates the cursor strategy
    // during historical_trades().
    RecoveryRestExecutor::validate_capability(&operation, &capabilities).unwrap();
}

#[test]
fn snapshot_capability_is_independent_of_trade_history() {
    let capabilities = capabilities("binance_spot");

    let snapshot = RecoveryOperation::DepthSnapshot { depth: Some(1000) };

    let history = RecoveryOperation::HistoricalTrades {
        page: HistoricalTradePage::latest(),
    };

    assert_eq!(
        RecoveryRestExecutor::validate_capability(&snapshot, &capabilities,).is_ok(),
        capabilities.supports_depth_snapshot(),
    );

    assert_eq!(
        RecoveryRestExecutor::validate_capability(&history, &capabilities,).is_ok(),
        capabilities.supports_historical_trades(),
    );
}
