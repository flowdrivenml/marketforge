mod reference_book;
use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    stream::request::QuantityType::{Base, Quote},
};
use serde_json::{Value, json};

// Instrument metadata captured from catalog.instrument_specs on 2026-10-24.
pub(super) fn instrument(profile: &str) -> super::InstrumentSpec {
    match profile {
        "bitget_spot" => (
            11545,
            ExchangeId::Bitget,
            MarketType::Spot,
            "BTCUSDT",
            Base,
            None,
            None,
        ),
        "bitget_linear" => (
            14215,
            ExchangeId::Bitget,
            MarketType::Linear,
            "BTCUSDT",
            Base,
            None,
            None,
        ),
        // TEST-ONLY MOCK: not registered in the Python-managed PostgreSQL catalog.
        // Quantity semantics follow the configured Bitget Inverse format.
        "bitget_inverse" => (
            900_000_001,
            ExchangeId::Bitget,
            MarketType::Inverse,
            "BTCUSD_CM",
            Quote,
            None,
            None,
        ),
        other => panic!("unknown fixture profile: {other}"),
    }
}

pub(super) fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    let native = super::symbol(profile);
    let inst_type = match profile {
        "bitget_spot" => "spot",
        "bitget_linear" => "USDT-FUTURES",
        "bitget_inverse" => "coin-futures",
        _ => unreachable!(),
    };
    let topic = if channel == "trades" {
        "publicTrade"
    } else {
        "books"
    };
    vec![json!({"instType": inst_type, "topic": topic, "symbol": native})]
}
