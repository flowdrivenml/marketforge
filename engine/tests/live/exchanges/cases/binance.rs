mod decoder;
mod futures_rest_sync;
mod rest_sync;
use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    stream::request::QuantityType::{Base, Contracts},
};
use serde_json::{Value, json};

// Instrument metadata captured from catalog.instrument_specs on 2026-10-24.
pub(super) fn instrument(profile: &str) -> super::InstrumentSpec {
    match profile {
        "binance_spot" => (
            6163,
            ExchangeId::Binance,
            MarketType::Spot,
            "BTCUSDT",
            Base,
            None,
            None,
        ),
        "binance_linear" => (
            9872,
            ExchangeId::Binance,
            MarketType::Linear,
            "BTCUSDT",
            Base,
            None,
            None,
        ),
        "binance_inverse" => (
            10792,
            ExchangeId::Binance,
            MarketType::Inverse,
            "BTCUSD_PERP",
            Contracts,
            Some("100"),
            Some("USD"),
        ),
        other => panic!("unknown fixture profile: {other}"),
    }
}

pub(super) fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    let native = super::symbol(profile);
    match channel {
        "trades" => vec![json!(format!("{}@trade", native.to_ascii_lowercase()))],
        "depth" => vec![json!(format!("{}@depth", native.to_ascii_lowercase()))],
        _ => unreachable!(),
    }
}
