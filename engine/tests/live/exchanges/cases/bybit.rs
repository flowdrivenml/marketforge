mod derivatives_reference_book;
use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    stream::request::QuantityType::{Base, Quote},
};
use serde_json::{Value, json};

// Instrument metadata captured from catalog.instrument_specs on 2026-10-24.
pub(super) fn instrument(profile: &str) -> super::InstrumentSpec {
    match profile {
        "bybit_spot" => (
            1,
            ExchangeId::Bybit,
            MarketType::Spot,
            "BTCUSDT",
            Base,
            None,
            None,
        ),
        "bybit_linear" => (
            688,
            ExchangeId::Bybit,
            MarketType::Linear,
            "BTCUSDT",
            Base,
            None,
            None,
        ),
        "bybit_inverse" => (
            1427,
            ExchangeId::Bybit,
            MarketType::Inverse,
            "BTCUSD",
            Quote,
            None,
            None,
        ),
        other => panic!("unknown fixture profile: {other}"),
    }
}

pub(super) fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    let native = super::symbol(profile);
    match channel {
        "trades" => vec![json!(format!(
            "publicTrade.{}",
            native.to_ascii_uppercase()
        ))],
        "depth" => vec![json!(format!(
            "orderbook.200.{}",
            native.to_ascii_uppercase()
        ))],
        _ => unreachable!(),
    }
}
