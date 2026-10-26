mod reference_book;
use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    stream::request::QuantityType::{Base, Contracts},
};
use serde_json::{Value, json};

// Instrument metadata captured from catalog.instrument_specs on 2026-10-24.
pub(super) fn instrument(profile: &str) -> super::InstrumentSpec {
    match profile {
        "okx_spot" => (
            3336,
            ExchangeId::Okx,
            MarketType::Spot,
            "BTC-USDT",
            Base,
            None,
            None,
        ),
        "okx_linear" => (
            4063,
            ExchangeId::Okx,
            MarketType::Linear,
            "BTC-USDT-SWAP",
            Contracts,
            Some("0.01"),
            Some("BTC"),
        ),
        "okx_inverse" => (
            4048,
            ExchangeId::Okx,
            MarketType::Inverse,
            "BTC-USD-SWAP",
            Contracts,
            Some("100"),
            Some("USD"),
        ),
        other => panic!("unknown fixture profile: {other}"),
    }
}

pub(super) fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    let native = super::symbol(profile);
    let native_channel = if channel == "trades" {
        "trades-all"
    } else {
        "books"
    };
    vec![json!({"channel": native_channel, "instId": native})]
}
