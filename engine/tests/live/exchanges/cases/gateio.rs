mod rest_sync;
use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    stream::request::QuantityType::{Base, Contracts},
};
use serde_json::{Value, json};

// Instrument metadata captured from catalog.instrument_specs on 2026-10-24.
pub(super) fn instrument(profile: &str) -> super::InstrumentSpec {
    match profile {
        "gateio_spot" => (
            15850,
            ExchangeId::Gateio,
            MarketType::Spot,
            "BTC_USDT",
            Base,
            None,
            None,
        ),
        "gateio_linear" => (
            17397,
            ExchangeId::Gateio,
            MarketType::Linear,
            "BTC_USDT",
            Contracts,
            Some("0.0001"),
            Some("BTC"),
        ),
        "gateio_inverse" => (
            18256,
            ExchangeId::Gateio,
            MarketType::Inverse,
            "BTC_USD",
            Contracts,
            Some("1"),
            Some("USD"),
        ),
        other => panic!("unknown fixture profile: {other}"),
    }
}

pub(super) fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    let native = super::symbol(profile);
    match channel {
        "trades" => vec![json!(native)],
        "depth" if profile == "gateio_spot" => vec![json!(native), json!("100ms")],
        "depth" => vec![json!(native), json!("100ms"), json!("100")],
        _ => unreachable!(),
    }
}
