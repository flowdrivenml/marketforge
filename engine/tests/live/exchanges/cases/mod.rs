mod binance;
mod bitget;
mod bybit;
mod gateio;
mod okx;

use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    stream::request::QuantityType,
};
use serde_json::Value;

type InstrumentSpec = (
    i64,
    ExchangeId,
    MarketType,
    &'static str,
    QuantityType,
    Option<&'static str>,
    Option<&'static str>,
);

pub(crate) const PROFILES: [&str; 15] = [
    "binance_spot",
    "binance_linear",
    "binance_inverse",
    "bybit_spot",
    "bybit_linear",
    "bybit_inverse",
    "okx_spot",
    "okx_linear",
    "okx_inverse",
    "bitget_spot",
    "bitget_linear",
    "bitget_inverse",
    "gateio_spot",
    "gateio_linear",
    "gateio_inverse",
];

pub(crate) fn instrument(profile: &str) -> InstrumentSpec {
    match profile.split('_').next().unwrap() {
        "binance" => binance::instrument(profile),
        "bybit" => bybit::instrument(profile),
        "bitget" => bitget::instrument(profile),
        "okx" => okx::instrument(profile),
        "gateio" => gateio::instrument(profile),
        other => panic!("unknown fixture exchange: {other}"),
    }
}

pub(crate) fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    match profile.split('_').next().unwrap() {
        "binance" => binance::expected_arguments(profile, channel),
        "bybit" => bybit::expected_arguments(profile, channel),
        "bitget" => bitget::expected_arguments(profile, channel),
        "okx" => okx::expected_arguments(profile, channel),
        "gateio" => gateio::expected_arguments(profile, channel),
        other => panic!("unknown fixture exchange: {other}"),
    }
}

pub(crate) fn symbol(profile: &str) -> &'static str {
    instrument(profile).3
}
