#![cfg(feature = "process")]

#[path = "depth_formats/common.rs"]
mod common;

#[path = "depth_formats/bybit/mod.rs"]
mod bybit;

#[path = "depth_formats/bitget/mod.rs"]
mod bitget;

#[path = "depth_formats/gateio/mod.rs"]
mod gateio;

#[path = "depth_formats/okx/mod.rs"]
mod okx;
