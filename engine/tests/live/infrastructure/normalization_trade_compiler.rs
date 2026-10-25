#![cfg(feature = "live")]

use std::path::PathBuf;

use marketforge_engine::live::{
    config::format::FormatConfigStore,
    infrastructure::normalization::{QuantityPrecisionPolicy, TradeNormalizationCompiler},
};

#[test]
fn compiles_all_real_websocket_trade_formats() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

    let formats = FormatConfigStore::load_dir(&root).expect("cannot load trade/depth formats");

    assert_eq!(formats.trade_count(), 15);

    let mut compiled = 0usize;

    for (name, config) in formats.trades() {
        let plan = TradeNormalizationCompiler::compile_websocket(
            config,
            10_000,
            QuantityPrecisionPolicy::Round { decimal_places: 18 },
        )
        .unwrap_or_else(|error| panic!("{name}: trade compilation failed: {error}"));

        compiled += 1;

        println!(
            "PASS  {name:<20} records={:<12} \
             semantics={:?} timestamp={:?}",
            plan.extraction.path(),
            plan.semantics,
            plan.fields.timestamp_unit(),
        );
    }

    assert_eq!(compiled, 15);

    println!();
    println!("==========================================");
    println!("MARKETFORGE TRADE FORMAT COMPILATION");
    println!("==========================================");
    println!("Formats loaded:   {}", formats.trade_count());
    println!("Formats compiled: {compiled}");
    println!("Result:           PASS");
}
