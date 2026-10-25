#![cfg(feature = "live")]

use std::{fs, path::PathBuf};

use serde_json::Value;

use marketforge_engine::live::{
    config::format::FormatConfigStore,
    infrastructure::normalization::{
        JsonExtractor, QuantityPrecisionPolicy, TradeNormalizationCompiler, TradeRecordSemantics,
    },
};

// ============================================================
// FIXTURE DIRECTORY
// ============================================================

fn fixture_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.cache/live/websocket-fixtures/trades")
}

fn format_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats")
}

// ============================================================
// CAPTURED NATIVE INSTRUMENTS
// ============================================================

/// Native symbols used by the fixture collector.
///
/// These are test fixture identities, not instrument
/// economic metadata.
///
/// Economic quantities will be validated separately
/// using authoritative NormalizationContext values.
fn fixture_symbol(profile: &str) -> &'static str {
    match profile {
        "binance_spot" | "binance_linear" => "BTCUSDT",

        "binance_inverse" => "BTCUSD_PERP",

        "bybit_spot" | "bybit_linear" => "BTCUSDT",

        "bybit_inverse" => "BTCUSD",

        "okx_spot" => "BTC-USDT",

        "okx_linear" => "BTC-USDT-SWAP",

        "okx_inverse" => "BTC-USD-SWAP",

        "bitget_spot" | "bitget_linear" => "BTCUSDT",

        "bitget_inverse" => "BTCUSD_CM",

        "gateio_spot" | "gateio_linear" => "BTC_USDT",

        "gateio_inverse" => "BTC_USD",

        other => panic!("unknown fixture profile: {other}"),
    }
}

// ============================================================
// MAIN TEST
// ============================================================

#[test]
fn normalizes_all_real_websocket_trade_fields() {
    let formats = FormatConfigStore::load_dir(&format_directory())
        .expect("failed to load format configurations");

    assert_eq!(formats.trade_count(), 15, "expected 15 trade formats",);

    let fixture_root = fixture_directory();

    assert!(
        fixture_root.is_dir(),
        "missing fixture directory: {}",
        fixture_root.display(),
    );

    println!();
    println!("{}", "=".repeat(72));
    println!("MARKETFORGE — REAL WEBSOCKET TRADE NORMALIZATION AUDIT");
    println!("{}", "=".repeat(72));

    let mut profiles_tested = 0usize;
    let mut messages_tested = 0usize;
    let mut records_tested = 0usize;

    // ========================================================
    // ITERATE OVER THE ACTUAL FORMAT REGISTRY
    // ========================================================

    for (profile, config) in formats.trades() {
        let fixture_path = fixture_root.join(format!("{profile}.json"));

        assert!(
            fixture_path.is_file(),
            "{profile}: missing WebSocket fixture: {}",
            fixture_path.display(),
        );

        // ====================================================
        // LOAD CAPTURED MESSAGES
        // ====================================================

        let bytes = fs::read(&fixture_path)
            .unwrap_or_else(|error| panic!("{profile}: cannot read fixture: {error}"));

        let fixture: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{profile}: invalid JSON fixture: {error}"));

        // The upgraded collector stores an array of
        // complete native messages.
        //
        // Also accept older single-message fixtures.
        let messages: Vec<&Value> = match &fixture {
            Value::Array(messages) => messages.iter().collect(),

            Value::Object(_) => vec![&fixture],

            other => panic!("{profile}: unexpected fixture type: {other}"),
        };

        assert!(!messages.is_empty(), "{profile}: empty fixture",);

        // ====================================================
        // COMPILE THE ACTUAL FORMAT
        // ====================================================

        let plan = TradeNormalizationCompiler::compile_websocket(
            config,
            10_000,
            QuantityPrecisionPolicy::Round { decimal_places: 18 },
        )
        .unwrap_or_else(|error| panic!("{profile}: format compilation failed: {error}"));

        assert!(
            matches!(plan.semantics, TradeRecordSemantics::Individual),
            "{profile}: expected individual trade semantics",
        );

        let symbol = fixture_symbol(profile);

        let mut profile_records = 0usize;

        // ====================================================
        // VALIDATE EACH ORIGINAL MESSAGE
        // ====================================================

        for (message_index, message) in messages.iter().enumerate() {
            // ================================================
            // ENVELOPE IDENTITY
            // ================================================

            plan.envelope
                .validate(message, symbol)
                .unwrap_or_else(|error| {
                    panic!(
                        "{profile}: message {message_index}: \
                         envelope validation failed: {error}"
                    )
                });

            // ================================================
            // RECORD EXTRACTION
            // ================================================

            let records = plan.extraction.extract(message).unwrap_or_else(|error| {
                panic!(
                    "{profile}: message {message_index}: \
                         record extraction failed: {error}"
                )
            });

            assert!(
                !records.is_empty(),
                "{profile}: message {message_index}: \
                 no trade records",
            );

            // ================================================
            // NATIVE TRADE FIELD NORMALIZATION
            // ================================================

            for (record_index, record) in records.iter().enumerate() {
                // --------------------------------------------
                // RECORD-LEVEL INSTRUMENT IDENTITY
                // --------------------------------------------

                if let Some(path) = plan.record_symbol_field.as_deref() {
                    let native_symbol =
                        JsonExtractor::required(record, path).unwrap_or_else(|error| {
                            panic!(
                                "{profile}: message {message_index}, \
                                 record {record_index}: \
                                 cannot read symbol: {error}"
                            )
                        });

                    assert_eq!(
                        native_symbol.as_str(),
                        Some(symbol),
                        "{profile}: message {message_index}, \
                         record {record_index}: \
                         native instrument mismatch",
                    );
                }

                // --------------------------------------------
                // TYPED NATIVE FIELDS
                // --------------------------------------------

                let fields = plan.fields.read(record).unwrap_or_else(|error| {
                    panic!(
                        "{profile}: message {message_index}, \
                             record {record_index}: \
                             field normalization failed: {error}"
                    )
                });

                // --------------------------------------------
                // AGGRESSOR SIDE
                // --------------------------------------------

                let resolved = plan
                    .side
                    .resolve(fields.native_side, fields.native_quantity)
                    .unwrap_or_else(|error| {
                        panic!(
                            "{profile}: message {message_index}, \
                             record {record_index}: \
                             side normalization failed: {error}"
                        )
                    });

                // --------------------------------------------
                // BASIC ECONOMIC INTEGRITY
                // --------------------------------------------

                assert!(
                    fields.price.is_sign_positive() && !fields.price.is_zero(),
                    "{profile}: invalid trade price",
                );

                assert!(
                    resolved.quantity.is_sign_positive() && !resolved.quantity.is_zero(),
                    "{profile}: invalid resolved quantity",
                );

                assert!(
                    fields.timestamp_ns >= 0,
                    "{profile}: negative event timestamp",
                );

                assert!(
                    fields.trade_id.is_some(),
                    "{profile}: missing native trade ID",
                );

                profile_records += 1;
            }

            messages_tested += 1;
        }

        assert!(
            profile_records > 0,
            "{profile}: zero normalized trade records",
        );

        profiles_tested += 1;
        records_tested += profile_records;

        println!(
            "PASS  {profile:<20} \
             messages={:<5} trades={:<5}",
            messages.len(),
            profile_records,
        );
    }

    // ========================================================
    // FINAL REPORT
    // ========================================================

    println!();
    println!("{}", "=".repeat(72));
    println!("WEBSOCKET NORMALIZATION REPORT");
    println!("{}", "=".repeat(72));

    println!("Profiles:       {profiles_tested}");
    println!("Messages:       {messages_tested}");
    println!("Trade records:  {records_tested}");

    assert_eq!(profiles_tested, 15);
    assert!(messages_tested >= 15);
    assert!(records_tested >= 15);

    println!("RESULT: PASS");
}
