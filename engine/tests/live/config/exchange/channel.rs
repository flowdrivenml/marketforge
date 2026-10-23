use std::{collections::BTreeMap, fs, path::Path};

use marketforge_engine::live::config::{ChannelConfig, ExchangeConfig};

use marketforge_engine::live::config::exchange::DepthValue;

// ============================================================
// HELPERS
// ============================================================

fn parse_channel(input: &str) -> ChannelConfig {
    toml::from_str(input).unwrap_or_else(|error| panic!("Failed to parse channel: {error}"))
}

fn config_dir() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

fn real_profiles() -> Vec<std::path::PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(config_dir())
        .expect("Cannot read exchange configuration directory")
        .map(|entry| entry.expect("Cannot read directory entry").path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "toml"))
        .collect();

    paths.sort();
    paths
}

// ============================================================
// BASIC CHANNEL CONFIGURATION
// ============================================================

#[test]
fn deserialize_basic_trade_channel() {
    let input = r#"
enabled = true
channel = "publicTrade"
format = "trades/bitget_spot"
trade_semantics = "individual"
records = "array"
payload_path = "data"
"#;

    let channel = parse_channel(input);

    assert!(channel.enabled);
    assert_eq!(channel.channel.as_deref(), Some("publicTrade"));
    assert_eq!(channel.format.as_deref(), Some("trades/bitget_spot"));
    assert_eq!(channel.trade_semantics.as_deref(), Some("individual"));
    assert_eq!(channel.records.as_deref(), Some("array"));
    assert_eq!(channel.payload_path.as_deref(), Some("data"));
}

#[test]
fn deserialize_aggregated_trade_channel() {
    let input = r#"
enabled = true
route = "public"
channel = "trades"
subscription_template = '{"channel":"trades","instId":"{symbol}"}'
format = "trades/okx_spot_aggregated"
trade_semantics = "aggregated"
records = "array"
payload_path = "data"
recovery_strategy = "coverage_evaluation"
"#;

    let channel = parse_channel(input);

    assert!(channel.enabled);
    assert_eq!(channel.route.as_deref(), Some("public"));
    assert_eq!(channel.trade_semantics.as_deref(), Some("aggregated"));
    assert_eq!(
        channel.recovery_strategy.as_deref(),
        Some("coverage_evaluation")
    );
}

#[test]
fn deserialize_trade_recovery_configuration() {
    let input = r#"
enabled = true
format = "trades/binance_spot"
trade_semantics = "individual"
recovery_strategy = "trade_id_reconciliation"
recovery_endpoint = "historical_trades"
"#;

    let channel = parse_channel(input);

    assert_eq!(
        channel.recovery_strategy.as_deref(),
        Some("trade_id_reconciliation")
    );

    assert_eq!(
        channel.recovery_endpoint.as_deref(),
        Some("historical_trades")
    );
}

#[test]
fn deserialize_subscription_templates() {
    let input = r#"
enabled = true
channel = "books"
stream_template = "orderbook.{depth}.{symbol_upper}"
subscription_template = '{"channel":"books","instId":"{symbol}"}'
subscription_payload = ["{symbol}", "{interval}", "{depth}"]
"#;

    let channel = parse_channel(input);

    assert_eq!(
        channel.stream_template.as_deref(),
        Some("orderbook.{depth}.{symbol_upper}")
    );

    assert!(channel.subscription_template.is_some());

    assert_eq!(
        channel.subscription_payload.as_deref(),
        Some(
            &[
                "{symbol}".to_string(),
                "{interval}".to_string(),
                "{depth}".to_string()
            ][..]
        )
    );
}

// ============================================================
// DEPTH CONFIGURATION
// ============================================================

#[test]
fn deserialize_numeric_depth() {
    let channel = parse_channel(
        r#"
enabled = true
depth = 400
"#,
    );

    assert_eq!(channel.depth, Some(DepthValue::Levels(400)));
}

#[test]
fn deserialize_exchange_defined_depth() {
    let channel = parse_channel(
        r#"
enabled = true
depth = "exchange_defined"
"#,
    );

    assert_eq!(
        channel.depth,
        Some(DepthValue::Named("exchange_defined".into()))
    );
}

#[test]
fn deserialize_supported_depths() {
    let channel = parse_channel(
        r#"
enabled = true
supported_depths = [1, 50, 200, 1000]
default_depth = 200
"#,
    );

    assert_eq!(channel.supported_depths, vec![1, 50, 200, 1000]);
    assert_eq!(channel.default_depth, Some(200));
}

#[test]
fn deserialize_push_intervals() {
    let channel = parse_channel(
        r#"
enabled = true

[push_intervals_ms]
"1" = 10
"50" = 20
"200" = 100
"1000" = 200
"#,
    );

    assert_eq!(channel.push_intervals_ms["1"], 10);
    assert_eq!(channel.push_intervals_ms["50"], 20);
    assert_eq!(channel.push_intervals_ms["200"], 100);
    assert_eq!(channel.push_intervals_ms["1000"], 200);
}

#[test]
fn deserialize_supported_intervals() {
    let channel = parse_channel(
        r#"
enabled = true
default_interval = "100ms"
supported_intervals = ["20ms", "100ms"]

[constraints]
"20ms" = [20]
"100ms" = [20, 50, 100]
"#,
    );

    assert_eq!(channel.default_interval.as_deref(), Some("100ms"));

    assert_eq!(channel.supported_intervals, vec!["20ms", "100ms"]);

    assert_eq!(channel.constraints["20ms"], vec![20]);
    assert_eq!(channel.constraints["100ms"], vec![20, 50, 100]);
}

#[test]
fn deserialize_push_interval_ms() {
    let channel = parse_channel(
        r#"
enabled = true
depth = 400
push_interval_ms = 100
"#,
    );

    assert_eq!(channel.push_interval_ms, Some(100));
}

// ============================================================
// SYNCHRONIZATION
// ============================================================

#[test]
fn deserialize_bybit_synchronization() {
    let channel = parse_channel(
        r#"
enabled = true

[synchronization]
strategy = "bybit_v5_snapshot_delta"
snapshot_source = "websocket"
snapshot_message_type = "snapshot"
delta_message_type = "delta"
snapshot_action = "replace"
reset_update_id = 1
update_id_field = "u"
cross_sequence_field = "seq"
require_consecutive_update_id = false
rest_snapshot_endpoint = "depth_snapshot"
"#,
    );

    let sync = channel.synchronization.expect("Missing synchronization");

    assert_eq!(sync.strategy, "bybit_v5_snapshot_delta");
    assert_eq!(sync.snapshot_source.as_deref(), Some("websocket"));
    assert_eq!(sync.reset_update_id, Some(1));
    assert_eq!(sync.update_id_field.as_deref(), Some("u"));
    assert_eq!(sync.cross_sequence_field.as_deref(), Some("seq"));
    assert_eq!(sync.require_consecutive_update_id, Some(false));
}

#[test]
fn deserialize_bitget_synchronization() {
    let channel = parse_channel(
        r#"
enabled = true

[synchronization]
strategy = "bitget_v3_books"
snapshot_source = "websocket"
snapshot_message_type = "snapshot"
delta_message_type = "update"
snapshot_action = "replace"
sequence_field = "seq"
previous_sequence_field = "pseq"
continuity_strategy = "previous_sequence_matches"
reset_previous_sequence_id = 0
checksum_strategy = "none"
"#,
    );

    let sync = channel.synchronization.expect("Missing synchronization");

    assert_eq!(sync.strategy, "bitget_v3_books");
    assert_eq!(sync.sequence_field.as_deref(), Some("seq"));
    assert_eq!(sync.previous_sequence_field.as_deref(), Some("pseq"));
    assert_eq!(sync.reset_previous_sequence_id, Some(0));
    assert_eq!(sync.checksum_strategy.as_deref(), Some("none"));
}

#[test]
fn deserialize_gateio_synchronization() {
    let channel = parse_channel(
        r#"
enabled = true

[synchronization]
strategy = "gate_futures_order_book_update"
snapshot_source = "rest"
snapshot_endpoint = "depth_snapshot"
snapshot_sequence_field = "id"
first_update_id_field = "U"
last_update_id_field = "u"
continuity_strategy = "range_covers_next_update"
initial_bridge_strategy = "next_update_in_range"
buffer_updates_before_snapshot = true
websocket_full_snapshot_field = "full"
websocket_full_snapshot_action = "replace"
"#,
    );

    let sync = channel.synchronization.expect("Missing synchronization");

    assert_eq!(sync.strategy, "gate_futures_order_book_update");
    assert_eq!(sync.snapshot_source.as_deref(), Some("rest"));
    assert_eq!(sync.snapshot_endpoint.as_deref(), Some("depth_snapshot"));
    assert_eq!(sync.first_update_id_field.as_deref(), Some("U"));
    assert_eq!(sync.last_update_id_field.as_deref(), Some("u"));
    assert_eq!(sync.buffer_updates_before_snapshot, Some(true));
}

#[test]
fn deserialize_flat_synchronization_fields() {
    let channel = parse_channel(
        r#"
enabled = true
synchronization_strategy = "okx_v5_books"
snapshot_source = "websocket"
snapshot_action = "replace"
sequence_field = "seqId"
previous_sequence_field = "prevSeqId"
checksum_strategy = "none"
"#,
    );

    assert_eq!(
        channel.synchronization_strategy.as_deref(),
        Some("okx_v5_books")
    );

    assert_eq!(channel.sequence_field.as_deref(), Some("seqId"));
    assert_eq!(
        channel.previous_sequence_field.as_deref(),
        Some("prevSeqId")
    );
}

// ============================================================
// QUANTITY SEMANTICS
// ============================================================

#[test]
fn deserialize_spot_quantity() {
    let channel = parse_channel(
        r#"
enabled = true

[quantity]
raw_unit = "base_asset"
update_mode = "absolute"
zero_quantity_action = "delete"
"#,
    );

    let quantity = channel.quantity.expect("Missing quantity configuration");

    assert_eq!(quantity.raw_unit.as_deref(), Some("base_asset"));
    assert_eq!(quantity.update_mode.as_deref(), Some("absolute"));
    assert_eq!(quantity.zero_quantity_action.as_deref(), Some("delete"));
}

#[test]
fn deserialize_derivative_quantity() {
    let channel = parse_channel(
        r#"
enabled = true

[quantity]
quantity_semantics = "instrument_metadata"
raw_unit = "contracts"
update_mode = "absolute"
zero_quantity_action = "delete"
"#,
    );

    let quantity = channel.quantity.expect("Missing quantity configuration");

    assert_eq!(
        quantity.quantity_semantics.as_deref(),
        Some("instrument_metadata")
    );

    assert_eq!(quantity.raw_unit.as_deref(), Some("contracts"));
}

#[test]
fn deserialize_unverified_quantity() {
    let channel = parse_channel(
        r#"
enabled = true

[quantity]
quantity_semantics = "instrument_metadata"
raw_unit = "requires_verification"
"#,
    );

    let quantity = channel.quantity.expect("Missing quantity configuration");

    assert_eq!(quantity.raw_unit.as_deref(), Some("requires_verification"));
}

#[test]
fn deserialize_flat_quantity_fields() {
    let channel = parse_channel(
        r#"
enabled = true
quantity_mode = "absolute"
zero_quantity_action = "delete"
"#,
    );

    assert_eq!(channel.quantity_mode.as_deref(), Some("absolute"));
    assert_eq!(channel.zero_quantity_action.as_deref(), Some("delete"));
}

// ============================================================
// OPTIONAL CHANNELS
// ============================================================

#[test]
fn deserialize_disabled_channel_without_format() {
    let channel = parse_channel(
        r#"
enabled = false
channel = "books5"
depth = 5
synchronization_strategy = "snapshot_only"
"#,
    );

    assert!(!channel.enabled);
    assert!(channel.format.is_none());
    assert_eq!(channel.depth, Some(DepthValue::Levels(5)));
}

#[test]
fn deserialize_bbo_channel() {
    let channel = parse_channel(
        r#"
enabled = false
route = "public"
channel = "bbo-tbt"
depth = 1
push_interval_ms = 10
synchronization_strategy = "snapshot_only"
"#,
    );

    assert!(!channel.enabled);
    assert_eq!(channel.channel.as_deref(), Some("bbo-tbt"));
    assert_eq!(channel.depth, Some(DepthValue::Levels(1)));
}

// ============================================================
// STRICT FIELD RECOGNITION
// ============================================================

#[test]
fn reject_unknown_channel_field() {
    let input = r#"
enabled = true
unknown_channel_setting = 123
"#;

    assert!(
        toml::from_str::<ChannelConfig>(input).is_err(),
        "Unknown channel field was accepted"
    );
}

#[test]
fn reject_unknown_synchronization_field() {
    let input = r#"
enabled = true

[synchronization]
strategy = "bitget_v3_books"
unknown_sync_setting = true
"#;

    assert!(
        toml::from_str::<ChannelConfig>(input).is_err(),
        "Unknown synchronization field was accepted"
    );
}

#[test]
fn reject_unknown_quantity_field() {
    let input = r#"
enabled = true

[quantity]
unknown_quantity_setting = "unexpected"
"#;

    assert!(
        toml::from_str::<ChannelConfig>(input).is_err(),
        "Unknown quantity field was accepted"
    );
}

#[test]
fn reject_missing_enabled_field() {
    let input = r#"
channel = "trades"
format = "trades/binance_spot"
"#;

    assert!(
        toml::from_str::<ChannelConfig>(input).is_err(),
        "Channel without required enabled field was accepted"
    );
}

#[test]
fn reject_invalid_depth_type() {
    let input = r#"
enabled = true
depth = true
"#;

    assert!(
        toml::from_str::<ChannelConfig>(input).is_err(),
        "Boolean depth was accepted"
    );
}

#[test]
fn reject_invalid_supported_depths() {
    let input = r#"
enabled = true
supported_depths = ["100", "200"]
"#;

    assert!(
        toml::from_str::<ChannelConfig>(input).is_err(),
        "String depth limits were accepted"
    );
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn channel_config_round_trip() {
    let input = r#"
enabled = true
channel = "books"
format = "depth/bitget_spot"
depth = "exchange_defined"
push_interval_ms = 50

[synchronization]
strategy = "bitget_v3_books"
snapshot_source = "websocket"
sequence_field = "seq"
previous_sequence_field = "pseq"

[quantity]
raw_unit = "base_asset"
update_mode = "absolute"
zero_quantity_action = "delete"
"#;

    let original: toml::Value = toml::from_str(input).expect("Invalid original TOML");

    let channel = parse_channel(input);

    let serialized = toml::to_string(&channel).expect("Failed to serialize channel");

    let reconstructed: toml::Value =
        toml::from_str(&serialized).expect("Invalid reconstructed TOML");

    assert_eq!(
        original, reconstructed,
        "Channel configuration changed during round trip"
    );
}

// ============================================================
// REAL EXCHANGE CONFIGURATIONS
// ============================================================

#[test]
fn deserialize_all_real_channel_configs() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15, "Expected 15 exchange profiles");

    let mut total_channels = 0;

    for path in paths {
        let contents = fs::read_to_string(&path).expect("Cannot read exchange configuration");

        let original: toml::Value = toml::from_str(&contents).expect("Invalid exchange TOML");

        let channels_value = original
            .get("channels")
            .expect("Missing channels section")
            .clone();

        let channels_toml =
            toml::to_string(&channels_value).expect("Cannot serialize channels section");

        // TOML root must be a table.
        let channels: BTreeMap<String, ChannelConfig> = toml::from_str(&channels_toml)
            .unwrap_or_else(|error| {
                panic!(
                    "Failed to deserialize channels in {}: {error}",
                    path.display()
                )
            });

        let reconstructed: toml::Value =
            toml::from_str(&toml::to_string(&channels).expect("Cannot serialize typed channels"))
                .expect("Invalid reconstructed channels");

        assert_eq!(
            channels_value,
            reconstructed,
            "Channel fields changed in {}",
            path.display()
        );

        println!(
            "PASS: {} — {} channels recognized and preserved",
            path.file_name().unwrap().to_string_lossy(),
            channels.len()
        );

        total_channels += channels.len();
    }

    println!("\nAll 15 exchange profiles validated; {total_channels} channels checked.");
}

// ============================================================
// FULL CONFIGURATION INTEGRATION
// ============================================================

#[test]
fn channel_configs_integrate_with_exchange_config() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15);

    for path in paths {
        let config = ExchangeConfig::load(&path).unwrap_or_else(|error| {
            panic!(
                "Failed to load exchange configuration {}: {error}",
                path.display()
            )
        });

        assert!(
            !config.channels.is_empty(),
            "Exchange '{}' has no channels",
            config.exchange.profile
        );

        for (name, channel) in &config.channels {
            if channel.enabled && matches!(name.as_str(), "trades" | "aggregate_trades" | "depth") {
                assert!(
                    channel.format.is_some(),
                    "Enabled channel '{}' in '{}' has no format",
                    name,
                    config.exchange.profile
                );
            }
        }

        println!(
            "PASS: {} — {} channels integrated successfully",
            config.exchange.profile,
            config.channels.len()
        );
    }

    println!("\nAll 15 exchange configurations passed channel integration.");
}
