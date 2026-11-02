use marketforge_engine::live::config::GeneralConfig;

const VALID_CONFIG: &str = r#"
schema_version = 1

[runtime]
shutdown_timeout_seconds = 15

[runtime.processing]
workers = 4
ready_queue_capacity = 1024
max_events_per_turn = 512

[publication]
socket_path = "data/.live/marketforge-data.sock"
max_connections = 64
subscriber_queue_bytes = 16777216

[control]
listen_address = "127.0.0.1:8080"

[recording]
enabled = false
batch_max_records = 10000
batch_max_delay_ms = 100

[metrics]
enabled = true
snapshot_interval_ms = 1000

[metrics.prometheus]
enabled = false
listen_address = "127.0.0.1:9091"
path = "/metrics"
max_concurrent_requests = 8
max_response_bytes = 4194304
cache_interval_ms = 1000

[logging]
level = "info"

[defaults.recovery]
initial_backoff_ms = 500
max_backoff_seconds = 60
jitter = true
"#;

fn config() -> GeneralConfig {
    toml::from_str(VALID_CONFIG).unwrap()
}

#[test]
fn accepts_valid_processing_config() {
    let config = config();

    config.validate().unwrap();

    let processing = config.runtime.processing.unwrap();

    assert_eq!(processing.workers, 4);
    assert_eq!(processing.ready_queue_capacity, 1024);
    assert_eq!(processing.max_events_per_turn, 512);
}

#[test]
fn rejects_zero_workers() {
    let mut config = config();

    config.runtime.processing.as_mut().unwrap().workers = 0;

    assert!(config.validate().is_err());
}

#[test]
fn rejects_zero_ready_queue_capacity() {
    let mut config = config();

    config
        .runtime
        .processing
        .as_mut()
        .unwrap()
        .ready_queue_capacity = 0;

    assert!(config.validate().is_err());
}

#[test]
fn rejects_zero_events_per_turn() {
    let mut config = config();

    config
        .runtime
        .processing
        .as_mut()
        .unwrap()
        .max_events_per_turn = 0;

    assert!(config.validate().is_err());
}

#[test]
fn processing_configuration_round_trips() {
    let original = config();

    let serialized = toml::to_string(&original).unwrap();

    let restored: GeneralConfig = toml::from_str(&serialized).unwrap();

    assert_eq!(original, restored);
}

#[test]
fn legacy_configuration_without_processing_remains_valid() {
    let legacy = VALID_CONFIG.replace(
        "[runtime.processing]\nworkers = 4\nready_queue_capacity = 1024\nmax_events_per_turn = 512\n",
        "",
    );

    let config: GeneralConfig = toml::from_str(&legacy).unwrap();

    assert!(config.runtime.processing.is_none());
    config.validate().unwrap();
}

#[test]
fn rejects_unknown_processing_fields() {
    let invalid = VALID_CONFIG.replace("workers = 4", "workers = 4\nunknown_setting = true");

    assert!(toml::from_str::<GeneralConfig>(&invalid).is_err());
}

#[test]
fn legacy_processing_defaults_bound_aggregate_buffers() {
    let processing = config().runtime.processing.unwrap();
    assert_eq!(processing.buffered_events, 65536);
    assert_eq!(processing.buffered_bytes, 64 * 1024 * 1024);
}
#[test]
fn aggregate_buffer_limits_must_be_positive() {
    for field in ["buffered_events", "buffered_bytes"] {
        let input = VALID_CONFIG.replace("workers = 4", &format!("workers = 4\n{field} = 0"));
        let config: GeneralConfig = toml::from_str(&input).unwrap();
        assert!(config.validate().is_err());
    }
}

#[test]
fn nondefault_aggregate_limits_serialize_and_round_trip() {
    let mut original = config();
    let processing = original.runtime.processing.as_mut().unwrap();
    processing.buffered_events = 12;
    processing.buffered_bytes = 4096;
    let serialized = toml::to_string(&original).unwrap();
    assert!(serialized.contains("buffered_events = 12"));
    assert!(serialized.contains("buffered_bytes = 4096"));
    assert_eq!(
        toml::from_str::<GeneralConfig>(&serialized).unwrap(),
        original
    );
}

#[test]
fn derived_runtime_limits_roundtrip_and_reject_unbounded_or_zero_values() {
    let mut config = config();
    let p = config.runtime.processing.as_mut().unwrap();
    p.derived.max_inputs = 3;
    p.derived.state_events = 20;
    p.derived.state_bytes = 4096;
    p.derived.startup_timeout_ms = 1000;
    p.derived.stall_timeout_ms = 100;
    p.derived.retry_delay_ms = 2;
    config.validate().unwrap();
    let encoded = toml::to_string(&config).unwrap();
    let restored: GeneralConfig = toml::from_str(&encoded).unwrap();
    assert_eq!(config, restored);
    for field in 0..7 {
        let mut bad = config.clone();
        let d = &mut bad.runtime.processing.as_mut().unwrap().derived;
        match field {
            0 => d.max_inputs = 0,
            1 => d.state_events = 0,
            2 => d.state_bytes = 0,
            3 => d.stall_timeout_ms = 0,
            4 => d.retry_delay_ms = 0,
            5 => d.startup_timeout_ms = 0,
            _ => d.startup_timeout_ms = 86_400_001,
        };
        assert!(bad.validate().is_err());
    }
}
