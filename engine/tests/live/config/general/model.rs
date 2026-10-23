use marketforge_engine::live::config::general::{
    ControlConfig, DefaultsConfig, GeneralConfig, LogLevel, LoggingConfig, MetricsConfig,
    PrometheusConfig, PublicationConfig, RecordingConfig, RecoveryDefaultsConfig, RuntimeConfig,
};

// ============================================================
// TEST FIXTURES
// ============================================================

const VALID_GENERAL: &str = r#"
schema_version = 1

[runtime]
shutdown_timeout_seconds = 15

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

fn parse_general() -> GeneralConfig {
    toml::from_str(VALID_GENERAL).unwrap()
}

// ============================================================
// GENERAL CONFIGURATION
// ============================================================

#[test]
fn deserialize_complete_general_config() {
    let config = parse_general();

    assert_eq!(config.schema_version, 1);
    assert_eq!(config.runtime.shutdown_timeout_seconds, 15);
    assert_eq!(config.publication.max_connections, 64);
    assert_eq!(config.control.listen_address, "127.0.0.1:8080");
    assert!(!config.recording.enabled);
    assert!(config.metrics.enabled);
    assert_eq!(config.logging.level, LogLevel::Info);
    assert_eq!(config.defaults.recovery.initial_backoff_ms, 500);
}

#[test]
fn general_config_round_trip() {
    let original = parse_general();

    let serialized = toml::to_string(&original).unwrap();
    let restored: GeneralConfig = toml::from_str(&serialized).unwrap();

    assert_eq!(original, restored);
}

#[test]
fn general_config_preserves_all_toml_fields() {
    let original: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    let config = parse_general();

    let serialized = toml::to_string(&config).unwrap();
    let restored: toml::Value = toml::from_str(&serialized).unwrap();

    assert_eq!(
        original, restored,
        "GeneralConfig serialization did not preserve all TOML fields"
    );
}

#[test]
fn reject_missing_schema_version() {
    let modified = VALID_GENERAL.replacen("schema_version = 1\n", "", 1);

    assert!(toml::from_str::<GeneralConfig>(&modified).is_err());
}

#[test]
fn reject_noninteger_schema_version() {
    let modified = VALID_GENERAL.replace("schema_version = 1", "schema_version = \"1\"");

    assert!(toml::from_str::<GeneralConfig>(&modified).is_err());
}

#[test]
fn reject_negative_schema_version() {
    let modified = VALID_GENERAL.replace("schema_version = 1", "schema_version = -1");

    assert!(toml::from_str::<GeneralConfig>(&modified).is_err());
}

#[test]
fn reject_unknown_root_field() {
    let modified = format!("unexpected_setting = true\n{VALID_GENERAL}");

    assert!(toml::from_str::<GeneralConfig>(&modified).is_err());
}

#[test]
fn reject_missing_runtime_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("runtime");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn reject_missing_publication_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("publication");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn reject_missing_control_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("control");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn reject_missing_recording_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("recording");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn reject_missing_metrics_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("metrics");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn reject_missing_logging_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("logging");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn reject_missing_defaults_section() {
    let mut value: toml::Value = toml::from_str(VALID_GENERAL).unwrap();

    value.as_table_mut().unwrap().remove("defaults");

    assert!(toml::from_str::<GeneralConfig>(&toml::to_string(&value).unwrap()).is_err());
}

// ============================================================
// RUNTIME
// ============================================================

#[test]
fn deserialize_runtime_config() {
    let config: RuntimeConfig = toml::from_str("shutdown_timeout_seconds = 30").unwrap();

    assert_eq!(config.shutdown_timeout_seconds, 30);
}

#[test]
fn reject_missing_runtime_timeout() {
    assert!(toml::from_str::<RuntimeConfig>("").is_err());
}

#[test]
fn reject_unknown_runtime_field() {
    let input = r#"
shutdown_timeout_seconds = 15
restart_automatically = true
"#;

    assert!(toml::from_str::<RuntimeConfig>(input).is_err());
}

// ============================================================
// PUBLICATION
// ============================================================

#[test]
fn deserialize_publication_config() {
    let input = r#"
socket_path = "data/.live/test.sock"
max_connections = 32
subscriber_queue_bytes = 1048576
"#;

    let config: PublicationConfig = toml::from_str(input).unwrap();

    assert_eq!(config.socket_path, "data/.live/test.sock");
    assert_eq!(config.max_connections, 32);
    assert_eq!(config.subscriber_queue_bytes, 1_048_576);
}

#[test]
fn reject_missing_publication_socket_path() {
    let input = r#"
max_connections = 64
subscriber_queue_bytes = 16777216
"#;

    assert!(toml::from_str::<PublicationConfig>(input).is_err());
}

#[test]
fn reject_missing_max_connections() {
    let input = r#"
socket_path = "data/.live/test.sock"
subscriber_queue_bytes = 16777216
"#;

    assert!(toml::from_str::<PublicationConfig>(input).is_err());
}

#[test]
fn reject_missing_subscriber_queue_bytes() {
    let input = r#"
socket_path = "data/.live/test.sock"
max_connections = 64
"#;

    assert!(toml::from_str::<PublicationConfig>(input).is_err());
}

#[test]
fn reject_removed_publication_enabled_field() {
    let input = r#"
socket_path = "data/.live/test.sock"
max_connections = 64
subscriber_queue_bytes = 16777216
enabled = true
"#;

    assert!(toml::from_str::<PublicationConfig>(input).is_err());
}

#[test]
fn reject_removed_publication_transport_field() {
    let input = r#"
socket_path = "data/.live/test.sock"
max_connections = 64
subscriber_queue_bytes = 16777216
transport = "unix_socket"
"#;

    assert!(toml::from_str::<PublicationConfig>(input).is_err());
}

#[test]
fn reject_negative_publication_capacity() {
    let input = r#"
socket_path = "data/.live/test.sock"
max_connections = -1
subscriber_queue_bytes = 16777216
"#;

    assert!(toml::from_str::<PublicationConfig>(input).is_err());
}

// ============================================================
// CONTROL REST API
// ============================================================

#[test]
fn deserialize_control_config() {
    let config: ControlConfig = toml::from_str(r#"listen_address = "127.0.0.1:8080""#).unwrap();

    assert_eq!(config.listen_address, "127.0.0.1:8080");
}

#[test]
fn reject_missing_control_address() {
    assert!(toml::from_str::<ControlConfig>("").is_err());
}

#[test]
fn reject_removed_control_socket_path() {
    let input = r#"
listen_address = "127.0.0.1:8080"
socket_path = "data/.live/control.sock"
"#;

    assert!(toml::from_str::<ControlConfig>(input).is_err());
}

#[test]
fn reject_unknown_control_field() {
    let input = r#"
listen_address = "127.0.0.1:8080"
authentication_mode = "none"
"#;

    assert!(toml::from_str::<ControlConfig>(input).is_err());
}

// ============================================================
// RECORDING
// ============================================================

#[test]
fn deserialize_recording_config() {
    let input = r#"
enabled = true
batch_max_records = 5000
batch_max_delay_ms = 250
"#;

    let config: RecordingConfig = toml::from_str(input).unwrap();

    assert!(config.enabled);
    assert_eq!(config.batch_max_records, 5_000);
    assert_eq!(config.batch_max_delay_ms, 250);
}

#[test]
fn deserialize_disabled_recording_config() {
    let input = r#"
enabled = false
batch_max_records = 10000
batch_max_delay_ms = 100
"#;

    let config: RecordingConfig = toml::from_str(input).unwrap();

    assert!(!config.enabled);
}

#[test]
fn reject_missing_recording_enabled() {
    let input = r#"
batch_max_records = 10000
batch_max_delay_ms = 100
"#;

    assert!(toml::from_str::<RecordingConfig>(input).is_err());
}

#[test]
fn reject_unknown_recording_field() {
    let input = r#"
enabled = false
batch_max_records = 10000
batch_max_delay_ms = 100
retention_days = 7
"#;

    assert!(toml::from_str::<RecordingConfig>(input).is_err());
}

// ============================================================
// METRICS
// ============================================================

#[test]
fn deserialize_metrics_config() {
    let input = r#"
enabled = true
snapshot_interval_ms = 1000

[prometheus]
enabled = false
listen_address = "127.0.0.1:9091"
path = "/metrics"
max_concurrent_requests = 8
max_response_bytes = 4194304
cache_interval_ms = 1000
"#;

    let config: MetricsConfig = toml::from_str(input).unwrap();

    assert!(config.enabled);
    assert_eq!(config.snapshot_interval_ms, 1_000);
    assert!(!config.prometheus.enabled);
}

#[test]
fn reject_missing_metrics_prometheus_section() {
    let input = r#"
enabled = true
snapshot_interval_ms = 1000
"#;

    assert!(toml::from_str::<MetricsConfig>(input).is_err());
}

#[test]
fn reject_unknown_metrics_field() {
    let input = r#"
enabled = true
snapshot_interval_ms = 1000
histogram_buckets = 16

[prometheus]
enabled = false
listen_address = "127.0.0.1:9091"
path = "/metrics"
max_concurrent_requests = 8
max_response_bytes = 4194304
cache_interval_ms = 1000
"#;

    assert!(toml::from_str::<MetricsConfig>(input).is_err());
}

// ============================================================
// PROMETHEUS
// ============================================================

#[test]
fn deserialize_prometheus_config() {
    let input = r#"
enabled = true
listen_address = "127.0.0.1:9091"
path = "/metrics"
max_concurrent_requests = 16
max_response_bytes = 8388608
cache_interval_ms = 500
"#;

    let config: PrometheusConfig = toml::from_str(input).unwrap();

    assert!(config.enabled);
    assert_eq!(config.listen_address, "127.0.0.1:9091");
    assert_eq!(config.path, "/metrics");
    assert_eq!(config.max_concurrent_requests, 16);
    assert_eq!(config.max_response_bytes, 8_388_608);
    assert_eq!(config.cache_interval_ms, 500);
}

#[test]
fn reject_missing_prometheus_path() {
    let input = r#"
enabled = true
listen_address = "127.0.0.1:9091"
max_concurrent_requests = 8
max_response_bytes = 4194304
cache_interval_ms = 1000
"#;

    assert!(toml::from_str::<PrometheusConfig>(input).is_err());
}

#[test]
fn reject_unknown_prometheus_field() {
    let input = r#"
enabled = false
listen_address = "127.0.0.1:9091"
path = "/metrics"
max_concurrent_requests = 8
max_response_bytes = 4194304
cache_interval_ms = 1000
authentication = "none"
"#;

    assert!(toml::from_str::<PrometheusConfig>(input).is_err());
}

// ============================================================
// LOGGING
// ============================================================

#[test]
fn deserialize_logging_config() {
    let config: LoggingConfig = toml::from_str(r#"level = "debug""#).unwrap();

    assert_eq!(config.level, LogLevel::Debug);
}

#[test]
fn deserialize_all_logging_levels() {
    let cases = [
        ("trace", LogLevel::Trace),
        ("debug", LogLevel::Debug),
        ("info", LogLevel::Info),
        ("warn", LogLevel::Warn),
        ("error", LogLevel::Error),
    ];

    for (value, expected) in cases {
        let input = format!("level = \"{value}\"");

        let config: LoggingConfig = toml::from_str(&input).unwrap();

        assert_eq!(config.level, expected);
        assert_eq!(config.level.as_str(), value);
    }
}

#[test]
fn reject_unsupported_logging_level() {
    assert!(toml::from_str::<LoggingConfig>(r#"level = "verbose""#).is_err());
}

#[test]
fn reject_uppercase_logging_level() {
    assert!(toml::from_str::<LoggingConfig>(r#"level = "INFO""#).is_err());
}

#[test]
fn reject_unknown_logging_field() {
    let input = r#"
level = "info"
output = "file"
"#;

    assert!(toml::from_str::<LoggingConfig>(input).is_err());
}

#[test]
fn logging_level_round_trip() {
    for level in [
        LogLevel::Trace,
        LogLevel::Debug,
        LogLevel::Info,
        LogLevel::Warn,
        LogLevel::Error,
    ] {
        let config = LoggingConfig { level };

        let serialized = toml::to_string(&config).unwrap();
        let restored: LoggingConfig = toml::from_str(&serialized).unwrap();

        assert_eq!(config, restored);
    }
}

// ============================================================
// DEFAULTS
// ============================================================

#[test]
fn deserialize_defaults_config() {
    let input = r#"
[recovery]
initial_backoff_ms = 500
max_backoff_seconds = 60
jitter = true
"#;

    let config: DefaultsConfig = toml::from_str(input).unwrap();

    assert_eq!(config.recovery.initial_backoff_ms, 500);
    assert_eq!(config.recovery.max_backoff_seconds, 60);
    assert!(config.recovery.jitter);
}

#[test]
fn reject_missing_recovery_defaults() {
    assert!(toml::from_str::<DefaultsConfig>("").is_err());
}

#[test]
fn reject_unknown_defaults_section() {
    let input = r#"
[recovery]
initial_backoff_ms = 500
max_backoff_seconds = 60
jitter = true

[retry]
count = 3
"#;

    assert!(toml::from_str::<DefaultsConfig>(input).is_err());
}

// ============================================================
// RECOVERY DEFAULTS
// ============================================================

#[test]
fn deserialize_recovery_defaults() {
    let input = r#"
initial_backoff_ms = 250
max_backoff_seconds = 30
jitter = false
"#;

    let config: RecoveryDefaultsConfig = toml::from_str(input).unwrap();

    assert_eq!(config.initial_backoff_ms, 250);
    assert_eq!(config.max_backoff_seconds, 30);
    assert!(!config.jitter);
}

#[test]
fn reject_missing_recovery_jitter() {
    let input = r#"
initial_backoff_ms = 500
max_backoff_seconds = 60
"#;

    assert!(toml::from_str::<RecoveryDefaultsConfig>(input).is_err());
}

#[test]
fn reject_removed_recovery_enabled_field() {
    let input = r#"
enabled = true
initial_backoff_ms = 500
max_backoff_seconds = 60
jitter = true
"#;

    assert!(toml::from_str::<RecoveryDefaultsConfig>(input).is_err());
}

#[test]
fn reject_unknown_recovery_field() {
    let input = r#"
initial_backoff_ms = 500
max_backoff_seconds = 60
jitter = true
max_attempts = 10
"#;

    assert!(toml::from_str::<RecoveryDefaultsConfig>(input).is_err());
}

// ============================================================
// SERDE VS SEMANTIC VALIDATION
// ============================================================

#[test]
fn zero_values_deserialize_but_fail_semantic_validation() {
    let modified = VALID_GENERAL.replace(
        "shutdown_timeout_seconds = 15",
        "shutdown_timeout_seconds = 0",
    );

    let config: GeneralConfig = toml::from_str(&modified).unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn non_loopback_address_deserializes_but_fails_validation() {
    let modified = VALID_GENERAL.replace(
        "listen_address = \"127.0.0.1:8080\"",
        "listen_address = \"0.0.0.0:8080\"",
    );

    let config: GeneralConfig = toml::from_str(&modified).unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn valid_general_config_passes_semantic_validation() {
    let config = parse_general();

    config.validate().unwrap();
}
