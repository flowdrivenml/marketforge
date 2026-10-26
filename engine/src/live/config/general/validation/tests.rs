use crate::live::config::general::GeneralConfig;

// ============================================================
// VALID CONFIGURATION FIXTURE
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

fn valid_config() -> GeneralConfig {
    toml::from_str(VALID_GENERAL).expect("Test fixture must deserialize")
}

fn assert_invalid(config: &GeneralConfig, expected: &str) {
    let error = config
        .validate()
        .expect_err("Expected configuration validation to fail");

    assert!(
        error.contains(expected),
        "Expected error containing {expected:?}, got: {error}"
    );
}

// ============================================================
// VALID CONFIGURATION
// ============================================================

#[test]
fn accept_valid_general_config() {
    valid_config().validate().unwrap();
}

#[test]
fn accept_valid_config_with_recording_enabled() {
    let mut config = valid_config();

    config.recording.enabled = true;

    config.validate().unwrap();
}

#[test]
fn accept_valid_config_with_prometheus_enabled() {
    let mut config = valid_config();

    config.metrics.prometheus.enabled = true;

    config.validate().unwrap();
}

#[test]
fn accept_valid_config_with_metrics_disabled() {
    let mut config = valid_config();

    config.metrics.enabled = false;

    config.validate().unwrap();
}

#[test]
fn accept_ipv6_loopback_control_address() {
    let mut config = valid_config();

    config.control.listen_address = "[::1]:8080".into();

    config.validate().unwrap();
}

#[test]
fn accept_absolute_unix_socket_path() {
    let mut config = valid_config();

    config.publication.socket_path = "/var/run/marketforge/marketforge-data.sock".into();

    config.validate().unwrap();
}

#[test]
fn accept_recovery_backoff_equal_to_maximum() {
    let mut config = valid_config();

    config.defaults.recovery.initial_backoff_ms = 60_000;
    config.defaults.recovery.max_backoff_seconds = 60;

    config.validate().unwrap();
}

#[test]
fn accept_disabled_recovery_jitter() {
    let mut config = valid_config();

    config.defaults.recovery.jitter = false;

    config.validate().unwrap();
}

// ============================================================
// SCHEMA VERSION
// ============================================================

#[test]
fn reject_schema_version_zero() {
    let mut config = valid_config();

    config.schema_version = 0;

    assert_invalid(&config, "schema_version");
}

#[test]
fn reject_future_schema_version() {
    let mut config = valid_config();

    config.schema_version = 2;

    assert_invalid(&config, "schema_version");
}

#[test]
fn reject_large_schema_version() {
    let mut config = valid_config();

    config.schema_version = u32::MAX;

    assert_invalid(&config, "schema_version");
}

// ============================================================
// RUNTIME
// ============================================================

#[test]
fn reject_zero_shutdown_timeout() {
    let mut config = valid_config();

    config.runtime.shutdown_timeout_seconds = 0;

    assert_invalid(&config, "shutdown_timeout_seconds");
}

#[test]
fn accept_positive_shutdown_timeout() {
    let mut config = valid_config();

    config.runtime.shutdown_timeout_seconds = 1;

    config.validate().unwrap();
}

// ============================================================
// PUBLICATION
// ============================================================

#[test]
fn reject_empty_publication_socket_path() {
    let mut config = valid_config();

    config.publication.socket_path = String::new();

    assert_invalid(&config, "socket_path");
}

#[test]
fn reject_publication_socket_path_with_leading_whitespace() {
    let mut config = valid_config();

    config.publication.socket_path = " data/.live/marketforge-data.sock".into();

    assert_invalid(&config, "whitespace");
}

#[test]
fn reject_publication_socket_path_with_trailing_whitespace() {
    let mut config = valid_config();

    config.publication.socket_path = "data/.live/marketforge-data.sock ".into();

    assert_invalid(&config, "whitespace");
}

#[test]
fn reject_publication_socket_path_with_nul_byte() {
    let mut config = valid_config();

    config.publication.socket_path = "data/.live/marketforge\0-data.sock".into();

    assert_invalid(&config, "NUL");
}

#[test]
fn reject_publication_socket_path_without_filename() {
    let mut config = valid_config();

    config.publication.socket_path = "/".into();

    assert_invalid(&config, "socket filename");
}

#[test]
fn reject_publication_socket_path_without_sock_extension() {
    let mut config = valid_config();

    config.publication.socket_path = "data/.live/marketforge-data".into();

    assert_invalid(&config, ".sock");
}

#[test]
fn reject_zero_max_connections() {
    let mut config = valid_config();

    config.publication.max_connections = 0;

    assert_invalid(&config, "max_connections");
}

#[test]
fn reject_zero_subscriber_queue_bytes() {
    let mut config = valid_config();

    config.publication.subscriber_queue_bytes = 0;

    assert_invalid(&config, "subscriber_queue_bytes");
}

#[test]
fn accept_minimum_positive_publication_limits() {
    let mut config = valid_config();

    config.publication.max_connections = 1;
    config.publication.subscriber_queue_bytes = 1;

    config.validate().unwrap();
}

// ============================================================
// CONTROL REST API
// ============================================================

#[test]
fn reject_empty_control_address() {
    let mut config = valid_config();

    config.control.listen_address = String::new();

    assert_invalid(&config, "control.listen_address");
}

#[test]
fn reject_invalid_control_address() {
    let mut config = valid_config();

    config.control.listen_address = "not-an-address".into();

    assert_invalid(&config, "control.listen_address");
}

#[test]
fn reject_control_address_without_port() {
    let mut config = valid_config();

    config.control.listen_address = "127.0.0.1".into();

    assert_invalid(&config, "control.listen_address");
}

#[test]
fn reject_control_address_with_invalid_port() {
    let mut config = valid_config();

    config.control.listen_address = "127.0.0.1:invalid".into();

    assert_invalid(&config, "control.listen_address");
}

#[test]
fn reject_control_port_zero() {
    let mut config = valid_config();

    config.control.listen_address = "127.0.0.1:0".into();

    assert_invalid(&config, "nonzero TCP port");
}

#[test]
fn reject_control_wildcard_ipv4_binding() {
    let mut config = valid_config();

    config.control.listen_address = "0.0.0.0:8080".into();

    assert_invalid(&config, "loopback");
}

#[test]
fn reject_control_wildcard_ipv6_binding() {
    let mut config = valid_config();

    config.control.listen_address = "[::]:8080".into();

    assert_invalid(&config, "loopback");
}

#[test]
fn reject_control_public_ipv4_binding() {
    let mut config = valid_config();

    config.control.listen_address = "8.8.8.8:8080".into();

    assert_invalid(&config, "loopback");
}

#[test]
fn reject_control_private_network_binding() {
    let mut config = valid_config();

    config.control.listen_address = "192.168.1.100:8080".into();

    assert_invalid(&config, "loopback");
}

#[test]
fn accept_ipv4_loopback_control_binding() {
    let mut config = valid_config();

    config.control.listen_address = "127.0.0.1:9000".into();

    config.validate().unwrap();
}

// ============================================================
// RECORDING
// ============================================================

#[test]
fn reject_zero_recording_batch_records() {
    let mut config = valid_config();

    config.recording.batch_max_records = 0;

    assert_invalid(&config, "batch_max_records");
}

#[test]
fn reject_zero_recording_batch_delay() {
    let mut config = valid_config();

    config.recording.batch_max_delay_ms = 0;

    assert_invalid(&config, "batch_max_delay_ms");
}

#[test]
fn reject_invalid_recording_settings_even_when_disabled() {
    let mut config = valid_config();

    config.recording.enabled = false;
    config.recording.batch_max_records = 0;

    assert_invalid(&config, "batch_max_records");
}

#[test]
fn accept_positive_recording_batch_settings() {
    let mut config = valid_config();

    config.recording.batch_max_records = 1;
    config.recording.batch_max_delay_ms = 1;

    config.validate().unwrap();
}

// ============================================================
// METRICS
// ============================================================

#[test]
fn reject_zero_metrics_snapshot_interval() {
    let mut config = valid_config();

    config.metrics.snapshot_interval_ms = 0;

    assert_invalid(&config, "snapshot_interval_ms");
}

#[test]
fn reject_invalid_metrics_settings_when_disabled() {
    let mut config = valid_config();

    config.metrics.enabled = false;
    config.metrics.snapshot_interval_ms = 0;

    assert_invalid(&config, "snapshot_interval_ms");
}

#[test]
fn accept_positive_metrics_snapshot_interval() {
    let mut config = valid_config();

    config.metrics.snapshot_interval_ms = 1;

    config.validate().unwrap();
}

// ============================================================
// PROMETHEUS ADDRESS
// ============================================================

#[test]
fn reject_invalid_prometheus_address() {
    let mut config = valid_config();

    config.metrics.prometheus.listen_address = "invalid-address".into();

    assert_invalid(&config, "metrics.prometheus.listen_address");
}

#[test]
fn reject_prometheus_address_without_port() {
    let mut config = valid_config();

    config.metrics.prometheus.listen_address = "127.0.0.1".into();

    assert_invalid(&config, "metrics.prometheus.listen_address");
}

#[test]
fn reject_prometheus_port_zero() {
    let mut config = valid_config();

    config.metrics.prometheus.listen_address = "127.0.0.1:0".into();

    assert_invalid(&config, "nonzero TCP port");
}

#[test]
fn accept_valid_ipv6_prometheus_address() {
    let mut config = valid_config();

    config.metrics.prometheus.listen_address = "[::1]:9091".into();

    config.validate().unwrap();
}

#[test]
fn accept_nonloopback_prometheus_address() {
    let mut config = valid_config();

    // Unlike the management API, the Prometheus endpoint
    // is not restricted to loopback by this validator.
    //
    // Deployment must still control exposure appropriately.
    config.metrics.prometheus.listen_address = "0.0.0.0:9091".into();

    config.validate().unwrap();
}

// ============================================================
// PROMETHEUS PATH
// ============================================================

#[test]
fn reject_empty_prometheus_path() {
    let mut config = valid_config();

    config.metrics.prometheus.path = String::new();

    assert_invalid(&config, "path");
}

#[test]
fn reject_prometheus_path_without_leading_slash() {
    let mut config = valid_config();

    config.metrics.prometheus.path = "metrics".into();

    assert_invalid(&config, "start with '/'");
}

#[test]
fn reject_prometheus_path_with_query() {
    let mut config = valid_config();

    config.metrics.prometheus.path = "/metrics?format=text".into();

    assert_invalid(&config, "query or fragment");
}

#[test]
fn reject_prometheus_path_with_fragment() {
    let mut config = valid_config();

    config.metrics.prometheus.path = "/metrics#section".into();

    assert_invalid(&config, "query or fragment");
}

#[test]
fn reject_prometheus_path_with_whitespace() {
    let mut config = valid_config();

    config.metrics.prometheus.path = "/my metrics".into();

    assert_invalid(&config, "whitespace");
}

#[test]
fn accept_custom_prometheus_path() {
    let mut config = valid_config();

    config.metrics.prometheus.path = "/internal/metrics".into();

    config.validate().unwrap();
}

// ============================================================
// PROMETHEUS RESOURCE LIMITS
// ============================================================

#[test]
fn reject_zero_prometheus_max_concurrent_requests() {
    let mut config = valid_config();

    config.metrics.prometheus.max_concurrent_requests = 0;

    assert_invalid(&config, "max_concurrent_requests");
}

#[test]
fn reject_zero_prometheus_max_response_bytes() {
    let mut config = valid_config();

    config.metrics.prometheus.max_response_bytes = 0;

    assert_invalid(&config, "max_response_bytes");
}

#[test]
fn reject_zero_prometheus_cache_interval() {
    let mut config = valid_config();

    config.metrics.prometheus.cache_interval_ms = 0;

    assert_invalid(&config, "cache_interval_ms");
}

#[test]
fn reject_invalid_prometheus_settings_when_disabled() {
    let mut config = valid_config();

    config.metrics.prometheus.enabled = false;
    config.metrics.prometheus.max_response_bytes = 0;

    assert_invalid(&config, "max_response_bytes");
}

#[test]
fn accept_minimum_positive_prometheus_limits() {
    let mut config = valid_config();

    config.metrics.prometheus.max_concurrent_requests = 1;
    config.metrics.prometheus.max_response_bytes = 1;
    config.metrics.prometheus.cache_interval_ms = 1;

    config.validate().unwrap();
}

// ============================================================
// CROSS-SECTION CONSTRAINTS
// ============================================================

#[test]
fn reject_same_control_and_prometheus_address_when_enabled() {
    let mut config = valid_config();

    config.metrics.prometheus.enabled = true;
    config.metrics.prometheus.listen_address = config.control.listen_address.clone();

    assert_invalid(&config, "must differ");
}

#[test]
fn accept_same_control_and_prometheus_address_when_disabled() {
    let mut config = valid_config();

    config.metrics.prometheus.enabled = false;
    config.metrics.prometheus.listen_address = config.control.listen_address.clone();

    config.validate().unwrap();
}

#[test]
fn accept_distinct_control_and_prometheus_ports() {
    let mut config = valid_config();

    config.metrics.prometheus.enabled = true;
    config.control.listen_address = "127.0.0.1:8080".into();
    config.metrics.prometheus.listen_address = "127.0.0.1:9091".into();

    config.validate().unwrap();
}

// ============================================================
// RECOVERY DEFAULTS
// ============================================================

#[test]
fn reject_zero_initial_recovery_backoff() {
    let mut config = valid_config();

    config.defaults.recovery.initial_backoff_ms = 0;

    assert_invalid(&config, "initial_backoff_ms");
}

#[test]
fn reject_zero_maximum_recovery_backoff() {
    let mut config = valid_config();

    config.defaults.recovery.max_backoff_seconds = 0;

    assert_invalid(&config, "max_backoff_seconds");
}

#[test]
fn reject_initial_backoff_exceeding_maximum() {
    let mut config = valid_config();

    config.defaults.recovery.initial_backoff_ms = 60_001;
    config.defaults.recovery.max_backoff_seconds = 60;

    assert_invalid(&config, "initial_backoff_ms");
}

#[test]
fn reject_maximum_recovery_backoff_conversion_overflow() {
    let mut config = valid_config();

    config.defaults.recovery.max_backoff_seconds = u64::MAX;

    assert_invalid(&config, "too large");
}

#[test]
fn accept_recovery_backoff_below_maximum() {
    let mut config = valid_config();

    config.defaults.recovery.initial_backoff_ms = 500;
    config.defaults.recovery.max_backoff_seconds = 60;

    config.validate().unwrap();
}

// ============================================================
// CONFIGURATION MUTATIONS
// ============================================================

#[test]
fn validation_does_not_modify_configuration() {
    let config = valid_config();
    let original = config.clone();

    config.validate().unwrap();

    assert_eq!(config, original);
}

#[test]
fn validation_can_be_repeated() {
    let config = valid_config();

    config.validate().unwrap();
    config.validate().unwrap();
    config.validate().unwrap();
}
