use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::live::config::{ConfigError, GeneralConfig, LogLevel};

// ============================================================
// FIXTURES
// ============================================================

fn general_config_path() -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/general.toml"
    ))
}

fn load_general() -> GeneralConfig {
    GeneralConfig::load(general_config_path()).expect("Failed to load general.toml")
}

// ============================================================
// TEMPORARY FILE HELPER
// ============================================================

struct TempToml {
    path: PathBuf,
}

impl TempToml {
    fn new(content: &str) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-general-test-{}-{timestamp}-{counter}.toml",
            std::process::id()
        ));

        fs::write(&path, content).unwrap();

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempToml {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

// ============================================================
// COMPLETE CONFIGURATION LOADING
// ============================================================

#[test]
fn load_real_general_config() {
    let config = load_general();

    assert_eq!(config.schema_version, 1);

    config.validate().unwrap();
}

#[test]
fn verify_real_general_config_values() {
    let config = load_general();

    assert_eq!(config.runtime.shutdown_timeout_seconds, 15);

    assert_eq!(
        config.publication.socket_path,
        "data/.live/marketforge-data.sock"
    );

    assert_eq!(config.publication.max_connections, 64);
    assert_eq!(config.publication.subscriber_queue_bytes, 16_777_216);

    assert_eq!(config.control.listen_address, "127.0.0.1:8080");

    assert!(!config.recording.enabled);
    assert_eq!(config.recording.batch_max_records, 10_000);
    assert_eq!(config.recording.batch_max_delay_ms, 100);

    assert!(config.metrics.enabled);
    assert_eq!(config.metrics.snapshot_interval_ms, 1_000);

    assert!(!config.metrics.prometheus.enabled);

    assert_eq!(config.metrics.prometheus.listen_address, "127.0.0.1:9091");

    assert_eq!(config.metrics.prometheus.path, "/metrics");
    assert_eq!(config.metrics.prometheus.max_concurrent_requests, 8);
    assert_eq!(config.metrics.prometheus.max_response_bytes, 4_194_304);
    assert_eq!(config.metrics.prometheus.cache_interval_ms, 1_000);

    assert_eq!(config.logging.level, LogLevel::Info);

    assert_eq!(config.defaults.recovery.initial_backoff_ms, 500);

    assert_eq!(config.defaults.recovery.max_backoff_seconds, 60);

    assert!(config.defaults.recovery.jitter);
}

// ============================================================
// TOML ROUND-TRIP PRESERVATION
// ============================================================

#[test]
fn real_general_config_preserves_all_fields() {
    let path = general_config_path();

    let original = fs::read_to_string(&path).unwrap();

    let config = GeneralConfig::load(&path).unwrap();

    let serialized = toml::to_string(&config).unwrap();

    let original_value: toml::Value = toml::from_str(&original).unwrap();

    let serialized_value: toml::Value = toml::from_str(&serialized).unwrap();

    assert_eq!(
        original_value, serialized_value,
        "General configuration lost or changed TOML fields"
    );
}

#[test]
fn deserialize_serialize_deserialize_preserves_config() {
    let original = load_general();

    let serialized = toml::to_string(&original).unwrap();

    let restored: GeneralConfig = toml::from_str(&serialized).unwrap();

    assert_eq!(original, restored);
}

#[test]
fn serialized_config_remains_valid() {
    let config = load_general();

    let serialized = toml::to_string(&config).unwrap();

    let restored: GeneralConfig = toml::from_str(&serialized).unwrap();

    restored.validate().unwrap();
}

// ============================================================
// CONFIGURATION ERRORS
// ============================================================

#[test]
fn reject_missing_general_config_file() {
    let path = std::env::temp_dir().join(format!(
        "marketforge-general-nonexistent-{}.toml",
        std::process::id()
    ));

    // Ensure the expected missing-file condition.
    let _ = fs::remove_file(&path);

    let result = GeneralConfig::load(&path);

    assert!(matches!(result, Err(ConfigError::Read { .. })));
}

#[test]
fn reject_invalid_toml_syntax() {
    let file = TempToml::new("schema_version = 1\n[runtime\ninvalid");

    let result = GeneralConfig::load(file.path());

    assert!(matches!(result, Err(ConfigError::Parse { .. })));
}

#[test]
fn reject_unknown_root_field() {
    let original = fs::read_to_string(general_config_path()).unwrap();

    let modified = format!("unknown_root_setting = true\n{original}");

    let file = TempToml::new(&modified);

    let result = GeneralConfig::load(file.path());

    assert!(matches!(result, Err(ConfigError::Parse { .. })));
}

#[test]
fn reject_missing_required_section() {
    let original = fs::read_to_string(general_config_path()).unwrap();

    let mut value: toml::Value = toml::from_str(&original).unwrap();

    value.as_table_mut().unwrap().remove("control");

    let modified = toml::to_string(&value).unwrap();

    let file = TempToml::new(&modified);

    let result = GeneralConfig::load(file.path());

    assert!(matches!(result, Err(ConfigError::Parse { .. })));
}

// ============================================================
// SEMANTIC VALIDATION THROUGH THE LOADER
// ============================================================

#[test]
fn reject_unsupported_schema_version_on_load() {
    let original = fs::read_to_string(general_config_path()).unwrap();

    let mut value: toml::Value = toml::from_str(&original).unwrap();

    value["schema_version"] = toml::Value::Integer(99);

    let file = TempToml::new(&toml::to_string(&value).unwrap());

    let result = GeneralConfig::load(file.path());

    assert!(matches!(result, Err(ConfigError::Validation { .. })));
}

#[test]
fn reject_invalid_control_address_on_load() {
    let original = fs::read_to_string(general_config_path()).unwrap();

    let mut value: toml::Value = toml::from_str(&original).unwrap();

    value["control"]["listen_address"] = toml::Value::String("0.0.0.0:8080".into());

    let file = TempToml::new(&toml::to_string(&value).unwrap());

    let result = GeneralConfig::load(file.path());

    assert!(matches!(result, Err(ConfigError::Validation { .. })));
}

#[test]
fn reject_zero_publication_capacity_on_load() {
    let original = fs::read_to_string(general_config_path()).unwrap();

    let mut value: toml::Value = toml::from_str(&original).unwrap();

    value["publication"]["subscriber_queue_bytes"] = toml::Value::Integer(0);

    let file = TempToml::new(&toml::to_string(&value).unwrap());

    let result = GeneralConfig::load(file.path());

    assert!(matches!(result, Err(ConfigError::Validation { .. })));
}

// ============================================================
// CONFIGURATION OWNERSHIP
// ============================================================

#[test]
fn loaded_config_is_independent_of_original_file() {
    let original = fs::read_to_string(general_config_path()).unwrap();

    let file = TempToml::new(&original);

    let config = GeneralConfig::load(file.path()).unwrap();

    fs::write(file.path(), "schema_version = 999\n").unwrap();

    assert_eq!(config.schema_version, 1);
    assert_eq!(config.publication.max_connections, 64);

    config.validate().unwrap();
}

#[test]
fn cloned_config_preserves_all_values() {
    let config = load_general();

    let cloned = config.clone();

    assert_eq!(config, cloned);
}
