use serde::{Deserialize, Serialize};

// ============================================================
// GENERAL CONFIGURATION
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneralConfig {
    pub schema_version: u32,

    pub runtime: RuntimeConfig,
    pub publication: PublicationConfig,
    pub control: ControlConfig,
    pub recording: RecordingConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub defaults: DefaultsConfig,
}

// ============================================================
// RUNTIME
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub shutdown_timeout_seconds: u64,
}

// ============================================================
// PUBLICATION
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationConfig {
    pub socket_path: String,

    /// Maximum number of simultaneous publication connections.
    ///
    /// Multiple connections from the same application count separately.
    pub max_connections: usize,

    /// Maximum queued payload bytes per publication connection.
    ///
    /// A connection that cannot keep up is disconnected.
    pub subscriber_queue_bytes: usize,
}

// ============================================================
// CONTROL REST API
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    /// Address on which the mandatory management REST API listens.
    ///
    /// Example: "127.0.0.1:8080"
    pub listen_address: String,
}

// ============================================================
// POSTGRESQL RECORDING
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingConfig {
    pub enabled: bool,

    pub batch_max_records: usize,
    pub batch_max_delay_ms: u64,
}

// ============================================================
// METRICS
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    pub enabled: bool,

    pub snapshot_interval_ms: u64,

    pub prometheus: PrometheusConfig,
}

// ============================================================
// PROMETHEUS
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    pub enabled: bool,

    pub listen_address: String,
    pub path: String,

    pub max_concurrent_requests: usize,
    pub max_response_bytes: usize,
    pub cache_interval_ms: u64,
}

// ============================================================
// LOGGING
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: LogLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

// ============================================================
// DEFAULTS
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultsConfig {
    pub recovery: RecoveryDefaultsConfig,
}

// ============================================================
// RECOVERY DEFAULTS
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDefaultsConfig {
    pub initial_backoff_ms: u64,

    pub max_backoff_seconds: u64,

    pub jitter: bool,
}
