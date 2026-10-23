use serde::{Deserialize, Serialize};

// ============================================================
// STREAM TARGET
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamAction {
    Enable,
    Disable,
    Restart,
    Reconnect,
    Resync,
}

// ============================================================
// STREAM MANAGEMENT
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamActionRequest {
    pub stream_id: String,
    pub action: StreamAction,
}

// ============================================================
// PUBLICATION RUNTIME CONFIGURATION
// ============================================================

/// Modify the active publication queue allowance.
///
/// Changes apply to existing and future connections.
///
/// Values from general.toml are restored after restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdatePublicationRequest {
    pub subscriber_queue_bytes: u64,
}

// ============================================================
// RECORDING RUNTIME CONFIGURATION
// ============================================================

/// Partial update to the recorder's runtime configuration.
///
/// None means the existing value remains unchanged.
///
/// All supplied changes must be validated and applied
/// atomically by the recording manager.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRecordingRequest {
    #[serde(default)]
    pub enabled: Option<bool>,

    #[serde(default)]
    pub batch_max_records: Option<usize>,

    #[serde(default)]
    pub batch_max_delay_ms: Option<u64>,
}

// ============================================================
// LOGGING RUNTIME CONFIGURATION
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateLoggingRequest {
    pub level: crate::live::config::general::LogLevel,
}

// ============================================================
// RUNTIME MANAGEMENT
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAction {
    Shutdown,
}

// ============================================================
// OPERATION RESULT
// ============================================================

/// The result of submitting a runtime management operation.
///
/// Accepted means the operation was scheduled or admitted.
///
/// It does not mean an asynchronous operation such as
/// reconnection or stream creation has completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Accepted,
    Completed,
    Rejected,
}

// ============================================================
// MANAGEMENT RESPONSE
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationResponse {
    pub status: OperationStatus,

    pub message: Option<String>,
}
