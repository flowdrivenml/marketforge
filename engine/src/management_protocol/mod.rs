//! Portable management contracts. Never acquire market demand merely to observe.
pub mod definitions;
use definitions::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Viewer,
    Operator,
    Administrator,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {detail} (correlation {correlation_id})")]
pub struct ManagementError {
    pub code: String,
    pub detail: String,
    pub correlation_id: String,
}
pub type Result<T> = std::result::Result<T, ManagementError>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    CreateSource {
        stream_id: String,
        request: CreateSourceStreamRequest,
        desired: DesiredStreamState,
    },
    CreateDerived {
        request: CreateDerivedStreamRequest,
        desired: DesiredStreamState,
    },
    EditDerived {
        stream_id: String,
        expected_revision: String,
        request: CreateDerivedStreamRequest,
        cascade: bool,
    },
    SetDesired {
        stream_id: String,
        expected_revision: String,
        desired: DesiredStreamState,
        cascade: bool,
    },
    Restart {
        stream_id: String,
        cascade: bool,
    },
    Delete {
        stream_id: String,
        expected_revision: String,
    },
    ResizeWorkers {
        workers: usize,
        save: bool,
    },
    SetObservationInterval {
        milliseconds: u64,
        save: bool,
    },
    Shutdown,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewRequest {
    pub action: Action,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyRequest {
    pub idempotency_key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePlan {
    pub id: String,
    pub daemon_instance: String,
    pub action: Action,
    pub graph_revision: String,
    pub pool_revision: String,
    pub config_revision: String,
    pub demand_revision: Option<String>,
    pub affected_streams: Vec<String>,
    pub transitions: Vec<String>,
    pub continuity_impact: String,
    pub persistence: String,
    pub warnings: Vec<String>,
    pub expires_in_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub owner: String,
    pub action: Action,
    pub state: String,
    pub cleanup_complete: bool,
    pub saved: bool,
    pub activation_requested: bool,
    pub runtime_ready: Option<bool>,
    pub detail: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseRequest {
    pub stream_id: String,
    pub seconds: u64,
    pub idempotency_key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewLease {
    pub idempotency_key: String,
    pub seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemandLease {
    pub id: String,
    pub owner: String,
    pub stream_id: String,
    pub remaining_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRelease {
    pub released: bool,
    pub stream_still_required: bool,
    pub explanation: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PageRequest {
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub filter: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamView {
    pub stream_id: String,
    pub definition: PersistedStreamDefinition,
    pub runtime: String,
    pub generation: String,
    pub readiness: String,
    pub reason: Option<String>,
    pub inputs: Vec<String>,
    pub reference_inputs: Vec<String>,
    pub dependents: Vec<String>,
    pub direct_owners: Vec<String>,
    pub why_running: Vec<String>,
    pub last_data_age_ms: Option<String>,
    pub integrity: serde_json::Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolView {
    pub revision: String,
    pub requested_target: usize,
    pub live_workers: usize,
    pub starting_workers: usize,
    pub retiring_workers: usize,
    pub executing_turns: usize,
    pub resizing: bool,
    pub resize_error: Option<String>,
    pub saved_startup_workers: Option<usize>,
    pub registered_processors: usize,
    pub ready_processors: Option<usize>,
    pub retained_payload_events: Option<usize>,
    pub retained_payload_bytes: Option<usize>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Overview {
    pub daemon_instance: String,
    pub observation_unix_ms: String,
    pub graph_revision: String,
    pub config_revision: String,
    pub event_cursor: String,
    pub serving: bool,
    pub shutting_down: bool,
    pub streams: Vec<StreamView>,
    pub pool: PoolView,
    pub metrics: Option<serde_json::Value>,
    pub operations: Vec<Operation>,
    pub consistency: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementEvent {
    pub cursor: String,
    pub daemon_instance: String,
    pub kind: String,
    pub operation_id: Option<String>,
    pub detail: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPage {
    pub events: Vec<ManagementEvent>,
    pub cursor: String,
    pub resync_required: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingView {
    pub name: String,
    pub effective: serde_json::Value,
    pub desired: serde_json::Value,
    pub mutability: String,
    pub saved: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    pub daemon_instance: String,
    pub role: Role,
    pub actions: Vec<String>,
    pub source_templates: Vec<CreateSourceStreamRequest>,
    pub limitations: Vec<String>,
}
