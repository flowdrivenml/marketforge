//! Secret-free management configuration contracts. The compiler owns catalog,
//! capability, dependency-cycle and economic validation; WP-09 owns persistence.
use super::{
    derived::DerivedDefinition,
    identity::{SourceStreamKey, StreamId},
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfigRevisionId(pub u64);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "value", rename_all = "snake_case")]
pub enum Override<T> {
    Inherit,
    Set(T),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsSettings {
    pub enabled: bool,
    pub detailed_timing: bool,
    pub snapshot_interval_ns: u64,
    pub timing_sample_one_in: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingSettings {
    pub enabled: bool,
    pub batch_events: u32,
    pub flush_interval_ns: u64,
    pub policy: super::delivery::DeliveryPolicy,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPolicy {
    pub enabled: bool,
    pub initial_backoff_ns: u64,
    pub max_backoff_ns: u64,
    pub max_attempts: u32,
    pub jitter: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBudgets {
    pub retention_memory_bytes: u64,
    pub retention_disk_bytes: u64,
    pub publisher_queue_events: u32,
    pub publisher_queue_bytes: u64,
    pub max_tasks: u32,
    pub max_file_descriptors: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalSettings {
    pub logging: LogLevel,
    pub metrics: MetricsSettings,
    pub recording: RecordingSettings,
    pub recovery: RecoveryPolicy,
    pub max_subscribers: u32,
    pub shutdown_deadline_ns: u64,
    pub budgets: ResourceBudgets,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDefinition {
    pub id: StreamId,
    pub key: SourceStreamKey,
    pub enabled: bool,
    pub recording: Override<RecordingSettings>,
    pub recovery: Override<RecoveryPolicy>,
    pub metrics: Override<MetricsSettings>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredConfiguration {
    pub schema_version: u16,
    pub global: GlobalSettings,
    pub sources: Vec<SourceDefinition>,
    pub derived: Vec<DerivedDefinition>,
    pub profiles: Vec<ProfileReference>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileReference {
    pub exchange: super::identity::ExchangeId,
    pub api_family: String,
    pub profile_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfigChange {
    SetLogging {
        level: LogLevel,
    },
    SetMetrics {
        settings: MetricsSettings,
    },
    SetRecordingDefaults {
        settings: RecordingSettings,
    },
    SetRecordingBatch {
        batch_events: u32,
        flush_interval_ns: u64,
    },
    SetMaxSubscribers {
        value: u32,
    },
    RegisterSource {
        definition: SourceDefinition,
    },
    SetSourceEnabled {
        stream: StreamId,
        enabled: bool,
    },
    RemoveSource {
        stream: StreamId,
    },
    SetSourceRecording {
        stream: StreamId,
        value: Override<RecordingSettings>,
    },
    SetSourceRecovery {
        stream: StreamId,
        value: Override<RecoveryPolicy>,
    },
    SetSourceMetrics {
        stream: StreamId,
        value: Override<MetricsSettings>,
    },
    PutDerived {
        definition: DerivedDefinition,
    },
    RemoveDerived {
        stream: super::identity::DerivedStreamId,
    },
    ReplaceProfile {
        profile: ProfileReference,
    },
    SetBudgets {
        budgets: ResourceBudgets,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigUpdate {
    pub expected_revision: ConfigRevisionId,
    pub changes: Vec<ConfigChange>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconfigurationBoundary {
    Immediate,
    NextMeasurement,
    NextBatch,
    FutureAdmission,
    StreamTransition,
    NewDerivedGeneration,
    ServiceRestart,
}
impl ConfigChange {
    pub fn boundary(&self) -> ReconfigurationBoundary {
        match self {
            Self::SetLogging { .. } => ReconfigurationBoundary::Immediate,
            Self::SetMetrics { .. } | Self::SetSourceMetrics { .. } => {
                ReconfigurationBoundary::NextMeasurement
            }
            Self::SetRecordingBatch { .. } => ReconfigurationBoundary::NextBatch,
            Self::SetMaxSubscribers { .. } => ReconfigurationBoundary::FutureAdmission,
            Self::PutDerived { .. } => ReconfigurationBoundary::NewDerivedGeneration,
            Self::ReplaceProfile { .. } | Self::SetBudgets { .. } => {
                ReconfigurationBoundary::ServiceRestart
            }
            _ => ReconfigurationBoundary::StreamTransition,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigIssue {
    pub path: String,
    pub code: super::error::ErrorCode,
    pub message: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub issues: Vec<ConfigIssue>,
    pub boundaries: Vec<ReconfigurationBoundary>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationRevision {
    pub id: ConfigRevisionId,
    pub parent: Option<ConfigRevisionId>,
    pub created_at_ns: i64,
    pub desired: DesiredConfiguration,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveConfiguration {
    pub desired: ConfigRevisionId,
    pub applied: Option<ConfigRevisionId>,
    pub adopted: Vec<(super::operation::ResourceVersion, ConfigRevisionId)>,
    pub resolved: ResolvedConfiguration,
}
impl DesiredConfiguration {
    pub fn validate_structure(&self, max_items: usize) -> Result<(), super::error::ErrorCode> {
        use super::error::ErrorCode;
        let g = &self.global;
        if self.schema_version != 1 {
            return Err(ErrorCode::UnsupportedVersion);
        }
        if self.sources.len() > max_items
            || self.derived.len() > max_items
            || self.profiles.len() > max_items
        {
            return Err(ErrorCode::Limit);
        }
        if g.metrics.snapshot_interval_ns == 0
            || g.metrics.timing_sample_one_in == 0
            || g.recording.batch_events == 0
            || g.recording.flush_interval_ns == 0
            || g.recovery.initial_backoff_ns == 0
            || g.recovery.max_backoff_ns < g.recovery.initial_backoff_ns
            || g.max_subscribers == 0
            || g.shutdown_deadline_ns == 0
            || g.budgets.max_tasks == 0
            || g.budgets.max_file_descriptors == 0
        {
            return Err(ErrorCode::InvalidInput);
        }
        if g.budgets.publisher_queue_events == 0 || g.budgets.publisher_queue_bytes == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        let mut identities = std::collections::HashSet::new();
        for s in &self.sources {
            if let Override::Set(m) = &s.metrics {
                if m.snapshot_interval_ns == 0 || m.timing_sample_one_in == 0 {
                    return Err(ErrorCode::InvalidInput);
                }
            }
            if let Override::Set(r) = &s.recording {
                if r.batch_events == 0 || r.flush_interval_ns == 0 {
                    return Err(ErrorCode::InvalidInput);
                }
            }
            if let Override::Set(r) = &s.recovery {
                if r.initial_backoff_ns == 0 || r.max_backoff_ns < r.initial_backoff_ns {
                    return Err(ErrorCode::InvalidInput);
                }
            }
            if s.id.0.trim().is_empty()
                || !identities.insert(s.id.0.clone())
                || s.key.validate().has_definite_failures()
            {
                return Err(ErrorCode::InvalidInput);
            }
        }
        for d in &self.derived {
            if let Override::Set(r) = &d.recording {
                if r.batch_events == 0 || r.flush_interval_ns == 0 {
                    return Err(ErrorCode::InvalidInput);
                }
            }
            match &d.transformation {
                super::derived::DerivedTransformation::Merge {
                    sources,
                    initial_window_ns,
                    max_window_ns,
                    max_buffer_events,
                    max_buffer_bytes,
                    ..
                } => {
                    if sources.is_empty()
                        || sources.len() > max_items
                        || max_window_ns < initial_window_ns
                        || *max_buffer_events == 0
                        || *max_buffer_bytes == 0
                    {
                        return Err(ErrorCode::InvalidInput);
                    }
                }
                super::derived::DerivedTransformation::Conversion {
                    target_currency,
                    max_reference_age_ns,
                    references,
                    ..
                } => {
                    if target_currency.trim().is_empty()
                        || *max_reference_age_ns == 0
                        || references.len() > max_items
                    {
                        return Err(ErrorCode::InvalidInput);
                    }
                }
            }
            if d.id.0.trim().is_empty() || !identities.insert(d.id.0.clone()) {
                return Err(ErrorCode::InvalidInput);
            }
        }
        Ok(())
    }
}

/// Fully resolved settings have no inheritance markers. Global budgets stay
/// global; resolving a source must never multiply the resource budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSource {
    pub id: StreamId,
    pub key: SourceStreamKey,
    pub enabled: bool,
    pub recording: RecordingSettings,
    pub recovery: RecoveryPolicy,
    pub metrics: MetricsSettings,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedDerived {
    pub id: super::identity::DerivedStreamId,
    pub enabled: bool,
    pub transformation: super::derived::DerivedTransformation,
    pub recording: RecordingSettings,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedConfiguration {
    pub global: GlobalSettings,
    pub sources: Vec<ResolvedSource>,
    pub derived: Vec<ResolvedDerived>,
    pub profiles: Vec<ProfileReference>,
}
impl<T: Clone> Override<T> {
    pub fn resolve(&self, inherited: &T) -> T {
        match self {
            Self::Inherit => inherited.clone(),
            Self::Set(v) => v.clone(),
        }
    }
}
impl DesiredConfiguration {
    /// Structural resolution only. Catalog/capability/dependency validation is
    /// still required from ConfigurationCompilerPort before persistence.
    pub fn resolve(
        &self,
        max_items: usize,
    ) -> Result<ResolvedConfiguration, super::error::ErrorCode> {
        self.validate_structure(max_items)?;
        Ok(ResolvedConfiguration {
            global: self.global.clone(),
            profiles: self.profiles.clone(),
            sources: self
                .sources
                .iter()
                .map(|s| ResolvedSource {
                    id: s.id.clone(),
                    key: s.key.clone(),
                    enabled: s.enabled,
                    recording: s.recording.resolve(&self.global.recording),
                    recovery: s.recovery.resolve(&self.global.recovery),
                    metrics: s.metrics.resolve(&self.global.metrics),
                })
                .collect(),
            derived: self
                .derived
                .iter()
                .map(|d| ResolvedDerived {
                    id: d.id.clone(),
                    enabled: d.enabled,
                    transformation: d.transformation.clone(),
                    recording: d.recording.resolve(&self.global.recording),
                })
                .collect(),
        })
    }
}

impl ConfigChange {
    pub fn validate_structure(&self, max_items: usize) -> Result<(), super::error::ErrorCode> {
        use super::error::ErrorCode;
        match self {
            Self::SetRecordingBatch {
                batch_events,
                flush_interval_ns,
            } if *batch_events == 0 || *flush_interval_ns == 0 => Err(ErrorCode::InvalidInput),
            Self::SetMaxSubscribers { value: 0 } => Err(ErrorCode::InvalidInput),
            Self::SetRecordingDefaults { settings }
                if settings.batch_events == 0 || settings.flush_interval_ns == 0 =>
            {
                Err(ErrorCode::InvalidInput)
            }
            Self::SetMetrics { settings }
                if settings.snapshot_interval_ns == 0 || settings.timing_sample_one_in == 0 =>
            {
                Err(ErrorCode::InvalidInput)
            }
            Self::RegisterSource { definition }
                if definition.id.0.trim().is_empty()
                    || definition.key.validate().has_definite_failures() =>
            {
                Err(ErrorCode::InvalidInput)
            }
            Self::PutDerived { definition } => {
                if definition.id.0.trim().is_empty() {
                    return Err(ErrorCode::InvalidInput);
                }
                match &definition.transformation {
                    super::derived::DerivedTransformation::Merge {
                        sources,
                        max_buffer_events,
                        max_buffer_bytes,
                        initial_window_ns,
                        max_window_ns,
                        ..
                    } if sources.is_empty()
                        || sources.len() > max_items
                        || *max_buffer_events == 0
                        || *max_buffer_bytes == 0
                        || max_window_ns < initial_window_ns =>
                    {
                        Err(ErrorCode::InvalidInput)
                    }
                    super::derived::DerivedTransformation::Conversion {
                        target_currency,
                        references,
                        max_reference_age_ns,
                        ..
                    } if target_currency.trim().is_empty()
                        || references.len() > max_items
                        || *max_reference_age_ns == 0 =>
                    {
                        Err(ErrorCode::InvalidInput)
                    }
                    _ => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationDiff {
    pub before: ConfigRevisionId,
    pub after: ConfigRevisionId,
    pub changes: Vec<ConfigChange>,
}
/// Restoring historical contents is compiled and persisted as a NEW revision;
/// it cannot rewind the revision counter or resurrect obsolete native handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationProposal {
    Changes(ConfigUpdate),
    Restore {
        expected_revision: ConfigRevisionId,
        contents: ConfigurationRevision,
    },
}
