//! Serialized derived-definition contracts, not merge/conversion implementations.
use super::{
    configuration::Override,
    identity::{DerivedStreamId, StreamId},
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum StreamRef {
    Source(StreamId),
    Derived(DerivedStreamId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeOrdering {
    Arrival,
    AdaptiveEventTime,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateEventPolicy {
    EmitLate,
    SideOutput,
    Fail,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFailurePolicy {
    ContinueDegraded,
    Fail,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferencePolicy {
    EventTime,
    LatestObserved,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DerivedTransformation {
    Merge {
        sources: Vec<StreamRef>,
        ordering: MergeOrdering,
        initial_window_ns: u64,
        max_window_ns: u64,
        late_events: LateEventPolicy,
        source_failure: SourceFailurePolicy,
        max_buffer_events: u32,
        max_buffer_bytes: u64,
    },
    Conversion {
        source: StreamRef,
        target_currency: String,
        reference_policy: ReferencePolicy,
        max_reference_age_ns: u64,
        references: Vec<StreamRef>,
        publish_unavailable: bool,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedDefinition {
    pub id: DerivedStreamId,
    pub enabled: bool,
    pub transformation: DerivedTransformation,
    pub recording: Override<super::configuration::RecordingSettings>,
}
