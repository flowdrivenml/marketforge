//! Immutable resolved settings with explicit inheritance provenance.
use crate::live::model::{
    configuration::*,
    error::ErrorCode,
    identity::{DerivedStreamId, StreamId},
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingOrigin {
    Global,
    Explicit,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOrigins {
    pub source: StreamId,
    pub recording: SettingOrigin,
    pub recovery: SettingOrigin,
    pub metrics: SettingOrigin,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePlan {
    resolved: ResolvedConfiguration,
    sources: Vec<SourceOrigins>,
    derived: Vec<(DerivedStreamId, SettingOrigin)>,
}
fn origin<T>(value: &Override<T>) -> SettingOrigin {
    match value {
        Override::Inherit => SettingOrigin::Global,
        Override::Set(_) => SettingOrigin::Explicit,
    }
}
impl EffectivePlan {
    pub fn resolve(desired: &DesiredConfiguration, max_items: usize) -> Result<Self, ErrorCode> {
        Ok(Self {
            resolved: desired.resolve(max_items)?,
            sources: desired
                .sources
                .iter()
                .map(|s| SourceOrigins {
                    source: s.id.clone(),
                    recording: origin(&s.recording),
                    recovery: origin(&s.recovery),
                    metrics: origin(&s.metrics),
                })
                .collect(),
            derived: desired
                .derived
                .iter()
                .map(|d| (d.id.clone(), origin(&d.recording)))
                .collect(),
        })
    }
    pub fn resolved(&self) -> &ResolvedConfiguration {
        &self.resolved
    }
    pub fn source_origins(&self) -> &[SourceOrigins] {
        &self.sources
    }
    pub fn derived_origins(&self) -> &[(DerivedStreamId, SettingOrigin)] {
        &self.derived
    }
}
