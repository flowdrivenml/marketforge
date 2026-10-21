//! Bounded nonblocking observation boundary. Dropped diagnostic samples are
//! counted explicitly; hot paths never await an exporter or management client.
use crate::live::model::{
    error::ErrorCode,
    observation::{HealthSnapshot, MetricSample},
    operation::ResourceVersion,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationAdmission {
    Accepted,
    DroppedCapacity,
    Rejected(ErrorCode),
    Disabled,
    SampledOut,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticEvent {
    pub owner: ResourceVersion,
    pub code: ErrorCode,
    pub observed_at_ns: i64,
}
pub trait ObservationSinkPort: Send + Sync {
    fn metric(&self, sample: MetricSample) -> ObservationAdmission;
    fn health(&self, snapshot: HealthSnapshot) -> ObservationAdmission;
    fn diagnostic(&self, event: DiagnosticEvent) -> ObservationAdmission;
}

/// Registration occurs before owner startup. The handle is bound to one owner
/// generation and a finite metric catalog; runtime values cannot create labels.
pub trait ScopedMetricsPort: Send + Sync {
    fn owner(&self) -> &ResourceVersion;
    fn record(
        &self,
        metric: crate::live::model::observation::MetricId,
        measurement: crate::live::model::observation::Measurement<
            crate::live::model::observation::MetricValue,
        >,
    ) -> Result<ObservationAdmission, ErrorCode>;
}
