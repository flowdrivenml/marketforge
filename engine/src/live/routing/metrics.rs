//! Finite native gauges sourced from the authoritative router observation.
//! Sampling never polls delivery or changes mandatory resource accounting.
use super::shutdown::RouterDrainSnapshot;
use crate::live::{
    model::{
        error::ErrorCode,
        observation::{Measurement, MetricId, MetricValue},
    },
    ports::observability::{ObservationAdmission, ScopedMetricsPort},
};

/// Bind this catalog to the runtime metric owner before sampling. These values
/// describe overlapping responsibilities, not distinct delivered events.
pub const ROUTER_METRICS: [MetricId; 6] = [
    MetricId::RouterAdmissionEvents,
    MetricId::RouterAdmissionBytes,
    MetricId::RouterRetainedDeliveries,
    MetricId::RouterTrackedObligations,
    MetricId::RouterTerminalReports,
    MetricId::RouterEscalatedReports,
];

impl RouterDrainSnapshot {
    /// Return each independent sampling result, including disabled, dropped,
    /// stale-generation and unbound-series results. No retries or allocations.
    /// Wall time labels samples; the collector owns monotonic staleness checks.
    #[must_use]
    pub fn record_metrics(
        &self,
        metrics: &dyn ScopedMetricsPort,
        observed_at_ns: i64,
    ) -> [(MetricId, Result<ObservationAdmission, ErrorCode>); 6] {
        let values = [
            self.admission_usage.items,
            self.admission_usage.bytes,
            self.delivery.retained_deliveries,
            self.delivery.tracked_obligations,
            self.delivery.journal_records,
            self.delivery.escalated_records,
        ];
        std::array::from_fn(|i| {
            let metric = ROUTER_METRICS[i];
            let result = u64::try_from(values[i])
                .map_err(|_| ErrorCode::Capacity)
                .and_then(|value| {
                    metrics.record(
                        metric,
                        Measurement::Available {
                            value: if metric == MetricId::RouterAdmissionBytes {
                                MetricValue::Bytes(value)
                            } else {
                                MetricValue::UnsignedGauge(value)
                            },
                            observed_at_ns,
                        },
                    )
                });
            (metric, result)
        })
    }
}
