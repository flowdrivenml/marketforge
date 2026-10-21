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

/// Cooperative sampling cadence. The runtime arms its timer at `next_deadline`;
/// late turns produce one current sample, never a burst of historical samples.
/// Disabled sampling does not read wall time or construct owner snapshots.
pub struct RouterMetricSampler {
    interval: std::time::Duration,
    next: Option<std::time::Instant>,
}

pub type RouterMetricResults = [(MetricId, Result<ObservationAdmission, ErrorCode>); 6];

impl RouterMetricSampler {
    pub fn new(
        interval: std::time::Duration,
        first: Option<std::time::Instant>,
    ) -> Result<Self, ErrorCode> {
        if interval.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            interval,
            next: first,
        })
    }

    pub fn next_deadline(&self) -> Option<std::time::Instant> {
        self.next
    }

    /// Runtime configuration supplies the next measurement boundary explicitly.
    pub fn schedule(&mut self, first: Option<std::time::Instant>) {
        self.next = first;
    }

    /// Every due attempt consumes its cadence, even if wall time or collection
    /// fails. Observational failures must not create an immediate retry loop.
    /// An unrepresentable next deadline disables sampling and returns Limit.
    pub fn sample_due(
        &mut self,
        now: std::time::Instant,
        wall: &dyn crate::live::foundation::clock::WallClock,
        metrics: &dyn ScopedMetricsPort,
        observe: impl FnOnce() -> RouterDrainSnapshot,
    ) -> Result<Option<RouterMetricResults>, ErrorCode> {
        let Some(next) = self.next else {
            return Ok(None);
        };
        if now < next {
            return Ok(None);
        }
        self.next = now.checked_add(self.interval);
        if self.next.is_none() {
            return Err(ErrorCode::Limit);
        }
        let timestamp = wall.unix_time_ns()?;
        Ok(Some(observe().record_metrics(metrics, timestamp)))
    }
}
