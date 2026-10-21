//! Finite prebound native series. Collection never waits for snapshots/exporters.
use super::histogram::BoundedHistogram;
use crate::live::{
    foundation::{
        clock::MonotonicClock,
        resources::{ResourceBudget, ResourceReservation, ResourceUsage},
    },
    model::{error::ErrorCode, observation::*, operation::ResourceVersion},
    ports::observability::{ObservationAdmission, ScopedMetricsPort},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

struct Series {
    metric: MetricId,
    measurement: Measurement<MetricValue>,
    received: Option<Instant>,
    histogram: BoundedHistogram,
    counter: u64,
    overflow: bool,
}
struct Scope {
    _capacity: ResourceReservation,
    binding: MetricBinding,
    active: AtomicBool,
    series: Mutex<Vec<Series>>,
}
struct Shared {
    enabled: AtomicBool,
    dropped: AtomicU64,
    clock: Arc<dyn MonotonicClock>,
}
/// Registration is a startup/control operation. The number of scopes and all
/// series are bounded, including retained obsolete handles. Replacement needs
/// spare capacity and invalidates handles from an older owner.
pub struct NativeMetrics {
    shared: Arc<Shared>,
    scopes: Mutex<Vec<Arc<Scope>>>,
    max_scopes: usize,
    max_series: usize,
    retained: ResourceBudget,
}
#[derive(Clone)]
pub struct NativeMetricHandle {
    shared: Arc<Shared>,
    scope: Arc<Scope>,
}
impl NativeMetrics {
    pub fn new(
        clock: Arc<dyn MonotonicClock>,
        max_scopes: usize,
        max_series: usize,
        enabled: bool,
    ) -> Result<Self, ErrorCode> {
        if max_scopes == 0 || max_series == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                enabled: AtomicBool::new(enabled),
                dropped: AtomicU64::new(0),
                clock,
            }),
            scopes: Mutex::new(Vec::new()),
            max_scopes,
            max_series,
            retained: ResourceBudget::new(ResourceUsage {
                items: max_scopes,
                bytes: max_series,
            })?,
        })
    }
    pub fn register(&self, binding: MetricBinding) -> Result<NativeMetricHandle, ErrorCode> {
        binding.validate(self.max_series)?;
        // Bound caller-controlled identity strings as well as series counts.
        if serde_json::to_vec(&binding.owner)
            .map_err(|_| ErrorCode::InvalidInput)?
            .len()
            > 4096
        {
            return Err(ErrorCode::Limit);
        }
        let mut scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        let replacement = scopes
            .iter()
            .position(|s| s.binding.owner.resource == binding.owner.resource);
        if let Some(i) = replacement {
            let old = &scopes[i].binding;
            if binding.owner.generation < old.owner.generation
                || (binding.owner.generation == old.owner.generation
                    && binding.reset_epoch <= old.reset_epoch)
            {
                return Err(ErrorCode::StaleGeneration);
            }
        }
        let count: usize = scopes
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != replacement)
            .map(|(_, s)| s.binding.metrics.len())
            .sum();
        if count
            .checked_add(binding.metrics.len())
            .is_none_or(|n| n > self.max_series)
            || (replacement.is_none() && scopes.len() >= self.max_scopes)
        {
            return Err(ErrorCode::Capacity);
        }
        let retained = self.retained.reserve(ResourceUsage {
            items: 1,
            bytes: binding.metrics.len(),
        })?;
        let scope = Arc::new(Scope {
            _capacity: retained,
            series: Mutex::new(
                binding
                    .metrics
                    .iter()
                    .map(|metric| Series {
                        metric: *metric,
                        measurement: Measurement::Unavailable {
                            reason: MissingMeasurement::NotCollected,
                        },
                        received: None,
                        histogram: BoundedHistogram::default(),
                        counter: 0,
                        overflow: false,
                    })
                    .collect(),
            ),
            binding,
            active: AtomicBool::new(true),
        });
        if let Some(i) = replacement {
            // Serialize invalidation with in-flight records before publishing
            // the replacement. Old snapshots cannot appear under the new scope.
            let _guard = scopes[i].series.lock().unwrap_or_else(|e| e.into_inner());
            scopes[i].active.store(false, Ordering::Release);
            drop(_guard);
            scopes[i] = scope.clone();
        } else {
            scopes.push(scope.clone());
        }
        Ok(NativeMetricHandle {
            shared: self.shared.clone(),
            scope,
        })
    }
    /// Disabling avoids optional work. Enabling fences existing handles: a
    /// fresh binding/reset epoch is required before collection resumes.
    pub fn set_enabled(&self, enabled: bool) {
        let scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        let old = self.shared.enabled.swap(enabled, Ordering::AcqRel);
        if old == enabled {
            return;
        }
        if enabled {
            for scope in scopes.iter() {
                let mut series = scope.series.lock().unwrap_or_else(|e| e.into_inner());
                scope.active.store(false, Ordering::Release);
                for s in series.iter_mut() {
                    s.measurement = Measurement::Unavailable {
                        reason: MissingMeasurement::NotCollected,
                    };
                    s.received = None;
                }
            }
        }
    }
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }
    pub fn snapshot(&self, stale_after: Duration) -> Vec<MetricSample> {
        let scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        let now = self.shared.clock.now();
        let enabled = self.shared.enabled.load(Ordering::Acquire);
        let mut result = Vec::new();
        for scope in scopes.iter() {
            let series = scope.series.lock().unwrap_or_else(|e| e.into_inner());
            for s in series.iter() {
                let measurement = if !enabled {
                    Measurement::Unavailable {
                        reason: MissingMeasurement::Disabled,
                    }
                } else if s
                    .received
                    .is_some_and(|at| now.saturating_duration_since(at) >= stale_after)
                {
                    match &s.measurement {
                        Measurement::Available {
                            value,
                            observed_at_ns,
                        } => Measurement::Stale {
                            last_value: Some(value.clone()),
                            observed_at_ns: *observed_at_ns,
                        },
                        other => other.clone(),
                    }
                } else {
                    s.measurement.clone()
                };
                result.push(MetricSample {
                    resource: scope.binding.owner.resource.clone(),
                    generation: scope.binding.owner.generation,
                    reset_epoch: scope.binding.reset_epoch,
                    metric: s.metric,
                    measurement,
                });
            }
        }
        result
    }
}
impl NativeMetricHandle {
    /// Lazy measurement construction: the disabled path executes no timer,
    /// allocation or optional measurement closure.
    pub fn record_lazy(
        &self,
        metric: MetricId,
        measurement: impl FnOnce() -> Measurement<MetricValue>,
    ) -> Result<ObservationAdmission, ErrorCode> {
        if !self.scope.active.load(Ordering::Acquire) {
            return Err(ErrorCode::StaleGeneration);
        }
        if !self.shared.enabled.load(Ordering::Acquire) {
            return Ok(ObservationAdmission::Disabled);
        }
        self.record(metric, measurement())
    }
    pub fn increment(
        &self,
        metric: MetricId,
        amount: u64,
        wall_ns: i64,
    ) -> Result<ObservationAdmission, ErrorCode> {
        self.update(metric, |s| {
            if metric.descriptor().0 != MetricKind::Counter {
                return Err(ErrorCode::InvalidInput);
            }
            if s.overflow {
                return Err(ErrorCode::Limit);
            }
            match s.counter.checked_add(amount) {
                Some(n) => {
                    s.counter = n;
                    s.measurement = Measurement::Available {
                        value: MetricValue::Counter(n),
                        observed_at_ns: wall_ns,
                    };
                }
                None => {
                    s.overflow = true;
                    s.measurement = Measurement::Unavailable {
                        reason: MissingMeasurement::Overflow,
                    };
                    return Err(ErrorCode::Limit);
                }
            }
            Ok(())
        })
    }
    pub fn observe_ns(
        &self,
        metric: MetricId,
        value: u64,
        wall_ns: i64,
    ) -> Result<ObservationAdmission, ErrorCode> {
        self.update(metric, |s| {
            if metric.descriptor().0 != MetricKind::Distribution {
                return Err(ErrorCode::InvalidInput);
            }
            if s.overflow {
                return Err(ErrorCode::Limit);
            }
            if let Err(e) = s.histogram.observe(value) {
                s.overflow = true;
                s.measurement = Measurement::Unavailable {
                    reason: MissingMeasurement::Overflow,
                };
                return Err(e);
            }
            s.measurement = Measurement::Available {
                value: MetricValue::Distribution(s.histogram.snapshot()),
                observed_at_ns: wall_ns,
            };
            Ok(())
        })
    }
    fn update(
        &self,
        metric: MetricId,
        action: impl FnOnce(&mut Series) -> Result<(), ErrorCode>,
    ) -> Result<ObservationAdmission, ErrorCode> {
        if !self.scope.active.load(Ordering::Acquire) {
            return Err(ErrorCode::StaleGeneration);
        }
        if !self.shared.enabled.load(Ordering::Acquire) {
            return Ok(ObservationAdmission::Disabled);
        }
        let Ok(mut series) = self.scope.series.try_lock() else {
            let _ = self
                .shared
                .dropped
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_add(1))
                });
            return Ok(ObservationAdmission::DroppedCapacity);
        };
        if !self.scope.active.load(Ordering::Acquire) {
            return Err(ErrorCode::StaleGeneration);
        }
        if !self.shared.enabled.load(Ordering::Acquire) {
            return Ok(ObservationAdmission::Disabled);
        }
        let s = series
            .iter_mut()
            .find(|s| s.metric == metric)
            .ok_or(ErrorCode::UnsupportedCapability)?;
        action(s)?;
        s.received = Some(self.shared.clock.now());
        Ok(ObservationAdmission::Accepted)
    }
}
impl ScopedMetricsPort for NativeMetricHandle {
    fn owner(&self) -> &ResourceVersion {
        &self.scope.binding.owner
    }
    fn record(
        &self,
        metric: MetricId,
        measurement: Measurement<MetricValue>,
    ) -> Result<ObservationAdmission, ErrorCode> {
        self.update(metric, |s| {
            self.scope.binding.sample(metric, measurement.clone())?;
            if s.overflow {
                return Err(ErrorCode::Limit);
            }
            if let Measurement::Available {
                value: MetricValue::Counter(new),
                ..
            } = &measurement
            {
                if *new < s.counter {
                    return Err(ErrorCode::InvalidInput);
                }
                s.counter = *new;
            }
            s.measurement = measurement;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{foundation::clock::ManualClock, model::operation::ResourceId};
    fn binding(generation: u64, epoch: u64, metrics: Vec<MetricId>) -> MetricBinding {
        MetricBinding {
            owner: ResourceVersion {
                resource: ResourceId::Runtime,
                generation,
            },
            reset_epoch: epoch,
            reset: ResetBoundary::OwnerGeneration,
            metrics,
        }
    }
    fn setup(enabled: bool) -> (Arc<ManualClock>, NativeMetrics, NativeMetricHandle) {
        let clock = Arc::new(ManualClock::new(Instant::now(), 0));
        let registry = NativeMetrics::new(clock.clone(), 2, 3, enabled).unwrap();
        let handle = registry
            .register(binding(1, 0, vec![MetricId::FailedTasksTotal]))
            .unwrap();
        (clock, registry, handle)
    }
    #[test]
    fn missing_zero_stale_and_wall_jumps_are_distinct() {
        let (clock, registry, h) = setup(true);
        assert!(matches!(
            registry.snapshot(Duration::from_secs(1))[0].measurement,
            Measurement::Unavailable {
                reason: MissingMeasurement::NotCollected
            }
        ));
        h.increment(MetricId::FailedTasksTotal, 0, 50).unwrap();
        clock.set_wall_ns(i64::MAX);
        assert!(matches!(
            registry.snapshot(Duration::from_secs(1))[0].measurement,
            Measurement::Available {
                value: MetricValue::Counter(0),
                ..
            }
        ));
        clock.advance(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            registry.snapshot(Duration::from_secs(1))[0].measurement,
            Measurement::Stale {
                last_value: Some(MetricValue::Counter(0)),
                ..
            }
        ));
    }
    #[test]
    fn disabled_is_lazy_and_reenable_requires_reset() {
        let (_, registry, h) = setup(false);
        assert_eq!(
            h.record_lazy(MetricId::FailedTasksTotal, || panic!(
                "disabled optional measurement"
            )),
            Ok(ObservationAdmission::Disabled)
        );
        registry.set_enabled(true);
        assert_eq!(
            h.increment(MetricId::FailedTasksTotal, 1, 0),
            Err(ErrorCode::StaleGeneration)
        );
        let h = registry
            .register(binding(1, 1, vec![MetricId::FailedTasksTotal]))
            .unwrap();
        assert_eq!(
            h.increment(MetricId::FailedTasksTotal, 1, 0),
            Ok(ObservationAdmission::Accepted)
        );
    }
    #[test]
    fn generation_reset_and_cardinality_are_fenced() {
        let (_, registry, old) = setup(true);
        assert!(matches!(
            registry.register(binding(1, 0, vec![MetricId::FailedTasksTotal])),
            Err(ErrorCode::StaleGeneration)
        ));
        let new = registry
            .register(binding(2, 0, vec![MetricId::FailedTasksTotal]))
            .unwrap();
        assert_eq!(
            old.increment(MetricId::FailedTasksTotal, 1, 0),
            Err(ErrorCode::StaleGeneration)
        );
        new.increment(MetricId::FailedTasksTotal, 2, 0).unwrap();
        assert_eq!(registry.snapshot(Duration::from_secs(1)).len(), 1);
        assert!(matches!(
            registry.register(binding(3, 0, vec![MetricId::FailedTasksTotal; 4])),
            Err(ErrorCode::Limit)
        ));
        let mut b = binding(
            1,
            0,
            vec![
                MetricId::ActiveConnections,
                MetricId::ActiveSubscribers,
                MetricId::ActiveRequests,
            ],
        );
        b.owner.resource = ResourceId::Configuration;
        assert!(matches!(registry.register(b), Err(ErrorCode::Capacity)));
    }
    #[test]
    fn overflow_is_sticky_until_explicit_reset() {
        let (_, registry, h) = setup(true);
        h.increment(MetricId::FailedTasksTotal, u64::MAX, 0)
            .unwrap();
        assert_eq!(
            h.increment(MetricId::FailedTasksTotal, 1, 0),
            Err(ErrorCode::Limit)
        );
        assert_eq!(
            h.increment(MetricId::FailedTasksTotal, 0, 0),
            Err(ErrorCode::Limit)
        );
        assert!(matches!(
            registry.snapshot(Duration::from_secs(1))[0].measurement,
            Measurement::Unavailable {
                reason: MissingMeasurement::Overflow
            }
        ));
    }
    #[test]
    fn contention_drops_optional_sample_without_waiting() {
        let (_, registry, h) = setup(true);
        let _guard = h.scope.series.lock().unwrap();
        assert_eq!(
            h.increment(MetricId::FailedTasksTotal, 1, 0),
            Ok(ObservationAdmission::DroppedCapacity)
        );
        assert_eq!(registry.dropped(), 1);
    }
    #[test]
    fn counter_reduction_cannot_hide_behind_missing_measurement() {
        let (_, _, h) = setup(true);
        h.increment(MetricId::FailedTasksTotal, 5, 0).unwrap();
        h.record(
            MetricId::FailedTasksTotal,
            Measurement::Unavailable {
                reason: MissingMeasurement::NotCollected,
            },
        )
        .unwrap();
        assert_eq!(
            h.record(
                MetricId::FailedTasksTotal,
                Measurement::Available {
                    value: MetricValue::Counter(4),
                    observed_at_ns: 0
                }
            ),
            Err(ErrorCode::InvalidInput)
        );
    }
}
