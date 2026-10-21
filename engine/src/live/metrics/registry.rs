//! Finite prebound native series. Collection never waits for snapshots/exporters.
use super::histogram::BoundedHistogram;
use crate::live::foundation::diagnostics::{DiagnosticHistory, RedactedDiagnostic};
use crate::live::ports::observability::{DiagnosticEvent, ObservationSinkPort};
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
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum DistributionSource {
    None,
    Local,
    External,
}
#[derive(Clone)]
struct Series {
    metric: MetricId,
    measurement: Measurement<MetricValue>,
    received: Option<Instant>,
    histogram: BoundedHistogram,
    distribution_source: DistributionSource,
    counter: u64,
    overflow: bool,
}
struct Scope {
    timing_tick: AtomicU32,
    _capacity: ResourceReservation,
    binding: MetricBinding,
    active: AtomicBool,
    series: Mutex<Vec<Series>>,
    health: Mutex<Option<(Instant, HealthSnapshot)>>,
    diagnostics: Mutex<DiagnosticHistory>,
}
struct Shared {
    enabled: AtomicBool,
    detailed_timing: AtomicBool,
    timing_one_in: AtomicU32,
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
                detailed_timing: AtomicBool::new(false),
                timing_one_in: AtomicU32::new(1),
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
            if binding.reset_epoch < old.reset_epoch
                || binding.owner.generation < old.owner.generation
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
            timing_tick: AtomicU32::new(0),
            health: Mutex::new(None),
            diagnostics: Mutex::new(DiagnosticHistory::new(64)?),
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
                        distribution_source: DistributionSource::None,
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
            if scope.binding.reset == ResetBoundary::RuntimeInstance
                && scopes[i].binding.reset == ResetBoundary::RuntimeInstance
                && scope.binding.reset_epoch == scopes[i].binding.reset_epoch
            {
                let mut next = scope.series.lock().unwrap_or_else(|e| e.into_inner());
                for series in next.iter_mut() {
                    if let Some(prior) = _guard.iter().find(|p| p.metric == series.metric) {
                        *series = prior.clone();
                    }
                }
            }
            let prior_health = scopes[i].health.lock().unwrap_or_else(|e| e.into_inner());
            let prior_diagnostics = scopes[i]
                .diagnostics
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if scope.binding.owner.generation == scopes[i].binding.owner.generation {
                *scope.health.lock().unwrap_or_else(|e| e.into_inner()) = prior_health.clone();
                *scope.diagnostics.lock().unwrap_or_else(|e| e.into_inner()) =
                    prior_diagnostics.clone();
            }
            scopes[i].active.store(false, Ordering::Release);
            drop(prior_diagnostics);
            drop(prior_health);
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
    /// Explicit owner retirement frees the registry slot. Outstanding obsolete
    /// handles remain fenced and charged until their owners release them.
    pub fn retire(&self, owner: &ResourceVersion) -> Result<(), ErrorCode> {
        let mut scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        let i = scopes
            .iter()
            .position(|s| s.binding.owner.resource == owner.resource)
            .ok_or(ErrorCode::NotFound)?;
        if &scopes[i].binding.owner != owner {
            return Err(ErrorCode::StaleGeneration);
        }
        let scope = scopes.remove(i);
        let _guard = scope.series.lock().unwrap_or_else(|e| e.into_inner());
        scope.active.store(false, Ordering::Release);
        Ok(())
    }
    pub fn configure(
        &self,
        settings: &crate::live::model::configuration::MetricsSettings,
    ) -> Result<(), ErrorCode> {
        if settings.snapshot_interval_ns == 0 || settings.timing_sample_one_in == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        self.shared
            .timing_one_in
            .store(settings.timing_sample_one_in, Ordering::Release);
        self.shared
            .detailed_timing
            .store(settings.detailed_timing, Ordering::Release);
        self.set_enabled(settings.enabled);
        Ok(())
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
    /// Current owner health remains independent of optional measurement mode.
    pub fn health_snapshot(&self, stale_after: Duration) -> Vec<HealthSnapshot> {
        let scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        let now = self.shared.clock.now();
        scopes
            .iter()
            .filter_map(|scope| {
                let state = scope.health.lock().unwrap_or_else(|e| e.into_inner());
                let (received, health) = state.as_ref()?;
                let mut health = health.clone();
                if (!scope.active.load(Ordering::Acquire)
                    || now.saturating_duration_since(*received) >= stale_after)
                    && health.status != HealthStatus::Unhealthy
                    && health.status != HealthStatus::Disabled
                {
                    health.status = HealthStatus::Unknown;
                    health.reasons = vec![HealthReason::StaleObservation];
                    for capability in &mut health.capabilities {
                        if capability.status != HealthStatus::Unhealthy
                            && capability.status != HealthStatus::Disabled
                        {
                            capability.status = HealthStatus::Unknown;
                            capability.reasons = vec![HealthReason::StaleObservation];
                        }
                    }
                }
                Some(health)
            })
            .collect()
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
    pub fn diagnostic_history(&self) -> Result<Vec<RedactedDiagnostic>, ErrorCode> {
        if !self.scope.active.load(Ordering::Acquire) {
            return Err(ErrorCode::StaleGeneration);
        }
        Ok(self
            .scope
            .diagnostics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot())
    }
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
        if !self.shared.enabled.load(Ordering::Acquire)
            || (metric.descriptor().0 == MetricKind::Distribution
                && !self.shared.detailed_timing.load(Ordering::Acquire))
        {
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
    /// Optional timing closure runs only on a selected detailed sample. The
    /// sequence is bounded sampling state, never a market-event identity.
    pub fn observe_ns_lazy(
        &self,
        metric: MetricId,
        wall_ns: i64,
        measure: impl FnOnce() -> u64,
    ) -> Result<ObservationAdmission, ErrorCode> {
        if !self.scope.active.load(Ordering::Acquire) {
            return Err(ErrorCode::StaleGeneration);
        }
        if !self.shared.enabled.load(Ordering::Acquire)
            || !self.shared.detailed_timing.load(Ordering::Acquire)
        {
            return Ok(ObservationAdmission::Disabled);
        }
        let one_in = self.shared.timing_one_in.load(Ordering::Acquire);
        let tick = self
            .scope
            .timing_tick
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(if n >= one_in - 1 { 0 } else { n + 1 })
            })
            .unwrap();
        if tick != 0 {
            return Ok(ObservationAdmission::SampledOut);
        }
        self.observe_ns(metric, measure(), wall_ns)
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
            if s.distribution_source == DistributionSource::External {
                return Err(ErrorCode::InvalidInput);
            }
            s.distribution_source = DistributionSource::Local;

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
        if !self.shared.enabled.load(Ordering::Acquire)
            || (metric.descriptor().0 == MetricKind::Distribution
                && !self.shared.detailed_timing.load(Ordering::Acquire))
        {
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
        if !self.shared.enabled.load(Ordering::Acquire)
            || (metric.descriptor().0 == MetricKind::Distribution
                && !self.shared.detailed_timing.load(Ordering::Acquire))
        {
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
            if let Measurement::Available {
                value: MetricValue::Distribution(_),
                ..
            } = &measurement
            {
                if s.distribution_source == DistributionSource::Local {
                    return Err(ErrorCode::InvalidInput);
                }
                s.distribution_source = DistributionSource::External;
            }
            s.measurement = measurement;
            Ok(())
        })
    }
}

impl ObservationSinkPort for NativeMetricHandle {
    fn metric(&self, sample: MetricSample) -> ObservationAdmission {
        if sample.resource != self.scope.binding.owner.resource
            || sample.generation != self.scope.binding.owner.generation
            || sample.reset_epoch != self.scope.binding.reset_epoch
        {
            return ObservationAdmission::Rejected(ErrorCode::StaleGeneration);
        }
        match self.record(sample.metric, sample.measurement) {
            Ok(outcome) => outcome,
            Err(code) => ObservationAdmission::Rejected(code),
        }
    }
    fn health(&self, snapshot: HealthSnapshot) -> ObservationAdmission {
        if snapshot.resource != self.scope.binding.owner.resource {
            return ObservationAdmission::Rejected(ErrorCode::InvalidInput);
        }
        if snapshot.capabilities.len() > 8
            || snapshot.reasons.len() > 16
            || snapshot.capabilities.iter().any(|c| c.reasons.len() > 16)
        {
            return ObservationAdmission::Rejected(ErrorCode::Limit);
        }
        if !self.scope.active.load(Ordering::Acquire) {
            return ObservationAdmission::Rejected(ErrorCode::StaleGeneration);
        }
        let Ok(mut state) = self.scope.health.try_lock() else {
            return self.dropped_observation();
        };
        if !self.scope.active.load(Ordering::Acquire) {
            return ObservationAdmission::Rejected(ErrorCode::StaleGeneration);
        }
        *state = Some((self.shared.clock.now(), snapshot));
        ObservationAdmission::Accepted
    }
    fn diagnostic(&self, event: DiagnosticEvent) -> ObservationAdmission {
        if event.owner != self.scope.binding.owner {
            return ObservationAdmission::Rejected(ErrorCode::StaleGeneration);
        }
        if !self.scope.active.load(Ordering::Acquire) {
            return ObservationAdmission::Rejected(ErrorCode::StaleGeneration);
        }
        let Ok(mut history) = self.scope.diagnostics.try_lock() else {
            return self.dropped_observation();
        };
        if !self.scope.active.load(Ordering::Acquire) {
            return ObservationAdmission::Rejected(ErrorCode::StaleGeneration);
        }
        match history.push(&event) {
            Ok(()) => ObservationAdmission::Accepted,
            Err(code) => ObservationAdmission::Rejected(code),
        }
    }
}
impl NativeMetricHandle {
    fn dropped_observation(&self) -> ObservationAdmission {
        let _ = self
            .shared
            .dropped
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
        ObservationAdmission::DroppedCapacity
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
    fn native_sink_keeps_health_and_diagnostics_when_optional_metrics_disabled() {
        let (clock, registry, h) = setup(false);
        let health = HealthSnapshot {
            resource: ResourceId::Runtime,
            status: HealthStatus::Unhealthy,
            reasons: vec![HealthReason::IntegrityViolation],
            capabilities: vec![],
            evaluated_at_ns: 0,
        };
        assert_eq!(h.health(health), ObservationAdmission::Accepted);
        assert_eq!(
            h.diagnostic(DiagnosticEvent {
                owner: h.owner().clone(),
                code: ErrorCode::Unavailable,
                observed_at_ns: 0
            }),
            ObservationAdmission::Accepted
        );
        assert_eq!(h.diagnostic_history().unwrap().len(), 1);
        clock.advance(Duration::from_secs(10)).unwrap();
        registry.set_enabled(true);
        assert_eq!(
            registry.health_snapshot(Duration::from_secs(1))[0].status,
            HealthStatus::Unhealthy
        );
        let _fresh = registry
            .register(binding(1, 1, vec![MetricId::FailedTasksTotal]))
            .unwrap();
        assert_eq!(
            registry.health_snapshot(Duration::from_secs(1))[0].status,
            HealthStatus::Unhealthy
        );

        assert_eq!(
            h.health(HealthSnapshot {
                resource: ResourceId::Runtime,
                status: HealthStatus::Healthy,
                reasons: vec![],
                capabilities: vec![],
                evaluated_at_ns: 0
            }),
            ObservationAdmission::Rejected(ErrorCode::StaleGeneration)
        );
    }
    #[test]
    fn runtime_reset_boundary_survives_owner_generation_change() {
        let (_, registry, old) = setup(true);
        // Rebind explicitly to the runtime reset domain.
        let mut b = binding(1, 1, vec![MetricId::FailedTasksTotal]);
        b.reset = ResetBoundary::RuntimeInstance;
        let h = registry.register(b.clone()).unwrap();
        drop(old);
        h.increment(MetricId::FailedTasksTotal, 7, 0).unwrap();
        b.owner.generation = 2;
        let next = registry.register(b).unwrap();
        next.increment(MetricId::FailedTasksTotal, 1, 0).unwrap();
        assert!(matches!(
            registry.snapshot(Duration::from_secs(1))[0].measurement,
            Measurement::Available {
                value: MetricValue::Counter(8),
                ..
            }
        ));
    }
    #[test]
    fn detailed_sampling_is_lazy_and_configurable() {
        let (_, registry, old) = setup(true);
        let h = registry
            .register(binding(2, 0, vec![MetricId::RuntimeSchedulerDelay]))
            .unwrap();
        drop(old);
        assert_eq!(
            h.observe_ns_lazy(MetricId::RuntimeSchedulerDelay, 0, || panic!(
                "timing disabled"
            )),
            Ok(ObservationAdmission::Disabled)
        );
        registry
            .configure(&crate::live::model::configuration::MetricsSettings {
                enabled: true,
                detailed_timing: true,
                snapshot_interval_ns: 1,
                timing_sample_one_in: 3,
            })
            .unwrap();
        let mut calls = 0;
        for _ in 0..10 {
            h.observe_ns_lazy(MetricId::RuntimeSchedulerDelay, 0, || {
                calls += 1;
                7
            })
            .unwrap();
        }
        assert_eq!(calls, 4);
        assert!(
            matches!(&registry.snapshot(Duration::from_secs(1))[0].measurement,Measurement::Available {value:MetricValue::Distribution(d),..} if d.count==4)
        );
    }
    #[test]
    fn retirement_is_fenced_and_retained_handles_still_cost_capacity() {
        let (_, registry, h) = setup(true);
        assert_eq!(
            registry.retire(&ResourceVersion {
                resource: ResourceId::Runtime,
                generation: 2
            }),
            Err(ErrorCode::StaleGeneration)
        );
        registry.retire(h.owner()).unwrap();
        assert!(registry.snapshot(Duration::from_secs(1)).is_empty());
        assert_eq!(
            h.increment(MetricId::FailedTasksTotal, 1, 0),
            Err(ErrorCode::StaleGeneration)
        );
        let next = registry
            .register(binding(2, 0, vec![MetricId::FailedTasksTotal]))
            .unwrap();
        assert!(matches!(
            registry.register(binding(3, 0, vec![MetricId::FailedTasksTotal])),
            Err(ErrorCode::Capacity)
        ));
        drop(h);
        assert!(
            registry
                .register(binding(3, 0, vec![MetricId::FailedTasksTotal]))
                .is_ok()
        );
        assert_eq!(
            next.increment(MetricId::FailedTasksTotal, 1, 0),
            Err(ErrorCode::StaleGeneration)
        );
    }
    #[test]
    fn raw_histogram_samples_cannot_silently_replace_external_summary() {
        let (_, registry, old) = setup(true);
        let h = registry
            .register(binding(2, 0, vec![MetricId::RuntimeSchedulerDelay]))
            .unwrap();
        drop(old);
        registry
            .configure(&crate::live::model::configuration::MetricsSettings {
                enabled: true,
                detailed_timing: true,
                snapshot_interval_ns: 1,
                timing_sample_one_in: 1,
            })
            .unwrap();
        let mut histogram = BoundedHistogram::default();
        histogram.observe(7).unwrap();
        h.record(
            MetricId::RuntimeSchedulerDelay,
            Measurement::Available {
                value: MetricValue::Distribution(histogram.snapshot()),
                observed_at_ns: 0,
            },
        )
        .unwrap();
        assert_eq!(
            h.observe_ns(MetricId::RuntimeSchedulerDelay, 8, 0),
            Err(ErrorCode::InvalidInput)
        );
        assert!(
            matches!(&registry.snapshot(Duration::from_secs(1))[0].measurement,Measurement::Available {value:MetricValue::Distribution(summary),..} if summary.count==1)
        );
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
