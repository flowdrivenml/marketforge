//! Capability-specific observation policy. This module has no mutation access
//! to processors, books, recovery or the dependency graph.
use crate::live::model::{error::ErrorCode, observation::*, operation::ResourceId};
use std::time::{Duration, Instant};
#[derive(Debug, Clone)]
pub struct CapabilityObservation {
    pub health: CapabilityHealth,
    pub received_at: Option<Instant>,
    /// Authoritative unresolved correctness failure; expiry cannot erase it.
    pub unresolved_failure: bool,
}
/// Inputs must include every capability required by this consumer. Optional
/// failures remain visible without contaminating unrelated consumer health.
pub fn evaluate_health(
    resource: ResourceId,
    observations: &[CapabilityObservation],
    now: Instant,
    wall_ns: i64,
    stale_after: Duration,
    enabled: bool,
) -> Result<HealthSnapshot, ErrorCode> {
    if observations.len() > 8 || stale_after.is_zero() {
        return Err(ErrorCode::Limit);
    }
    let mut capabilities = Vec::with_capacity(observations.len());
    let mut reasons = Vec::new();
    let mut status = if enabled {
        HealthStatus::Healthy
    } else {
        HealthStatus::Disabled
    };
    let mut required = 0;
    for (i, observation) in observations.iter().enumerate() {
        if observations[..i]
            .iter()
            .any(|o| o.health.capability == observation.health.capability)
            || observation.health.reasons.len() > 16
        {
            return Err(ErrorCode::InvalidInput);
        }
        let mut health = observation.health.clone();
        if observation.unresolved_failure {
            health.status = HealthStatus::Unhealthy;
        } else if observation.received_at.is_none() {
            health.status = HealthStatus::Unknown;
            health.reasons = vec![HealthReason::NoObservation];
        } else if observation
            .received_at
            .is_some_and(|at| at > now || now.saturating_duration_since(at) >= stale_after)
        {
            health.status = HealthStatus::Unknown;
            health.reasons = vec![HealthReason::StaleObservation];
        }
        if health.required && enabled {
            required += 1;
            // A disabled required capability is missing evidence, never healthy.
            let candidate = if health.status == HealthStatus::Disabled {
                HealthStatus::Unknown
            } else {
                health.status
            };
            if severity(candidate) > severity(status) {
                status = candidate;
            }
            for reason in &health.reasons {
                if !reasons.contains(reason) {
                    reasons.push(*reason);
                }
            }
        }
        capabilities.push(health);
    }
    if enabled && required == 0 {
        status = HealthStatus::Unknown;
        reasons.push(HealthReason::NoObservation);
    }
    if !enabled {
        reasons = vec![HealthReason::Disabled];
    }
    Ok(HealthSnapshot {
        resource,
        status,
        reasons,
        capabilities,
        evaluated_at_ns: wall_ns,
    })
}
fn severity(status: HealthStatus) -> u8 {
    match status {
        HealthStatus::Healthy | HealthStatus::Disabled => 0,
        HealthStatus::Degraded => 1,
        HealthStatus::Unknown => 2,
        HealthStatus::Unhealthy => 3,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn observation(capability: Capability, required: bool, now: Instant) -> CapabilityObservation {
        CapabilityObservation {
            health: CapabilityHealth {
                capability,
                status: HealthStatus::Healthy,
                required,
                reasons: vec![],
            },
            received_at: Some(now),
            unresolved_failure: false,
        }
    }
    #[test]
    fn optional_failure_does_not_contaminate_publication() {
        let now = Instant::now();
        let publication = observation(Capability::Publication, true, now);
        let mut recording = observation(Capability::Recording, false, now);
        recording.unresolved_failure = true;
        let s = evaluate_health(
            ResourceId::Runtime,
            &[publication, recording],
            now,
            0,
            Duration::from_secs(1),
            true,
        )
        .unwrap();
        assert_eq!(s.status, HealthStatus::Healthy);
        assert_eq!(s.capabilities[1].status, HealthStatus::Unhealthy);
    }
    #[test]
    fn stale_success_is_unknown_but_confirmed_failure_survives() {
        let now = Instant::now();
        let mut o = observation(Capability::BookIntegrity, true, now);
        let later = now + Duration::from_secs(2);
        let eval = |o| {
            evaluate_health(
                ResourceId::Runtime,
                &[o],
                later,
                0,
                Duration::from_secs(1),
                true,
            )
            .unwrap()
        };
        assert_eq!(eval(o.clone()).status, HealthStatus::Unknown);
        o.unresolved_failure = true;
        o.health.reasons = vec![HealthReason::IntegrityViolation];
        assert_eq!(eval(o).status, HealthStatus::Unhealthy);
    }
    #[test]
    fn missing_disabled_and_duplicate_inputs() {
        let now = Instant::now();
        assert_eq!(
            evaluate_health(
                ResourceId::Runtime,
                &[],
                now,
                0,
                Duration::from_secs(1),
                true
            )
            .unwrap()
            .status,
            HealthStatus::Unknown
        );
        assert_eq!(
            evaluate_health(
                ResourceId::Runtime,
                &[],
                now,
                0,
                Duration::from_secs(1),
                false
            )
            .unwrap()
            .status,
            HealthStatus::Disabled
        );
        let o = observation(Capability::Acquisition, true, now);
        assert_eq!(
            evaluate_health(
                ResourceId::Runtime,
                &[o.clone(), o],
                now,
                0,
                Duration::from_secs(1),
                true
            ),
            Err(ErrorCode::InvalidInput)
        );
    }
}

/// Configured diagnostic hysteresis. Integrity failures are separately latched
/// until explicit authoritative recovery; low metric values cannot clear them.
#[derive(Debug, Clone, Copy)]
pub struct PressurePolicy {
    pub enter_degraded: u64,
    pub leave_degraded: u64,
    pub stabilize: Duration,
}
impl PressurePolicy {
    pub fn validate(self) -> Result<Self, ErrorCode> {
        if self.leave_degraded >= self.enter_degraded {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(self)
    }
}
#[derive(Default)]
pub struct PressureHealth {
    degraded: bool,
    clear_since: Option<Instant>,
    unresolved_failure: bool,
}
impl PressureHealth {
    pub fn observe(
        &mut self,
        policy: PressurePolicy,
        value: Option<u64>,
        now: Instant,
        confirmed_failure: bool,
        authoritative_recovery: bool,
    ) -> Result<HealthStatus, ErrorCode> {
        policy.validate()?;
        if authoritative_recovery {
            self.unresolved_failure = false;
        }
        if confirmed_failure {
            self.unresolved_failure = true;
        }
        if self.unresolved_failure {
            return Ok(HealthStatus::Unhealthy);
        }
        let Some(value) = value else {
            self.clear_since = None;
            return Ok(HealthStatus::Unknown);
        };
        if value >= policy.enter_degraded {
            self.degraded = true;
            self.clear_since = None;
        } else if self.degraded && value <= policy.leave_degraded {
            let since = self.clear_since.get_or_insert(now);
            if now.saturating_duration_since(*since) >= policy.stabilize {
                self.degraded = false;
                self.clear_since = None;
            }
        } else {
            self.clear_since = None;
        }
        Ok(if self.degraded {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        })
    }
}
#[cfg(test)]
mod pressure_tests {
    use super::*;
    #[test]
    fn configured_stabilization_never_delays_integrity_failure() {
        let policy = PressurePolicy {
            enter_degraded: 80,
            leave_degraded: 60,
            stabilize: Duration::from_secs(2),
        };
        let mut state = PressureHealth::default();
        let now = Instant::now();
        assert_eq!(
            state.observe(policy, Some(80), now, false, false),
            Ok(HealthStatus::Degraded)
        );
        assert_eq!(
            state.observe(policy, Some(50), now, false, false),
            Ok(HealthStatus::Degraded)
        );
        assert_eq!(
            state.observe(policy, Some(50), now + Duration::from_secs(2), false, false),
            Ok(HealthStatus::Healthy)
        );
        assert_eq!(
            state.observe(policy, Some(0), now, true, false),
            Ok(HealthStatus::Unhealthy)
        );
        assert_eq!(
            state.observe(policy, None, now + Duration::from_secs(10), false, false),
            Ok(HealthStatus::Unhealthy)
        );
        assert_eq!(
            state.observe(policy, Some(0), now + Duration::from_secs(11), false, true),
            Ok(HealthStatus::Healthy)
        );
    }
}
