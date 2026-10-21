//! Checked aggregation of a bounded snapshot. Incomplete input never becomes
//! a fictitious complete zero; duplicate owner scopes cannot double-count.
use crate::live::model::{error::ErrorCode, observation::*};
pub fn sum_counters(
    samples: &[MetricSample],
    metric: MetricId,
    max_samples: usize,
    observed_at_ns: i64,
) -> Result<Measurement<MetricValue>, ErrorCode> {
    if samples.len() > max_samples {
        return Err(ErrorCode::Limit);
    }
    if metric.descriptor().0 != MetricKind::Counter {
        return Err(ErrorCode::InvalidInput);
    }
    let mut total = 0u64;
    let mut matched = false;
    let mut missing = None;
    let mut stale = false;
    for (i, sample) in samples
        .iter()
        .enumerate()
        .filter(|(_, s)| s.metric == metric)
    {
        sample.validate()?;
        if samples[..i]
            .iter()
            .any(|s| s.metric == metric && s.resource == sample.resource)
        {
            return Err(ErrorCode::InvalidInput);
        }
        matched = true;
        match &sample.measurement {
            Measurement::Available {
                value: MetricValue::Counter(n),
                ..
            } => {
                let Some(next) = total.checked_add(*n) else {
                    return Ok(Measurement::Unavailable {
                        reason: MissingMeasurement::Overflow,
                    });
                };
                total = next;
            }
            Measurement::Stale { .. } => stale = true,
            Measurement::Unavailable { reason } => {
                missing = Some(*reason);
            }
            _ => return Err(ErrorCode::InvalidInput),
        }
    }
    if !matched {
        return Ok(Measurement::Unavailable {
            reason: MissingMeasurement::NotCollected,
        });
    }
    if let Some(reason) = missing {
        return Ok(Measurement::Unavailable { reason });
    }
    if stale {
        return Ok(Measurement::Stale {
            last_value: None,
            observed_at_ns,
        });
    }
    Ok(Measurement::Available {
        value: MetricValue::Counter(total),
        observed_at_ns,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::model::{identity::StreamId, operation::ResourceId};
    fn sample(id: &str, n: u64) -> MetricSample {
        MetricSample {
            resource: ResourceId::Source(StreamId(id.into())),
            generation: 1,
            reset_epoch: 0,
            metric: MetricId::FailedTasksTotal,
            measurement: Measurement::Available {
                value: MetricValue::Counter(n),
                observed_at_ns: 0,
            },
        }
    }
    #[test]
    fn checked_sum_no_duplicates_no_false_zero() {
        let metric = MetricId::FailedTasksTotal;
        assert_eq!(
            sum_counters(&[sample("a", 2), sample("b", 3)], metric, 2, 5).unwrap(),
            Measurement::Available {
                value: MetricValue::Counter(5),
                observed_at_ns: 5
            }
        );
        assert_eq!(
            sum_counters(&[sample("a", 2), sample("a", 3)], metric, 2, 5),
            Err(ErrorCode::InvalidInput)
        );
        assert_eq!(
            sum_counters(&[sample("a", u64::MAX), sample("b", 1)], metric, 2, 5).unwrap(),
            Measurement::Unavailable {
                reason: MissingMeasurement::Overflow
            }
        );
        assert_eq!(
            sum_counters(&[], metric, 2, 5).unwrap(),
            Measurement::Unavailable {
                reason: MissingMeasurement::NotCollected
            }
        );
        let mut stale = sample("b", 3);
        stale.measurement = Measurement::Stale {
            last_value: Some(MetricValue::Counter(3)),
            observed_at_ns: 0,
        };
        assert!(matches!(
            sum_counters(&[sample("a", 2), stale], metric, 2, 5).unwrap(),
            Measurement::Stale {
                last_value: None,
                ..
            }
        ));
    }
}

/// One bounded rate estimator per bound counter. Counter identity/reset changes
/// and unavailable input start a new baseline; wall-clock jumps are irrelevant.
#[derive(Default)]
pub struct CounterRate {
    prior: Option<(
        crate::live::model::operation::ResourceId,
        u64,
        u64,
        MetricId,
        u64,
        std::time::Instant,
    )>,
}
impl CounterRate {
    pub fn observe(
        &mut self,
        sample: &MetricSample,
        now: std::time::Instant,
        wall_ns: i64,
    ) -> Result<Measurement<MetricValue>, ErrorCode> {
        sample.validate()?;
        if sample.metric.descriptor().0 != MetricKind::Counter {
            return Err(ErrorCode::InvalidInput);
        }
        let current = match &sample.measurement {
            Measurement::Available {
                value: MetricValue::Counter(n),
                ..
            } => *n,
            Measurement::Unavailable { reason } => {
                self.prior = None;
                return Ok(Measurement::Unavailable { reason: *reason });
            }
            Measurement::Stale { .. } => {
                self.prior = None;
                return Ok(Measurement::Stale {
                    last_value: None,
                    observed_at_ns: wall_ns,
                });
            }
            _ => return Err(ErrorCode::InvalidInput),
        };
        let next = (
            sample.resource.clone(),
            sample.generation,
            sample.reset_epoch,
            sample.metric,
            current,
            now,
        );
        let rate = if let Some((resource, generation, reset, metric, prior, at)) = &self.prior {
            if resource != &sample.resource
                || *generation != sample.generation
                || *reset != sample.reset_epoch
                || *metric != sample.metric
            {
                Measurement::Unavailable {
                    reason: MissingMeasurement::NotCollected,
                }
            } else {
                let delta = current.checked_sub(*prior).ok_or(ErrorCode::InvalidInput)?;
                let elapsed = now
                    .checked_duration_since(*at)
                    .ok_or(ErrorCode::InvalidInput)?;
                if elapsed.is_zero() {
                    return Ok(Measurement::Unavailable {
                        reason: MissingMeasurement::NotCollected,
                    });
                }
                let ns = u64::try_from(elapsed.as_nanos()).map_err(|_| ErrorCode::Limit)?;
                let rate = rust_decimal::Decimal::from(delta)
                    .checked_mul(rust_decimal::Decimal::from(1_000_000_000u64))
                    .and_then(|v| v.checked_div(rust_decimal::Decimal::from(ns)))
                    .ok_or(ErrorCode::Limit)?;
                Measurement::Available {
                    value: MetricValue::Rate(crate::live::model::codec::ExactDecimal(rate)),
                    observed_at_ns: wall_ns,
                }
            }
        } else {
            Measurement::Unavailable {
                reason: MissingMeasurement::NotCollected,
            }
        };
        self.prior = Some(next);
        Ok(rate)
    }
}
#[cfg(test)]
mod rate_tests {
    use super::*;
    use crate::live::model::{codec::ExactDecimal, operation::ResourceId};
    #[test]
    fn monotonic_rate_zero_reset_and_decrease() {
        let mut rate = CounterRate::default();
        let now = std::time::Instant::now();
        let mut sample = MetricSample {
            resource: ResourceId::Runtime,
            generation: 1,
            reset_epoch: 0,
            metric: MetricId::FailedTasksTotal,
            measurement: Measurement::Available {
                value: MetricValue::Counter(10),
                observed_at_ns: 100,
            },
        };
        assert!(matches!(
            rate.observe(&sample, now, 0).unwrap(),
            Measurement::Unavailable { .. }
        ));
        sample.measurement = Measurement::Available {
            value: MetricValue::Counter(14),
            observed_at_ns: -10,
        };
        assert_eq!(
            rate.observe(&sample, now + std::time::Duration::from_secs(2), -20)
                .unwrap(),
            Measurement::Available {
                value: MetricValue::Rate(ExactDecimal(rust_decimal::Decimal::from(2))),
                observed_at_ns: -20
            }
        );
        sample.measurement = Measurement::Available {
            value: MetricValue::Counter(0),
            observed_at_ns: 0,
        };
        assert_eq!(
            rate.observe(&sample, now + std::time::Duration::from_secs(3), 0),
            Err(ErrorCode::InvalidInput)
        );
        sample.reset_epoch = 1;
        assert!(matches!(
            rate.observe(&sample, now + std::time::Duration::from_secs(3), 0)
                .unwrap(),
            Measurement::Unavailable { .. }
        ));
        assert_eq!(
            rate.observe(&sample, now + std::time::Duration::from_secs(4), 0)
                .unwrap(),
            Measurement::Available {
                value: MetricValue::Rate(ExactDecimal(rust_decimal::Decimal::ZERO)),
                observed_at_ns: 0
            }
        );
    }
}
