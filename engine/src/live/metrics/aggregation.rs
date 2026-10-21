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
