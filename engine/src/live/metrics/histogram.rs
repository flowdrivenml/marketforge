//! Fixed logarithmic buckets. Quantiles are bucket upper estimates, bounded by
//! observed extrema; no event samples or dynamically allocated bucket labels.
use crate::live::model::{error::ErrorCode, observation::LatencySummary};
#[derive(Debug, Clone)]
pub struct BoundedHistogram {
    buckets: [u64; 65],
    count: u64,
    sum: u64,
    min: u64,
    max: u64,
}
impl Default for BoundedHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; 65],
            count: 0,
            sum: 0,
            min: u64::MAX,
            max: 0,
        }
    }
}
impl BoundedHistogram {
    pub fn observe(&mut self, value: u64) -> Result<(), ErrorCode> {
        let index = (64 - value.leading_zeros()) as usize;
        let count = self.count.checked_add(1).ok_or(ErrorCode::Limit)?;
        let sum = self.sum.checked_add(value).ok_or(ErrorCode::Limit)?;
        self.buckets[index] = self.buckets[index].checked_add(1).ok_or(ErrorCode::Limit)?;
        self.count = count;
        self.sum = sum;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        Ok(())
    }
    fn quantile(&self, percent: u64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = (u128::from(self.count) * u128::from(percent)).div_ceil(100);
        let mut cumulative = 0u128;
        for (i, count) in self.buckets.iter().enumerate() {
            cumulative += u128::from(*count);
            if cumulative >= rank {
                let upper = if i == 64 { u64::MAX } else { (1u64 << i) - 1 };
                return Some(upper.min(self.max).max(self.min));
            }
        }
        None
    }
    pub fn snapshot(&self) -> LatencySummary {
        LatencySummary {
            count: self.count,
            sum_ns: self.sum,
            min_ns: (self.count != 0).then_some(self.min),
            p50_ns: self.quantile(50),
            p95_ns: self.quantile(95),
            p99_ns: self.quantile(99),
            max_ns: (self.count != 0).then_some(self.max),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_extrema_quantiles_and_overflow() {
        let mut h = BoundedHistogram::default();
        h.snapshot().validate().unwrap();
        for n in 0..100 {
            h.observe(n).unwrap();
        }
        let s = h.snapshot();
        s.validate().unwrap();
        assert_eq!(
            (s.count, s.sum_ns, s.min_ns, s.max_ns),
            (100, 4950, Some(0), Some(99))
        );
        assert_eq!(s.p50_ns, Some(63));
        let mut h = BoundedHistogram::default();
        h.observe(u64::MAX).unwrap();
        let before = h.snapshot();
        assert_eq!(h.observe(1), Err(ErrorCode::Limit));
        assert_eq!(h.snapshot(), before);
    }
}
