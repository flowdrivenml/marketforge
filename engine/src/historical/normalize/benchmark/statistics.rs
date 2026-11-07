use super::Result;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Statistics {
    pub sample_count: usize,
    pub minimum: f64,
    pub maximum: f64,
    pub mean: f64,
    pub median: f64,
    pub standard_deviation: Option<f64>,
    pub p95: f64,
    pub coefficient_of_variation: Option<f64>,
}
/// Nonnegative performance observations. Welford sample variance; nearest-rank P95.
pub fn statistics(values: &[f64]) -> Result<Option<Statistics>> {
    if values.is_empty() {
        return Ok(None);
    }
    if values.iter().any(|x| !x.is_finite() || *x < 0.) {
        return Err("invalid nonnegative finite observation".into());
    }
    let mut mean = 0.;
    let mut m2 = 0.;
    for (i, &x) in values.iter().enumerate() {
        let delta = x - mean;
        mean += delta / ((i + 1) as f64);
        m2 += delta * (x - mean);
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let n = ordered.len();
    let median = if n % 2 == 1 {
        ordered[n / 2]
    } else {
        ordered[n / 2 - 1] / 2. + ordered[n / 2] / 2.
    };
    let standard_deviation = (n > 1).then(|| (m2 / (n - 1) as f64).max(0.).sqrt());
    if !mean.is_finite() || standard_deviation.is_some_and(|v| !v.is_finite()) {
        return Err("statistical arithmetic overflow".into());
    }
    Ok(Some(Statistics {
        sample_count: n,
        minimum: ordered[0],
        maximum: ordered[n - 1],
        mean,
        median,
        standard_deviation,
        p95: ordered[n - n / 20 - 1],
        coefficient_of_variation: standard_deviation
            .filter(|_| mean != 0.)
            .map(|s| s / mean.abs()),
    }))
}
pub fn finite_ratio(numerator: f64, denominator: f64) -> Option<f64> {
    if numerator.is_finite() && denominator.is_finite() && denominator > 0. {
        let value = numerator / denominator;
        value.is_finite().then_some(value)
    } else {
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_known_statistics() {
        let s = statistics(&[1., 2., 3., 4., 5.]).unwrap().unwrap();
        assert_eq!(s.mean, 3.);
        assert_eq!(s.median, 3.);
        assert_eq!(s.minimum, 1.);
        assert_eq!(s.maximum, 5.);
        assert_eq!(s.p95, 5.);
        assert!((s.standard_deviation.unwrap() - 1.5811388300841898).abs() < 1e-14);
        assert!((s.coefficient_of_variation.unwrap() - 0.5270462766947299).abs() < 1e-14);
        assert_eq!(statistics(&[4., 1., 2., 3.]).unwrap().unwrap().median, 2.5);
        let s = statistics(&(1..=100).map(|i| i as f64).collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        assert_eq!(s.p95, 95.);
    }
    #[test]
    fn empty_single_zero_invalid() {
        assert!(statistics(&[]).unwrap().is_none());
        let s = statistics(&[7.]).unwrap().unwrap();
        assert_eq!(s.standard_deviation, None);
        assert_eq!(s.coefficient_of_variation, None);
        assert_eq!(
            statistics(&[0., 0.])
                .unwrap()
                .unwrap()
                .coefficient_of_variation,
            None
        );
        for value in [f64::NAN, f64::INFINITY, -1.] {
            assert!(statistics(&[value]).is_err());
        }
        assert_eq!(finite_ratio(1., 0.), None);
    }
}
