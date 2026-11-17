//! Processing choices are independent of integrity acceptance and resource safety.
use crate::shared::error::{MarketForgeError, Result};
use serde::{Deserialize, Serialize};
fn invalid(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
pub fn milliseconds(value: u64) -> Result<i64> {
    i64::try_from(value)
        .ok()
        .and_then(|v| v.checked_mul(1_000_000))
        .ok_or_else(|| invalid("millisecond processing bound overflows canonical nanoseconds"))
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Selection {
    #[default]
    Earlier,
    Later,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossAction {
    #[default]
    Fail,
    MarkDegraded,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeOverlap {
    #[default]
    Strict,
    PreferSource,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepthOverlap {
    #[default]
    Strict,
    SnapshotRecovery,
    PreferSource,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwitchBoundary {
    #[default]
    Snapshot,
    SourceBoundary,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeTimestamp {
    #[default]
    Strict,
    Preserve,
    Reorder,
    Discard,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepthTimestamp {
    #[default]
    Strict,
    Preserve,
    SnapshotRecovery,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PreferSource {
    pub selection: Selection,
    pub on_unique_data_loss: LossAction,
    pub max_excluded_events: u64,
    pub max_excluded_rate: Option<f64>,
    pub minimum_samples: u64,
    pub max_excluded_interval_ms: Option<u64>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DepthPreferSource {
    pub selection: Selection,
    pub switch_boundary: SwitchBoundary,
    pub on_unique_data_loss: LossAction,
    pub max_excluded_events: u64,
    pub max_excluded_rate: Option<f64>,
    pub minimum_samples: u64,
    pub max_excluded_interval_ms: Option<u64>,
}
impl DepthPreferSource {
    pub fn loss(&self) -> PreferSource {
        PreferSource {
            selection: self.selection,
            on_unique_data_loss: self.on_unique_data_loss,
            max_excluded_events: self.max_excluded_events,
            max_excluded_rate: self.max_excluded_rate,
            minimum_samples: self.minimum_samples,
            max_excluded_interval_ms: self.max_excluded_interval_ms,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TradeReconciliation {
    pub overlap_policy: TradeOverlap,
    pub prefer_source: PreferSource,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DepthReconciliation {
    pub overlap_policy: DepthOverlap,
    pub prefer_source: DepthPreferSource,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Reconciliation {
    pub trades: TradeReconciliation,
    pub depth: DepthReconciliation,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TradeTimestampConfig {
    pub policy: TradeTimestamp,
    pub max_regression_ms: u64,
    pub max_reorder_window_ms: u64,
    pub max_discarded_events: u64,
    pub max_discarded_rate: Option<f64>,
    pub minimum_samples: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DepthTimestampConfig {
    pub policy: DepthTimestamp,
    pub max_regression_ms: u64,
    pub require_sequence_continuity: bool,
    pub require_recovery_snapshot: bool,
    pub max_discarded_events: u64,
}
impl Default for DepthTimestampConfig {
    fn default() -> Self {
        Self {
            policy: DepthTimestamp::Strict,
            max_regression_ms: 0,
            require_sequence_continuity: true,
            require_recovery_snapshot: true,
            max_discarded_events: 0,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TimestampRegression {
    pub trades: TradeTimestampConfig,
    pub depth: DepthTimestampConfig,
}
fn rate(rate: Option<f64>) -> Result<()> {
    if rate.is_some_and(|r| !r.is_finite() || !(0.0..=1.0).contains(&r)) {
        Err(invalid(
            "processing loss rate must be finite and between zero and one",
        ))
    } else {
        if let Some(rate) = rate {
            rust_decimal::Decimal::from_str_exact(&rate.to_string())
                .map_err(|_| invalid("loss rate exceeds supported decimal precision"))?;
        }
        Ok(())
    }
}

impl PreferSource {
    pub fn validate(&self) -> Result<()> {
        rate(self.max_excluded_rate)?;
        if let Some(ms) = self.max_excluded_interval_ms {
            milliseconds(ms)?;
        }
        // Count is always an effective finite bound, including below a rate floor.
        // A zero count means no permission to exclude, never an absent limit.
        if self.on_unique_data_loss == LossAction::MarkDegraded && self.max_excluded_events == 0 {
            return Err(invalid(
                "mark_degraded requires an explicit positive max_excluded_events; a rate/sample floor alone cannot authorize loss",
            ));
        }
        Ok(())
    }
}
pub fn validate(reconciliation: &Reconciliation, timestamp: &TimestampRegression) -> Result<()> {
    reconciliation.trades.prefer_source.validate()?;
    reconciliation.depth.prefer_source.loss().validate()?;
    let t = &timestamp.trades;
    let d = &timestamp.depth;
    milliseconds(t.max_regression_ms)?;
    milliseconds(t.max_reorder_window_ms)?;
    milliseconds(d.max_regression_ms)?;
    rate(t.max_discarded_rate)?;
    if t.policy == TradeTimestamp::Reorder
        && (t.max_reorder_window_ms == 0
            || t.max_regression_ms == 0
            || t.max_reorder_window_ms < t.max_regression_ms)
    {
        return Err(invalid(
            "trade reorder requires positive max_regression_ms and max_reorder_window_ms >= max_regression_ms",
        ));
    }
    if t.policy == TradeTimestamp::Discard
        && (t.max_discarded_events == 0 || t.max_regression_ms == 0)
    {
        return Err(invalid(
            "trade discard requires explicit positive regression and event limits",
        ));
    }
    if !d.require_sequence_continuity || !d.require_recovery_snapshot {
        return Err(invalid(
            "depth sequence validation and verified snapshot initialization cannot be disabled; unsequenced snapshot feeds do not require an invented sequence field",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_and_configuration_validation() {
        let r: Reconciliation = toml::from_str("").unwrap();
        let t: TimestampRegression = toml::from_str("").unwrap();
        validate(&r, &t).unwrap();
        assert_eq!(r.trades.overlap_policy, TradeOverlap::Strict);
        assert_eq!(t.depth.policy, DepthTimestamp::Strict);
        let r: Reconciliation = toml::from_str(
            r#"
            [trades]
            overlap_policy="prefer_source"
            [trades.prefer_source]
            selection="later"
            on_unique_data_loss="mark_degraded"
            max_excluded_events=5
            max_excluded_rate=0.01
            minimum_samples=1000
            max_excluded_interval_ms=1000
            [depth]
            overlap_policy="snapshot_recovery"
        "#,
        )
        .unwrap();
        validate(&r, &t).unwrap();
        for text in [
            "[trades]\noverlap_policy='unknown'",
            "[depth.prefer_source]\nselection='priority'",
            "[trades.prefer_source]\nmax_excluded_events=-1",
            "[depth]\nextra=true",
        ] {
            assert!(toml::from_str::<Reconciliation>(text).is_err(), "{text}");
        }
        for text in [
            "[trades.prefer_source]\nmax_excluded_rate=nan",
            "[trades.prefer_source]\nmax_excluded_rate=1.1",
            "[trades.prefer_source]\non_unique_data_loss='mark_degraded'\nmax_excluded_rate=0.01\nminimum_samples=1000",
            "[depth.prefer_source]\nmax_excluded_interval_ms=18446744073709551615",
        ] {
            let r = toml::from_str::<Reconciliation>(text).unwrap();
            assert!(validate(&r, &t).is_err(), "{text}");
        }
        for text in [
            "[trades]\npolicy='reorder'",
            "[trades]\npolicy='discard'\nmax_regression_ms=1",
            "[depth]\nrequire_sequence_continuity=false",
            "[depth]\nrequire_recovery_snapshot=false",
            "[depth]\nmax_regression_ms=18446744073709551615",
            "[trades]\nmax_discarded_rate=-0.1",
        ] {
            let t = toml::from_str::<TimestampRegression>(text).unwrap();
            assert!(validate(&r, &t).is_err(), "{text}");
        }
        for policy in ["preserve", "reorder", "discard"] {
            let t:TimestampRegression=toml::from_str(&format!("[trades]\npolicy='{policy}'\nmax_regression_ms=1\nmax_reorder_window_ms=1\nmax_discarded_events=1")).unwrap();
            validate(&r, &t).unwrap();
        }
        assert!(milliseconds(u64::MAX).is_err());
        assert_eq!(milliseconds(1).unwrap(), 1_000_000);
    }
}
