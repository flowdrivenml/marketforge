//! Shared processing/loss decisions; integrity observations remain in their own
//! existing evaluator. State is per admitted stream/source, never per event report.
use crate::shared::{
    error::{MarketForgeError, Result},
    historical::{ProcessingConfig, policies::*},
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
fn invalid(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
#[derive(Clone, Default)]
pub(crate) struct Active {
    pub reconciliation: Reconciliation,
    pub timestamp: TimestampRegression,
    pub deferred_trades: bool,
}
thread_local! {static ACTIVE:RefCell<Active>=RefCell::new(Active::default());}
pub(crate) struct Scope(Option<Active>);
impl Scope {
    pub fn enter(config: &ProcessingConfig, deferred_trades: bool) -> Self {
        Self(Some(ACTIVE.with(|v| {
            v.replace(Active {
                reconciliation: config.reconciliation.clone(),
                timestamp: config.timestamp_regression.clone(),
                deferred_trades,
            })
        })))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(old) = self.0.take() {
            ACTIVE.with(|v| v.replace(old));
        }
    }
}
pub(crate) fn active() -> Active {
    ACTIVE.with(|v| v.borrow().clone())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionSample {
    pub timestamp_ns: i64,
    pub source_record: u64,
    pub canonical_rows: u64,
    pub timestamp_policy: bool,
    pub known_unique: bool,
    pub chosen_task_id: Option<u64>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Outcomes {
    pub decision_samples: Vec<DecisionSample>,
    pub logical_source_events: u64,
    pub raw_timestamp_regressions: u64,
    pub unresolved_timestamp_regressions: u64,
    pub known_unique_event_exclusions: u64,
    pub unverified_observation_exclusions: u64,
    pub overlap_exclusions: u64,
    pub timestamp_exclusions: u64,
    pub excluded_canonical_rows: u64,
    pub original_canonical_rows: u64,
    pub published_logical_events: u64,
    pub published_native_order: bool,
    pub policy_extension: bool,
    pub staged_canonical_rows: u64,
    pub staged_logical_events: u64,
    pub reordered: bool,
    pub recovery_segments: u64,
    pub source_switches: u64,
    pub unproven_continuity: bool,
    pub max_overlap_span_ns: u64,
    pub max_timestamp_span_ns: u64,
    #[serde(skip)]
    span: Option<(bool, bool, i64, i64)>,
}
impl Outcomes {
    pub fn exclude(
        &mut self,
        timestamp: i64,
        rows: u64,
        timestamp_policy: bool,
        known: bool,
        source_record: u64,
        chosen_task_id: Option<u64>,
    ) -> Result<()> {
        if self.decision_samples.len() < 8 {
            self.decision_samples.push(DecisionSample {
                timestamp_ns: timestamp,
                source_record,
                canonical_rows: rows,
                timestamp_policy,
                known_unique: known,
                chosen_task_id,
            });
        }
        let counter = if known {
            &mut self.known_unique_event_exclusions
        } else {
            &mut self.unverified_observation_exclusions
        };
        *counter = counter
            .checked_add(1)
            .ok_or_else(|| invalid("loss counter overflow"))?;
        let reason = if timestamp_policy {
            &mut self.timestamp_exclusions
        } else {
            &mut self.overlap_exclusions
        };
        *reason = reason
            .checked_add(1)
            .ok_or_else(|| invalid("loss reason counter overflow"))?;
        self.excluded_canonical_rows = self
            .excluded_canonical_rows
            .checked_add(rows)
            .ok_or_else(|| invalid("excluded row counter overflow"))?;
        self.track_exclusion_span(timestamp, timestamp_policy)
    }
    pub(crate) fn reset_spans(&mut self) {
        self.span = None;
        self.max_overlap_span_ns = 0;
        self.max_timestamp_span_ns = 0;
    }
    pub(crate) fn track_exclusion_span(
        &mut self,
        timestamp: i64,
        timestamp_policy: bool,
    ) -> Result<()> {
        let (overlap, temporal, minimum, maximum) = match self.span {
            Some((overlap, temporal, a, b)) => (
                overlap || !timestamp_policy,
                temporal || timestamp_policy,
                a.min(timestamp),
                b.max(timestamp),
            ),
            None => (!timestamp_policy, timestamp_policy, timestamp, timestamp),
        };
        self.span = Some((overlap, temporal, minimum, maximum));
        let duration = u64::try_from(i128::from(maximum) - i128::from(minimum))
            .map_err(|_| invalid("excluded span duration overflow"))?;
        if overlap {
            self.max_overlap_span_ns = self.max_overlap_span_ns.max(duration);
        }
        if temporal {
            self.max_timestamp_span_ns = self.max_timestamp_span_ns.max(duration);
        }
        Ok(())
    }
    pub fn retain(&mut self) {
        self.span = None;
    }
    pub fn exclusions(&self) -> Result<u64> {
        self.known_unique_event_exclusions
            .checked_add(self.unverified_observation_exclusions)
            .ok_or_else(|| invalid("loss sum overflow"))
    }
}
fn enforce_rate(
    count: u64,
    denominator: u64,
    rate: Option<f64>,
    minimum: u64,
    label: &str,
) -> Result<()> {
    use rust_decimal::Decimal;
    if let Some(rate) = rate
        && denominator >= minimum
        && denominator > 0
    {
        let bound = Decimal::from_str_exact(&rate.to_string())
            .map_err(|_| invalid("loss rate is outside supported decimal precision"))?;
        let actual = Decimal::from(count)
            .checked_div(Decimal::from(denominator))
            .ok_or_else(|| invalid("loss rate arithmetic overflow"))?;
        if actual > bound {
            return Err(invalid(format!(
                "{label} loss rate exceeded: {count}/{denominator} > {rate}"
            )));
        }
    }
    Ok(())
}
pub(crate) fn enforce_source_overlap(settings: &PreferSource, outcomes: &Outcomes) -> Result<()> {
    if outcomes.overlap_exclusions > 0 {
        if settings.on_unique_data_loss == LossAction::Fail {
            return Err(invalid(
                "source preference requires unique/unverified observation exclusion, but on_unique_data_loss=fail",
            ));
        }
        if outcomes.overlap_exclusions > settings.max_excluded_events {
            return Err(invalid(format!(
                "overlap loss count exceeded: {} > {}",
                outcomes.overlap_exclusions, settings.max_excluded_events
            )));
        }
        if let Some(ms) = settings.max_excluded_interval_ms
            && outcomes.max_overlap_span_ns > milliseconds(ms)? as u64
        {
            return Err(invalid(
                "overlap contiguous excluded interval exceeds configured limit",
            ));
        }
    }
    Ok(())
}
/// Enforce counts job-wide after all streams, before manifest publication. Rates
/// use accepted logical observations minus verified duplicate observations;
/// unknown uniqueness is conservatively charged, not relabelled as unique.
pub(crate) fn evaluate(
    config: &ProcessingConfig,
    depth: bool,
    report: &super::metrics::ProcessingMetricsReport,
) -> Result<bool> {
    let mut denominator = 0u64;
    let mut overlap = 0u64;
    let mut timestamp = 0u64;
    let mut degraded = false;
    let settings = if depth {
        config.reconciliation.depth.prefer_source.loss()
    } else {
        config.reconciliation.trades.prefer_source.clone()
    };
    for task in &report.tasks {
        if let Some(source) = &task.source {
            let o = &source.processing;
            denominator = denominator
                .checked_add(
                    o.logical_source_events
                        .checked_sub(source.duplicates)
                        .ok_or_else(|| invalid("duplicate count exceeds logical source events"))?,
                )
                .ok_or_else(|| invalid("loss denominator overflow"))?;
            overlap = overlap
                .checked_add(o.overlap_exclusions)
                .ok_or_else(|| invalid("job overlap loss overflow"))?;
            timestamp = timestamp
                .checked_add(o.timestamp_exclusions)
                .ok_or_else(|| invalid("job timestamp loss overflow"))?;
            enforce_source_overlap(&settings, o)?;
            degraded |=
                o.exclusions()? > 0 || o.unproven_continuity || o.raw_timestamp_regressions > 0;
        }
    }
    if overlap > settings.max_excluded_events {
        return Err(invalid(format!(
            "job-wide overlap excluded events exceeded: {overlap} > {}",
            settings.max_excluded_events
        )));
    }
    enforce_rate(
        overlap,
        denominator,
        settings.max_excluded_rate,
        settings.minimum_samples,
        "overlap",
    )?;
    let max = if depth {
        config.timestamp_regression.depth.max_discarded_events
    } else {
        config.timestamp_regression.trades.max_discarded_events
    };
    if timestamp > max {
        return Err(invalid(format!(
            "job-wide timestamp discarded events exceeded: {timestamp} > {max}"
        )));
    }
    if !depth {
        enforce_rate(
            timestamp,
            denominator,
            config.timestamp_regression.trades.max_discarded_rate,
            config.timestamp_regression.trades.minimum_samples,
            "timestamp",
        )?;
    }
    Ok(degraded)
}
/// Exact ordering of arbitrary-length unsigned decimal native identifiers.
/// No float, fixed-width parse, lexical-number comparison or consecutiveness.
pub(crate) fn numeric_id_cmp(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    if a.is_empty()
        || b.is_empty()
        || !a.bytes().all(|b| b.is_ascii_digit())
        || !b.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let a = a.trim_start_matches('0');
    let b = b.trim_start_matches('0');
    Some(a.len().cmp(&b.len()).then_with(|| a.cmp(b)))
}

/// A total tie order even when sources contain mixed native-ID representations.
/// Numeric IDs precede other representations; nonnumeric/missing IDs retain
/// the stable source-position tie order. Numeric ordering is never lexical.
pub(crate) fn native_id_order(a: Option<&str>, b: Option<&str>) -> std::cmp::Ordering {
    let numeric =
        |s: Option<&str>| s.is_some_and(|s| !s.is_empty() && s.bytes().all(|v| v.is_ascii_digit()));
    match (numeric(a), numeric(b)) {
        (true, true) => numeric_id_cmp(a.unwrap(), b.unwrap()).unwrap(),
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}
