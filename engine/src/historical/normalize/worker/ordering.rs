//! Task/member-local concrete-stream checks. Cross-archive ordering is not inferred.
use super::integrity::enforce_integrity;
use crate::{
    historical::{
        job::{IntegrityPolicy, SourceOrdering, WorkTask},
        normalize::metrics::{
            IntegrityCategory, IntegrityDiagnostic, IntegrityScope, ScopedIntegrityMetrics,
            TaskMetrics,
        },
    },
    shared::error::{MarketForgeError, Result},
};
pub(super) fn supported(task: &WorkTask) -> Result<()> {
    if task.source_ordering == SourceOrdering::TimestampSortable
        && task.raw_schema.get("workbook_sheets").is_none()
    {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "task {} timestamp_sortable source has no supported sorting path; stateful depth is never reordered by timestamps",
            task.task_id.0
        )));
    }
    Ok(())
}
pub(super) fn regresses(previous: Option<i64>, timestamp: i64) -> bool {
    previous.is_some_and(|p| timestamp < p)
}
pub(super) struct TimestampOrder<'a> {
    previous: Option<i64>,
    high_water: Option<i64>,
    sequence: Option<u64>,
    trade: bool,
    processing: crate::historical::normalize::policies::Active,
    scope: IntegrityScope,
    policy: &'a IntegrityPolicy,
    previous_job: &'a ScopedIntegrityMetrics,
}
impl<'a> TimestampOrder<'a> {
    pub fn new(
        task: &WorkTask,
        scope: &IntegrityScope,
        policy: &'a IntegrityPolicy,
        previous_job: &'a ScopedIntegrityMetrics,
    ) -> Result<Self> {
        supported(task)?;
        Ok(Self {
            previous: None,
            high_water: None,
            sequence: None,
            trade: task.normalizations[0].target_schema
                == crate::shared::normalization::TargetSchema::Trade,
            processing: crate::historical::normalize::policies::active(),
            scope: scope.clone(),
            policy,
            previous_job,
        })
    }
    pub fn check(&self, metrics: &mut TaskMetrics, record: u64, timestamp: i64) -> Result<()> {
        let Some(previous) = self.previous else {
            return Ok(());
        };
        let violated = regresses(self.previous, timestamp);
        metrics.scoped_integrity.record(
            &self.scope,
            IntegrityCategory::TimestampRegression,
            violated,
            Some(timestamp),
        )?;
        if violated {
            if let Some(source) = metrics.source.as_mut() {
                source.processing.raw_timestamp_regressions = source
                    .processing
                    .raw_timestamp_regressions
                    .checked_add(1)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "raw timestamp anomaly count overflow".into(),
                        )
                    })?;
            }
            let message = format!(
                "stream {} timestamp regression: previous accepted {previous}, current {timestamp}",
                self.scope.stream_id
            );
            metrics.integrity.record(IntegrityDiagnostic {
                category: IntegrityCategory::TimestampRegression,
                task_id: self.scope.task_id,
                source_record: Some(record),
                event_timestamp_ns: Some(timestamp),
                message: message.clone(),
            })?;
            enforce_integrity(
                self.policy,
                metrics,
                self.previous_job,
                IntegrityCategory::TimestampRegression,
                &message,
            )?;
        }
        if self.trade && self.processing.deferred_trades {
            return Ok(());
        }
        let amount = self
            .high_water
            .filter(|p| timestamp < *p)
            .map(|p| {
                p.checked_sub(timestamp).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "timestamp regression subtraction overflow".into(),
                    )
                })
            })
            .transpose()?
            .unwrap_or(0);
        if amount > 0 {
            use crate::shared::historical::policies::{
                DepthTimestamp, TradeTimestamp, milliseconds,
            };
            let permitted = if self.trade {
                milliseconds(self.processing.timestamp.trades.max_regression_ms)?
            } else {
                milliseconds(self.processing.timestamp.depth.max_regression_ms)?
            };
            let strict = if self.trade {
                self.processing.timestamp.trades.policy == TradeTimestamp::Strict
            } else {
                self.processing.timestamp.depth.policy == DepthTimestamp::Strict
            };
            if strict || amount > permitted {
                return Err(MarketForgeError::RecordIntegrity {
                    category: IntegrityCategory::TimestampRegression,
                    message: format!(
                        "processing timestamp regression: high-water={:?}, current={timestamp}, regression_ns={amount}, permitted_ns={permitted}, strict={strict}",
                        self.high_water
                    ),
                });
            }
            if let Some(source) = metrics.source.as_mut() {
                source.processing.unresolved_timestamp_regressions += 1;
                source.processing.published_native_order = true;
            }
        }
        Ok(())
    }
    pub fn check_depth(
        &mut self,
        metrics: &mut TaskMetrics,
        record: u64,
        source: &crate::shared::domain::canonical::DepthSourceEventMetadata,
        snapshot: bool,
    ) -> Result<()> {
        if snapshot
            && source
                .sequence_end
                .zip(self.sequence)
                .is_some_and(|(now, old)| now < old)
        {
            self.previous = None;
            self.high_water = None;
            if let Some(s) = metrics.source.as_mut() {
                s.processing.recovery_segments += 1;
            }
        }
        self.check(metrics, record, source.event_timestamp_ns)
    }
    pub fn accept_depth(&mut self, timestamp: i64, sequence: Option<u64>) {
        self.accept(timestamp);
        self.sequence = sequence;
    }
    /// A degradation policy may accept a regressed event in source order. Track
    /// the immediately previous accepted event, never a rejected record.
    pub fn accept(&mut self, timestamp: i64) {
        self.previous = Some(timestamp);
        self.high_water = Some(self.high_water.map_or(timestamp, |p| p.max(timestamp)));
    }
}
