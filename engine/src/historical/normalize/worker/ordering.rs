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
        Ok(())
    }
    /// A degradation policy may accept a regressed event in source order. Track
    /// the immediately previous accepted event, never a rejected record.
    pub fn accept(&mut self, timestamp: i64) {
        self.previous = Some(timestamp);
    }
}
