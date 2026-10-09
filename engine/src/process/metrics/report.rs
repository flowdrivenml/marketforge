use serde::{Deserialize, Serialize};

use super::{
    counters::ProcessingCounters, integrity::IntegrityMetrics, windows::ScopedIntegrityMetrics,
};
use crate::{
    error::{MarketForgeError, Result},
    job::WorkTask,
};
// -----------------------------------------------------------------------------
// Live task metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskMetrics {
    pub counters: ProcessingCounters,
    pub integrity: IntegrityMetrics,
    pub scoped_integrity: ScopedIntegrityMetrics,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

impl TaskMetrics {
    pub fn record_timestamp(&mut self, timestamp: i64) {
        self.start_timestamp_ns = Some(
            self.start_timestamp_ns
                .map_or(timestamp, |current| current.min(timestamp)),
        );

        self.end_timestamp_ns = Some(
            self.end_timestamp_ns
                .map_or(timestamp, |current| current.max(timestamp)),
        );
    }
}
impl TaskMetrics {
    pub fn into_report(self, task: &WorkTask) -> TaskMetricsReport {
        TaskMetricsReport {
            task_id: task.task_id.0,
            stream_id: task.stream_id.0.clone(),

            counters: self.counters,
            integrity: self.integrity,
            scoped_integrity: self.scoped_integrity,

            start_timestamp_ns: self.start_timestamp_ns,
            end_timestamp_ns: self.end_timestamp_ns,
        }
    }
}

// -----------------------------------------------------------------------------
// Persistent task report
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMetricsReport {
    pub task_id: u64,
    pub stream_id: String,

    pub counters: ProcessingCounters,
    pub integrity: IntegrityMetrics,
    pub scoped_integrity: ScopedIntegrityMetrics,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Persistent job report
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingMetricsReport {
    pub counters: ProcessingCounters,
    pub integrity: IntegrityMetrics,
    pub scoped_integrity: ScopedIntegrityMetrics,

    pub tasks: Vec<TaskMetricsReport>,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

impl ProcessingMetricsReport {
    pub fn add_task(&mut self, task: TaskMetricsReport) -> Result<()> {
        if self
            .tasks
            .iter()
            .any(|existing| existing.task_id == task.task_id)
        {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "duplicate task metrics report: {}",
                task.task_id
            )));
        }

        // Aggregate into temporary values first.
        // A failure must leave the original report unchanged.

        let mut counters = self.counters.clone();
        counters.merge(&task.counters)?;

        let mut integrity = self.integrity.clone();
        integrity.merge(&task.integrity)?;

        let mut scoped_integrity = self.scoped_integrity.clone();
        scoped_integrity.merge(&task.scoped_integrity)?;

        let start_timestamp_ns = match (self.start_timestamp_ns, task.start_timestamp_ns) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };

        let end_timestamp_ns = match (self.end_timestamp_ns, task.end_timestamp_ns) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };

        // Commit only after every aggregation succeeds.

        self.counters = counters;
        self.integrity = integrity;
        self.scoped_integrity = scoped_integrity;

        self.start_timestamp_ns = start_timestamp_ns;
        self.end_timestamp_ns = end_timestamp_ns;

        self.tasks.push(task);

        Ok(())
    }
}
