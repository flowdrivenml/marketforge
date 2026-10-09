use std::path::{Path, PathBuf};

use crate::{
    error::{MarketForgeError, Result},
    job::{ParquetResourceConfig, WorkTask},
};

use super::config::ProcessingConfig;
use super::metrics::ScopedIntegrityMetrics;
use super::{
    metrics::TaskMetrics,
    parquet::ParquetTradeWriter,
    worker::{TradeSink, process_trade_task_with_metrics},
};
use crate::job::IntegrityPolicy;
use crate::job::TargetSchema;

// -----------------------------------------------------------------------------
// Task execution result
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct TradeTaskExecution {
    pub task_id: u64,
    pub output_path: PathBuf,
    pub metrics: TaskMetrics,
    pub error: Option<MarketForgeError>,
}

impl TradeTaskExecution {
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

// -----------------------------------------------------------------------------
// Generic task execution result
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct TaskExecution {
    pub task_id: u64,

    pub output_paths: Vec<PathBuf>,

    pub metrics: TaskMetrics,

    pub error: Option<MarketForgeError>,
}

impl TaskExecution {
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

impl From<TradeTaskExecution> for TaskExecution {
    fn from(execution: TradeTaskExecution) -> Self {
        Self {
            task_id: execution.task_id,

            output_paths: vec![execution.output_path],

            metrics: execution.metrics,

            error: execution.error,
        }
    }
}

// -----------------------------------------------------------------------------
// Isolated trade task execution
// -----------------------------------------------------------------------------

pub fn execute_trade_task(
    task: &WorkTask,
    policy: &IntegrityPolicy,
    parquet: &ParquetResourceConfig,
    staging: &Path,
) -> TradeTaskExecution {
    let output_path = staging
        .join("tasks")
        .join(format!("task-{:06}", task.task_id.0))
        .join("trades");

    let mut metrics = TaskMetrics::default();

    let result = execute_trade_task_inner(task, policy, parquet, &output_path, &mut metrics);

    TradeTaskExecution {
        task_id: task.task_id.0,
        output_path,
        metrics,
        error: result.err(),
    }
}

fn execute_trade_task_inner(
    task: &WorkTask,
    policy: &IntegrityPolicy,
    parquet: &ParquetResourceConfig,
    output_path: &Path,
    metrics: &mut TaskMetrics,
) -> Result<()> {
    let mut writer = ParquetTradeWriter::new(output_path, parquet.clone())?;

    // Job-level integrity is evaluated after task aggregation.
    // Here, the worker receives an empty previous-task context.
    let previous = ScopedIntegrityMetrics::default();

    // Parallel workers enforce fatal categories immediately.
    // Job-wide count and rate thresholds are evaluated by the executor.
    let mut worker_policy = policy.clone();

    for rule in [
        &mut worker_policy.parse_failure,
        &mut worker_policy.invalid_record,
        &mut worker_policy.sequence_gap,
        &mut worker_policy.timestamp_regression,
        &mut worker_policy.missing_snapshot,
        &mut worker_policy.invalid_book,
        &mut worker_policy.transformation_failure,
    ] {
        rule.max_count = None;
    }

    process_trade_task_with_metrics(task, &mut writer, metrics, &worker_policy, &previous)?;
    writer.finish()?;

    if writer.is_failed() || !writer.is_finished() {
        return Err(MarketForgeError::InvalidConfiguration(
            "task Parquet writer did not finalize successfully".to_owned(),
        ));
    }

    let writer_metrics = writer.metrics();

    if writer_metrics.trades_written != metrics.counters.events_written {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "task {} trade count mismatch: metrics={}, writer={}",
            task.task_id.0, metrics.counters.events_written, writer_metrics.trades_written,
        )));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Generic task dispatcher
// -----------------------------------------------------------------------------

pub fn execute_task(task: &WorkTask, config: &ProcessingConfig, staging: &Path) -> TaskExecution {
    let unsupported = |message: String| TaskExecution {
        task_id: task.task_id.0,
        output_paths: Vec::new(),
        metrics: TaskMetrics::default(),
        error: Some(MarketForgeError::InvalidConfiguration(message)),
    };

    // Each task must declare exactly one normalization target.
    if task.normalizations.len() != 1 {
        return unsupported(format!(
            "task {} requires exactly one normalization target, found {}",
            task.task_id.0,
            task.normalizations.len(),
        ));
    }

    match task.normalizations[0].target_schema {
        TargetSchema::Trade => execute_trade_task(
            task,
            &config.integrity_policy,
            &config.resources.parquet,
            staging,
        )
        .into(),

        _ => unsupported(format!(
            "unsupported normalization target for task {}",
            task.task_id.0,
        )),
    }
}
