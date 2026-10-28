use super::progress::ProgressTaskId;
use super::{
    config::ProcessingConfig,
    metrics::{ScopedIntegrityMetrics, TaskMetrics},
    parquet::{ParquetDepthWriter, ParquetTradeWriter},
    progress::{ProgressReporter, WorkerProgress},
    sequence_policy::resolve_sequence_policy,
    worker::{process_depth_task_with_metrics, process_trade_task_with_metrics},
};
use crate::{
    historical::job::{IntegrityPolicy, WorkTask},
    shared::domain::book::SequencePolicy,
    shared::error::{MarketForgeError, Result},
    shared::normalization::TargetSchema,
    shared::persistence::config::ParquetResourceConfig,
};
use std::path::{Path, PathBuf};
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
pub fn execute_trade_task_with_progress(
    task: &WorkTask,
    policy: &IntegrityPolicy,
    parquet: &ParquetResourceConfig,
    staging: &Path,
    reporter: &ProgressReporter,
    progress_id: ProgressTaskId,
) -> TradeTaskExecution {
    let output_path = staging
        .join("tasks")
        .join(format!("task-{:06}", task.task_id.0))
        .join("trades");

    let mut metrics = TaskMetrics::default();
    let mut progress = WorkerProgress::new(reporter, progress_id);

    let result = execute_trade_task_inner_with_progress(
        task,
        policy,
        parquet,
        &output_path,
        &mut metrics,
        &mut progress,
    );

    progress.finish(&metrics.counters);

    TradeTaskExecution {
        task_id: task.task_id.0,
        output_path,
        metrics,
        error: result.err(),
    }
}
fn execute_trade_task_inner_with_progress(
    task: &WorkTask,
    policy: &IntegrityPolicy,
    parquet: &ParquetResourceConfig,
    output_path: &Path,
    metrics: &mut TaskMetrics,
    progress: &mut WorkerProgress,
) -> Result<()> {
    let mut writer = ParquetTradeWriter::new(output_path, parquet.clone())?;

    let previous = ScopedIntegrityMetrics::default();
    let worker_policy = worker_integrity_policy(policy);

    super::worker::process_trade_task_with_metrics_and_progress(
        task,
        &mut writer,
        metrics,
        &worker_policy,
        &previous,
        progress,
    )?;

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
pub fn execute_depth_task_with_progress(
    task: &WorkTask,
    config: &ProcessingConfig,
    staging: &Path,
    reporter: &ProgressReporter,
    progress_id: ProgressTaskId,
) -> TaskExecution {
    let output_path = staging
        .join("tasks")
        .join(format!("task-{:06}", task.task_id.0))
        .join("depth");

    let mut metrics = TaskMetrics::default();
    let mut progress = WorkerProgress::new(reporter, progress_id);

    let result = execute_depth_task_inner_with_progress(
        task,
        config,
        &output_path,
        &mut metrics,
        &mut progress,
    );

    progress.finish(&metrics.counters);

    TaskExecution {
        task_id: task.task_id.0,
        output_paths: vec![output_path],
        metrics,
        error: result.err(),
    }
}
fn execute_depth_task_inner_with_progress(
    task: &WorkTask,
    config: &ProcessingConfig,
    output_path: &Path,
    metrics: &mut TaskMetrics,
    progress: &mut WorkerProgress,
) -> Result<()> {
    let sequence_policy = resolve_sequence_policy(task)?;

    let mut writer = ParquetDepthWriter::new(output_path, config.resources.parquet.clone())?;

    let previous = ScopedIntegrityMetrics::default();
    let worker_policy = worker_integrity_policy(&config.integrity_policy);

    super::worker::process_depth_task_with_metrics_and_progress(
        task,
        &mut writer,
        metrics,
        &worker_policy,
        &previous,
        sequence_policy,
        progress,
    )?;

    writer.finish()?;

    if writer.is_failed() || !writer.is_finished() {
        return Err(MarketForgeError::InvalidConfiguration(
            "depth Parquet writer did not finalize successfully".to_owned(),
        ));
    }

    let writer_metrics = writer.metrics();

    if writer_metrics.levels_written != metrics.counters.events_written {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "task {} depth level count mismatch: metrics={}, writer={}",
            task.task_id.0, metrics.counters.events_written, writer_metrics.levels_written,
        )));
    }

    Ok(())
}
pub fn execute_task_with_progress(
    task: &WorkTask,
    config: &ProcessingConfig,
    staging: &Path,
    reporter: &ProgressReporter,
    progress_id: ProgressTaskId,
) -> TaskExecution {
    if !reporter.is_enabled() {
        return execute_task(task, config, staging);
    }

    let unsupported = |message: String| TaskExecution {
        task_id: task.task_id.0,
        output_paths: Vec::new(),
        metrics: TaskMetrics::default(),
        error: Some(MarketForgeError::InvalidConfiguration(message)),
    };

    if task.normalizations.is_empty() {
        return unsupported(format!(
            "task {} contains no normalization configurations",
            task.task_id.0,
        ));
    }

    let target = task.normalizations[0].target_schema;

    if task
        .normalizations
        .iter()
        .any(|normalization| normalization.target_schema != target)
    {
        return unsupported(format!(
            "task {} mixes trade and depth normalization targets",
            task.task_id.0,
        ));
    }

    match target {
        TargetSchema::Trade => {
            if task.normalizations.len() != 1 {
                return unsupported(format!(
                    "task {} requires exactly one trade normalization",
                    task.task_id.0,
                ));
            }

            execute_trade_task_with_progress(
                task,
                &config.integrity_policy,
                &config.resources.parquet,
                staging,
                reporter,
                progress_id,
            )
            .into()
        }

        TargetSchema::Depth => {
            execute_depth_task_with_progress(task, config, staging, reporter, progress_id)
        }
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

    let worker_policy = worker_integrity_policy(policy);

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
// Isolated depth task execution
// -----------------------------------------------------------------------------

pub fn execute_depth_task(
    task: &WorkTask,
    config: &ProcessingConfig,
    staging: &Path,
) -> TaskExecution {
    let output_path = staging
        .join("tasks")
        .join(format!("task-{:06}", task.task_id.0))
        .join("depth");

    let mut metrics = TaskMetrics::default();

    let result = execute_depth_task_inner(task, config, &output_path, &mut metrics);

    TaskExecution {
        task_id: task.task_id.0,
        output_paths: vec![output_path],
        metrics,
        error: result.err(),
    }
}

fn execute_depth_task_inner(
    task: &WorkTask,
    config: &ProcessingConfig,
    output_path: &Path,
    metrics: &mut TaskMetrics,
) -> Result<()> {
    // -------------------------------------------------------------------------
    // Resolve source sequence semantics
    // -------------------------------------------------------------------------

    let sequence_policy: SequencePolicy = resolve_sequence_policy(task)?;

    // -------------------------------------------------------------------------
    // Initialize isolated Parquet writer
    // -------------------------------------------------------------------------

    let mut writer = ParquetDepthWriter::new(output_path, config.resources.parquet.clone())?;

    // -------------------------------------------------------------------------
    // Prepare integrity policy
    // -------------------------------------------------------------------------

    let previous = ScopedIntegrityMetrics::default();

    let worker_policy = worker_integrity_policy(&config.integrity_policy);

    // -------------------------------------------------------------------------
    // Process depth archive
    // -------------------------------------------------------------------------

    process_depth_task_with_metrics(
        task,
        &mut writer,
        metrics,
        &worker_policy,
        &previous,
        sequence_policy,
    )?;

    // -------------------------------------------------------------------------
    // Finalize canonical data and reconstruction metadata
    // -------------------------------------------------------------------------

    writer.finish()?;

    if writer.is_failed() || !writer.is_finished() {
        return Err(MarketForgeError::InvalidConfiguration(
            "depth Parquet writer did not finalize successfully".to_owned(),
        ));
    }

    // -------------------------------------------------------------------------
    // Verify canonical row accounting
    // -------------------------------------------------------------------------

    let writer_metrics = writer.metrics();

    if writer_metrics.levels_written != metrics.counters.events_written {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "task {} depth level count mismatch: metrics={}, writer={}",
            task.task_id.0, metrics.counters.events_written, writer_metrics.levels_written,
        )));
    }

    Ok(())
}
// -----------------------------------------------------------------------------
// Generic task dispatcher
// -----------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Validate normalization configuration
    // -------------------------------------------------------------------------

    if task.normalizations.is_empty() {
        return unsupported(format!(
            "task {} contains no normalization configurations",
            task.task_id.0,
        ));
    }

    let target = task.normalizations[0].target_schema;

    // Every normalization must target the same canonical data type.
    if task
        .normalizations
        .iter()
        .any(|normalization| normalization.target_schema != target)
    {
        return unsupported(format!(
            "task {} mixes trade and depth normalization targets",
            task.task_id.0,
        ));
    }

    // -------------------------------------------------------------------------
    // Dispatch task
    // -------------------------------------------------------------------------

    match target {
        TargetSchema::Trade => {
            // Trade tasks currently require exactly one normalization.
            if task.normalizations.len() != 1 {
                return unsupported(format!(
                    "task {} requires exactly one trade normalization",
                    task.task_id.0,
                ));
            }

            execute_trade_task(
                task,
                &config.integrity_policy,
                &config.resources.parquet,
                staging,
            )
            .into()
        }

        TargetSchema::Depth => {
            // Depth tasks may contain multiple normalization rules.
            execute_depth_task(task, config, staging)
        }
    }
}

fn worker_integrity_policy(policy: &IntegrityPolicy) -> IntegrityPolicy {
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
        rule.max_rate = None;
        rule.windows.daily = None;
        rule.windows.hourly = None;
    }

    worker_policy
}
