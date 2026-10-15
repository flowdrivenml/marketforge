use super::ProcessingStatus;
use super::failure::ProcessingFailureReport;
use super::progress::ProgressReporter;
use super::progress::ProgressTaskId;
use super::{
    ProcessingErrorCode, ProcessingResult,
    config::{ProcessingConfig, validate_resources},
    manifest::{DATASET_MANIFEST_FILENAME, DatasetManifest},
    metrics::{IntegrityEvaluation, ProcessingMetricsReport, evaluate_integrity_policy},
};
use super::{
    resources::{ResourceBudget, estimate_task_resources},
    scheduler::execute_parallel_budgeted,
    task::{TaskExecution, execute_task_with_progress},
};
use crate::{
    error::{MarketForgeError, Result},
    job::{
        ContentType, ProcessingJob, ProcessingOperation, TargetSchema,
        validate_processing_job_structure,
    },
};
use std::{fs, path::Path};

// -----------------------------------------------------------------------------
// Processing job execution
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct ExecutionContext {
    pub(crate) report: ProcessingMetricsReport,
    pub(crate) failed_task_id: Option<u64>,
    pub(crate) integrity_evaluation: Option<IntegrityEvaluation>,
}

pub fn execute_processing_job(
    job: &ProcessingJob,
    config: &ProcessingConfig,
) -> Result<ProcessingResult> {
    execute_processing_job_with_progress(job, config, &ProgressReporter::disabled())
}

pub fn execute_processing_job_with_progress(
    job: &ProcessingJob,
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<ProcessingResult> {
    validate_processing_job_structure(job)?;
    validate_resources(&config.resources)?;

    let result = match job.operation {
        ProcessingOperation::Process => execute_process(job, config, reporter),
    };

    match &result {
        Ok(processing_result) => {
            reporter.finished(processing_result.status == ProcessingStatus::Complete);
        }
        Err(_) => reporter.finished(false),
    }

    result
}

// -----------------------------------------------------------------------------
// Raw archive processing
// -----------------------------------------------------------------------------

fn execute_process(
    job: &ProcessingJob,
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<ProcessingResult> {
    // -------------------------------------------------------------------------
    // Validate output content type
    // -------------------------------------------------------------------------

    let expected_target = match job.output.content_type {
        ContentType::Trades => TargetSchema::Trade,
        ContentType::Depth => TargetSchema::Depth,

        ContentType::Combined => {
            return Ok(ProcessingResult::failed(
                job.job_id,
                job.dataset_id,
                ProcessingErrorCode::InvalidJob,
                "combined processing output is not supported yet",
            ));
        }
    };

    // -------------------------------------------------------------------------
    // Validate task normalization targets
    // -------------------------------------------------------------------------

    for task in &job.tasks {
        if task.normalizations.is_empty() {
            return Ok(ProcessingResult::failed(
                job.job_id,
                job.dataset_id,
                ProcessingErrorCode::InvalidJob,
                format!(
                    "task {} contains no normalization configurations",
                    task.task_id.0
                ),
            ));
        }

        if task
            .normalizations
            .iter()
            .any(|normalization| normalization.target_schema != expected_target)
        {
            return Ok(ProcessingResult::failed(
                job.job_id,
                job.dataset_id,
                ProcessingErrorCode::InvalidJob,
                format!(
                    "task {} normalization target does not match job content type",
                    task.task_id.0
                ),
            ));
        }

        if expected_target == TargetSchema::Trade && task.normalizations.len() != 1 {
            return Ok(ProcessingResult::failed(
                job.job_id,
                job.dataset_id,
                ProcessingErrorCode::InvalidJob,
                format!(
                    "task {} must contain exactly one trade normalization",
                    task.task_id.0
                ),
            ));
        }
    }

    if job.output.dataset_path.exists() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "final dataset path already exists: {}",
            job.output.dataset_path.display()
        )));
    }

    prepare_staging(job)?;

    let mut context = ExecutionContext::default();

    let result = process_tasks(job, config, &mut context, reporter);

    match result {
        Ok(result) => Ok(result),
        Err(error) => persist_processing_failure(job, context, error),
    }
}

// -----------------------------------------------------------------------------
// Failure persistence
// -----------------------------------------------------------------------------

pub(crate) fn persist_processing_failure(
    job: &ProcessingJob,
    context: ExecutionContext,
    error: MarketForgeError,
) -> Result<ProcessingResult> {
    let error_message = error.to_string();

    let failure = ProcessingFailureReport::new(
        job,
        context.failed_task_id,
        ProcessingErrorCode::ProcessingFailure,
        &error_message,
        context.report,
        context.integrity_evaluation,
    );

    let failure_path = failure
        .write_to(&job.output.staging_path)
        .map_err(|report_error| {
            MarketForgeError::InvalidConfiguration(format!(
                "processing failed: {error_message}; \
                 additionally failed to persist failure report: {report_error}"
            ))
        })?;

    eprintln!(
        "Processing failed: job={}, task={:?}, report={}",
        job.job_id.0,
        context.failed_task_id,
        failure_path.display(),
    );

    Ok(ProcessingResult::failed(
        job.job_id,
        job.dataset_id,
        ProcessingErrorCode::ProcessingFailure,
        error_message,
    ))
}

// -----------------------------------------------------------------------------
// Multi-task processing and commit
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Task scheduling
// -----------------------------------------------------------------------------

fn process_tasks(
    job: &ProcessingJob,
    config: &ProcessingConfig,
    context: &mut ExecutionContext,
    reporter: &ProgressReporter,
) -> Result<ProcessingResult> {
    let staging = &job.output.staging_path;

    // Validate source accessibility before resource estimation.
    // Preserve the responsible task ID if preparation fails.
    for task in &job.tasks {
        if let Err(error) = estimate_task_resources(task, &config.resources.parquet) {
            context.failed_task_id = Some(task.task_id.0);
            return Err(error);
        }
    }

    let budget = ResourceBudget::new(
        config.resources.memory_budget_bytes,
        config.resources.scratch_budget_bytes,
    )?;

    let executions = execute_parallel_budgeted(
        job.tasks.iter().collect::<Vec<_>>(),
        config.resources.workers,
        &budget,
        |task| estimate_task_resources(task, &config.resources.parquet),
        |task| {
            let task_id = task.task_id.0;
            let progress_id = ProgressTaskId::new(job.job_id.0, task_id);

            reporter.task_started(
                progress_id,
                format!("{:?}", task.exchange),
                task.symbol.clone(),
                format!("{:?}", job.output.content_type),
                task.input_path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| task.input_path.display().to_string()),
            );

            let execution =
                execute_task_with_progress(task, config, staging, reporter, progress_id);

            if let Some(error) = &execution.error {
                reporter.task_failed(progress_id, error.to_string());
            } else {
                reporter.task_completed(progress_id, &execution.metrics.counters);
            }

            Ok(execution)
        },
    )?;

    let executions = executions
        .into_iter()
        .collect::<Result<Vec<TaskExecution>>>()?;

    reporter.finalizing();

    finalize_processing_job(job, config, executions, context)
}

// -----------------------------------------------------------------------------
// Generic job finalization
// -----------------------------------------------------------------------------

pub(crate) fn finalize_processing_job(
    job: &ProcessingJob,
    config: &ProcessingConfig,
    executions: Vec<TaskExecution>,
    context: &mut ExecutionContext,
) -> Result<ProcessingResult> {
    let staging = &job.output.staging_path;

    // -------------------------------------------------------------------------
    // Validate task execution count
    // -------------------------------------------------------------------------

    if executions.len() != job.tasks.len() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "job {} task execution count mismatch: expected={}, actual={}",
            job.job_id.0,
            job.tasks.len(),
            executions.len(),
        )));
    }

    // -------------------------------------------------------------------------
    // Aggregate all task metrics
    // -------------------------------------------------------------------------

    let mut first_error = None;

    for (task, execution) in job.tasks.iter().zip(executions) {
        let task_id = task.task_id.0;

        if execution.task_id != task_id {
            context.failed_task_id = Some(task_id);

            return Err(MarketForgeError::InvalidConfiguration(format!(
                "job {} task execution mismatch: expected={}, actual={}",
                job.job_id.0, task_id, execution.task_id,
            )));
        }

        // Preserve the first failed task ID before metrics aggregation.
        if execution.error.is_some() && first_error.is_none() {
            context.failed_task_id = Some(task_id);
        }

        // Preserve the authoritative metrics for every attempted task.
        context
            .report
            .add_task(execution.metrics.into_report(task))?;

        if let Some(error) = execution.error {
            if first_error.is_none() {
                first_error = Some((task_id, error));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Reject failed tasks after preserving all metrics
    // -------------------------------------------------------------------------

    if let Some((task_id, error)) = first_error {
        context.failed_task_id = Some(task_id);
        return Err(error);
    }

    context.failed_task_id = None;

    // -------------------------------------------------------------------------
    // Validate completed-task accounting
    // -------------------------------------------------------------------------

    context.report.counters.validate()?;

    // -------------------------------------------------------------------------
    // Final integrity evaluation
    // -------------------------------------------------------------------------

    let integrity =
        evaluate_integrity_policy(&config.integrity_policy, &context.report.scoped_integrity)?;

    context.integrity_evaluation = Some(integrity.clone());

    if integrity.is_failed() {
        return Err(MarketForgeError::InvalidCanonical(format!(
            "final integrity policy failed: {} threshold violations",
            integrity.total_policy_violations,
        )));
    }

    // -------------------------------------------------------------------------
    // Construct dataset manifest
    // -------------------------------------------------------------------------

    let manifest =
        DatasetManifest::from_processing_job(job, staging, context.report.clone(), integrity)?;

    // -------------------------------------------------------------------------
    // Verify parallel output layout
    // -------------------------------------------------------------------------

    for file in &manifest.files {
        if !file.path.starts_with("tasks/") {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "unexpected Parquet output path in parallel dataset: {}",
                file.path,
            )));
        }
    }

    // -------------------------------------------------------------------------
    // Write manifest
    // -------------------------------------------------------------------------

    manifest.write_to(staging)?;

    let staged_manifest = staging.join(DATASET_MANIFEST_FILENAME);

    if !staged_manifest.is_file() {
        return Err(MarketForgeError::InvalidConfiguration(
            "dataset manifest was not created".to_owned(),
        ));
    }

    // -------------------------------------------------------------------------
    // Transactional dataset commit
    // -------------------------------------------------------------------------

    commit_dataset(staging, &job.output.dataset_path)?;

    let committed_manifest = job.output.dataset_path.join(DATASET_MANIFEST_FILENAME);

    // -------------------------------------------------------------------------
    // Construct processing result
    // -------------------------------------------------------------------------

    let mut result = ProcessingResult::complete(job.job_id, job.dataset_id);

    result.manifest_path = Some(committed_manifest.to_string_lossy().into_owned());

    result.events_written = manifest.events_written;
    result.files_written = manifest.files_written;

    result.start_timestamp_ns = manifest.start_timestamp_ns;
    result.end_timestamp_ns = manifest.end_timestamp_ns;

    eprintln!(
        "Processing committed: records_read={}, matched={}, skipped={}, rejected={}, written={}, files={}, integrity={:?}, dataset={}",
        manifest.processing_metrics.counters.records_read,
        manifest.processing_metrics.counters.records_matched,
        manifest
            .processing_metrics
            .counters
            .records_skipped_instrument,
        manifest.processing_metrics.counters.records_rejected,
        result.events_written,
        result.files_written,
        manifest.integrity_status,
        job.output.dataset_path.display(),
    );

    Ok(result)
}

// -----------------------------------------------------------------------------
// Transactional dataset commit
// -----------------------------------------------------------------------------

pub(crate) fn commit_dataset(staging: &Path, dataset_path: &Path) -> Result<()> {
    if !staging.is_dir() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "staging directory does not exist: {}",
            staging.display(),
        )));
    }

    if dataset_path.exists() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "final dataset path already exists: {}",
            dataset_path.display(),
        )));
    }

    let parent = dataset_path.parent().ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(
            "final dataset path has no parent directory".to_owned(),
        )
    })?;

    fs::create_dir_all(parent).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to create dataset parent directory {}: {error}",
            parent.display(),
        ))
    })?;

    // Rename staging into the final dataset location.
    //
    // This must remain a same-filesystem operation.
    // Do not fall back to copying files.

    fs::rename(staging, dataset_path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to commit dataset {} -> {}: {error}",
            staging.display(),
            dataset_path.display(),
        ))
    })?;

    Ok(())
}

pub(crate) fn prepare_staging_path(path: &Path) -> Result<()> {
    if path.exists() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "staging path already exists: {}",
            path.display(),
        )));
    }

    fs::create_dir_all(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to create staging path {}: {error}",
            path.display(),
        ))
    })?;

    Ok(())
}

// -----------------------------------------------------------------------------
// Staging preparation
// -----------------------------------------------------------------------------

pub(crate) fn prepare_staging(job: &ProcessingJob) -> Result<()> {
    prepare_staging_path(&job.output.staging_path)
}
