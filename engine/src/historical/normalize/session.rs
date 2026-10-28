use std::{collections::HashSet, path::PathBuf};

use super::progress::{ProgressReporter, ProgressTaskId};
use super::{
    ProcessingResult,
    executor::{
        ExecutionContext, finalize_processing_job, persist_processing_failure, prepare_staging,
    },
};
use super::{
    config::{ProcessingConfig, validate_resources},
    resources::{ResourceBudget, estimate_task_resources},
    scheduler::execute_parallel_budgeted,
    task::{TaskExecution, execute_task, execute_task_with_progress},
};
use crate::{historical::job::ContentType, shared::normalization::TargetSchema};
use crate::{
    historical::job::{ProcessingJob, WorkTask, validate_processing_job_structure},
    shared::error::{MarketForgeError, Result},
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProcessingSessionResult {
    pub jobs: Vec<ProcessingResult>,
    pub total_tasks: usize,
}
#[derive(Debug)]
struct JobPreflightFailure {
    task_id: u64,
    error: MarketForgeError,
}

// -----------------------------------------------------------------------------
// Complete processing session
// -----------------------------------------------------------------------------

pub fn execute_processing_session(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
) -> Result<ProcessingSessionResult> {
    execute_processing_session_with_progress(jobs, config, &ProgressReporter::disabled())
}
pub fn execute_processing_session_with_progress(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<ProcessingSessionResult> {
    // Execute the existing processing session.
    let result = execute_processing_session_inner(jobs, config, reporter);

    // Determine whether every job completed successfully.
    let success = result.as_ref().is_ok_and(|session| {
        session
            .jobs
            .iter()
            .all(|job| job.status == super::ProcessingStatus::Complete)
    });

    // Always notify the dashboard, including on errors.
    reporter.finished(success);

    result
}
fn execute_processing_session_inner(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<ProcessingSessionResult> {
    validate_resources(&config.resources)?;
    validate_session(jobs)?;

    // -------------------------------------------------------------
    // Validate output paths before creating staging directories
    // -------------------------------------------------------------

    for job in jobs {
        if job.output.dataset_path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "final dataset already exists: {}",
                job.output.dataset_path.display(),
            )));
        }

        if job.output.staging_path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "staging already exists: {}",
                job.output.staging_path.display(),
            )));
        }
    }

    // -------------------------------------------------------------
    // Job-level source preflight
    // -------------------------------------------------------------

    let mut preflight_failures = HashMap::new();
    let mut eligible_jobs = Vec::new();
    let mut eligible_indices = Vec::new();

    for (job_index, job) in jobs.iter().enumerate() {
        let mut failure = None;

        for task in &job.tasks {
            if let Err(error) = estimate_task_resources(task, &config.resources.parquet) {
                failure = Some(JobPreflightFailure {
                    task_id: task.task_id.0,
                    error,
                });

                break;
            }
        }

        match failure {
            Some(failure) => {
                if reporter.is_enabled() {
                    let reason = failure.error.to_string();

                    for task in &job.tasks {
                        let id = ProgressTaskId::new(job.job_id.0, task.task_id.0);

                        if task.task_id.0 == failure.task_id {
                            reporter.task_failed(id, reason.clone());
                        } else {
                            reporter.task_skipped(id, "Job rejected during preflight".to_owned());
                        }
                    }
                }

                preflight_failures.insert(job_index, failure);
            }

            None => {
                eligible_indices.push(job_index);
                eligible_jobs.push(job.clone());
            }
        }
    }

    // -------------------------------------------------------------
    // Prepare staging for every job
    // -------------------------------------------------------------

    for job in jobs {
        prepare_staging(job)?;
    }

    // -------------------------------------------------------------
    // Execute eligible jobs through the global worker pool
    // -------------------------------------------------------------

    let session = execute_session_tasks_with_progress(&eligible_jobs, config, reporter)?;

    let total_tasks = jobs.iter().map(|job| job.tasks.len()).sum();

    // -------------------------------------------------------------
    // Map successful scheduling results to original job indices
    // -------------------------------------------------------------

    let mut executions_by_job = HashMap::new();

    for (eligible_index, job_results) in session.jobs.into_iter().enumerate() {
        let original_index = eligible_indices[eligible_index];

        executions_by_job.insert(original_index, job_results.executions);
    }

    // -------------------------------------------------------------
    // Finalize every job independently
    // -------------------------------------------------------------

    let mut results = Vec::with_capacity(jobs.len());

    for (job_index, job) in jobs.iter().enumerate() {
        let mut context = ExecutionContext::default();
        reporter.finalizing();

        let result = if let Some(failure) = preflight_failures.remove(&job_index) {
            context.failed_task_id = Some(failure.task_id);

            persist_processing_failure(job, context, failure.error)?
        } else {
            let executions = executions_by_job.remove(&job_index).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "missing session executions for job {}",
                    job.job_id.0,
                ))
            })?;

            match finalize_processing_job(job, config, executions, &mut context) {
                Ok(result) => result,

                Err(error) => persist_processing_failure(job, context, error)?,
            }
        };

        results.push(result);
    }

    Ok(ProcessingSessionResult {
        jobs: results,
        total_tasks,
    })
}

// -----------------------------------------------------------------------------
// Session task
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ScheduledTask<'a> {
    pub job_index: usize,
    pub task_index: usize,
    pub task: &'a WorkTask,
    pub staging_path: &'a std::path::Path,
}

// -----------------------------------------------------------------------------
// Session result
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct JobTaskResults {
    pub job_index: usize,
    pub executions: Vec<TaskExecution>,
}

#[derive(Debug)]
pub struct ProcessingSessionTasks {
    pub jobs: Vec<JobTaskResults>,
    pub total_tasks: usize,
}

// -----------------------------------------------------------------------------
// Global task scheduling
// -----------------------------------------------------------------------------

pub fn execute_session_tasks(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
) -> Result<ProcessingSessionTasks> {
    execute_session_tasks_with_progress(jobs, config, &ProgressReporter::disabled())
}

pub fn execute_session_tasks_with_progress(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<ProcessingSessionTasks> {
    validate_session(jobs)?;

    let mut scheduled = Vec::new();

    for (job_index, job) in jobs.iter().enumerate() {
        for (task_index, task) in job.tasks.iter().enumerate() {
            scheduled.push(ScheduledTask {
                job_index,
                task_index,
                task,
                staging_path: &job.output.staging_path,
            });
        }
    }

    let total_tasks = scheduled.len();

    let budget = ResourceBudget::new(
        config.resources.memory_budget_bytes,
        config.resources.scratch_budget_bytes,
    )?;

    let executions = execute_parallel_budgeted(
        scheduled,
        config.resources.workers,
        &budget,
        |scheduled| estimate_task_resources(scheduled.task, &config.resources.parquet),
        |scheduled| {
            let task = scheduled.task;

            let job_id = jobs[scheduled.job_index].job_id.0;

            let progress_id = ProgressTaskId::new(job_id, task.task_id.0);

            if reporter.is_enabled() {
                reporter.task_started(
                    progress_id,
                    format!("{:?}", task.exchange),
                    task.symbol.clone(),
                    format!("{:?}", jobs[scheduled.job_index].output.content_type),
                    task.input_path
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| task.input_path.display().to_string()),
                );
            }

            let execution = if reporter.is_enabled() {
                execute_task_with_progress(
                    task,
                    config,
                    scheduled.staging_path,
                    reporter,
                    progress_id,
                )
            } else {
                execute_task(task, config, scheduled.staging_path)
            };

            if let Some(error) = &execution.error {
                reporter.task_failed(progress_id, error.to_string());
            } else {
                reporter.task_completed(progress_id, &execution.metrics.counters);
            }

            Ok((scheduled.job_index, scheduled.task_index, execution))
        },
    )?;

    // -------------------------------------------------------------------------
    // Group results by their originating jobs
    // -------------------------------------------------------------------------

    let mut grouped: Vec<Vec<(usize, TaskExecution)>> =
        (0..jobs.len()).map(|_| Vec::new()).collect();

    for execution in executions {
        let (job_index, task_index, result) = execution?;

        grouped[job_index].push((task_index, result));
    }

    let jobs = grouped
        .into_iter()
        .enumerate()
        .map(|(job_index, mut executions)| {
            executions.sort_by_key(|(task_index, _)| *task_index);

            JobTaskResults {
                job_index,
                executions: executions
                    .into_iter()
                    .map(|(_, execution)| execution)
                    .collect(),
            }
        })
        .collect();

    Ok(ProcessingSessionTasks { jobs, total_tasks })
}

// -----------------------------------------------------------------------------
// Session validation
// -----------------------------------------------------------------------------

fn validate_session(jobs: &[ProcessingJob]) -> Result<()> {
    let mut paths = Vec::<PathBuf>::new();

    for job in jobs {
        // Validate each job before scheduling its tasks.
        validate_processing_job_structure(job)?;

        let staging = &job.output.staging_path;
        let dataset = &job.output.dataset_path;

        // Staging and final dataset paths must be distinct.
        if staging == dataset {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "job {} staging and dataset paths are identical",
                job.job_id.0,
            )));
        }

        // Reject overlapping paths across all jobs.
        for path in [staging, dataset] {
            if paths
                .iter()
                .any(|existing| path.starts_with(existing) || existing.starts_with(path))
            {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "overlapping processing session output path: {}",
                    path.display(),
                )));
            }

            paths.push(path.clone());
        }

        // Reject duplicate task IDs within each job.
        let mut task_ids = HashSet::new();

        for task in &job.tasks {
            if !task_ids.insert(task.task_id.0) {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "duplicate task ID {} in job {}",
                    task.task_id.0, job.job_id.0,
                )));
            }

            // -------------------------------------------------------------------------
            // Validate normalization targets
            // -------------------------------------------------------------------------

            if task.normalizations.is_empty() {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "task {} contains no normalization configurations",
                    task.task_id.0,
                )));
            }

            let expected_target = match job.output.content_type {
                ContentType::Trades => TargetSchema::Trade,
                ContentType::Depth => TargetSchema::Depth,

                ContentType::Combined => {
                    return Err(MarketForgeError::InvalidConfiguration(format!(
                        "job {} uses unsupported combined output",
                        job.job_id.0,
                    )));
                }
            };

            // All normalizations must match the job's declared output type.
            if task
                .normalizations
                .iter()
                .any(|normalization| normalization.target_schema != expected_target)
            {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "task {} normalization target does not match job {} content type",
                    task.task_id.0, job.job_id.0,
                )));
            }

            // Trades require exactly one normalization.
            // Depth may contain multiple snapshot/update normalizations.
            if expected_target == TargetSchema::Trade && task.normalizations.len() != 1 {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "task {} requires exactly one trade normalization",
                    task.task_id.0,
                )));
            }
        }
    }

    Ok(())
}
