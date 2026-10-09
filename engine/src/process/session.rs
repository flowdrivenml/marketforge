use std::{collections::HashSet, path::PathBuf};

use crate::{
    error::{MarketForgeError, Result},
    job::{ProcessingJob, WorkTask, validate_processing_job_structure},
};

use super::{
    config::{ProcessingConfig, validate_resources},
    scheduler::execute_parallel,
    task::{TaskExecution, execute_task},
};

use super::{
    ProcessingResult,
    executor::{
        ExecutionContext, finalize_processing_job, persist_processing_failure, prepare_staging,
    },
};

#[derive(Debug)]
pub struct ProcessingSessionResult {
    pub jobs: Vec<ProcessingResult>,
    pub total_tasks: usize,
}

// -----------------------------------------------------------------------------
// Complete processing session
// -----------------------------------------------------------------------------

pub fn execute_processing_session(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
) -> Result<ProcessingSessionResult> {
    validate_resources(&config.resources)?;
    validate_session(jobs)?;

    // -------------------------------------------------------------------------
    // Prepare isolated staging directories
    // -------------------------------------------------------------------------

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

    for job in jobs {
        prepare_staging(job)?;
    }

    // -------------------------------------------------------------------------
    // Execute all tasks using one global worker pool
    // -------------------------------------------------------------------------

    let session = execute_session_tasks(jobs, config)?;

    // -------------------------------------------------------------------------
    // Finalize each job independently
    // -------------------------------------------------------------------------

    let mut results = Vec::with_capacity(jobs.len());

    for (job, job_results) in jobs.iter().zip(session.jobs) {
        let mut context = ExecutionContext::default();

        let result = finalize_processing_job(job, config, job_results.executions, &mut context);

        let result = match result {
            Ok(result) => result,

            Err(error) => persist_processing_failure(job, context, error)?,
        };

        results.push(result);
    }

    Ok(ProcessingSessionResult {
        jobs: results,
        total_tasks: session.total_tasks,
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

    let executions = execute_parallel(scheduled, config.resources.workers as usize, |scheduled| {
        let execution = execute_task(scheduled.task, config, scheduled.staging_path);

        Ok((scheduled.job_index, scheduled.task_index, execution))
    })?;

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

            if task.normalizations.len() != 1 {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "task {} requires exactly one normalization target",
                    task.task_id.0,
                )));
            }
        }
    }

    Ok(())
}
