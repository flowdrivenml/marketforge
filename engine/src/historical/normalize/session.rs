use std::{collections::HashSet, path::PathBuf};

use super::progress::{ProgressReporter, ProgressTaskId};
use super::{
    ProcessingResult,
    executor::{ExecutionContext, persist_processing_failure, prepare_staging},
};
use super::{
    config::{ProcessingConfig, validate_processing_config},
    resources::{ResourceBudget, estimate_task_resources_with_limits},
    scheduler::execute_parallel_budgeted_observed,
    task::TaskExecution,
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
    task_id: Option<u64>,
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
    let result = execute_processing_session_inner(jobs, config, reporter, &mut |_| {});

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
    on_result: &mut dyn FnMut(&ProcessingResult),
) -> Result<ProcessingSessionResult> {
    let diagnostics = jobs.first().and_then(|job| {
        super::progress::diagnostics::Diagnostics::start(
            config,
            job.output
                .staging_path
                .parent()
                .unwrap_or(std::path::Path::new(".")),
            reporter,
        )
    });
    for job in jobs {
        reporter.record_workload(job);
        reporter.stream_phase(job.job_id.0, "", "planning/admission");
    }
    let result = execute_processing_session_inner_impl(jobs, config, reporter, on_result);
    if let Some(diagnostics) = diagnostics {
        diagnostics.finish(result.as_ref().is_ok_and(|s| {
            s.jobs
                .iter()
                .all(|j| j.status == super::ProcessingStatus::Complete)
        }));
    }
    result
}
fn execute_processing_session_inner_impl(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
    on_result: &mut dyn FnMut(&ProcessingResult),
) -> Result<ProcessingSessionResult> {
    validate_processing_config(config)?;
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
    let mut retained_admitted = 0u64;
    let mut maximum_admitted = 0u64;

    for (job_index, job) in jobs.iter().enumerate() {
        let mut failure = None;

        for task in &job.tasks {
            if let Err(error) = estimate_task_resources_with_limits(
                task,
                &config.resources.parquet,
                &config.resources.normalization_limits,
            ) {
                failure = Some(JobPreflightFailure {
                    task_id: Some(task.task_id.0),
                    error,
                });

                break;
            }
        }

        if failure.is_none() {
            let mut next_retained = retained_admitted
                .checked_add(super::resources::estimate::failure_report_memory(
                    job,
                    &config.resources.normalization_limits,
                    config.resources.workers,
                )?)
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("failure report reserve overflow".into())
                })?;
            let mut next_maximum = maximum_admitted;
            let admission = (|| -> Result<()> {
                for task in &job.tasks {
                    next_retained = next_retained
                        .checked_add(super::resources::retained_task_memory(
                            task,
                            &config.resources.normalization_limits,
                        )?)
                        .ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration(
                                "retained admission overflow".into(),
                            )
                        })?;
                }
                for unit in super::streams::plan(job)? {
                    let _scope = crate::shared::historical::limits::LimitScope::enter(
                        config.resources.normalization_limits,
                        format!("job={}, stream={}", job.job_id.0, unit.key),
                    );
                    let required = super::streams::estimate(&unit, config)?;
                    next_maximum = next_maximum.max(required.memory_bytes).max(
                        super::resources::job_finalization_memory(
                            &config.resources.parquet,
                            job,
                            &config.resources.normalization_limits,
                        )?,
                    );
                    crate::shared::historical::limits::check(
                        "memory bytes",
                        config.resources.memory_budget_bytes,
                        next_retained.checked_add(next_maximum).ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration("admission overflow".into())
                        })?,
                        "stream and retained-state admission",
                    )?;
                    crate::shared::historical::limits::check(
                        "open files",
                        config.resources.normalization_limits.open_files,
                        required.open_files.checked_add(8).ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration(
                                "file accounting overflow".into(),
                            )
                        })?,
                        "stream admission",
                    )?;
                }
                Ok(())
            })();
            if let Err(error) = admission {
                failure = Some(JobPreflightFailure {
                    task_id: None,
                    error,
                });
            } else {
                retained_admitted = next_retained;
                maximum_admitted = next_maximum;
            }
        }

        match failure {
            Some(failure) => {
                if reporter.is_enabled() {
                    let reason = failure.error.to_string();

                    for task in &job.tasks {
                        let id = ProgressTaskId::new(job.job_id.0, task.task_id.0);

                        if Some(task.task_id.0) == failure.task_id {
                            reporter.task_failed(id, reason.clone());
                        } else {
                            reporter.task_skipped(id, "Job rejected during preflight".to_owned());
                        }
                    }
                }

                reporter.emit(super::progress::ProgressEvent::JobFinished {
                    job_id: job.job_id.0,
                    success: false,
                });
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
        prepare_staging(job, config)?;
    }

    // -------------------------------------------------------------
    // Execute eligible jobs through the global worker pool
    // -------------------------------------------------------------

    let session = execute_session_tasks_with_progress(&eligible_jobs, config, reporter)?;
    for job in jobs {
        reporter.stream_phase(job.job_id.0, "", "finalization resource admission");
    }
    let _finalization = session.reserve_finalization()?;

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
            context.failed_task_id = failure.task_id;

            persist_processing_failure(job, context, failure.error)?
        } else {
            let executions = executions_by_job.remove(&job_index).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "missing session executions for job {}",
                    job.job_id.0,
                ))
            })?;

            match super::executor::finalize_processing_job_with_progress(
                job,
                config,
                executions,
                &mut context,
                reporter,
            ) {
                Ok(result) => result,

                Err(error) => persist_processing_failure(job, context, error)?,
            }
        };

        reporter.job_outcome(&result);
        on_result(&result);
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

pub(crate) type ReportReservations = (
    crate::shared::historical::budget::OwnedResourceReservation,
    crate::shared::historical::budget::OwnedResourceReservation,
);

#[derive(Debug)]
pub struct ProcessingSessionTasks {
    pub jobs: Vec<JobTaskResults>,
    pub total_tasks: usize,
    _retained_reservation: crate::shared::historical::budget::OwnedResourceReservation,
    budget: std::sync::Arc<ResourceBudget>,
    finalization_memory: u64,
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
    validate_processing_config(config)?;
    validate_session(jobs)?;

    let mut scheduled = Vec::new();
    for (job_index, job) in jobs.iter().enumerate() {
        for unit in super::streams::plan(job)? {
            for (_, task) in &unit.tasks {
                reporter.phase(
                    ProgressTaskId::new(job.job_id.0, task.task_id.0),
                    &unit.key,
                    "queued for resource admission",
                );
            }
            scheduled.push((job_index, unit));
        }
    }
    let total_tasks = jobs.iter().map(|job| job.tasks.len()).sum();
    let budget = std::sync::Arc::new(ResourceBudget::with_file_limit(
        config.resources.memory_budget_bytes,
        config.resources.scratch_budget_bytes,
        config.resources.normalization_limits.open_files,
    )?);
    let retained = super::resources::retained_jobs_memory(
        jobs,
        &config.resources.normalization_limits,
        config.resources.workers,
    )?;
    // Session logging, metadata, provenance and finalization handle headroom.
    let headroom = 8;
    let finalization_memory = jobs.iter().try_fold(0u64, |maximum, job| -> Result<u64> {
        Ok(maximum.max(super::resources::job_finalization_memory(
            &config.resources.parquet,
            job,
            &config.resources.normalization_limits,
        )?))
    })?;
    let mut maximum = finalization_memory;
    for (_, unit) in &scheduled {
        let required = super::streams::estimate(unit, config)?;
        maximum = maximum.max(required.memory_bytes);
        crate::shared::historical::limits::check(
            "open files",
            config.resources.normalization_limits.open_files,
            required.open_files.checked_add(headroom).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("file accounting overflow".into())
            })?,
            "session admission",
        )?;
    }
    crate::shared::historical::limits::check(
        "memory bytes",
        config.resources.memory_budget_bytes,
        retained.checked_add(maximum).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("retained accounting overflow".into())
        })?,
        "session retained-state admission",
    )?;
    let retained_reservation = budget.acquire_owned(super::resources::ResourceRequirements {
        memory_bytes: retained,
        scratch_bytes: 0,
        open_files: headroom,
    })?;
    let report_storage_failed = std::sync::atomic::AtomicBool::new(false);
    let executions = execute_parallel_budgeted_observed(
        scheduled,
        config.resources.workers,
        &budget,
        |(_, unit)| super::streams::estimate(unit, config),
        |(job_index, unit)| {
            let stopped = report_storage_failed.load(std::sync::atomic::Ordering::Acquire);
            let execution = if stopped {
                Err(MarketForgeError::NotAttempted {
                    reason: "session stopped after report persistence failure".into(),
                })
            } else {
                super::streams::execute(&unit, config, reporter)
            };
            let results = match execution {
                Ok(results) => results,
                Err(error) => {
                    let mut original =
                        Some(if matches!(error, MarketForgeError::NotAttempted { .. }) {
                            error
                        } else {
                            MarketForgeError::StreamFailure {
                                stream: unit.key.clone(),
                                stage: "stream execution",
                                source: Box::new(error),
                            }
                        });
                    unit.tasks
                        .iter()
                        .map(|(index, task)| {
                            (
                                *index,
                                TaskExecution {
                                    task_id: task.task_id.0,
                                    output_paths: vec![],
                                    metrics: Default::default(),
                                    error: Some(original.take().unwrap_or_else(|| {
                                        MarketForgeError::NotAttempted {
                                            reason: "source not attempted: stream setup failed"
                                                .into(),
                                        }
                                    })),
                                },
                            )
                        })
                        .collect()
                }
            };
            if results.iter().any(|(_, e)| {
                !e.metrics.scoped_integrity.files.is_empty()
                    || !e.metrics.scoped_integrity.hourly.is_empty()
                    || !e.metrics.scoped_integrity.daily.is_empty()
            }) {
                report_storage_failed.store(true, std::sync::atomic::Ordering::Release);
            }
            let stream_error = results.iter().find_map(|(_, e)| {
                e.error
                    .as_ref()
                    .filter(|error| matches!(error, MarketForgeError::StreamFailure { .. }))
            });
            if let Some(error) = stream_error {
                reporter.emit(super::progress::ProgressEvent::StreamFailed {
                    job_id: unit.job.job_id.0,
                    stream: unit.key.clone(),
                    task_ids: unit.tasks.iter().map(|(_, t)| t.task_id.0).collect(),
                    message: error.to_string(),
                });
            }
            for (_, execution) in &results {
                let id = ProgressTaskId::new(unit.job.job_id.0, execution.task_id);
                match &execution.error {
                    None if execution
                        .metrics
                        .source
                        .as_ref()
                        .is_some_and(|s| s.redundant_of_task.is_some()) =>
                    {
                        reporter.task_skipped(
                            id,
                            "Equivalent source selection already normalized".into(),
                        );
                    }
                    None if stream_error.is_none() => {
                        reporter.task_completed(id, &execution.metrics.counters)
                    }
                    Some(MarketForgeError::StreamFailure { .. }) | None => (),
                    Some(MarketForgeError::NotAttempted { reason })
                        if execution.metrics.counters.records_read == 0 =>
                    {
                        reporter.task_skipped(id, reason.clone())
                    }
                    Some(MarketForgeError::NotAttempted { .. }) => (),
                    Some(error) => reporter.task_failed(id, error.to_string()),
                }
            }
            Ok((job_index, results))
        },
        |(_, unit), waiting, elapsed, queued| {
            reporter.resource_snapshot("scheduler",serde_json::json!({"queue_occupancy":queued,"workers_limit":config.resources.workers}));
            let id = ProgressTaskId::new(unit.job.job_id.0, 0);
            reporter.stream_phase(
                id.job_id,
                &unit.key,
                if waiting {
                    "resource admission wait"
                } else {
                    "admitted"
                },
            );
            reporter.resource_wait(id, &unit.key, elapsed);
        },
    )?;

    // -------------------------------------------------------------------------
    // Group results by their originating jobs
    // -------------------------------------------------------------------------

    let mut grouped: Vec<Vec<(usize, TaskExecution)>> =
        (0..jobs.len()).map(|_| Vec::new()).collect();

    for execution in executions {
        let (job_index, results) = execution?;
        grouped[job_index].extend(results);
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

    Ok(ProcessingSessionTasks {
        jobs,
        total_tasks,
        _retained_reservation: retained_reservation,
        budget,
        finalization_memory,
    })
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

/// Preserve definitive earlier job results if a later session operation errors.
pub fn execute_processing_session_reporting(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
    on_result: &mut dyn FnMut(&ProcessingResult),
) -> Result<ProcessingSessionResult> {
    execute_processing_session_inner(jobs, config, &ProgressReporter::disabled(), on_result)
}

impl ProcessingSessionTasks {
    pub(crate) fn into_report_ownership(
        self,
        finalization: crate::shared::historical::budget::OwnedResourceReservation,
    ) -> (Vec<JobTaskResults>, ReportReservations) {
        (self.jobs, (self._retained_reservation, finalization))
    }
    pub(crate) fn reserve_finalization(
        &self,
    ) -> Result<crate::shared::historical::budget::OwnedResourceReservation> {
        self.budget
            .acquire_owned(super::resources::ResourceRequirements {
                memory_bytes: self.finalization_memory,
                scratch_bytes: 0,
                open_files: 0,
            })
    }
}
