use super::ProcessingStatus;
use super::failure::ProcessingFailureReport;
use super::progress::ProgressReporter;
use super::{
    ProcessingErrorCode, ProcessingResult,
    config::{ProcessingConfig, validate_processing_config},
    manifest::{DATASET_MANIFEST_FILENAME, DatasetManifest},
    metrics::{IntegrityEvaluation, ProcessingMetricsReport, evaluate_integrity_policy},
};
use super::{resources::estimate_task_resources_with_limits, task::TaskExecution};
use crate::{
    historical::job::{
        ContentType, ProcessingJob, ProcessingOperation, validate_processing_job_structure,
    },
    shared::error::{MarketForgeError, Result},
    shared::normalization::TargetSchema,
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
    // A direct job may return a processing error before failure.json is written.
    // Keep both retained observations and its drain workspace admitted through
    // that final consumer, rather than releasing them with process_tasks.
    _report_reservations: Option<super::session::ReportReservations>,
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
    let diagnostics = super::progress::diagnostics::Diagnostics::start(
        config,
        job.output.staging_path.parent().unwrap_or(Path::new(".")),
        reporter,
    );
    reporter.stream_phase(job.job_id.0, "", "planning/admission");
    reporter.record_workload(job);
    validate_processing_job_structure(job)?;
    validate_processing_config(config)?;

    let result = match job.operation {
        ProcessingOperation::Process => execute_process(job, config, reporter),
    };

    reporter.emit(super::progress::ProgressEvent::JobFinished {
        job_id: job.job_id.0,
        success: result
            .as_ref()
            .is_ok_and(|r| r.status == ProcessingStatus::Complete),
    });
    match &result {
        Ok(processing_result) => {
            reporter.job_outcome(processing_result);
            reporter.finished(processing_result.status == ProcessingStatus::Complete);
        }
        Err(_) => reporter.finished(false),
    }

    if let Some(diagnostics) = diagnostics {
        diagnostics.finish(
            result
                .as_ref()
                .is_ok_and(|r| r.status == ProcessingStatus::Complete),
        );
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

    prepare_staging(job, config)?;

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
    mut context: ExecutionContext,
    error: MarketForgeError,
) -> Result<ProcessingResult> {
    fn scratch_limit(error: &MarketForgeError) -> bool {
        match error {
            MarketForgeError::ResourceLimit(e) => e.resource == "scratch bytes",
            MarketForgeError::StreamFailure { source, .. }
            | MarketForgeError::ReconciliationFailure { source, .. } => scratch_limit(source),
            MarketForgeError::ReportPersistence { processing, .. } => scratch_limit(processing),
            _ => false,
        }
    }
    fn primary(error: &MarketForgeError) -> &MarketForgeError {
        match error {
            MarketForgeError::ReportPersistence { processing, .. } => primary(processing),
            other => other,
        }
    }
    let error_code = match primary(&error) {
        error if scratch_limit(error) => ProcessingErrorCode::ScratchBudgetExceeded,
        MarketForgeError::SourceOpen { source, .. }
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            ProcessingErrorCode::MissingSource
        }
        MarketForgeError::ArchiveMemberNotFound { .. } => ProcessingErrorCode::MissingSource,
        MarketForgeError::SourceRead { .. }
        | MarketForgeError::SourceOpen { .. }
        | MarketForgeError::ArchiveOpen { .. }
        | MarketForgeError::AmbiguousArchiveMembers { .. } => ProcessingErrorCode::CorruptSource,
        _ => ProcessingErrorCode::ProcessingFailure,
    };
    let error_message = error.to_string();
    if let Some(task) = context
        .failed_task_id
        .and_then(|id| job.tasks.iter().find(|t| t.task_id.0 == id))
        && !context
            .report
            .tasks
            .iter()
            .any(|t| t.task_id == task.task_id.0)
    {
        let mut report = super::metrics::TaskMetrics::default().into_report(task);
        report.failure = Some(super::metrics::TaskFailure::new(job, task, &error));
        context.report.add_task(report)?;
    }

    // Staged writer submissions are unpublished on every failed job. Preserve
    // finalized staging counts separately; no failed report claims committed rows.
    for task in &mut context.report.tasks {
        if let Some(source) = task.source.as_mut() {
            source.processing.published_logical_events = 0;
        }
        task.counters.rows_unpublished = task
            .counters
            .rows_unpublished
            .checked_add(task.counters.events_written)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "failed publication row count overflow".into(),
                )
            })?;
        task.counters.events_written = 0;
    }
    context.report.counters.rows_unpublished = context
        .report
        .counters
        .rows_unpublished
        .checked_add(context.report.counters.events_written)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("failed publication row count overflow".into())
        })?;
    context.report.counters.events_written = 0;
    let failure = ProcessingFailureReport::new(
        job,
        context.failed_task_id,
        error_code,
        &error_message,
        context.report,
        context.integrity_evaluation,
    );

    let mut result =
        ProcessingResult::failed(job.job_id, job.dataset_id, error_code, error_message);
    match failure.write_to(&job.output.staging_path) {
        Ok(path) if path.is_file() => {
            result.failure_report_path = Some(path.to_string_lossy().into_owned());
        }
        Ok(_) => {}
        Err(error) => {
            failure.processing_metrics.preserve_detail_evidence();
            if let Some(failure) = &mut result.failure {
                failure.message.push_str(&format!(
                    "; additionally failed to persist failure report: {error}"
                ));
            }
        }
    }
    crate::historical::normalize::progress::log::diagnostic(format_args!(
        "Processing failed: job={}, task={:?}, report={:?}",
        job.job_id.0, context.failed_task_id, result.failure_report_path
    ));
    Ok(result)
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
    // Validate source accessibility before resource estimation.
    // Preserve the responsible task ID if preparation fails.
    for task in &job.tasks {
        if let Err(error) = estimate_task_resources_with_limits(
            task,
            &config.resources.parquet,
            &config.resources.normalization_limits,
        ) {
            context.failed_task_id = Some(task.task_id.0);
            return Err(error);
        }
    }

    let session = super::session::execute_session_tasks_with_progress(
        std::slice::from_ref(job),
        config,
        reporter,
    )?;
    reporter.stream_phase(job.job_id.0, "", "finalization resource admission");
    let finalization = session.reserve_finalization()?;
    let (mut jobs, guards) = session.into_report_ownership(finalization);
    context._report_reservations = Some(guards);
    let executions = jobs.remove(0).executions;

    reporter.finalizing();

    finalize_processing_job_with_progress(job, config, executions, context, reporter)
}

// -----------------------------------------------------------------------------
// Generic job finalization
// -----------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn finalize_processing_job(
    job: &ProcessingJob,
    config: &ProcessingConfig,
    executions: Vec<TaskExecution>,
    context: &mut ExecutionContext,
) -> Result<ProcessingResult> {
    finalize_processing_job_with_progress(
        job,
        config,
        executions,
        context,
        &ProgressReporter::disabled(),
    )
}
pub(crate) fn finalize_processing_job_with_progress(
    job: &ProcessingJob,
    config: &ProcessingConfig,
    executions: Vec<TaskExecution>,
    context: &mut ExecutionContext,
    reporter: &ProgressReporter,
) -> Result<ProcessingResult> {
    reporter.stream_phase(job.job_id.0, "", "finalization and integrity evaluation");
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
    let mut first_skipped = None;

    for (task, mut execution) in job.tasks.iter().zip(executions) {
        let task_id = task.task_id.0;

        if execution.task_id != task_id {
            context.failed_task_id = Some(task_id);

            return Err(MarketForgeError::InvalidConfiguration(format!(
                "job {} task execution mismatch: expected={}, actual={}",
                job.job_id.0, task_id, execution.task_id,
            )));
        }

        // Preserve the first failed task ID before metrics aggregation.
        if execution
            .error
            .as_ref()
            .is_some_and(|e| !matches!(e, MarketForgeError::NotAttempted { .. }))
            && first_error.is_none()
        {
            context.failed_task_id = match execution.error.as_ref() {
                Some(MarketForgeError::StreamFailure { .. }) => None,
                _ => Some(task_id),
            };
        }

        if let Some(source) = execution.metrics.source.as_mut() {
            source.processing.staged_canonical_rows = execution.metrics.counters.events_written;
            if job.output.content_type == crate::historical::job::ContentType::Trades {
                source.processing.published_logical_events =
                    execution.metrics.counters.events_written;
            }
            source.processing.staged_logical_events = source.processing.published_logical_events;
        }
        let mut task_report = execution.metrics.into_report(task);
        task_report.failure = execution
            .error
            .as_ref()
            .map(|e| super::metrics::TaskFailure::new(job, task, e));
        context.report.add_task(task_report)?;

        if let Some(error) = execution.error {
            if matches!(error, MarketForgeError::NotAttempted { .. }) {
                if first_skipped.is_none() {
                    first_skipped = Some(error);
                }
            } else if first_error.is_none() {
                first_error = Some((context.failed_task_id, error));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Reject failed tasks after preserving all metrics
    // -------------------------------------------------------------------------

    if let Some((task_id, error)) = first_error {
        context.failed_task_id = task_id;
        return Err(error);
    }

    context.failed_task_id = None;
    if let Some(error) = first_skipped {
        return Err(error);
    }
    if context.report.counters.rows_unpublished > 0
        || context.report.counters.records_unprocessed > 0
        || context.report.counters.records_unrouted > 0
    {
        return Err(MarketForgeError::InvalidConfiguration(
            "cannot publish incomplete canonical submission or unresolved source records".into(),
        ));
    }
    super::family::validate_selection(job, &context.report)?;

    // -------------------------------------------------------------------------
    // Validate completed-task accounting
    // -------------------------------------------------------------------------

    context.report.counters.validate()?;

    // -------------------------------------------------------------------------
    // Final integrity evaluation
    // -------------------------------------------------------------------------

    let processing_degraded = super::policies::evaluate(
        config,
        job.output.content_type == ContentType::Depth,
        &context.report,
    )?;
    let mut integrity =
        evaluate_integrity_policy(&config.integrity_policy, &context.report.scoped_integrity)?;

    if processing_degraded && integrity.status == super::metrics::IntegrityStatus::Clean {
        integrity.status = super::metrics::IntegrityStatus::Degraded;
    }
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

    let phase_id = super::progress::ProgressTaskId::new(job.job_id.0, 0);
    reporter.stream_phase(phase_id.job_id, "", "manifest validation");
    let manifest = DatasetManifest::from_processing_job_ref(
        job,
        staging,
        &context.report,
        integrity,
        config.resources.normalization_limits,
        serde_json::json!({"reconciliation":config.reconciliation,"timestamp_regression":config.timestamp_regression}),
    )?;

    // -------------------------------------------------------------------------
    // Verify parallel output layout
    // -------------------------------------------------------------------------

    for file in &manifest.files {
        if !file.path.starts_with("tasks/") && !file.path.starts_with("streams/") {
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

    reporter.stream_phase(phase_id.job_id, "", "dataset publication");
    super::manifest::write_publication_checksums(
        staging,
        [
            "manifest.json",
            "processing-job.json",
            "processing-runtime.json",
        ]
        .into_iter()
        .map(std::path::PathBuf::from)
        .chain(
            manifest
                .files
                .iter()
                .map(|file| std::path::PathBuf::from(&file.path)),
        )
        .chain(
            manifest
                .auxiliary_files
                .iter()
                .map(|file| std::path::PathBuf::from(&file.path)),
        ),
    )
    .map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!("publication checksums: {error}"))
    })?;
    commit_dataset(staging, &job.output.dataset_path)?;
    reporter.stream_phase(phase_id.job_id, "", "committed dataset complete");
    reporter.resource_snapshot(
        &format!("job={}:commit", job.job_id.0),
        serde_json::json!({"committed":true,"staged_output_rows":manifest.events_written}),
    );

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

    crate::historical::normalize::progress::log::diagnostic(format_args!(
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
    ));

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

    if dataset_path.symlink_metadata().is_ok() {
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

    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, staging, CWD, dataset_path, RenameFlags::NOREPLACE).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to commit dataset {} -> {}: {error}",
            staging.display(),
            dataset_path.display(),
        ))
    })?;

    fs::File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!("cannot sync dataset parent: {error}"))
        })?;

    Ok(())
}

pub(crate) fn prepare_staging_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create staging parent {}: {error}",
                parent.display(),
            ))
        })?;
    }
    fs::create_dir(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to create fresh staging directory {}: {error}",
            path.display(),
        ))
    })?;

    Ok(())
}

// -----------------------------------------------------------------------------
// Staging preparation
// -----------------------------------------------------------------------------

pub(crate) fn prepare_staging(job: &ProcessingJob, config: &ProcessingConfig) -> Result<()> {
    prepare_staging_path(&job.output.staging_path)?;
    // Commit the resolved, exact configuration alongside the dataset. Includes
    // immutable profile identity/fingerprint without changing canonical schemas.
    let path = job.output.staging_path.join("processing-job.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| MarketForgeError::InvalidConfiguration(format!("{}: {e}", path.display())))?;
    // Runtime configuration is authoritative; the legacy job fields may differ.
    let mut resolved = job.clone();
    resolved.resources = config.resources.clone();
    resolved.integrity_policy = config.integrity_policy.clone();
    let bytes = serde_json::to_vec_pretty(&resolved)
        .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
    use sha2::{Digest, Sha256};
    use std::io::Write;
    let hash: String = super::hex_digest(Sha256::digest(&bytes));
    let mut file = file;
    file.write_all(&bytes)
        .map_err(|e| MarketForgeError::InvalidConfiguration(format!("{}: {e}", path.display())))?;
    std::fs::write(
        job.output.staging_path.join("processing-job.sha256"),
        format!("{hash}  processing-job.json\n"),
    )
    .map_err(|e| MarketForgeError::InvalidConfiguration(format!("provenance checksum: {e}")))?;
    // Execution-only tuning belongs to runtime provenance, never job JSON.
    let runtime = serde_json::to_vec_pretty(config)
        .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
    let runtime_hash: String = super::hex_digest(Sha256::digest(&runtime));
    fs::write(
        job.output.staging_path.join("processing-runtime.json"),
        runtime,
    )
    .map_err(|e| MarketForgeError::InvalidConfiguration(format!("runtime provenance: {e}")))?;
    fs::write(
        job.output.staging_path.join("processing-runtime.sha256"),
        format!("{runtime_hash}  processing-runtime.json\n"),
    )
    .map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!("runtime provenance checksum: {e}"))
    })?;
    Ok(())
}

#[cfg(test)]
mod failure_path_tests {
    use super::*;
    #[test]
    fn unavailable_failure_report_preserves_definitive_failure() {
        use crate::{
            historical::config::normalization_fixture::bybit_spot_trade_job,
            shared::historical::load_processing_config,
        };
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let config = load_processing_config(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/shared/historical/config/fixtures/runtime.toml"),
            &root,
        )
        .unwrap();
        let mut job = bybit_spot_trade_job(&config).unwrap();
        let temp = tempfile::tempdir().unwrap();
        job.output.staging_path = temp.path().join("not-created");
        let result = persist_processing_failure(
            &job,
            ExecutionContext::default(),
            MarketForgeError::InvalidConfiguration("original output failure".into()),
        )
        .unwrap();
        assert_eq!(result.status, ProcessingStatus::Failed);
        assert!(result.manifest_path.is_none() && result.failure_report_path.is_none());
        let failure = result.failure.unwrap();
        assert!(failure.message.contains("original output failure"));
        assert!(failure.message.contains("failed to persist failure report"));
    }
}
