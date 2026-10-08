use std::{fs, path::Path};

use crate::{
    error::{MarketForgeError, Result},
    job::{
        ContentType, ProcessingJob, ProcessingOperation, TargetSchema,
        validate_processing_job_structure,
    },
};

use super::{
    ProcessingErrorCode, ProcessingResult,
    config::{ProcessingConfig, validate_resources},
    manifest::{DATASET_MANIFEST_FILENAME, DatasetManifest},
    parquet::ParquetTradeWriter,
    worker::{TradeSink, process_trade_task},
};

// -----------------------------------------------------------------------------
// Processing job execution
// -----------------------------------------------------------------------------

pub fn execute_processing_job(
    job: &ProcessingJob,
    config: &ProcessingConfig,
) -> Result<ProcessingResult> {
    validate_processing_job_structure(job)?;
    validate_resources(&config.resources)?;

    match job.operation {
        ProcessingOperation::Process => execute_process(job, config),

        ProcessingOperation::Merge => Ok(ProcessingResult::failed(
            job.job_id,
            job.dataset_id,
            ProcessingErrorCode::ProcessingFailure,
            "merge execution is not implemented yet",
        )),
    }
}

// -----------------------------------------------------------------------------
// Raw archive processing
// -----------------------------------------------------------------------------

fn execute_process(job: &ProcessingJob, config: &ProcessingConfig) -> Result<ProcessingResult> {
    if job.output.content_type != ContentType::Trades {
        return Ok(ProcessingResult::failed(
            job.job_id,
            job.dataset_id,
            ProcessingErrorCode::ProcessingFailure,
            "only trade processing is currently implemented",
        ));
    }

    // Reject unsupported task configurations before creating staging.
    for task in &job.tasks {
        let trade_normalizations = task
            .normalizations
            .iter()
            .filter(|normalization| normalization.target_schema == TargetSchema::Trade)
            .count();

        if trade_normalizations != 1 || task.normalizations.len() != 1 {
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

    // Never overwrite an existing dataset.
    if job.output.dataset_path.exists() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "final dataset path already exists: {}",
            job.output.dataset_path.display()
        )));
    }

    prepare_staging(job)?;

    // Processing failures preserve staging for debugging.
    let result = process_trade_tasks(job, config);

    match result {
        Ok(result) => Ok(result),

        Err(error) => Ok(ProcessingResult::failed(
            job.job_id,
            job.dataset_id,
            ProcessingErrorCode::ProcessingFailure,
            error.to_string(),
        )),
    }
}

// -----------------------------------------------------------------------------
// Multi-task processing and commit
// -----------------------------------------------------------------------------

fn process_trade_tasks(job: &ProcessingJob, config: &ProcessingConfig) -> Result<ProcessingResult> {
    let staging = &job.output.staging_path;
    let output_dir = staging.join("trades");

    let mut writer = ParquetTradeWriter::new(&output_dir, config.resources.parquet.clone())?;

    let mut records_read = 0u64;
    let mut records_matched = 0u64;
    let mut records_skipped_instrument = 0u64;

    // -------------------------------------------------------------
    // Process every source archive
    // -------------------------------------------------------------

    for task in &job.tasks {
        let metrics = process_trade_task(task, &mut writer)?;

        records_read = records_read
            .checked_add(metrics.records_read)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("records_read counter overflow".to_owned())
            })?;

        records_matched = records_matched
            .checked_add(metrics.records_matched)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "records_matched counter overflow".to_owned(),
                )
            })?;

        records_skipped_instrument = records_skipped_instrument
            .checked_add(metrics.records_skipped_instrument)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "records_skipped_instrument counter overflow".to_owned(),
                )
            })?;
    }

    // -------------------------------------------------------------
    // Finalize Parquet output
    // -------------------------------------------------------------

    writer.finish()?;

    if writer.is_failed() || !writer.is_finished() {
        return Err(MarketForgeError::InvalidConfiguration(
            "Parquet writer did not finalize successfully".to_owned(),
        ));
    }

    let writer_metrics = writer.metrics().clone();

    if writer_metrics.trades_written != records_matched {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "trade count mismatch: matched={}, written={}",
            records_matched, writer_metrics.trades_written,
        )));
    }

    // Release file handles before verification and commit.
    drop(writer);

    // -------------------------------------------------------------
    // Inspect output and build dataset manifest
    // -------------------------------------------------------------

    let manifest = DatasetManifest::from_processing_job(job, staging)?;

    verify_manifest(&manifest, &writer_metrics)?;

    // -------------------------------------------------------------
    // Write manifest into staging
    // -------------------------------------------------------------

    manifest.write_to(staging)?;

    let staged_manifest = staging.join(DATASET_MANIFEST_FILENAME);

    if !staged_manifest.is_file() {
        return Err(MarketForgeError::InvalidConfiguration(
            "dataset manifest was not created".to_owned(),
        ));
    }

    // -------------------------------------------------------------
    // Commit dataset
    // -------------------------------------------------------------

    commit_dataset(staging, &job.output.dataset_path)?;

    let committed_manifest = job.output.dataset_path.join(DATASET_MANIFEST_FILENAME);

    // -------------------------------------------------------------
    // Construct successful result
    // -------------------------------------------------------------

    let mut result = ProcessingResult::complete(job.job_id, job.dataset_id);

    result.manifest_path = Some(committed_manifest.to_string_lossy().into_owned());

    result.events_written = manifest.events_written;
    result.files_written = manifest.files_written;

    result.start_timestamp_ns = manifest.start_timestamp_ns;
    result.end_timestamp_ns = manifest.end_timestamp_ns;

    eprintln!(
        "Processing committed: records_read={}, matched={}, skipped={}, written={}, files={}, dataset={}",
        records_read,
        records_matched,
        records_skipped_instrument,
        result.events_written,
        result.files_written,
        job.output.dataset_path.display(),
    );

    Ok(result)
}

// -----------------------------------------------------------------------------
// Manifest verification
// -----------------------------------------------------------------------------

fn verify_manifest(
    manifest: &DatasetManifest,
    writer_metrics: &super::parquet::ParquetWriterMetrics,
) -> Result<()> {
    if manifest.events_written != writer_metrics.trades_written {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "manifest trade count mismatch: manifest={}, writer={}",
            manifest.events_written, writer_metrics.trades_written,
        )));
    }

    if manifest.files_written != writer_metrics.files_written {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "manifest file count mismatch: manifest={}, writer={}",
            manifest.files_written, writer_metrics.files_written,
        )));
    }

    if manifest.start_timestamp_ns != writer_metrics.start_timestamp_ns {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "manifest minimum timestamp mismatch: manifest={:?}, writer={:?}",
            manifest.start_timestamp_ns, writer_metrics.start_timestamp_ns,
        )));
    }

    if manifest.end_timestamp_ns != writer_metrics.end_timestamp_ns {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "manifest maximum timestamp mismatch: manifest={:?}, writer={:?}",
            manifest.end_timestamp_ns, writer_metrics.end_timestamp_ns,
        )));
    }

    if manifest.files_written == 0 {
        return Err(MarketForgeError::InvalidConfiguration(
            "cannot commit dataset without Parquet files".to_owned(),
        ));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Transactional dataset commit
// -----------------------------------------------------------------------------

fn commit_dataset(staging: &Path, dataset_path: &Path) -> Result<()> {
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

// -----------------------------------------------------------------------------
// Staging preparation
// -----------------------------------------------------------------------------

fn prepare_staging(job: &ProcessingJob) -> Result<()> {
    let path = &job.output.staging_path;

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
