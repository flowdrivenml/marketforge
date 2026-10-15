use super::atomic::atomic_publish_directory;
use super::receipt::ConversionExecutionReceipt;
use super::source_validation::validate_source_preservation_with_progress;
use super::validate_converted_depth_parallel_with_progress;
use super::{
    combined_parallel_validation::validate_converted_combined_parallel_with_progress,
    source_validation::SourcePreservationMode,
};
use super::{manifest::ConversionManifest, validation::validate_converted_trades_with_progress};
use crate::process::convert::progress::{ConversionPhase, ConversionProgressReporter};
use crate::process::merge::UnifiedMergeMode;
use crate::{
    error::{MarketForgeError, Result},
    job::ConvertJob,
    process::merge::PhysicalMergeManifest,
};
use std::path::{Path, PathBuf};
// -----------------------------------------------------------------------------
// Publication result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ConversionPublication {
    pub dataset_path: PathBuf,
    pub manifest_path: PathBuf,
    pub events_written: u64,
    pub files_written: u64,
}

// -----------------------------------------------------------------------------
// Publish validated conversion
// -----------------------------------------------------------------------------

pub fn publish_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging_directory: &Path,
) -> Result<ConversionPublication> {
    publish_conversion_with_progress(
        job,
        source_manifest,
        staging_directory,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn publish_conversion_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging_directory: &Path,
    reporter: &ConversionProgressReporter,
) -> Result<ConversionPublication> {
    job.validate()?;
    source_manifest.validate()?;

    let final_directory = &job.output.dataset_path;

    // -------------------------------------------------------------------------
    // Validate filesystem state
    // -------------------------------------------------------------------------

    if !staging_directory.is_dir() {
        return invalid(format!(
            "conversion staging directory does not exist: {}",
            staging_directory.display()
        ));
    }

    if final_directory.exists() {
        return invalid(format!(
            "converted dataset already exists: {}",
            final_directory.display()
        ));
    }

    if staging_directory == final_directory {
        return invalid("staging and final directories must differ");
    }

    // -------------------------------------------------------------------------
    // Validate conversion output
    // -------------------------------------------------------------------------
    reporter.phase_started(ConversionPhase::PhysicalValidation);
    let validation = validate_converted_trades_with_progress(
        staging_directory,
        job.conversion.max_staleness_ns,
        reporter,
    )?;
    reporter.phase_completed(ConversionPhase::PhysicalValidation);

    reporter.phase_started(ConversionPhase::ReceiptReconciliation);

    let receipt = ConversionExecutionReceipt::load(staging_directory)?;

    receipt.reconcile(job.job_id, &validation)?;

    // -------------------------------------------------------------------------
    // Reconcile source, execution receipt, and physical output
    // -------------------------------------------------------------------------

    if source_manifest.events_written != validation.events {
        return invalid(format!(
            "conversion source/output event count mismatch: \
         source={}, output={}",
            source_manifest.events_written, validation.events,
        ));
    }
    if source_manifest.events_written != receipt.events_read {
        return invalid(format!(
            "conversion source/receipt event count mismatch: \
         source={}, receipt={}",
            source_manifest.events_written, receipt.events_read,
        ));
    }

    if validation.events != source_manifest.events_written {
        return invalid("conversion output event count mismatch");
    }
    reporter.phase_completed(ConversionPhase::ReceiptReconciliation);

    // -------------------------------------------------------------------------
    // Validate source preservation
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::SourcePreservation);

    let preservation = validate_source_preservation_with_progress(
        &job.input_merge,
        source_manifest,
        staging_directory,
        SourcePreservationMode::Full,
        8,
        reporter,
    )?;

    if preservation.events_compared != validation.events {
        return invalid("source preservation event count mismatch");
    }

    reporter.phase_completed(ConversionPhase::SourcePreservation);

    // -------------------------------------------------------------------------
    // Construct authoritative manifest
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::ManifestConstruction);

    let manifest = ConversionManifest::build(
        job,
        source_manifest,
        &validation,
        &preservation,
        receipt.reference_updates,
        staging_directory,
    )?;

    manifest.validate()?;

    reporter.phase_completed(ConversionPhase::ManifestConstruction);

    // -------------------------------------------------------------------------
    // Write manifest into staging
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::PublicationPreparation);

    let staging_manifest = manifest.write(staging_directory)?;

    reporter.phase_completed(ConversionPhase::PublicationPreparation);

    // -------------------------------------------------------------------------
    // Publish directory
    // -------------------------------------------------------------------------
    reporter.phase_started(ConversionPhase::Commit);

    atomic_publish_directory(staging_directory, final_directory)?;

    reporter.phase_completed(ConversionPhase::Commit);

    Ok(ConversionPublication {
        dataset_path: final_directory.clone(),
        manifest_path: final_directory.join(
            staging_manifest
                .file_name()
                .ok_or_else(|| invalid_error("invalid manifest filename"))?,
        ),
        events_written: manifest.statistics.events_written,
        files_written: manifest.output.files_written,
    })
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

pub fn publish_depth_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging_directory: &Path,
    workers: usize,
) -> Result<ConversionPublication> {
    publish_depth_conversion_with_progress(
        job,
        source_manifest,
        staging_directory,
        workers,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn publish_depth_conversion_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging_directory: &Path,
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<ConversionPublication> {
    job.validate()?;
    source_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Depth {
        return invalid("depth publication requires depth-only source");
    }

    if workers == 0 {
        return invalid("depth publication requires positive worker count");
    }

    let final_directory = &job.output.dataset_path;

    if !staging_directory.is_dir() {
        return invalid("depth conversion staging directory does not exist");
    }

    if final_directory.exists() {
        return invalid("converted depth dataset already exists");
    }

    if staging_directory == final_directory {
        return invalid("staging and final directories must differ");
    }
    reporter.phase_started(ConversionPhase::PhysicalValidation);
    let validation = validate_converted_depth_parallel_with_progress(
        staging_directory,
        job.conversion.max_staleness_ns,
        workers,
        reporter,
    )?;
    reporter.phase_completed(ConversionPhase::PhysicalValidation);
    reporter.phase_started(ConversionPhase::ReceiptReconciliation);
    // Reconcile materialization receipt.
    let receipt = ConversionExecutionReceipt::load(staging_directory)?;
    receipt.reconcile_depth(job.job_id, &validation)?;

    if source_manifest.events_written != validation.events
        || source_manifest.events_written != receipt.events_read
    {
        return invalid("depth source/output event count mismatch");
    }
    reporter.phase_completed(ConversionPhase::ReceiptReconciliation);

    // Verify original source columns remain unchanged.
    reporter.phase_started(ConversionPhase::SourcePreservation);

    let preservation = validate_source_preservation_with_progress(
        &job.input_merge,
        source_manifest,
        staging_directory,
        SourcePreservationMode::Full,
        7,
        reporter,
    )?;

    if preservation.events_compared != validation.events {
        return invalid("depth source preservation count mismatch");
    }

    reporter.phase_completed(ConversionPhase::SourcePreservation);

    // Construct authoritative depth manifest.
    reporter.phase_started(ConversionPhase::ManifestConstruction);

    let manifest = ConversionManifest::build_depth(
        job,
        source_manifest,
        &validation,
        &preservation,
        receipt.reference_updates,
        staging_directory,
    )?;

    manifest.validate()?;

    reporter.phase_completed(ConversionPhase::ManifestConstruction);

    // Write manifest into staging before publication.
    reporter.phase_started(ConversionPhase::PublicationPreparation);

    let staging_manifest = manifest.write(staging_directory)?;

    reporter.phase_completed(ConversionPhase::PublicationPreparation);

    // Atomic no-overwrite publication.
    reporter.phase_started(ConversionPhase::Commit);

    atomic_publish_directory(staging_directory, final_directory)?;

    reporter.phase_completed(ConversionPhase::Commit);

    Ok(ConversionPublication {
        dataset_path: final_directory.clone(),
        manifest_path: final_directory.join(
            staging_manifest
                .file_name()
                .ok_or_else(|| invalid_error("invalid manifest filename"))?,
        ),
        events_written: manifest.statistics.events_written,
        files_written: manifest.output.files_written,
    })
}

pub fn publish_combined_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging_directory: &Path,
    workers: usize,
) -> Result<ConversionPublication> {
    publish_combined_conversion_with_progress(
        job,
        source_manifest,
        staging_directory,
        workers,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn publish_combined_conversion_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    staging_directory: &Path,
    workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<ConversionPublication> {
    job.validate()?;
    source_manifest.validate()?;

    // -------------------------------------------------------------------------
    // Validate source mode and resources
    // -------------------------------------------------------------------------

    if source_manifest.mode != UnifiedMergeMode::Combined {
        return invalid("combined publication requires combined source");
    }

    if workers == 0 {
        return invalid("combined publication requires positive worker count");
    }

    let final_directory = &job.output.dataset_path;

    // -------------------------------------------------------------------------
    // Validate filesystem state
    // -------------------------------------------------------------------------

    if !staging_directory.is_dir() {
        return invalid(format!(
            "combined conversion staging directory does not exist: {}",
            staging_directory.display()
        ));
    }

    if final_directory.exists() {
        return invalid(format!(
            "converted combined dataset already exists: {}",
            final_directory.display()
        ));
    }

    if staging_directory == final_directory {
        return invalid("staging and final directories must differ");
    }

    // -------------------------------------------------------------------------
    // Parallel physical validation
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::PhysicalValidation);

    let validation = validate_converted_combined_parallel_with_progress(
        staging_directory,
        job.conversion.max_staleness_ns,
        workers,
        reporter,
    )?;

    reporter.phase_completed(ConversionPhase::PhysicalValidation);

    reporter.phase_started(ConversionPhase::ReceiptReconciliation);

    let receipt = ConversionExecutionReceipt::load(staging_directory)?;

    receipt.reconcile_combined(job.job_id, &validation)?;

    if receipt.events_read != source_manifest.events_written
        || receipt.events_written != source_manifest.events_written
    {
        return invalid("combined source/receipt event count mismatch");
    }

    if validation.events != source_manifest.events_written
        || validation.trade_events != source_manifest.trade_events
        || validation.depth_events != source_manifest.depth_events
    {
        return invalid("combined source/output event count mismatch");
    }
    reporter.phase_completed(ConversionPhase::ReceiptReconciliation);
    // -------------------------------------------------------------------------
    // Exact source preservation
    // -------------------------------------------------------------------------
    reporter.phase_started(ConversionPhase::SourcePreservation);

    let preservation = validate_source_preservation_with_progress(
        &job.input_merge,
        source_manifest,
        staging_directory,
        SourcePreservationMode::Full,
        8,
        reporter,
    )?;

    if preservation.events_compared != validation.events {
        return invalid("combined source preservation count mismatch");
    }

    reporter.phase_completed(ConversionPhase::SourcePreservation);

    // -------------------------------------------------------------------------
    // Construct authoritative combined manifest
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::ManifestConstruction);

    let manifest = ConversionManifest::build_combined(
        job,
        source_manifest,
        &validation,
        &preservation,
        receipt.reference_updates,
        staging_directory,
    )?;

    manifest.validate()?;

    reporter.phase_completed(ConversionPhase::ManifestConstruction);

    // -------------------------------------------------------------------------
    // Write manifest into staging
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::PublicationPreparation);

    let staging_manifest = manifest.write(staging_directory)?;

    reporter.phase_completed(ConversionPhase::PublicationPreparation);

    // -------------------------------------------------------------------------
    // Atomic no-overwrite publication
    // -------------------------------------------------------------------------

    reporter.phase_started(ConversionPhase::Commit);

    atomic_publish_directory(staging_directory, final_directory)?;

    reporter.phase_completed(ConversionPhase::Commit);

    Ok(ConversionPublication {
        dataset_path: final_directory.clone(),

        manifest_path: final_directory.join(
            staging_manifest
                .file_name()
                .ok_or_else(|| invalid_error("invalid manifest filename"))?,
        ),

        events_written: manifest.statistics.events_written,
        files_written: manifest.output.files_written,
    })
}
