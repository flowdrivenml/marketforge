use crate::historical::convert::progress::{
    ConversionPartitionKey, ConversionPhase, ConversionProgress, ConversionProgressReporter,
    ConversionWorkerProgress,
};
use std::path::Path;

use {
    crate::historical::convert::ConversionPlan,
    crate::historical::convert::output::ConvertedParquetWriter,
    crate::historical::convert::output::ConvertedTradeBatchBuilder,
    crate::historical::job::ConvertJob, crate::historical::job::MergeParquetConfig,
    crate::historical::merge::PhysicalMergeManifest, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

use crate::historical::convert::execution::{
    CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader, TradeConversionKernel,
};

// -----------------------------------------------------------------------------
// Metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct PhysicalConversionMetrics {
    pub events_read: u64,
    pub events_written: u64,

    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,
    pub failures: [u64; 6],

    pub reference_updates: u64,

    pub files_written: u64,
    pub row_groups_written: u64,
}

// -----------------------------------------------------------------------------
// Physical conversion
// -----------------------------------------------------------------------------

pub fn materialize_trade_conversion(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
) -> Result<PhysicalConversionMetrics> {
    materialize_trade_conversion_with_progress(
        job,
        manifest,
        plan,
        staging_directory,
        parquet_config,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn materialize_trade_conversion_with_progress(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
    reporter: &ConversionProgressReporter,
) -> Result<PhysicalConversionMetrics> {
    let key = ConversionPartitionKey { partition_id: 0 };
    let mut partition_started = false;
    let result = (|| {
        job.validate()?;
        manifest.validate()?;

        if !matches!(
            manifest.mode,
            crate::historical::merge::UnifiedMergeMode::Trades
        ) {
            return invalid("physical trade conversion requires trades merge");
        }

        if staging_directory.exists() {
            return invalid(format!(
                "conversion staging directory already exists: {}",
                staging_directory.display()
            ));
        }

        let mut reader =
            PhysicalConversionReader::new(&job.input_merge, manifest, CONVERSION_READ_BATCH_SIZE)?;

        let kernel = TradeConversionKernel::new(job, plan)?;

        let causal =
            manifest.timeline_ordering == crate::historical::job::MergeTimelineOrdering::Causal;
        if causal {
            reporter.phase_started(ConversionPhase::TimestampScan);
        }
        let mut state = super::super::shared::reader::initial_reference_state_with_scan_progress(
            &job.input_merge,
            manifest,
            reporter,
        )?;
        reporter.check_interrupted()?;
        if causal {
            reporter.phase_completed(ConversionPhase::TimestampScan);
        }
        reporter.phase_started(ConversionPhase::PhysicalConversion);
        reporter.partitions_planned([(key, manifest.events_written)], 1);
        reporter.partition_started(key);
        partition_started = true;
        let mut progress = ConversionWorkerProgress::new(reporter, key);
        let mut metrics = PhysicalConversionMetrics::default();

        let mut builder = None;
        let mut writer = None;

        while let Some(batch) = reader.next_batch()? {
            progress.check_interrupted()?;
            if builder.is_none() {
                let trade_builder = ConvertedTradeBatchBuilder::new(batch.schema())?;

                let parquet_writer = ConvertedParquetWriter::new(
                    staging_directory,
                    trade_builder.output_schema(),
                    parquet_config,
                )?;

                builder = Some(trade_builder);
                writer = Some(parquet_writer);
            }

            // -------------------------------------------------------------
            // Shared conversion logic
            // -------------------------------------------------------------

            let rows = kernel.convert_batch(&batch, &mut state, &mut metrics)?;

            // -------------------------------------------------------------
            // Construct converted Arrow batch
            // -------------------------------------------------------------

            let output = builder
                .as_ref()
                .expect("conversion builder initialized")
                .build(&batch, &rows)?;

            // -------------------------------------------------------------
            // Write converted Parquet
            // -------------------------------------------------------------

            writer
                .as_mut()
                .expect("conversion writer initialized")
                .write_batch(&output)?;
            let physical = writer
                .as_ref()
                .expect("conversion writer initialized")
                .metrics();
            metrics.events_written = physical.events_written;
            metrics.files_written = physical.files_written;
            metrics.row_groups_written = physical.row_groups_written;
            progress.maybe_update(&metrics.progress());
        }

        if let Some(writer) = writer.as_mut() {
            writer.finish()?;

            metrics.events_written = writer.metrics().events_written;

            metrics.files_written = writer.metrics().files_written;

            metrics.row_groups_written = writer.metrics().row_groups_written;
        }

        if metrics.events_read != manifest.events_written
            || metrics.events_written != manifest.events_written
        {
            return invalid("physical conversion event count mismatch");
        }

        let accounted = metrics
            .identity_events
            .checked_add(metrics.converted_events)
            .and_then(|value| value.checked_add(metrics.unavailable_events))
            .ok_or_else(|| invalid_error("conversion status counter overflow"))?;

        if accounted != metrics.events_written {
            return invalid("physical conversion status accounting mismatch");
        }

        reporter.check_interrupted()?;
        Ok(metrics)
    })();
    match &result {
        Ok(metrics) => {
            reporter.partition_completed(key, metrics.progress());
            reporter.materialization_completed(metrics.progress());
        }
        Err(error) if partition_started => reporter.partition_failed(key, error.to_string()),
        Err(_) => {}
    }
    result
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

impl PhysicalConversionMetrics {
    pub fn progress(&self) -> ConversionProgress {
        ConversionProgress {
            events_read: self.events_read,
            events_written: self.events_written,

            identity_events: self.identity_events,
            converted_events: self.converted_events,
            unavailable_events: self.unavailable_events,
            failures: self.failures,

            trade_events: self.events_read,

            internal_reference_updates: self.reference_updates,

            files_written: self.files_written,
            row_groups_written: self.row_groups_written,

            ..ConversionProgress::default()
        }
    }
}
