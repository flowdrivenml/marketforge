use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;

use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::{
    historical::job::MergeParquetConfig,
    shared::error::{MarketForgeError, Result},
};

use {
    crate::historical::merge::materialize::builders::schema::physical_combined_schema,
    crate::historical::merge::materialize::builders::schema::physical_depth_schema,
    crate::historical::merge::materialize::builders::schema::physical_trade_schema,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeMode,
};

// -----------------------------------------------------------------------------
// Metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalWriterMetrics {
    pub events_written: u64,
    pub trade_events: u64,
    pub depth_events: u64,
    pub canonical_rows: u64,

    pub files_written: u64,
    pub row_groups_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Physical Parquet writer
// -----------------------------------------------------------------------------

pub struct PhysicalParquetWriter {
    output_dir: PathBuf,

    mode: UnifiedMergeMode,
    schema: SchemaRef,

    row_group_target_bytes: u64,
    file_target_bytes: u64,
    compression_level: i32,

    writer: Option<ArrowWriter<File>>,

    pending_bytes: u64,
    next_file_index: u64,

    metrics: PhysicalWriterMetrics,

    finished: bool,
    failed: bool,
}

impl PhysicalParquetWriter {
    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    pub fn new(
        output_dir: impl AsRef<Path>,
        mode: UnifiedMergeMode,
        config: &MergeParquetConfig,
    ) -> Result<Self> {
        let schema = match mode {
            UnifiedMergeMode::Trades => physical_trade_schema(),
            UnifiedMergeMode::Depth => physical_depth_schema(),
            UnifiedMergeMode::Combined => physical_combined_schema(),
        };

        Self::with_schema(output_dir, mode, schema, config)
    }

    pub fn with_schema(
        output_dir: impl AsRef<Path>,
        mode: UnifiedMergeMode,
        schema: SchemaRef,
        config: &MergeParquetConfig,
    ) -> Result<Self> {
        let row_group_target_bytes = config
            .row_group_target_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| invalid_error("row-group target overflow"))?;

        let file_target_bytes = config
            .file_target_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| invalid_error("file target overflow"))?;

        if row_group_target_bytes == 0 {
            return invalid("row-group target must be positive");
        }

        if file_target_bytes < row_group_target_bytes {
            return invalid("file target cannot be smaller than row-group target");
        }

        ZstdLevel::try_new(config.compression_level)
            .map_err(|error| invalid_error(format!("invalid ZSTD compression level: {error}")))?;

        let output_dir = output_dir.as_ref().to_path_buf();

        fs::create_dir_all(&output_dir).map_err(|error| {
            invalid_error(format!(
                "cannot create physical output directory {}: {error}",
                output_dir.display()
            ))
        })?;

        Ok(Self {
            output_dir,
            mode,
            schema,
            row_group_target_bytes,
            file_target_bytes,
            compression_level: config.compression_level,
            writer: None,
            pending_bytes: 0,
            next_file_index: 0,
            metrics: PhysicalWriterMetrics::default(),
            finished: false,
            failed: false,
        })
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    pub fn metrics(&self) -> &PhysicalWriterMetrics {
        &self.metrics
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    // -------------------------------------------------------------------------
    // State validation
    // -------------------------------------------------------------------------

    fn ensure_writable(&self) -> Result<()> {
        if self.failed {
            return invalid("physical writer is in a failed state");
        }

        if self.finished {
            return invalid("physical writer has already been finalized");
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Open output file
    // -------------------------------------------------------------------------

    fn open_file(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        let path = self
            .output_dir
            .join(format!("part-{:06}.parquet", self.next_file_index));

        if path.exists() {
            return invalid(format!(
                "physical output file already exists: {}",
                path.display()
            ));
        }

        let file = File::create(&path).map_err(|error| {
            invalid_error(format!(
                "cannot create physical Parquet file {}: {error}",
                path.display()
            ))
        })?;

        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(self.compression_level).map_err(|error| {
                    invalid_error(format!("invalid compression level: {error}"))
                })?,
            ))
            .build();

        let writer = ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(properties))
            .map_err(|error| {
                invalid_error(format!(
                    "cannot initialize physical Parquet writer: {error}"
                ))
            })?;

        self.writer = Some(writer);

        self.pending_bytes = 0;

        self.next_file_index = self
            .next_file_index
            .checked_add(1)
            .ok_or_else(|| invalid_error("file index overflow"))?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Write physical batch
    // -------------------------------------------------------------------------

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_writable()?;

        let result = self.write_batch_inner(batch);

        if result.is_err() {
            self.failed = true;
        }

        result
    }

    fn write_batch_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if batch.schema().as_ref() != self.schema.as_ref() {
            return invalid("physical Arrow schema mismatch");
        }

        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| invalid_error("missing physical event timestamp"))?;

        // Validate chronological ordering, including batch boundaries.
        let mut previous = self.metrics.end_timestamp_ns;

        for row in 0..timestamps.len() {
            if timestamps.is_null(row) {
                return invalid("null physical event timestamp");
            }

            let timestamp = timestamps.value(row);

            if let Some(last) = previous {
                if timestamp < last {
                    return invalid(format!(
                        "physical event timestamp regression: \
                         previous={}, current={}",
                        last, timestamp
                    ));
                }
            }

            previous = Some(timestamp);
        }

        let first_timestamp = timestamps.value(0);
        let last_timestamp = timestamps.value(timestamps.len() - 1);

        // Count event types before writing.
        let (trade_count, depth_count) = count_event_kinds(batch, self.mode)?;

        let canonical_rows = batch
            .column_by_name("canonical_row_count")
            .and_then(|column| column.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| invalid_error("missing canonical row count"))?;

        let mut canonical_count = 0_u64;

        for row in 0..canonical_rows.len() {
            if canonical_rows.is_null(row) {
                return invalid("null canonical row count");
            }

            canonical_count = canonical_count
                .checked_add(canonical_rows.value(row))
                .ok_or_else(|| invalid_error("canonical row-count overflow"))?;
        }

        let batch_events =
            u64::try_from(batch.num_rows()).map_err(|_| invalid_error("event count overflow"))?;

        // Preflight metric overflow before performing I/O.
        let next_events = self
            .metrics
            .events_written
            .checked_add(batch_events)
            .ok_or_else(|| invalid_error("event counter overflow"))?;

        let next_trades = self
            .metrics
            .trade_events
            .checked_add(trade_count)
            .ok_or_else(|| invalid_error("trade counter overflow"))?;

        let next_depth = self
            .metrics
            .depth_events
            .checked_add(depth_count)
            .ok_or_else(|| invalid_error("depth counter overflow"))?;

        let next_canonical = self
            .metrics
            .canonical_rows
            .checked_add(canonical_count)
            .ok_or_else(|| invalid_error("canonical counter overflow"))?;

        let estimated_bytes = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| invalid_error("batch memory overflow"))?;

        let next_pending = self
            .pending_bytes
            .checked_add(estimated_bytes)
            .ok_or_else(|| invalid_error("pending byte overflow"))?;

        self.open_file()?;

        let writer = self.writer.as_mut().expect("physical writer initialized");

        writer.write(batch).map_err(|error| {
            invalid_error(format!("failed to write physical Arrow batch: {error}"))
        })?;

        self.metrics.events_written = next_events;
        self.metrics.trade_events = next_trades;
        self.metrics.depth_events = next_depth;
        self.metrics.canonical_rows = next_canonical;

        self.metrics.start_timestamp_ns =
            Some(self.metrics.start_timestamp_ns.unwrap_or(first_timestamp));

        self.metrics.end_timestamp_ns = Some(last_timestamp);

        self.pending_bytes = next_pending;

        if self.pending_bytes >= self.row_group_target_bytes {
            self.flush_row_group()?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Flush Parquet row group
    // -------------------------------------------------------------------------

    fn flush_row_group(&mut self) -> Result<()> {
        if self.pending_bytes == 0 {
            return Ok(());
        }

        let Some(writer) = self.writer.as_mut() else {
            return invalid("cannot flush unopened physical writer");
        };

        writer.flush().map_err(|error| {
            invalid_error(format!("failed to flush physical row group: {error}"))
        })?;

        self.metrics.row_groups_written = self
            .metrics
            .row_groups_written
            .checked_add(1)
            .ok_or_else(|| invalid_error("row-group counter overflow"))?;

        self.pending_bytes = 0;

        if writer.bytes_written() as u64 >= self.file_target_bytes {
            self.close_file()?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Close Parquet file
    // -------------------------------------------------------------------------

    fn close_file(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        writer.close().map_err(|error| {
            invalid_error(format!("failed to finalize physical Parquet file: {error}"))
        })?;

        self.metrics.files_written = self
            .metrics
            .files_written
            .checked_add(1)
            .ok_or_else(|| invalid_error("file counter overflow"))?;

        self.pending_bytes = 0;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Finalization
    // -------------------------------------------------------------------------

    pub fn finish(&mut self) -> Result<()> {
        if self.failed {
            return invalid("cannot finalize failed physical writer");
        }

        if self.finished {
            return Ok(());
        }

        let result = self.finish_inner();

        if result.is_err() {
            self.failed = true;
        }

        result
    }

    fn finish_inner(&mut self) -> Result<()> {
        self.flush_row_group()?;
        self.close_file()?;

        self.finished = true;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Event-kind accounting
// -----------------------------------------------------------------------------

fn count_event_kinds(batch: &RecordBatch, mode: UnifiedMergeMode) -> Result<(u64, u64)> {
    let rows =
        u64::try_from(batch.num_rows()).map_err(|_| invalid_error("batch row count overflow"))?;

    match mode {
        UnifiedMergeMode::Trades => Ok((rows, 0)),

        UnifiedMergeMode::Depth => Ok((0, rows)),

        UnifiedMergeMode::Combined => {
            use arrow_array::UInt8Array;

            let kinds = batch
                .column_by_name("event_kind")
                .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
                .ok_or_else(|| invalid_error("missing combined event kind"))?;

            let mut trades = 0_u64;
            let mut depth = 0_u64;

            for row in 0..kinds.len() {
                if kinds.is_null(row) {
                    return invalid("null combined event kind");
                }

                match kinds.value(row) {
                    0 => trades += 1,
                    1 => depth += 1,
                    _ => return invalid("unsupported combined event kind"),
                }
            }

            Ok((trades, depth))
        }
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
