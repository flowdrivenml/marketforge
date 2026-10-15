use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::{
    error::{MarketForgeError, Result},
    job::MergeParquetConfig,
};

use super::timeline::{TimelineBatchBuilder, TimelineRecord, timeline_schema};

// -----------------------------------------------------------------------------
// Writer metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimelineWriterMetrics {
    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Timeline writer
// -----------------------------------------------------------------------------

pub struct TimelineWriter {
    output_dir: PathBuf,

    row_group_target_bytes: u64,
    file_target_bytes: u64,
    compression_level: i32,

    batch: TimelineBatchBuilder,
    batch_bytes: u64,

    writer: Option<ArrowWriter<File>>,
    current_file_bytes: u64,
    next_file_index: u64,

    metrics: TimelineWriterMetrics,

    finished: bool,
    failed: bool,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl TimelineWriter {
    pub fn new(output_dir: impl AsRef<Path>, config: &MergeParquetConfig) -> Result<Self> {
        let row_group_target_bytes = config
            .row_group_target_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| invalid_error("row-group target overflow"))?;

        let file_target_bytes = config
            .file_target_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| invalid_error("file target overflow"))?;

        if row_group_target_bytes == 0 {
            return invalid("row-group target must be greater than zero");
        }

        if file_target_bytes < row_group_target_bytes {
            return invalid("file target cannot be smaller than row-group target");
        }

        ZstdLevel::try_new(config.compression_level)
            .map_err(|error| invalid_error(format!("invalid ZSTD compression level: {error}")))?;

        let output_dir = output_dir.as_ref().to_path_buf();

        fs::create_dir_all(&output_dir).map_err(|error| {
            invalid_error(format!(
                "failed to create timeline directory {}: {error}",
                output_dir.display()
            ))
        })?;

        Ok(Self {
            output_dir,

            row_group_target_bytes,
            file_target_bytes,
            compression_level: config.compression_level,

            batch: TimelineBatchBuilder::with_capacity(8192),
            batch_bytes: 0,

            writer: None,
            current_file_bytes: 0,
            next_file_index: 0,

            metrics: TimelineWriterMetrics::default(),

            finished: false,
            failed: false,
        })
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    pub fn metrics(&self) -> &TimelineWriterMetrics {
        &self.metrics
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.failed {
            return invalid("timeline writer is in a failed state");
        }

        if self.finished {
            return invalid("timeline writer has already been finalized");
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Open Parquet file
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
                "timeline Parquet output already exists: {}",
                path.display()
            ));
        }

        let file = File::create(&path).map_err(|error| {
            invalid_error(format!(
                "failed to create timeline Parquet file {}: {error}",
                path.display()
            ))
        })?;

        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(self.compression_level).map_err(|error| {
                    invalid_error(format!("invalid timeline compression level: {error}"))
                })?,
            ))
            .build();

        let writer =
            ArrowWriter::try_new(file, timeline_schema(), Some(properties)).map_err(|error| {
                invalid_error(format!("failed to initialize timeline writer: {error}"))
            })?;

        self.writer = Some(writer);
        self.current_file_bytes = 0;

        self.next_file_index = self
            .next_file_index
            .checked_add(1)
            .ok_or_else(|| invalid_error("timeline file index overflow"))?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Flush batch
    // -------------------------------------------------------------------------

    fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        self.open_file()?;

        let batch = self.batch.finish()?.expect("nonempty timeline batch");

        let writer = self.writer.as_mut().expect("writer initialized");

        writer
            .write(&batch)
            .map_err(|error| invalid_error(format!("failed to write timeline batch: {error}")))?;

        writer.flush().map_err(|error| {
            invalid_error(format!("failed to flush timeline row group: {error}"))
        })?;

        self.metrics.events_written = self
            .metrics
            .events_written
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| invalid_error("timeline event counter overflow"))?;

        self.batch_bytes = 0;

        // Track approximate serialized bytes.
        self.current_file_bytes = writer.bytes_written() as u64;

        if self.current_file_bytes >= self.file_target_bytes {
            self.close_file()?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Close file
    // -------------------------------------------------------------------------

    fn close_file(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        writer.close().map_err(|error| {
            invalid_error(format!("failed to finalize timeline Parquet file: {error}"))
        })?;

        self.metrics.files_written = self
            .metrics
            .files_written
            .checked_add(1)
            .ok_or_else(|| invalid_error("timeline file counter overflow"))?;

        self.current_file_bytes = 0;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Write event
    // -------------------------------------------------------------------------

    pub fn write(&mut self, record: TimelineRecord) -> Result<()> {
        self.ensure_writable()?;

        // Nine fixed-width columns:
        // i64 + u32 + 5*u64 + 2*u8 = 54 bytes.
        //
        // Reserve additional memory for Arrow buffers and overhead.
        const ESTIMATED_RECORD_BYTES: u64 = 80;

        let result = self.write_inner(record, ESTIMATED_RECORD_BYTES);

        if result.is_err() {
            self.failed = true;
        }

        result
    }

    fn write_inner(&mut self, record: TimelineRecord, estimated_bytes: u64) -> Result<()> {
        self.batch_bytes = self
            .batch_bytes
            .checked_add(estimated_bytes)
            .ok_or_else(|| invalid_error("timeline batch byte counter overflow"))?;

        self.batch.push(record);

        self.metrics.start_timestamp_ns = Some(
            self.metrics
                .start_timestamp_ns
                .map_or(record.event_timestamp_ns, |current| {
                    current.min(record.event_timestamp_ns)
                }),
        );

        self.metrics.end_timestamp_ns = Some(
            self.metrics
                .end_timestamp_ns
                .map_or(record.event_timestamp_ns, |current| {
                    current.max(record.event_timestamp_ns)
                }),
        );

        if self.batch_bytes >= self.row_group_target_bytes {
            self.flush_batch()?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Finalization
    // -------------------------------------------------------------------------

    pub fn finish(&mut self) -> Result<()> {
        if self.failed {
            return invalid("cannot finalize failed timeline writer");
        }

        if self.finished {
            return Ok(());
        }

        if let Err(error) = self.flush_batch() {
            self.failed = true;
            return Err(error);
        }

        if let Err(error) = self.close_file() {
            self.failed = true;
            return Err(error);
        }

        self.finished = true;

        Ok(())
    }
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
