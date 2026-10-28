use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow_array::{Array, Int64Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::output::writers::unified_timeline::UnifiedTimelineRecord,
    crate::historical::merge::output::writers::unified_timeline::unified_timeline_schema,
};

// -----------------------------------------------------------------------------
// Ordering key
// -----------------------------------------------------------------------------

type TimelineKey = (i64, u32, u64, u64);

// -----------------------------------------------------------------------------
// Streaming timeline reader
// -----------------------------------------------------------------------------

pub struct MaterializationTimelineReader {
    files: Vec<PathBuf>,
    next_file: usize,

    reader: Option<ParquetRecordBatchReader>,
    batch: Option<RecordBatch>,
    batch_row: usize,

    batch_size: usize,

    previous_key: Option<TimelineKey>,
    records_read: u64,
}

impl MaterializationTimelineReader {
    pub fn new(files: Vec<PathBuf>, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return invalid("timeline reader batch size must be positive");
        }

        if files.iter().any(|path| {
            path.extension().and_then(|extension| extension.to_str()) != Some("parquet")
        }) {
            return invalid("timeline reader requires Parquet files");
        }

        // Caller supplies files in manifest order.
        // Reject duplicate or incorrectly ordered paths.
        for pair in files.windows(2) {
            if pair[0] >= pair[1] {
                return invalid("timeline files must be strictly ordered by path");
            }
        }

        Ok(Self {
            files,
            next_file: 0,
            reader: None,
            batch: None,
            batch_row: 0,
            batch_size,
            previous_key: None,
            records_read: 0,
        })
    }

    pub fn open_directory(directory: &Path, batch_size: usize) -> Result<Self> {
        let mut files = Vec::new();

        for entry in std::fs::read_dir(directory).map_err(|error| {
            invalid_error(format!(
                "cannot read timeline directory {}: {error}",
                directory.display()
            ))
        })? {
            let entry = entry.map_err(|error| {
                invalid_error(format!("cannot inspect timeline directory entry: {error}"))
            })?;

            let path = entry.path();

            if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                files.push(path);
            }
        }

        files.sort();

        Self::new(files, batch_size)
    }

    // -------------------------------------------------------------------------
    // Open next Parquet file
    // -------------------------------------------------------------------------

    fn open_next_file(&mut self) -> Result<bool> {
        if self.next_file >= self.files.len() {
            return Ok(false);
        }

        let path = &self.files[self.next_file];

        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "cannot open timeline file {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect timeline file {}: {error}",
                path.display()
            ))
        })?;

        if builder.schema().as_ref() != unified_timeline_schema().as_ref() {
            return invalid(format!(
                "unexpected unified timeline schema: {}",
                path.display()
            ));
        }

        let reader = builder
            .with_batch_size(self.batch_size)
            .build()
            .map_err(|error| {
                invalid_error(format!(
                    "cannot initialize timeline reader {}: {error}",
                    path.display()
                ))
            })?;

        self.reader = Some(reader);
        self.batch = None;
        self.batch_row = 0;
        self.next_file += 1;

        Ok(true)
    }

    // -------------------------------------------------------------------------
    // Read next chronological reference
    // -------------------------------------------------------------------------

    pub fn next_record(&mut self) -> Result<Option<UnifiedTimelineRecord>> {
        loop {
            if let Some(batch) = &self.batch {
                if self.batch_row < batch.num_rows() {
                    let record = decode_record(batch, self.batch_row)?;

                    validate_reference(&record)?;

                    let key = (
                        record.event_timestamp_ns,
                        record.stream_rank,
                        record.task_id,
                        record.event_ordinal,
                    );

                    if let Some(previous) = self.previous_key {
                        if key < previous {
                            return invalid(format!(
                                "timeline ordering regression: \
                                 previous={previous:?}, current={key:?}"
                            ));
                        }
                    }

                    self.records_read = self
                        .records_read
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("timeline record counter overflow"))?;

                    self.previous_key = Some(key);
                    self.batch_row += 1;

                    return Ok(Some(record));
                }
            }

            // Current batch exhausted.
            self.batch = None;
            self.batch_row = 0;

            if let Some(reader) = self.reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        self.batch = Some(batch);
                        continue;
                    }

                    Some(Err(error)) => {
                        return invalid(format!("timeline Parquet decoding failed: {error}"));
                    }

                    None => {
                        self.reader = None;
                    }
                }
            }

            if !self.open_next_file()? {
                return Ok(None);
            }
        }
    }

    pub fn records_read(&self) -> u64 {
        self.records_read
    }

    pub fn files_opened(&self) -> usize {
        self.next_file
    }
}

// -----------------------------------------------------------------------------
// Arrow record decoding
// -----------------------------------------------------------------------------

fn decode_record(batch: &RecordBatch, row: usize) -> Result<UnifiedTimelineRecord> {
    macro_rules! required {
        ($name:expr, $type:ty) => {{
            let array = batch
                .column_by_name($name)
                .and_then(|column| column.as_any().downcast_ref::<$type>())
                .ok_or_else(|| {
                    invalid_error(format!("missing or invalid timeline column: {}", $name))
                })?;

            if array.is_null(row) {
                return invalid(format!("null required timeline field: {}", $name));
            }

            array.value(row)
        }};
    }

    macro_rules! optional {
        ($name:expr, $type:ty) => {{
            let array = batch
                .column_by_name($name)
                .and_then(|column| column.as_any().downcast_ref::<$type>())
                .ok_or_else(|| {
                    invalid_error(format!("missing or invalid timeline column: {}", $name))
                })?;

            if array.is_null(row) {
                None
            } else {
                Some(array.value(row))
            }
        }};
    }

    Ok(UnifiedTimelineRecord {
        event_timestamp_ns: required!("event_timestamp_ns", Int64Array),
        stream_rank: required!("stream_rank", UInt32Array),
        event_kind: required!("event_kind", UInt8Array),

        dataset_id: required!("dataset_id", UInt64Array),
        task_id: required!("task_id", UInt64Array),
        event_ordinal: required!("event_ordinal", UInt64Array),

        canonical_row_offset: required!("canonical_row_offset", UInt64Array),
        canonical_row_count: required!("canonical_row_count", UInt64Array),

        event_boundary: optional!("event_boundary", UInt8Array),
        source_operation: optional!("source_operation", UInt8Array),
    })
}

// -----------------------------------------------------------------------------
// Reference validation
// -----------------------------------------------------------------------------

fn validate_reference(record: &UnifiedTimelineRecord) -> Result<()> {
    match record.event_kind {
        0 => {
            if record.canonical_row_count != 1 {
                return invalid("trade reference must contain exactly one canonical row");
            }

            if record.event_boundary.is_some() || record.source_operation.is_some() {
                return invalid("trade reference contains depth metadata");
            }
        }

        1 => {
            let boundary = record
                .event_boundary
                .ok_or_else(|| invalid_error("depth reference missing event boundary"))?;

            let operation = record
                .source_operation
                .ok_or_else(|| invalid_error("depth reference missing source operation"))?;

            if boundary > 1 {
                return invalid("invalid depth event boundary");
            }

            if operation > 2 {
                return invalid("invalid depth source operation");
            }
        }

        _ => return invalid("unsupported timeline event kind"),
    }

    record
        .canonical_row_offset
        .checked_add(record.canonical_row_count)
        .ok_or_else(|| invalid_error("canonical row range overflow"))?;

    Ok(())
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
