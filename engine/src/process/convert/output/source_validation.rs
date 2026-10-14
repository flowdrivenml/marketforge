use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{
    error::{MarketForgeError, Result},
    process::{
        convert::execution::{CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader},
        merge::PhysicalMergeManifest,
    },
};

// -----------------------------------------------------------------------------
// Validation metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourcePreservationMetrics {
    pub events_compared: u64,
    pub columns_compared: usize,
    pub source_batches: u64,
    pub converted_batches: u64,
    pub converted_files: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourcePreservationMode {
    Full,
    Prefix(u64),
}

// -----------------------------------------------------------------------------
// Converted dataset cursor
// -----------------------------------------------------------------------------

struct ConvertedBatchReader {
    files: Vec<PathBuf>,
    file_index: usize,

    reader: Option<ParquetRecordBatchReader>,
    schema: Option<SchemaRef>,

    batch_size: usize,

    batches_read: u64,
    files_opened: u64,
}

impl ConvertedBatchReader {
    fn new(directory: &Path, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return invalid("converted batch size must be positive");
        }

        let files = super::files::discover_conversion_files(directory)?;

        if files.is_empty() {
            return invalid("converted dataset contains no Parquet files");
        }

        Ok(Self {
            files,
            file_index: 0,
            reader: None,
            schema: None,
            batch_size,
            batches_read: 0,
            files_opened: 0,
        })
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = &mut self.reader {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        self.batches_read += 1;

                        return Ok(Some(batch));
                    }

                    Some(Err(error)) => {
                        return invalid(format!("converted Parquet read failed: {error}"));
                    }

                    None => {
                        self.reader = None;
                    }
                }
            }

            if self.file_index >= self.files.len() {
                return Ok(None);
            }

            self.open_next_file()?;
        }
    }

    fn open_next_file(&mut self) -> Result<()> {
        let path = &self.files[self.file_index];

        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "cannot open converted Parquet {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect converted Parquet {}: {error}",
                path.display()
            ))
        })?;

        let schema = builder.schema().clone();

        if let Some(expected) = &self.schema {
            if schema != *expected {
                return invalid(format!("converted schema mismatch: {}", path.display()));
            }
        } else {
            self.schema = Some(schema);
        }

        self.reader = Some(
            builder
                .with_batch_size(self.batch_size)
                .build()
                .map_err(|error| {
                    invalid_error(format!("cannot construct converted reader: {error}"))
                })?,
        );

        self.file_index += 1;
        self.files_opened += 1;

        Ok(())
    }
}

pub fn validate_source_preservation(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    converted_directory: &Path,
) -> Result<SourcePreservationMetrics> {
    validate_source_preservation_with_mode(
        source_directory,
        source_manifest,
        converted_directory,
        SourcePreservationMode::Full,
        8,
    )
}

pub fn validate_depth_source_preservation(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    converted_directory: &Path,
    max_events: Option<u64>,
) -> Result<SourcePreservationMetrics> {
    use crate::process::merge::UnifiedMergeMode;

    if source_manifest.mode != UnifiedMergeMode::Depth {
        return invalid("depth source preservation requires depth-only merge");
    }

    let mode = match max_events {
        Some(limit) => SourcePreservationMode::Prefix(limit),
        None => SourcePreservationMode::Full,
    };

    validate_source_preservation_with_mode(
        source_directory,
        source_manifest,
        converted_directory,
        mode,
        7,
    )
}

// -----------------------------------------------------------------------------
// Streaming source preservation validation
// -----------------------------------------------------------------------------

pub fn validate_source_preservation_with_mode(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    converted_directory: &Path,
    mode: SourcePreservationMode,
    appended_columns: usize,
) -> Result<SourcePreservationMetrics> {
    source_manifest.validate()?;
    if appended_columns == 0 {
        return invalid("source preservation requires appended columns");
    }

    let expected_events = match mode {
        SourcePreservationMode::Full => source_manifest.events_written,

        SourcePreservationMode::Prefix(limit) => {
            if limit == 0 {
                return invalid("source preservation prefix must be positive");
            }

            if limit > source_manifest.events_written {
                return invalid("source preservation prefix exceeds source event count");
            }

            limit
        }
    };
    let mut source = PhysicalConversionReader::new(
        source_directory,
        source_manifest,
        CONVERSION_READ_BATCH_SIZE,
    )?;

    let mut converted = ConvertedBatchReader::new(converted_directory, CONVERSION_READ_BATCH_SIZE)?;

    let mut metrics = SourcePreservationMetrics::default();

    let mut source_batch: Option<RecordBatch> = None;
    let mut converted_batch: Option<RecordBatch> = None;

    let mut source_offset = 0;
    let mut converted_offset = 0;

    let mut expected_source_schema: Option<SchemaRef> = None;

    loop {
        if metrics.events_compared == expected_events {
            if converted_batch
                .as_ref()
                .is_some_and(|batch| converted_offset < batch.num_rows())
            {
                return invalid("converted dataset contains events beyond preservation limit");
            }

            if converted.next_batch()?.is_some() {
                return invalid("converted dataset contains additional events");
            }

            break;
        }
        // ---------------------------------------------------------------------
        // Advance source cursor
        // ---------------------------------------------------------------------

        if source_batch
            .as_ref()
            .is_none_or(|batch| source_offset >= batch.num_rows())
        {
            source_batch = source.next_batch()?;
            source_offset = 0;

            if source_batch.is_some() {
                metrics.source_batches += 1;
            }
        }

        // ---------------------------------------------------------------------
        // Advance converted cursor
        // ---------------------------------------------------------------------

        if converted_batch
            .as_ref()
            .is_none_or(|batch| converted_offset >= batch.num_rows())
        {
            converted_batch = converted.next_batch()?;
            converted_offset = 0;

            if converted_batch.is_some() {
                metrics.converted_batches += 1;
            }
        }

        // ---------------------------------------------------------------------
        // End-of-stream validation
        // ---------------------------------------------------------------------

        match (&source_batch, &converted_batch) {
            (None, None) => break,

            (None, Some(_)) => {
                return invalid("converted dataset contains additional events");
            }

            (Some(_), None) => {
                return invalid("converted dataset is missing source events");
            }

            (Some(_), Some(_)) => {}
        }

        let original = source_batch.as_ref().unwrap();
        let output = converted_batch.as_ref().unwrap();

        // ---------------------------------------------------------------------
        // Validate source schema preservation
        // ---------------------------------------------------------------------

        if expected_source_schema.is_none() {
            expected_source_schema = Some(original.schema());

            let source_fields = original.schema();
            let converted_fields = output.schema();

            if converted_fields.fields().len() != source_fields.fields().len() + appended_columns {
                return invalid("converted dataset has unexpected column count");
            }

            for (index, field) in source_fields.fields().iter().enumerate() {
                if converted_fields.field(index) != field.as_ref() {
                    return invalid(format!("source schema field mismatch at index {index}"));
                }
            }

            metrics.columns_compared = source_fields.fields().len();
        }

        // ---------------------------------------------------------------------
        // Compare logical row slices
        // ---------------------------------------------------------------------

        let source_remaining = original.num_rows() - source_offset;
        let converted_remaining = output.num_rows() - converted_offset;

        let remaining =
            usize::try_from(expected_events - metrics.events_compared).unwrap_or(usize::MAX);

        let count = source_remaining.min(converted_remaining).min(remaining);

        if count == 0 {
            return invalid("unexpected empty comparison slice");
        }

        for index in 0..metrics.columns_compared {
            let source_slice = original.column(index).slice(source_offset, count);

            let converted_slice = output.column(index).slice(converted_offset, count);

            if source_slice.to_data() != converted_slice.to_data() {
                return invalid(format!(
                    "source preservation mismatch: \
                     column={}, event_offset={}",
                    original.schema().field(index).name(),
                    metrics.events_compared,
                ));
            }
        }

        metrics.events_compared = metrics
            .events_compared
            .checked_add(count as u64)
            .ok_or_else(|| invalid_error("source preservation event counter overflow"))?;

        source_offset += count;
        converted_offset += count;
    }

    // -------------------------------------------------------------------------
    // Final accounting
    // -------------------------------------------------------------------------

    if metrics.events_compared != expected_events {
        return invalid(format!(
            "source preservation count mismatch: expected {}, compared {}",
            source_manifest.events_written, metrics.events_compared,
        ));
    }

    metrics.converted_files = converted.files_opened;

    Ok(metrics)
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
