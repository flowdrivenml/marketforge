use std::{fs::File, path::Path};

use arrow_array::{Array, Int64Array, RecordBatch, UInt8Array, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{
    error::{MarketForgeError, Result},
    formats::depth::{DepthEventBoundary, DepthSourceOperation},
    process::parquet::{IndexedDepthEvent, depth_event_schema},
};
use parquet::{
    arrow::arrow_reader::{ArrowReaderOptions, RowSelection},
    file::metadata::PageIndexPolicy,
    file::page_index::column_index::ColumnIndexMetaData,
};

pub struct DepthEventCursor {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,

    range: Option<(i64, i64)>,
}

impl DepthEventCursor {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to open depth event index {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| invalid_error(error.to_string()))?;

        if builder.schema().as_ref() != depth_event_schema().as_ref() {
            return invalid(format!("unexpected depth event schema: {}", path.display()));
        }

        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|error| invalid_error(error.to_string()))?;

        Ok(Self {
            reader,
            batch: None,
            row: 0,
            range: None,
        })
    }
    pub fn open_range(path: &Path, start_ns: i64, end_ns: i64) -> Result<Self> {
        if start_ns >= end_ns {
            return invalid(format!("invalid depth event range: [{start_ns}, {end_ns})"));
        }

        let mut cursor = Self::open(path)?;

        cursor.range = Some((start_ns, end_ns));

        Ok(cursor)
    }
    pub fn open_indexed_range(path: &Path, start_ns: i64, end_ns: i64) -> Result<Self> {
        if start_ns >= end_ns {
            return invalid(format!(
                "invalid indexed depth event range: [{start_ns}, {end_ns})"
            ));
        }

        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "failed to open depth event index {}: {error}",
                path.display()
            ))
        })?;

        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
            .map_err(|error| invalid_error(error.to_string()))?;

        if builder.schema().as_ref() != depth_event_schema().as_ref() {
            return invalid(format!("unexpected depth event schema: {}", path.display()));
        }

        let metadata = builder.metadata();

        let Some(page_index) = metadata.page_index() else {
            return Self::open_range(path, start_ns, end_ns);
        };

        // Find the physical timestamp column.
        let timestamp_column = builder
            .schema()
            .index_of("event_timestamp_ns")
            .map_err(|error| invalid_error(error.to_string()))?;

        let mut ranges = Vec::new();
        let mut global_row_offset = 0_usize;

        for (group_index, group) in metadata.row_groups().iter().enumerate() {
            let group_rows = usize::try_from(group.num_rows())
                .map_err(|_| invalid_error("invalid row-group size"))?;

            let Some(column_index) = page_index.column_index(group_index, timestamp_column) else {
                return Self::open_range(path, start_ns, end_ns);
            };

            let Some(offset_index) = page_index.offset_index(group_index, timestamp_column) else {
                return Self::open_range(path, start_ns, end_ns);
            };

            let page_locations = offset_index.page_locations();

            let ColumnIndexMetaData::INT64(index) = column_index else {
                return Self::open_range(path, start_ns, end_ns);
            };

            if column_index.num_pages() as usize != page_locations.len() {
                return Self::open_range(path, start_ns, end_ns);
            }

            for page_number in 0..column_index.num_pages() as usize {
                let first_row = usize::try_from(page_locations[page_number].first_row_index)
                    .map_err(|_| invalid_error("invalid page row index"))?;

                let last_row = if page_number + 1 < page_locations.len() {
                    usize::try_from(page_locations[page_number + 1].first_row_index)
                        .map_err(|_| invalid_error("invalid page row index"))?
                } else {
                    group_rows
                };

                if first_row >= last_row || last_row > group_rows {
                    return Self::open_range(path, start_ns, end_ns);
                }

                let (Some(min), Some(max)) =
                    (index.min_value(page_number), index.max_value(page_number))
                else {
                    // Unknown statistics: include the page conservatively.
                    ranges.push((global_row_offset + first_row)..(global_row_offset + last_row));
                    continue;
                };

                if *max < start_ns || *min >= end_ns {
                    continue;
                }

                ranges.push((global_row_offset + first_row)..(global_row_offset + last_row));
            }

            global_row_offset = global_row_offset
                .checked_add(group_rows)
                .ok_or_else(|| invalid_error("row offset overflow"))?;
        }

        // Construct selection over the complete file.
        let selection =
            RowSelection::from_consecutive_ranges(ranges.into_iter(), global_row_offset);

        let reader = builder
            .with_row_selection(selection)
            .with_batch_size(8192)
            .build()
            .map_err(|error| invalid_error(error.to_string()))?;

        Ok(Self {
            reader,
            batch: None,
            row: 0,
            range: Some((start_ns, end_ns)),
        })
    }

    pub fn next_event(&mut self) -> Result<Option<IndexedDepthEvent>> {
        loop {
            if let Some(batch) = &self.batch {
                if self.row < batch.num_rows() {
                    let event = decode_event(batch, self.row)?;

                    self.row += 1;

                    if let Some((start_ns, end_ns)) = self.range {
                        if event.event_timestamp_ns < start_ns {
                            continue;
                        }

                        if event.event_timestamp_ns >= end_ns {
                            return Ok(None);
                        }
                    }

                    return Ok(Some(event));
                }
            }

            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.batch = Some(batch);
                    self.row = 0;
                }

                Some(Err(error)) => {
                    return invalid(format!("failed to read depth event index: {error}"));
                }

                None => return Ok(None),
            }
        }
    }
}

fn decode_event(batch: &RecordBatch, row: usize) -> Result<IndexedDepthEvent> {
    let u64_column = |name: &str| -> Result<&UInt64Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid_error(format!("invalid UInt64 column: {name}")))
    };

    let i64_column = |name: &str| -> Result<&Int64Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| invalid_error(format!("invalid Int64 column: {name}")))
    };

    let u8_column = |name: &str| -> Result<&UInt8Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
            .ok_or_else(|| invalid_error(format!("invalid UInt8 column: {name}")))
    };

    let optional_u64 = |name: &str| -> Result<Option<u64>> {
        let column = u64_column(name)?;

        Ok((!column.is_null(row)).then(|| column.value(row)))
    };

    let optional_i64 = |name: &str| -> Result<Option<i64>> {
        let column = i64_column(name)?;

        Ok((!column.is_null(row)).then(|| column.value(row)))
    };

    let event_boundary = match u8_column("event_boundary")?.value(row) {
        0 => DepthEventBoundary::Changes,
        1 => DepthEventBoundary::Initialization,

        value => {
            return invalid(format!("unknown depth event boundary: {value}"));
        }
    };

    let source_operation = match u8_column("source_operation")?.value(row) {
        0 => DepthSourceOperation::Snapshot,
        1 => DepthSourceOperation::AbsoluteUpdate,
        2 => DepthSourceOperation::RelativeUpdate,

        value => {
            return invalid(format!("unknown depth source operation: {value}"));
        }
    };

    Ok(IndexedDepthEvent {
        event_ordinal: u64_column("event_ordinal")?.value(row),
        canonical_row_offset: u64_column("canonical_row_offset")?.value(row),
        canonical_row_count: u64_column("canonical_row_count")?.value(row),

        event_timestamp_ns: i64_column("event_timestamp_ns")?.value(row),
        system_timestamp_ns: optional_i64("system_timestamp_ns")?,

        sequence_start: optional_u64("sequence_start")?,
        sequence_end: optional_u64("sequence_end")?,

        event_boundary,
        source_operation,
    })
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
