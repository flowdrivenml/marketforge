use std::{fs::File, path::Path};

use arrow_array::{Array, Int64Array, RecordBatch};

use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
            RowSelection,
        },
    },
    file::metadata::PageIndexPolicy,
    file::page_index::column_index::ColumnIndexMetaData,
};

use crate::shared::error::{MarketForgeError, Result};

// -----------------------------------------------------------------------------
// Trade row reference
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedTrade {
    pub event_timestamp_ns: i64,

    /// Original zero-based row position within the Parquet file.
    pub file_row_index: u64,
}

// -----------------------------------------------------------------------------
// Trade cursor
// -----------------------------------------------------------------------------

pub struct TradeCursor {
    reader: ParquetRecordBatchReader,

    batch: Option<RecordBatch>,
    batch_row: usize,

    next_file_row_index: u64,

    rows_read: u64,
    previous_timestamp_ns: Option<i64>,
    selected_ranges: Vec<std::ops::Range<u64>>,
    selected_range_index: usize,
    range: Option<(i64, i64)>,
    indexed: bool,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl TradeCursor {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "failed to open trade Parquet file {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "failed to inspect trade Parquet file {}: {error}",
                path.display()
            ))
        })?;

        // ---------------------------------------------------------------------
        // Validate timestamp column
        // ---------------------------------------------------------------------

        let schema = builder.schema();

        let timestamp_index = schema.index_of("event_timestamp_ns").map_err(|error| {
            invalid_error(format!("trade Parquet missing timestamp column: {error}"))
        })?;

        let timestamp_field = schema.field(timestamp_index);

        if timestamp_field.data_type() != &arrow_schema::DataType::Int64
            || timestamp_field.is_nullable()
        {
            return invalid("trade event_timestamp_ns must be non-nullable Int64");
        }

        // ---------------------------------------------------------------------
        // Project only the timestamp column
        // ---------------------------------------------------------------------

        let projection = ProjectionMask::roots(builder.parquet_schema(), [timestamp_index]);

        let reader = builder
            .with_projection(projection)
            .with_batch_size(8192)
            .build()
            .map_err(|error| {
                invalid_error(format!("failed to initialize trade cursor: {error}"))
            })?;

        Ok(Self {
            reader,
            batch: None,
            batch_row: 0,

            next_file_row_index: 0,

            rows_read: 0,
            previous_timestamp_ns: None,

            selected_ranges: Vec::new(),
            selected_range_index: 0,

            range: None,
            indexed: false,
        })
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn rows_read(&self) -> u64 {
        self.rows_read
    }

    // -------------------------------------------------------------------------
    // Read next trade
    // -------------------------------------------------------------------------

    pub fn next_trade(&mut self) -> Result<Option<IndexedTrade>> {
        loop {
            // -----------------------------------------------------------------
            // Consume current Arrow batch
            // -----------------------------------------------------------------

            if let Some(batch) = &self.batch {
                if self.batch_row < batch.num_rows() {
                    let timestamps = batch
                        .column_by_name("event_timestamp_ns")
                        .ok_or_else(|| invalid_error("projected trade timestamp column missing"))?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| invalid_error("invalid projected trade timestamp type"))?;

                    if timestamps.is_null(self.batch_row) {
                        return invalid("null canonical trade timestamp");
                    }

                    let timestamp = timestamps.value(self.batch_row);

                    // ---------------------------------------------------------
                    // Validate ordering
                    // ---------------------------------------------------------

                    if let Some(previous) = self.previous_timestamp_ns {
                        if timestamp < previous {
                            return invalid(format!(
                                "canonical trade timestamp regression: \
                                 previous={previous}, current={timestamp}"
                            ));
                        }
                    }

                    let row_index = self.next_file_row_index;

                    self.next_file_row_index = self
                        .next_file_row_index
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("trade row index overflow"))?;

                    if self.indexed {
                        while let Some(range) = self.selected_ranges.get(self.selected_range_index)
                        {
                            if self.next_file_row_index < range.end {
                                break;
                            }

                            self.selected_range_index += 1;

                            if let Some(next) = self.selected_ranges.get(self.selected_range_index)
                            {
                                self.next_file_row_index = next.start;
                            }
                        }
                    }

                    self.rows_read = self
                        .rows_read
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("trade row counter overflow"))?;

                    self.previous_timestamp_ns = Some(timestamp);
                    self.batch_row += 1;

                    // Apply exact half-open timestamp filtering.
                    if let Some((start_ns, end_ns)) = self.range {
                        if timestamp < start_ns {
                            continue;
                        }

                        if timestamp >= end_ns {
                            return Ok(None);
                        }
                    }

                    return Ok(Some(IndexedTrade {
                        event_timestamp_ns: timestamp,
                        file_row_index: row_index,
                    }));
                }
            }

            // -----------------------------------------------------------------
            // Load next Arrow batch
            // -----------------------------------------------------------------

            let Some(batch) = self.reader.next() else {
                self.batch = None;
                return Ok(None);
            };

            let batch = batch.map_err(|error| {
                invalid_error(format!("failed to read trade Parquet batch: {error}"))
            })?;

            self.batch = Some(batch);
            self.batch_row = 0;
        }
    }
    pub fn open_indexed_range(path: &Path, start_ns: i64, end_ns: i64) -> Result<Self> {
        if start_ns >= end_ns {
            return invalid(format!(
                "invalid trade cursor range: [{start_ns}, {end_ns})"
            ));
        }

        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "failed to open trade Parquet file {}: {error}",
                path.display()
            ))
        })?;

        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);

        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
            .map_err(|error| {
                invalid_error(format!("failed to inspect indexed trade file: {error}"))
            })?;

        let timestamp_index = builder
            .schema()
            .index_of("event_timestamp_ns")
            .map_err(|error| invalid_error(error.to_string()))?;

        let field = builder.schema().field(timestamp_index);

        if field.data_type() != &arrow_schema::DataType::Int64 || field.is_nullable() {
            return invalid("trade timestamp must be non-nullable Int64");
        }

        let projection = ProjectionMask::roots(builder.parquet_schema(), [timestamp_index]);

        let metadata = builder.metadata();

        let Some(page_index) = metadata.page_index() else {
            return Self::open_range_fallback(path, start_ns, end_ns);
        };

        let mut selected_ranges = Vec::new();
        let mut global_offset = 0_u64;

        for (group_index, group) in metadata.row_groups().iter().enumerate() {
            let group_rows = u64::try_from(group.num_rows())
                .map_err(|_| invalid_error("invalid trade row-group size"))?;

            let Some(column_index) = page_index.column_index(group_index, timestamp_index) else {
                return Self::open_range_fallback(path, start_ns, end_ns);
            };

            let Some(offset_index) = page_index.offset_index(group_index, timestamp_index) else {
                return Self::open_range_fallback(path, start_ns, end_ns);
            };

            let locations = offset_index.page_locations();

            let ColumnIndexMetaData::INT64(index) = column_index else {
                return Self::open_range_fallback(path, start_ns, end_ns);
            };

            if column_index.num_pages() as usize != locations.len() {
                return Self::open_range_fallback(path, start_ns, end_ns);
            }
            if group_rows > 0 && locations.is_empty() {
                return Self::open_range_fallback(path, start_ns, end_ns);
            }

            if let Some(first) = locations.first() {
                if first.first_row_index != 0 {
                    return Self::open_range_fallback(path, start_ns, end_ns);
                }
            }

            for page_number in 0..column_index.num_pages() as usize {
                let first_row = u64::try_from(locations[page_number].first_row_index)
                    .map_err(|_| invalid_error("invalid page row offset"))?;

                let last_row = if page_number + 1 < locations.len() {
                    u64::try_from(locations[page_number + 1].first_row_index)
                        .map_err(|_| invalid_error("invalid page row offset"))?
                } else {
                    group_rows
                };

                if first_row >= last_row
                    || last_row > group_rows
                    || (page_number > 0
                        && first_row
                            <= u64::try_from(locations[page_number - 1].first_row_index)
                                .map_err(|_| invalid_error("invalid previous page offset"))?)
                {
                    return Self::open_range_fallback(path, start_ns, end_ns);
                }

                // Missing page statistics must not cause data loss.
                let overlaps = match (index.min_value(page_number), index.max_value(page_number)) {
                    (Some(min), Some(max)) => *max >= start_ns && *min < end_ns,
                    _ => true,
                };

                if overlaps {
                    selected_ranges.push((global_offset + first_row)..(global_offset + last_row));
                }
            }

            global_offset = global_offset
                .checked_add(group_rows)
                .ok_or_else(|| invalid_error("trade row offset overflow"))?;
        }

        // -------------------------------------------------------------------------
        // Construct Parquet row selection
        // -------------------------------------------------------------------------

        let total_rows = usize::try_from(global_offset)
            .map_err(|_| invalid_error("trade row count overflow"))?;

        let ranges =
            selected_ranges
                .iter()
                .map(|range| {
                    Ok(usize::try_from(range.start)
                        .map_err(|_| invalid_error("row offset overflow"))?
                        ..usize::try_from(range.end)
                            .map_err(|_| invalid_error("row offset overflow"))?)
                })
                .collect::<Result<Vec<_>>>()?;

        let selection = RowSelection::from_consecutive_ranges(ranges.into_iter(), total_rows);

        let reader = builder
            .with_projection(projection)
            .with_row_selection(selection)
            .with_batch_size(8192)
            .build()
            .map_err(|error| {
                invalid_error(format!("failed to build indexed trade cursor: {error}"))
            })?;

        let first_row = selected_ranges.first().map_or(0, |range| range.start);

        Ok(Self {
            reader,
            batch: None,
            batch_row: 0,

            next_file_row_index: first_row,

            rows_read: 0,
            previous_timestamp_ns: None,

            selected_ranges,
            selected_range_index: 0,
            range: Some((start_ns, end_ns)),
            indexed: true,
        })
    }
    fn open_range_fallback(path: &Path, start_ns: i64, end_ns: i64) -> Result<Self> {
        let mut cursor = Self::open(path)?;

        cursor.range = Some((start_ns, end_ns));

        Ok(cursor)
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
