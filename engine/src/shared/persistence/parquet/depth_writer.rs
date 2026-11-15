use super::{DepthEventBatchBuilder, IndexedDepthEvent, depth_event_schema};
use crate::shared::domain::book::boundary::DepthBoundaryManifest;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use crate::{
    shared::domain::book::{BookSegment, SegmentTracker},
    shared::domain::canonical::{DepthEventBoundary, DepthProcessingOutcome},
    shared::error::{MarketForgeError, Result},
    shared::persistence::config::ParquetResourceConfig,
};

use super::{DepthBatchBuilder, DepthSegmentManifest, depth_schema, write_depth_segments};

// -----------------------------------------------------------------------------
// Writer metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetDepthWriterMetrics {
    pub levels_written: u64,
    pub outcomes_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Parquet depth writer
// -----------------------------------------------------------------------------

pub struct ParquetDepthWriter {
    output_dir: PathBuf,
    resources: ParquetResourceConfig,

    batch: DepthBatchBuilder,
    batch_bytes: u64,

    writer: Option<ArrowWriter<File>>,
    current_file_bytes: u64,
    next_file_index: u64,

    segments: SegmentTracker,
    metrics: ParquetDepthWriterMetrics,

    finished: bool,
    failed: bool,
    event_batch: DepthEventBatchBuilder,
    event_writer: Option<ArrowWriter<File>>,
    events_written: u64,
    last_event_ordinal: Option<u64>,
    boundary_manifest: Option<DepthBoundaryManifest>,
    source_writer: Option<BufWriter<File>>,
    active_source: Option<(u64, u64, u64)>,
    source_count: u64,
    source_needs_initialization: bool,
}

impl ParquetDepthWriter {
    pub fn new(output_dir: impl AsRef<Path>, resources: ParquetResourceConfig) -> Result<Self> {
        if resources.row_group_target_bytes == 0 || resources.file_target_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet resource targets must be greater than zero".to_owned(),
            ));
        }

        if resources.file_target_bytes < resources.row_group_target_bytes {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet file target must be at least the row-group target".to_owned(),
            ));
        }

        let output_dir = output_dir.as_ref().to_path_buf();

        fs::create_dir_all(&output_dir).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create depth output directory {}: {error}",
                output_dir.display()
            ))
        })?;

        Ok(Self {
            output_dir,
            resources,

            batch: DepthBatchBuilder::new(),
            batch_bytes: 0,

            writer: None,
            current_file_bytes: 0,
            next_file_index: 0,

            // Source-event index
            event_batch: DepthEventBatchBuilder::with_capacity(8192),
            event_writer: None,
            events_written: 0,
            last_event_ordinal: None,

            segments: SegmentTracker::new(),
            metrics: ParquetDepthWriterMetrics::default(),

            finished: false,
            failed: false,
            boundary_manifest: None,
            source_writer: None,
            active_source: None,
            source_count: 0,
            source_needs_initialization: false,
        })
    }

    /// Enable consolidated source boundaries before accepting outcomes.
    pub fn enable_sources(&mut self) -> Result<()> {
        self.ensure_writable()?;
        if self.source_writer.is_some() || self.events_written != 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "source mode must be enabled once before writing".into(),
            ));
        }
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.output_dir.join("boundary.json"))
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(b"{\"version\":2,\"continuity\":\"independent_sources\",\"sources\":[")
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        self.source_writer = Some(writer);
        Ok(())
    }
    pub fn begin_source(&mut self, task_id: u64) -> Result<()> {
        self.ensure_writable()?;
        if self.source_writer.is_none() || self.active_source.is_some() {
            return Err(MarketForgeError::InvalidConfiguration(
                "previous depth source boundary missing or source mode disabled".into(),
            ));
        }
        self.active_source = Some((task_id, self.metrics.levels_written, self.events_written));
        self.last_event_ordinal = None;
        self.source_needs_initialization = true;
        Ok(())
    }

    pub fn metrics(&self) -> &ParquetDepthWriterMetrics {
        &self.metrics
    }

    pub fn segments(&self) -> &[BookSegment] {
        self.segments.segments()
    }

    pub fn next_event_offset(&self) -> u64 {
        self.segments.next_event_offset()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }
    pub fn events_indexed(&self) -> u64 {
        self.events_written
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    // -------------------------------------------------------------------------
    // Writer state
    // -------------------------------------------------------------------------

    fn ensure_writable(&self) -> Result<()> {
        if self.finished {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot write to finished depth writer".to_owned(),
            ));
        }

        if self.failed {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot write to failed depth writer".to_owned(),
            ));
        }

        Ok(())
    }

    fn fail<T>(&mut self, error: MarketForgeError) -> Result<T> {
        self.failed = true;
        Err(error)
    }

    // -------------------------------------------------------------------------
    // Open Parquet file
    // -------------------------------------------------------------------------

    fn open_file(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        if let Some(limits) = crate::shared::historical::limits::current() {
            crate::shared::historical::limits::check(
                "stream output files",
                limits.metric_entries,
                self.next_file_index.checked_add(1).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("file count overflow".into())
                })?,
                "writer file creation",
            )?;
        }
        let path = self
            .output_dir
            .join(format!("part-{:06}.parquet", self.next_file_index));

        if path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "depth Parquet output already exists: {}",
                path.display()
            )));
        }

        let file = File::create(&path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create depth Parquet file {}: {error}",
                path.display()
            ))
        })?;

        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).map_err(
                |error| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "invalid ZSTD compression level: {error}"
                    ))
                },
            )?))
            .build();

        let writer =
            ArrowWriter::try_new(file, depth_schema(), Some(properties)).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to initialize depth Parquet writer: {error}"
                ))
            })?;

        self.writer = Some(writer);
        self.current_file_bytes = 0;

        self.next_file_index = self.next_file_index.checked_add(1).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("depth Parquet file index overflow".to_owned())
        })?;

        Ok(())
    }

    fn open_event_writer(&mut self) -> Result<()> {
        if self.event_writer.is_some() {
            return Ok(());
        }

        let path = self.output_dir.join("events.parquet");

        if path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "depth event index already exists: {}",
                path.display()
            )));
        }

        let file = File::create(&path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create depth event index: {error}"
            ))
        })?;

        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).map_err(
                |error| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "invalid event-index compression level: {error}"
                    ))
                },
            )?))
            .build();

        self.event_writer = Some(
            ArrowWriter::try_new(file, depth_event_schema(), Some(properties)).map_err(
                |error| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "failed to initialize event-index writer: {error}"
                    ))
                },
            )?,
        );

        Ok(())
    }

    fn flush_event_batch(&mut self) -> Result<()> {
        if self.event_batch.is_empty() {
            return Ok(());
        }

        self.open_event_writer()?;

        let batch = self
            .event_batch
            .finish()?
            .expect("nonempty event-index batch");

        super::footprint::check_row_group_growth(
            self.event_writer
                .as_ref()
                .expect("event writer initialized")
                .flushed_row_groups()
                .len(),
            batch.num_rows(),
        )?;
        self.event_writer
            .as_mut()
            .expect("event writer initialized")
            .write(&batch)
            .map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to write depth event-index batch: {error}"
                ))
            })?;

        self.event_writer
            .as_mut()
            .unwrap()
            .flush()
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        Ok(())
    }

    fn close_event_writer(&mut self) -> Result<()> {
        self.flush_event_batch()?;

        if let Some(writer) = self.event_writer.take() {
            writer.close().map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to finalize depth event index: {error}"
                ))
            })?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Flush Arrow batch
    // -------------------------------------------------------------------------

    fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        self.open_file()?;

        let batch = self.batch.finish()?.expect("nonempty depth batch");

        let writer = self.writer.as_mut().expect("writer initialized");

        super::footprint::check_row_group_growth(
            writer.flushed_row_groups().len(),
            batch.num_rows(),
        )?;
        writer.write(&batch).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to write depth Parquet batch: {error}"
            ))
        })?;

        // Flush at the configured batch boundary rather than retaining Arrow's
        // default million-row group in addition to the accounted batch.
        writer.flush().map_err(|e| {
            MarketForgeError::InvalidConfiguration(format!("depth row-group flush: {e}"))
        })?;

        self.current_file_bytes = self
            .current_file_bytes
            .checked_add(batch.get_array_memory_size() as u64)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "depth Parquet file byte counter overflow".to_owned(),
                )
            })?;

        self.batch_bytes = 0;

        if self.current_file_bytes >= self.resources.file_target_bytes {
            self.close_file()?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Close current file
    // -------------------------------------------------------------------------

    fn close_file(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        writer.close().map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to close depth Parquet file: {error}"
            ))
        })?;

        self.current_file_bytes = 0;

        self.metrics.files_written =
            self.metrics.files_written.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "depth Parquet file counter overflow".to_owned(),
                )
            })?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Estimate canonical level memory
    // -------------------------------------------------------------------------

    fn estimate_level_bytes(level: &crate::shared::domain::canonical::L2LevelUpdate) -> u64 {
        // Approximate Arrow memory consumption, not compressed file size.
        128 + level.envelope.symbol.len() as u64 + level.envelope.stream_id.len() as u64
    }

    // -------------------------------------------------------------------------
    // Accept complete depth outcome
    // -------------------------------------------------------------------------

    fn accept_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        let count = outcome.events.len();
        if self.source_writer.is_some() && self.active_source.is_none() {
            return Err(MarketForgeError::InvalidConfiguration(
                "depth outcome outside source".into(),
            ));
        }
        if self.source_needs_initialization
            && outcome.boundary != DepthEventBoundary::Initialization
        {
            return Err(MarketForgeError::InvalidConfiguration(
                "new source requires independent initialization".into(),
            ));
        }

        if outcome.boundary == DepthEventBoundary::Initialization && count == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "empty depth initialization".to_owned(),
            ));
        }

        if outcome.boundary == DepthEventBoundary::Changes && self.segments.segments().is_empty() {
            return Err(MarketForgeError::InvalidConfiguration(
                "depth changes before initialization".to_owned(),
            ));
        }

        let first_timestamp = outcome
            .events
            .first()
            .map(|event| event.envelope.event_timestamp_ns);

        // Validate that all levels belong to one source event.
        if let Some(timestamp) = first_timestamp {
            if outcome
                .events
                .iter()
                .any(|event| event.envelope.event_timestamp_ns != timestamp)
            {
                return Err(MarketForgeError::InvalidConfiguration(
                    "depth outcome contains multiple timestamps".to_owned(),
                ));
            }
        }

        let next_count = self
            .segments
            .segments()
            .len()
            .checked_add(usize::from(
                outcome.boundary == DepthEventBoundary::Initialization,
            ))
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("segment count overflow".into())
            })?;
        let requested = (next_count as u64)
            .checked_mul(std::mem::size_of::<BookSegment>() as u64)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("segment footprint overflow".into())
            })?;
        if let Some(limits) = crate::shared::historical::limits::current() {
            crate::shared::historical::limits::check(
                "segment metadata entries",
                limits.metric_entries,
                next_count as u64,
                "depth writer segment growth",
            )?;
        }
        // Existing shared allowance, now checked before cloning or growing it.
        let permitted = self.resources.row_group_target_bytes.max(1024 * 1024);
        if crate::shared::historical::limits::current().is_some() {
            crate::shared::historical::limits::check(
                "segment metadata bytes",
                permitted,
                requested,
                "depth writer segment growth",
            )?;
        } else if requested > permitted {
            return Err(MarketForgeError::InvalidConfiguration(
                "depth reconstruction segment metadata exceeds bounded writer allowance".into(),
            ));
        }
        // Prepare the next segment state before modifying writer state.
        let mut next_segments = self.segments.clone();

        match outcome.boundary {
            DepthEventBoundary::Initialization => {
                next_segments
                    .begin_segment(first_timestamp.expect("nonempty initialization"), count)?;
            }

            DepthEventBoundary::Changes => {
                next_segments.record_changes(count)?;
            }
        }

        let metadata_bytes = (next_segments.segments().len() as u64)
            .checked_mul(std::mem::size_of::<BookSegment>() as u64)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("segment metadata size overflow".into())
            })?;
        if metadata_bytes > self.resources.row_group_target_bytes.max(1024 * 1024) {
            return Err(MarketForgeError::InvalidConfiguration(
                "depth reconstruction segment metadata exceeds bounded writer allowance".into(),
            ));
        }

        let count_u64 = u64::try_from(count).map_err(|_| {
            MarketForgeError::InvalidConfiguration("depth outcome event count overflow".to_owned())
        })?;

        let next_levels_written = self
            .metrics
            .levels_written
            .checked_add(count_u64)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "depth levels-written counter overflow".to_owned(),
                )
            })?;

        let next_outcomes_written =
            self.metrics
                .outcomes_written
                .checked_add(1)
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "depth outcomes-written counter overflow".to_owned(),
                    )
                })?;

        // Validate Decimal256 representability before buffering.
        for level in &outcome.events {
            super::decimal::decimal_to_i256(level.price)?;

            for quantity in [
                level.quantity_base,
                level.quantity_quote,
                level.quantity_contracts,
            ] {
                if let Some(value) = quantity {
                    super::decimal::decimal_to_i256(value)?;
                }
            }
        }

        let additional_bytes = outcome.events.iter().try_fold(0u64, |total, level| {
            total
                .checked_add(Self::estimate_level_bytes(level))
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "depth batch byte counter overflow".to_owned(),
                    )
                })
        })?;

        let next_batch_bytes = self
            .batch_bytes
            .checked_add(additional_bytes)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "depth batch byte counter overflow".to_owned(),
                )
            })?;

        // -------------------------------------------------------------------------
        // Prepare source-event index
        // -------------------------------------------------------------------------

        let indexed_event = IndexedDepthEvent::from_outcome(&outcome, self.metrics.levels_written)?;

        if let Some(previous) = self.last_event_ordinal {
            if indexed_event.event_ordinal <= previous {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "non-increasing depth source ordinal: previous={previous}, current={}",
                    indexed_event.event_ordinal,
                )));
            }
        }

        let next_events_written = self.events_written.checked_add(1).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("depth source-event counter overflow".to_owned())
        })?;

        let event_ordinal = indexed_event.event_ordinal;

        // Commit accepted canonical levels in source order.
        self.batch.extend(outcome.events);
        self.event_batch.push(indexed_event);

        self.source_needs_initialization = false;
        self.events_written = next_events_written;
        self.last_event_ordinal = Some(event_ordinal);

        self.batch_bytes = next_batch_bytes;
        self.segments = next_segments;

        self.metrics.levels_written = next_levels_written;
        self.metrics.outcomes_written = next_outcomes_written;

        if let Some(timestamp) = first_timestamp {
            self.metrics.start_timestamp_ns = Some(
                self.metrics
                    .start_timestamp_ns
                    .map_or(timestamp, |current| current.min(timestamp)),
            );

            self.metrics.end_timestamp_ns = Some(
                self.metrics
                    .end_timestamp_ns
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
        }

        if self.batch_bytes >= self.resources.row_group_target_bytes {
            self.flush_batch()?;
        }
        if self.event_batch.len() >= 8192 {
            self.flush_event_batch()?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Finalize
    // -------------------------------------------------------------------------

    fn finalize(&mut self) -> Result<()> {
        // -------------------------------------------------------------------------
        // Flush and close all Parquet files
        // -------------------------------------------------------------------------

        self.flush_batch()?;
        self.close_file()?;
        self.close_event_writer()?;

        // -------------------------------------------------------------------------
        // Validate canonical row accounting
        // -------------------------------------------------------------------------

        if self.segments.next_event_offset() != self.metrics.levels_written {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "depth segment accounting mismatch: offsets={}, levels={}",
                self.segments.next_event_offset(),
                self.metrics.levels_written,
            )));
        }

        // -------------------------------------------------------------------------
        // Build persistent reconstruction manifest
        // -------------------------------------------------------------------------

        let manifest = DepthSegmentManifest::new(
            self.metrics.levels_written,
            self.segments.segments().to_vec(),
        )?;

        // -------------------------------------------------------------------------
        // Persist reconstruction metadata
        // -------------------------------------------------------------------------

        write_depth_segments(&self.output_dir, &manifest)?;

        if let Some(writer) = &mut self.source_writer {
            if self.active_source.is_some() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "unfinished depth source boundary".into(),
                ));
            }
            writer
                .write_all(b"]}")
                .and_then(|()| writer.flush())
                .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        } else {
            let boundary = self.boundary_manifest.as_ref().ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("depth boundary metadata missing".to_owned())
            })?;

            let path = self.output_dir.join("boundary.json");

            if path.exists() {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "depth boundary metadata already exists: {}",
                    path.display()
                )));
            }

            let file = File::create(&path).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to create depth boundary metadata: {error}"
                ))
            })?;

            serde_json::to_writer_pretty(file, boundary).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to serialize depth boundary metadata: {error}"
                ))
            })?;
        }
        self.finished = true;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// DepthSink implementation
// -----------------------------------------------------------------------------

impl ParquetDepthWriter {
    pub fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        self.ensure_writable()?;

        if let Err(error) = self.accept_outcome(outcome) {
            return self.fail(error);
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        self.ensure_writable()?;

        if let Err(error) = self.finalize() {
            return self.fail(error);
        }

        Ok(())
    }
    pub fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()> {
        self.ensure_writable()?;

        if let Some(writer) = &mut self.source_writer {
            let (task_id, start_row, start_event) = self.active_source.take().ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "depth boundary without active source".into(),
                )
            })?;
            let source = super::DepthSourceBoundary {
                task_id,
                start_row,
                end_row: self.metrics.levels_written,
                start_event,
                end_event: self.events_written,
                boundary,
            };
            if self.source_count > 0 {
                writer
                    .write_all(b",")
                    .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
            }
            serde_json::to_writer(&mut *writer, &source)
                .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
            writer
                .flush()
                .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
            self.source_count = self.source_count.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("source count overflow".into())
            })?;
            return Ok(());
        }

        if self.boundary_manifest.is_some() {
            return self.fail(MarketForgeError::InvalidConfiguration(
                "depth boundary metadata already provided".to_owned(),
            ));
        }

        self.boundary_manifest = Some(boundary);

        Ok(())
    }
}
