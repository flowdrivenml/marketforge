use super::DepthTaskInventory;
use super::progress::MergeWorkerProgress;
use super::{
    materialize_combined_builder::{PhysicalCombinedBatchBuilder, PhysicalCombinedEvent},
    materialize_decoder::GroupedCanonicalDecoder,
    materialize_depth_builder::{PhysicalDepthBatchBuilder, PhysicalDepthEvent},
    materialize_depth_metadata::DepthMetadataResolver,
    materialize_inventory::CanonicalRowIndex,
    materialize_planner::CanonicalReadPlanner,
    materialize_reader::CanonicalParquetReader,
    materialize_resolver::CanonicalTaskKey,
    materialize_resources::MaterializationWorkerLimits,
    materialize_row_groups::CanonicalRowGroupIndex,
    materialize_timeline_reader::MaterializationTimelineReader,
    materialize_trade_builder::PhysicalTradeBatchBuilder,
    materialize_window::{MaterializationWindow, MaterializationWindowBuilder},
    materialize_writer::{PhysicalParquetWriter, PhysicalWriterMetrics},
    trade_inventory::TradeTaskInventory,
    unified_manifest::UnifiedMergeStreamKind,
    unified_manifest::{UnifiedMergeMode, UnifiedMergeStream},
};
use crate::{
    error::{MarketForgeError, Result},
    job::MergeParquetConfig,
};
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

// -----------------------------------------------------------------------------
// Partition input
// -----------------------------------------------------------------------------

pub struct MaterializationPartitionInput<'a> {
    pub partition_id: u32,
    pub start_ns: i64,
    pub end_ns: i64,

    pub timeline_files: &'a [PathBuf],

    pub trade_inventory: &'a [TradeTaskInventory],
    pub depth_inventory: &'a [DepthTaskInventory],

    pub streams: &'a [UnifiedMergeStream],

    pub mode: UnifiedMergeMode,
    pub parquet: &'a MergeParquetConfig,
    pub limits: MaterializationWorkerLimits,

    pub staging: &'a Path,
}

#[derive(Debug, Clone)]
pub struct MaterializationPartitionResult {
    pub partition_id: u32,
    pub start_ns: i64,
    pub end_ns: i64,

    pub windows_processed: u64,
    pub references_read: u64,

    pub writer_metrics: PhysicalWriterMetrics,
}

// -----------------------------------------------------------------------------
// Worker state
// -----------------------------------------------------------------------------

pub struct MaterializationPartitionWorker {
    planner: CanonicalReadPlanner,
    decoder: GroupedCanonicalDecoder,
    depth_metadata: DepthMetadataResolver,

    streams: HashMap<(u64, u32), UnifiedMergeStream>,
    profile_planning: Duration,
    profile_decoding: Duration,
    profile_building: Duration,
    profile_writing: Duration,
}

impl MaterializationPartitionWorker {
    pub fn new(input: &MaterializationPartitionInput<'_>) -> Result<Self> {
        if input.start_ns >= input.end_ns {
            return invalid("invalid materialization partition time range");
        }

        let reader = CanonicalParquetReader::new(input.limits.reader_batch_size)?;

        let decoder = GroupedCanonicalDecoder::new(reader, input.limits.max_open_cursors);

        let mut worker = Self {
            planner: CanonicalReadPlanner::new(),
            decoder,
            depth_metadata: DepthMetadataResolver::new(),
            streams: HashMap::new(),
            profile_planning: Duration::ZERO,
            profile_decoding: Duration::ZERO,
            profile_building: Duration::ZERO,
            profile_writing: Duration::ZERO,
        };

        worker.register_streams(input.streams)?;
        worker.register_trade_tasks(input.trade_inventory)?;
        worker.register_depth_tasks(input.depth_inventory, input.start_ns, input.end_ns)?;

        Ok(worker)
    }

    // -------------------------------------------------------------------------
    // Stream identity
    // -------------------------------------------------------------------------

    fn register_streams(&mut self, streams: &[UnifiedMergeStream]) -> Result<()> {
        for stream in streams {
            let key = (stream.dataset_id, stream.stream_rank);

            if self.streams.insert(key, stream.clone()).is_some() {
                return invalid(format!(
                    "duplicate materialization stream: \
                     dataset={}, rank={}",
                    stream.dataset_id, stream.stream_rank
                ));
            }
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Trade inventories
    // -------------------------------------------------------------------------

    fn register_trade_tasks(&mut self, tasks: &[TradeTaskInventory]) -> Result<()> {
        for task in tasks {
            let files = task
                .files
                .iter()
                .map(|file| (file.path.clone(), file.rows))
                .collect::<Vec<_>>();

            self.register_canonical_task(
                CanonicalTaskKey {
                    dataset_id: task.dataset_id,
                    task_id: task.task_id,
                    event_kind: 0,
                },
                files,
            )?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Depth inventories
    // -------------------------------------------------------------------------

    fn register_depth_tasks(
        &mut self,
        tasks: &[DepthTaskInventory],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<()> {
        for task in tasks {
            let mut files = Vec::with_capacity(task.canonical_files.len());

            for path in &task.canonical_files {
                let index = CanonicalRowGroupIndex::open(path)?;

                files.push((path.to_path_buf(), index.total_rows()));
            }

            self.register_canonical_task(
                CanonicalTaskKey {
                    dataset_id: task.dataset_id,
                    task_id: task.task_id,
                    event_kind: 1,
                },
                files,
            )?;

            self.depth_metadata.register_task_range(
                task.dataset_id,
                task.task_id,
                &task.events_path,
                start_ns,
                end_ns,
            )?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Canonical task registration
    // -------------------------------------------------------------------------

    fn register_canonical_task(
        &mut self,
        key: CanonicalTaskKey,
        files: Vec<(PathBuf, u64)>,
    ) -> Result<()> {
        let mut registered = HashSet::new();

        for (path, expected_rows) in &files {
            if !registered.insert(path.clone()) {
                return invalid(format!("duplicate canonical file: {}", path.display()));
            }

            let index = CanonicalRowGroupIndex::open(path)?;

            if index.total_rows() != *expected_rows {
                return invalid(format!(
                    "canonical file row-count mismatch: \
                     file={}, expected={}, actual={}",
                    path.display(),
                    expected_rows,
                    index.total_rows()
                ));
            }

            self.planner.register_row_groups(index.clone())?;
            self.decoder.register_row_groups(index)?;
        }

        let task_index = CanonicalRowIndex::new(files)?;

        self.planner.register_task(key, task_index)?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    pub fn registered_streams(&self) -> usize {
        self.streams.len()
    }

    pub fn registered_depth_tasks(&self) -> usize {
        self.depth_metadata.registered_tasks()
    }
    pub fn execute(
        &mut self,
        input: &MaterializationPartitionInput<'_>,
    ) -> Result<MaterializationPartitionResult> {
        self.execute_impl(input, None)
    }

    pub fn execute_with_progress(
        &mut self,
        input: &MaterializationPartitionInput<'_>,
        progress: &mut MergeWorkerProgress,
    ) -> Result<MaterializationPartitionResult> {
        self.execute_impl(input, Some(progress))
    }

    fn execute_impl(
        &mut self,
        input: &MaterializationPartitionInput<'_>,
        mut progress: Option<&mut MergeWorkerProgress>,
    ) -> Result<MaterializationPartitionResult> {
        if input.start_ns >= input.end_ns {
            return invalid("invalid materialization partition range");
        }

        let mut timeline = MaterializationTimelineReader::new(
            input.timeline_files.to_vec(),
            input.limits.reader_batch_size,
        )?;

        let mut windows = MaterializationWindowBuilder::new(input.limits.batch_limits);

        let output_dir = input
            .staging
            .join("timeline")
            .join(format!("partition-{:06}", input.partition_id));

        let mut writer = PhysicalParquetWriter::new(&output_dir, input.mode, input.parquet)?;

        let mut windows_processed = 0_u64;

        while let Some(reference) = timeline.next_record()? {
            if reference.event_timestamp_ns < input.start_ns
                || reference.event_timestamp_ns >= input.end_ns
            {
                return invalid(format!(
                    "timeline reference outside partition: \
                 timestamp={}, range=[{}, {})",
                    reference.event_timestamp_ns, input.start_ns, input.end_ns
                ));
            }

            if let Some(window) = windows.push(reference)? {
                self.materialize_window(&window, input.mode, &mut writer)?;
                windows_processed += 1;

                if let Some(reporter) = progress.as_deref_mut() {
                    reporter.maybe_update(writer.metrics().events_written);
                }
            }
        }

        if let Some(window) = windows.finish() {
            self.materialize_window(&window, input.mode, &mut writer)?;
            windows_processed += 1;

            if let Some(reporter) = progress.as_deref_mut() {
                reporter.maybe_update(writer.metrics().events_written);
            }
        }

        writer.finish()?;
        if let Some(reporter) = progress.as_deref_mut() {
            reporter.finish(writer.metrics().events_written);
        }
        if std::env::var_os("MARKETFORGE_PROFILE").is_some() {
            eprintln!(
                "[materialize-profile] partition={} windows={} \
         planning={:.3}s decoding={:.3}s \
         building={:.3}s writing={:.3}s \
         group_requests={} unique_groups={} repeated_groups={}
                cursor_openings={} cursor_evictions={} peak_open_cursors={}",
                input.partition_id,
                windows_processed,
                self.profile_planning.as_secs_f64(),
                self.profile_decoding.as_secs_f64(),
                self.profile_building.as_secs_f64(),
                self.profile_writing.as_secs_f64(),
                self.decoder.profile_row_group_requests(),
                self.decoder.profile_unique_row_groups(),
                self.decoder.profile_repeated_row_groups(),
                self.decoder.cursor_openings(),
                self.decoder.cursor_evictions(),
                self.decoder.peak_open_cursors(),
            );
        }

        Ok(MaterializationPartitionResult {
            partition_id: input.partition_id,
            start_ns: input.start_ns,
            end_ns: input.end_ns,
            windows_processed,
            references_read: timeline.records_read(),
            writer_metrics: writer.metrics().clone(),
        })
    }
    fn materialize_window(
        &mut self,
        window: &MaterializationWindow,
        mode: UnifiedMergeMode,
        writer: &mut PhysicalParquetWriter,
    ) -> Result<()> {
        if window.is_empty() {
            return Ok(());
        }

        let started = Instant::now();

        let plan = self.planner.plan(&window.references)?;

        self.profile_planning += started.elapsed();

        if plan.canonical_rows != window.canonical_rows {
            return invalid(format!(
                "materialization window row mismatch: \
             expected={}, planned={}",
                window.canonical_rows, plan.canonical_rows
            ));
        }

        let started = Instant::now();

        let resolved = self.decoder.decode(&plan)?;

        self.profile_decoding += started.elapsed();

        if resolved.len() != window.event_count() {
            return invalid("materialization event-count mismatch");
        }

        let building_started = Instant::now();

        let batch = match mode {
            UnifiedMergeMode::Trades => PhysicalTradeBatchBuilder::new().build(&resolved)?,

            UnifiedMergeMode::Depth => {
                let mut inputs = Vec::with_capacity(resolved.len());

                for event in &resolved {
                    let reference = &event.reference;

                    if reference.event_kind != 1 {
                        return invalid("trade event encountered in depth-only materialization");
                    }

                    let stream = self
                        .streams
                        .get(&(reference.dataset_id, reference.stream_rank))
                        .ok_or_else(|| {
                            invalid_error(format!(
                                "unregistered materialization stream: \
             dataset={}, rank={}",
                                reference.dataset_id, reference.stream_rank
                            ))
                        })?;

                    if stream.kind != UnifiedMergeStreamKind::Depth {
                        return invalid("depth event stream-kind mismatch");
                    }

                    let metadata = self.depth_metadata.resolve(reference)?;

                    inputs.push(PhysicalDepthEvent {
                        event,
                        metadata,
                        stream,
                    });
                }

                PhysicalDepthBatchBuilder::new().build(&inputs)?
            }

            UnifiedMergeMode::Combined => {
                let mut inputs = Vec::with_capacity(resolved.len());

                for event in &resolved {
                    let reference = &event.reference;

                    let stream = self
                        .streams
                        .get(&(reference.dataset_id, reference.stream_rank))
                        .ok_or_else(|| {
                            invalid_error(format!(
                                "unregistered materialization stream: dataset={}, rank={}",
                                reference.dataset_id, reference.stream_rank
                            ))
                        })?;

                    let metadata = match reference.event_kind {
                        0 => {
                            if stream.kind != UnifiedMergeStreamKind::Trade {
                                return invalid("trade event stream-kind mismatch");
                            }

                            None
                        }

                        1 => {
                            if stream.kind != UnifiedMergeStreamKind::Depth {
                                return invalid("depth event stream-kind mismatch");
                            }

                            Some(self.depth_metadata.resolve(reference)?)
                        }

                        _ => return invalid("unsupported materialization event kind"),
                    };

                    inputs.push(PhysicalCombinedEvent {
                        event,
                        stream,
                        depth_metadata: metadata,
                    });
                }

                PhysicalCombinedBatchBuilder::new().build(&inputs)?
            }
        };

        self.profile_building += building_started.elapsed();

        if let Some(batch) = batch {
            let started = Instant::now();

            writer.write_batch(&batch)?;

            self.profile_writing += started.elapsed();
        }

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
