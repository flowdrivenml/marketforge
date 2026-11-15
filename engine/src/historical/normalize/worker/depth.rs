use super::{
    integrity::{enforce_integrity, record_integrity_error},
    sink::DepthSink,
};
use crate::historical::normalize::boundary::{BoundaryBookState, DepthBoundaryTracker};
use crate::historical::normalize::progress::WorkerProgress;
use crate::shared::formats::depth::DepthEventBoundary;
use crate::{
    shared::formats::depth::{DepthCsvAdapter, DepthSnapshotAssembler},
    shared::io::decode::{CsvDecoder, XlsxDepthDecoder},
};
use std::io::Read;
use {
    super::decoded::with_source_reader, crate::historical::job::IntegrityPolicy,
    crate::historical::job::WorkTask, crate::historical::normalize::metrics::IntegrityCategory,
    crate::historical::normalize::metrics::IntegrityScope,
    crate::historical::normalize::metrics::ScopedIntegrityMetrics,
    crate::historical::normalize::metrics::TaskMetrics,
    crate::shared::domain::book::SequencePolicy, crate::shared::error::MarketForgeError,
    crate::shared::error::Result, crate::shared::formats::depth::DepthContext,
    crate::shared::formats::depth::DepthProcessor, crate::shared::io::source::JsonlDecoder,
    crate::shared::normalization::TargetSchema,
};

// -----------------------------------------------------------------------------
// Depth worker
// -----------------------------------------------------------------------------

pub fn process_depth_task_with_metrics<S: DepthSink>(
    task: &WorkTask,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    sequence_policy: SequencePolicy,
) -> Result<()> {
    let _limits = super::resource_scope(task);
    process_depth_task_with_metrics_impl(
        task,
        sink,
        metrics,
        policy,
        previous_job_integrity,
        sequence_policy,
        None,
    )
}

pub fn process_depth_task_with_metrics_and_progress<S: DepthSink>(
    task: &WorkTask,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    sequence_policy: SequencePolicy,
    progress: &mut WorkerProgress,
) -> Result<()> {
    let _limits = super::resource_scope(task);
    process_depth_task_with_metrics_impl(
        task,
        sink,
        metrics,
        policy,
        previous_job_integrity,
        sequence_policy,
        Some(progress),
    )
}

fn process_depth_task_with_metrics_impl<S: DepthSink>(
    task: &WorkTask,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    sequence_policy: SequencePolicy,
    mut progress: Option<&mut WorkerProgress>,
) -> Result<()> {
    let _limits = super::resource_scope(task);
    super::decoded::ensure_routed(task)?;
    super::begin_observation(task, metrics);
    // -------------------------------------------------------------------------
    // Validate depth normalization configuration
    // -------------------------------------------------------------------------

    if task.normalizations.is_empty()
        || task
            .normalizations
            .iter()
            .any(|normalization| normalization.target_schema != TargetSchema::Depth)
    {
        return Err(MarketForgeError::InvalidConfiguration(
            "depth task requires depth normalization configurations".to_owned(),
        ));
    }

    // -------------------------------------------------------------------------
    // Processing context
    // -------------------------------------------------------------------------

    let context = DepthContext {
        exchange: task.exchange,
        instrument_id: task.instrument_id,
        symbol: task.symbol.clone(),
        stream_id: task.stream_id.0.clone(),
    };

    let scope = IntegrityScope {
        task_id: task.task_id.0,
        stream_id: task.stream_id.0.clone(),
        source_file: task.input_path.to_string_lossy().into_owned(),
    };

    // -------------------------------------------------------------------------
    // Source reader
    // -------------------------------------------------------------------------

    let mut ordering =
        super::ordering::TimestampOrder::new(task, &scope, policy, previous_job_integrity)?;
    let result = with_source_reader(
        &task.input_path,
        task.source_compression,
        task.archive_member.as_deref(),
        |reader| {
            // -----------------------------------------------------------------
            // Initialize depth processor
            // -----------------------------------------------------------------

            let mut processor = DepthProcessor::new(
                &task.normalizations,
                task.instrument.clone(),
                context,
                sequence_policy,
            )?;

            // -----------------------------------------------------------------
            // Snapshot depth metadata
            // -----------------------------------------------------------------

            let snapshot_depth = task
                .raw_schema
                .get("order_book")
                .and_then(|value| value.get("snapshot_depth"));

            let bid_depth = snapshot_depth
                .and_then(|value| value.get("bids"))
                .and_then(|value| value.as_u64());

            let ask_depth = snapshot_depth
                .and_then(|value| value.get("asks"))
                .and_then(|value| value.as_u64());

            let depth_per_side = match (bid_depth, ask_depth) {
                (Some(bids), Some(asks)) if bids == asks => {
                    Some(u32::try_from(bids).map_err(|_| {
                        MarketForgeError::InvalidConfiguration(
                            "snapshot depth exceeds u32".to_owned(),
                        )
                    })?)
                }

                _ => None,
            };

            // -----------------------------------------------------------------
            // Initialize boundary tracker
            // -----------------------------------------------------------------

            let mut boundary_tracker = DepthBoundaryTracker::new(
                task.exchange,
                task.instrument_id,
                task.symbol.clone(),
                task.stream_id.0.clone(),
                sequence_policy,
                depth_per_side,
            );

            // -----------------------------------------------------------------
            // Source format dispatch
            // -----------------------------------------------------------------

            match super::super::resources::source_format(task)? {
                // -------------------------------------------------------------
                // CSV depth processing
                // -------------------------------------------------------------
                super::super::resources::SourceFormat::Csv => {
                    process_csv_depth(
                        reader,
                        task,
                        &mut processor,
                        &mut boundary_tracker,
                        sink,
                        metrics,
                        policy,
                        previous_job_integrity,
                        &scope,
                        &mut ordering,
                        progress.as_deref_mut(),
                    )?;
                }

                // -------------------------------------------------------------
                // JSONL depth processing
                // -------------------------------------------------------------
                super::super::resources::SourceFormat::Jsonl => {
                    let mut decoder = JsonlDecoder::new(reader);
                    let mut cached = super::decoded::take_json();

                    let mut source_record = 0u64;

                    // ---------------------------------------------------------
                    // Process JSONL source records
                    // ---------------------------------------------------------

                    loop {
                        if let Some(reporter) = progress.as_deref_mut() {
                            reporter.maybe_update(&metrics.counters);
                        }

                        let next = if let Some(records) = cached.as_mut() {
                            match records.next() {
                                Some(Ok((ordinal, value))) => {
                                    source_record = ordinal.checked_sub(1).ok_or_else(|| {
                                        MarketForgeError::InvalidConfiguration(
                                            "zero physical record ordinal".into(),
                                        )
                                    })?;
                                    Ok(Some(value))
                                }
                                Some(Err(error)) => return Err(error),
                                None => Ok(None),
                            }
                        } else {
                            decoder.next_json()
                        };
                        let record = match next {
                            Ok(Some(record)) => record,

                            Ok(None) => break,

                            // -------------------------------------------------
                            // JSON parsing failure
                            // -------------------------------------------------
                            Err(crate::shared::io::source::JsonlError::Source(source)) => {
                                return Err(crate::shared::historical::limits::source_error(
                                    task.input_path.clone(),
                                    source,
                                ));
                            }
                            Err(crate::shared::io::source::JsonlError::Record(error)) => {
                                source_record = source_record.checked_add(1).ok_or_else(|| {
                                    MarketForgeError::InvalidConfiguration(
                                        "source record ordinal overflow".into(),
                                    )
                                })?;

                                increment(&mut metrics.counters.records_read, "records_read")?;

                                // A corrupted message may contain missing
                                // book updates. Reconstruction is unsafe
                                // until another authoritative snapshot.
                                processor.invalidate_synchronization();
                                boundary_tracker.invalidate();

                                record_integrity_error(
                                    task.task_id.0,
                                    &scope,
                                    metrics,
                                    IntegrityCategory::ParseFailure,
                                    source_record,
                                    None,
                                    error.to_string(),
                                )?;

                                // Instrument identity cannot be established.
                                increment(
                                    &mut metrics.counters.records_rejected_unmatched,
                                    "records_rejected_unmatched",
                                )?;

                                enforce_integrity(
                                    policy,
                                    metrics,
                                    previous_job_integrity,
                                    IntegrityCategory::ParseFailure,
                                    &error.to_string(),
                                )?;

                                continue;
                            }
                        };

                        // -----------------------------------------------------
                        // Source record accounting
                        // -----------------------------------------------------

                        source_record = source_record.checked_add(1).ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration(
                                "source record ordinal overflow".into(),
                            )
                        })?;

                        increment(&mut metrics.counters.records_read, "records_read")?;

                        // -----------------------------------------------------
                        // Normalize and reconstruct depth
                        // -----------------------------------------------------

                        let result = processor.process_record_with_boundary(&record);
                        let event_timestamp_ns = processor
                            .observations()
                            .event_timestamp_ns
                            .or_else(|| processor.event_timestamp(&record));
                        observe_depth(
                            metrics,
                            &scope,
                            &processor,
                            result.as_ref().err(),
                            true,
                            event_timestamp_ns,
                        )?;
                        let outcome = match result {
                            Ok(outcome) => outcome,

                            // ---------------------------------------------
                            // Canonical integrity failure
                            // ---------------------------------------------
                            Err(MarketForgeError::RecordIntegrity { category, message }) => {
                                record_integrity_error(
                                    task.task_id.0,
                                    &scope,
                                    metrics,
                                    category,
                                    source_record,
                                    event_timestamp_ns,
                                    message.clone(),
                                )?;

                                // Temporary accounting convention:
                                // the processor does not yet report
                                // whether identity matching succeeded
                                // before the integrity failure.
                                increment(
                                    &mut metrics.counters.records_rejected_unmatched,
                                    "records_rejected_unmatched",
                                )?;

                                // Conservative recovery:
                                // any rejected depth message might
                                // contain state-changing information.
                                processor.invalidate_synchronization();
                                boundary_tracker.invalidate();

                                enforce_integrity(
                                    policy,
                                    metrics,
                                    previous_job_integrity,
                                    category,
                                    &message,
                                )?;

                                continue;
                            }

                            Err(error) => return Err(error),
                        };

                        // -----------------------------------------------------
                        // Accept complete canonical outcome
                        // -----------------------------------------------------

                        accept_outcome(
                            outcome,
                            source_record,
                            1,
                            &mut processor,
                            sink,
                            metrics,
                            &mut boundary_tracker,
                            &mut ordering,
                        )?;
                    }
                }
                super::super::resources::SourceFormat::Xlsx => {
                    process_xlsx_depth(
                        reader,
                        task,
                        &mut processor,
                        &mut boundary_tracker,
                        sink,
                        metrics,
                        policy,
                        previous_job_integrity,
                        &scope,
                        &mut ordering,
                        progress.as_deref_mut(),
                    )?;
                }
            }

            // -----------------------------------------------------------------
            // Capture final reconstructed book
            // -----------------------------------------------------------------

            boundary_tracker.set_final_state(BoundaryBookState::from_book(processor.book()));

            // -----------------------------------------------------------------
            // Finalize boundary metadata
            // -----------------------------------------------------------------

            let boundary_manifest = boundary_tracker.finish();

            sink.write_boundary(boundary_manifest)?;

            Ok(())
        },
    );

    // -------------------------------------------------------------------------
    // Task finalization
    // -------------------------------------------------------------------------

    let outcome = match result {
        Ok(()) => {
            increment(&mut metrics.counters.tasks_completed, "tasks_completed")?;

            metrics.counters.validate()?;

            Ok(())
        }

        Err(error) => {
            increment(&mut metrics.counters.tasks_failed, "tasks_failed")?;

            Err(error)
        }
    };

    if let Some(reporter) = progress {
        reporter.finish(&metrics.counters);
    }

    super::finish_observation(metrics);
    outcome
}

fn process_csv_depth<R: Read, S: DepthSink>(
    reader: R,
    task: &WorkTask,
    processor: &mut DepthProcessor,
    boundary_tracker: &mut DepthBoundaryTracker,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    scope: &IntegrityScope,
    ordering: &mut super::ordering::TimestampOrder<'_>,
    mut progress: Option<&mut WorkerProgress>,
) -> Result<()> {
    let mut decoder = CsvDecoder::with_headers(reader, false);
    let adapter = DepthCsvAdapter::new(&task.raw_schema)?;
    let mut assembler = DepthSnapshotAssembler::new();

    let mut source_ordinal = 0u64;
    let mut snapshot_rows = 0u64;
    let mut snapshot_timestamp = None;

    let cached = super::decoded::take_csv();
    let rows: Box<
        dyn Iterator<
                Item = (
                    Option<u64>,
                    std::result::Result<csv::ByteRecord, csv::Error>,
                ),
            > + '_,
    > = if let Some((_, rows)) = cached {
        Box::new(rows.map(|row| match row {
            Ok((ordinal, row)) => (Some(ordinal), Ok(row)),
            Err(e) => (None, Err(e)),
        }))
    } else {
        Box::new(decoder.records().map(|row| (None, row)))
    };
    for (physical_ordinal, row) in rows {
        if let Some(reporter) = progress.as_deref_mut() {
            reporter.maybe_update(&metrics.counters);
        }
        let previous_source_ordinal = source_ordinal;
        source_ordinal = if let Some(ordinal) = physical_ordinal {
            ordinal
        } else {
            source_ordinal.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("CSV source ordinal overflow".to_owned())
            })?
        };

        increment(&mut metrics.counters.records_read, "records_read")?;

        // -------------------------------------------------------------
        // Decode source row
        // -------------------------------------------------------------

        let record = match row {
            Ok(row) => adapter.adapt(&row),

            Err(error) if error.is_io_error() => {
                return Err(super::integrity::csv_source_error(task, error));
            }
            Err(error) => Err(MarketForgeError::RecordIntegrity {
                category: IntegrityCategory::ParseFailure,
                message: error.to_string(),
            }),
        };

        let record = match record {
            Ok(record) => record,

            Err(error) => {
                processor.invalidate_synchronization();
                boundary_tracker.invalidate();

                // Previously accumulated snapshot rows were matched but
                // cannot be committed because the snapshot is incomplete.
                reject_snapshot_rows(metrics, snapshot_rows)?;

                assembler = DepthSnapshotAssembler::new();
                snapshot_rows = 0;
                snapshot_timestamp = None;

                record_integrity_error(
                    task.task_id.0,
                    scope,
                    metrics,
                    IntegrityCategory::ParseFailure,
                    source_ordinal,
                    None,
                    error.to_string(),
                )?;

                increment(
                    &mut metrics.counters.records_rejected_unmatched,
                    "records_rejected_unmatched",
                )?;

                enforce_integrity(
                    policy,
                    metrics,
                    previous_job_integrity,
                    IntegrityCategory::ParseFailure,
                    &error.to_string(),
                )?;

                continue;
            }
        };

        // -------------------------------------------------------------
        // Classify source row
        // -------------------------------------------------------------

        let is_snapshot = match processor.is_scalar_snapshot_row(&record) {
            Ok(value) => value,
            Err(error) => {
                let timestamp = processor.event_timestamp(&record);
                metrics.scoped_integrity.record(
                    scope,
                    IntegrityCategory::ParseFailure,
                    false,
                    timestamp,
                )?;
                if let MarketForgeError::RecordIntegrity { category, message } = &error {
                    record_integrity_error(
                        task.task_id.0,
                        scope,
                        metrics,
                        *category,
                        source_ordinal,
                        timestamp,
                        message.clone(),
                    )?;
                    increment(
                        &mut metrics.counters.records_rejected_unmatched,
                        "records_rejected_unmatched",
                    )?;
                }
                return Err(error);
            }
        };

        if is_snapshot {
            let result = processor.extract_scalar_snapshot_row(&record);
            let timestamp = result
                .as_ref()
                .ok()
                .map(|v| v.0)
                .or_else(|| processor.event_timestamp(&record));
            metrics.scoped_integrity.record(
                scope,
                IntegrityCategory::ParseFailure,
                false,
                timestamp,
            )?;
            let (timestamp_ns, sequence, side, level) = match result {
                Ok(row) => {
                    metrics.scoped_integrity.record(
                        scope,
                        IntegrityCategory::InvalidRecord,
                        false,
                        timestamp,
                    )?;
                    row
                }
                Err(MarketForgeError::RecordIntegrity { category, message }) => {
                    record_integrity_error(
                        task.task_id.0,
                        scope,
                        metrics,
                        category,
                        source_ordinal,
                        timestamp,
                        message.clone(),
                    )?;
                    increment(
                        &mut metrics.counters.records_rejected_unmatched,
                        "records_rejected_unmatched",
                    )?;
                    return Err(MarketForgeError::RecordIntegrity { category, message });
                }
                Err(error) => return Err(error),
            };

            assembler.push(timestamp_ns, sequence, side, level)?;

            snapshot_timestamp = Some(timestamp_ns);
            snapshot_rows = snapshot_rows.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("snapshot row counter overflow".into())
            })?;

            continue;
        }

        // -------------------------------------------------------------
        // Flush pending snapshot before relative updates
        // -------------------------------------------------------------

        // Decoding this physical update succeeded even if pending assembly fails.
        metrics.scoped_integrity.record(
            scope,
            IntegrityCategory::ParseFailure,
            false,
            processor.event_timestamp(&record),
        )?;
        if assembler.has_pending() {
            let finished = assembler.finish();
            if let Err(MarketForgeError::RecordIntegrity { category, message }) = &finished {
                record_snapshot_failure(
                    task,
                    scope,
                    metrics,
                    *category,
                    previous_source_ordinal,
                    snapshot_rows,
                    snapshot_timestamp,
                    message.clone(),
                )?;
            }
            let snapshot = finished?.ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "pending snapshot unexpectedly missing".to_owned(),
                )
            })?;

            let timestamp = Some(snapshot.timestamp_ns);
            let result = processor.process_assembled_snapshot(snapshot);
            observe_depth(
                metrics,
                scope,
                processor,
                result.as_ref().err(),
                false,
                timestamp,
            )?;
            if let Err(MarketForgeError::RecordIntegrity { category, message }) = &result {
                record_snapshot_failure(
                    task,
                    scope,
                    metrics,
                    *category,
                    previous_source_ordinal,
                    snapshot_rows,
                    timestamp,
                    message.clone(),
                )?;
            }
            let outcome = result?;

            accept_outcome(
                outcome,
                previous_source_ordinal,
                snapshot_rows,
                processor,
                sink,
                metrics,
                boundary_tracker,
                ordering,
            )?;

            snapshot_rows = 0;
            snapshot_timestamp = None;
        }

        // -------------------------------------------------------------
        // Apply relative update
        // -------------------------------------------------------------

        let result = processor.process_scalar_relative_update(&record);
        let event_timestamp_ns = processor
            .observations()
            .event_timestamp_ns
            .or_else(|| processor.event_timestamp(&record));
        observe_depth(
            metrics,
            scope,
            processor,
            result.as_ref().err(),
            false,
            event_timestamp_ns,
        )?;
        let outcome = match result {
            Ok(outcome) => outcome,

            Err(MarketForgeError::RecordIntegrity { category, message }) => {
                processor.invalidate_synchronization();
                boundary_tracker.invalidate();

                record_integrity_error(
                    task.task_id.0,
                    scope,
                    metrics,
                    category,
                    source_ordinal,
                    event_timestamp_ns,
                    message.clone(),
                )?;

                increment(
                    &mut metrics.counters.records_rejected_unmatched,
                    "records_rejected_unmatched",
                )?;

                enforce_integrity(policy, metrics, previous_job_integrity, category, &message)?;

                continue;
            }

            Err(error) => return Err(error),
        };

        accept_outcome(
            outcome,
            source_ordinal,
            1,
            processor,
            sink,
            metrics,
            boundary_tracker,
            ordering,
        )?;
    }

    // -------------------------------------------------------------
    // Flush snapshot at EOF
    // -------------------------------------------------------------

    if assembler.has_pending() {
        processor.reset_observations();
        let mut event_timestamp_ns = snapshot_timestamp;
        let result = assembler
            .finish()
            .and_then(|snapshot| {
                snapshot.ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "pending snapshot unexpectedly missing".to_owned(),
                    )
                })
            })
            .and_then(|snapshot| {
                event_timestamp_ns = Some(snapshot.timestamp_ns);
                processor.process_assembled_snapshot(snapshot)
            });
        observe_depth(
            metrics,
            scope,
            processor,
            result.as_ref().err(),
            false,
            event_timestamp_ns,
        )?;

        match result {
            Ok(outcome) => {
                accept_outcome(
                    outcome,
                    source_ordinal,
                    snapshot_rows,
                    processor,
                    sink,
                    metrics,
                    boundary_tracker,
                    ordering,
                )?;
            }

            Err(MarketForgeError::RecordIntegrity { category, message }) => {
                processor.invalidate_synchronization();
                boundary_tracker.invalidate();

                // All pending rows belonged to the failed snapshot.
                reject_snapshot_rows(metrics, snapshot_rows)?;

                metrics.integrity.record(
                    crate::historical::normalize::metrics::IntegrityDiagnostic {
                        category,
                        task_id: task.task_id.0,
                        source_record: Some(source_ordinal),
                        event_timestamp_ns,
                        message: message.clone(),
                    },
                )?;

                metrics
                    .scoped_integrity
                    .record(scope, category, true, event_timestamp_ns)?;

                enforce_integrity(policy, metrics, previous_job_integrity, category, &message)?;
            }

            Err(error) => return Err(error),
        }
    }

    Ok(())
}

fn accept_outcome<S: DepthSink>(
    mut outcome: crate::shared::formats::depth::DepthProcessingOutcome,
    source_ordinal: u64,
    source_rows: u64,
    processor: &mut DepthProcessor,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    boundary_tracker: &mut DepthBoundaryTracker,
    ordering: &mut super::ordering::TimestampOrder<'_>,
) -> Result<()> {
    if source_rows == 0 {
        return Err(MarketForgeError::InvalidConfiguration(
            "accepted depth outcome must contain at least one source row".to_owned(),
        ));
    }

    outcome.source.source_event_ordinal = Some(source_ordinal);
    let timestamp = outcome.source.event_timestamp_ns;
    if let Err(error) = ordering.check(metrics, source_ordinal, timestamp) {
        processor.invalidate_synchronization();
        boundary_tracker.invalidate();
        reject_snapshot_rows(metrics, source_rows)?;
        return Err(error);
    }

    // ---------------------------------------------------------
    // Validate canonical outcome
    // ---------------------------------------------------------

    let event_count = u64::try_from(outcome.events.len()).map_err(|_| {
        MarketForgeError::InvalidConfiguration("depth event count overflow".to_owned())
    })?;

    if outcome.boundary == DepthEventBoundary::Initialization && outcome.events.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "depth initialization produced no canonical levels".to_owned(),
        ));
    }

    let first_timestamp = outcome
        .events
        .first()
        .map(|event| event.envelope.event_timestamp_ns);

    let last_timestamp = outcome
        .events
        .last()
        .map(|event| event.envelope.event_timestamp_ns);

    // ---------------------------------------------------------
    // Prevalidate counter capacity
    // ---------------------------------------------------------

    let next_records_matched = metrics
        .counters
        .records_matched
        .checked_add(source_rows)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("records_matched counter overflow".to_owned())
        })?;

    let next_records_processed = metrics
        .counters
        .records_processed
        .checked_add(source_rows)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("records_processed counter overflow".to_owned())
        })?;

    let next_events_normalized = metrics
        .counters
        .events_normalized
        .checked_add(event_count)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("events_normalized counter overflow".to_owned())
        })?;

    let next_events_written = metrics
        .counters
        .events_written
        .checked_add(event_count)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("events_written counter overflow".to_owned())
        })?;

    // ---------------------------------------------------------
    // Capture reconstruction boundary
    // ---------------------------------------------------------

    let source_metadata = outcome.source.clone();
    let event_boundary = outcome.boundary;

    let initial_state = if event_boundary == DepthEventBoundary::Initialization {
        BoundaryBookState::from_book(processor.book())
    } else {
        None
    };

    // ---------------------------------------------------------
    // Commit complete source event
    // ---------------------------------------------------------

    let accepted = metrics
        .accepted_depth_events
        .checked_add(1)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("accepted depth events overflow".into())
        })?;
    sink.write_outcome(outcome)?;
    if let Some(source) = metrics.source.as_mut() {
        source.observe(timestamp);
    }
    metrics.accepted_depth_events = accepted;
    ordering.accept(timestamp);

    boundary_tracker.record_accepted(&source_metadata, event_boundary, initial_state);

    // ---------------------------------------------------------
    // Commit metrics
    // ---------------------------------------------------------

    metrics.counters.records_matched = next_records_matched;
    metrics.counters.records_processed = next_records_processed;

    metrics.counters.events_normalized = next_events_normalized;
    metrics.counters.events_written = next_events_written;

    if let Some(timestamp) = first_timestamp {
        metrics.record_timestamp(timestamp);
    }

    if let Some(timestamp) = last_timestamp {
        metrics.record_timestamp(timestamp);
    }

    Ok(())
}
// -----------------------------------------------------------------------------
// Counter helper
// -----------------------------------------------------------------------------

fn reject_snapshot_rows(metrics: &mut TaskMetrics, count: u64) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    // Calculate both counters before modifying either.
    let next_rejected = metrics
        .counters
        .records_rejected
        .checked_add(count)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "snapshot rejected-record counter overflow".to_owned(),
            )
        })?;

    let next_matched = metrics
        .counters
        .records_matched
        .checked_add(count)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "snapshot matched-record counter overflow".to_owned(),
            )
        })?;

    // Commit both counters together.
    metrics.counters.records_rejected = next_rejected;
    metrics.counters.records_matched = next_matched;

    Ok(())
}

fn increment(counter: &mut u64, name: &str) -> Result<()> {
    *counter = counter.checked_add(1).ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!("processing counter overflow: {name}"))
    })?;

    Ok(())
}

fn process_xlsx_depth<R: Read, S: DepthSink>(
    mut reader: R,
    task: &WorkTask,
    processor: &mut DepthProcessor,
    boundary_tracker: &mut DepthBoundaryTracker,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    scope: &IntegrityScope,
    ordering: &mut super::ordering::TimestampOrder<'_>,
    mut progress: Option<&mut WorkerProgress>,
) -> Result<()> {
    use std::io::Cursor;

    // -------------------------------------------------------------------------
    // Buffer XLSX workbook
    // -------------------------------------------------------------------------

    let limits = crate::shared::historical::limits::current().unwrap_or_default();
    let bytes = crate::shared::io::bounded::read_bounded(
        &mut reader,
        limits.workbook_bytes,
        "workbook input",
    )
    .map_err(|error| {
        crate::shared::historical::limits::source_error(task.input_path.clone(), error)
    })?;

    // -------------------------------------------------------------------------
    // Decode and chronologically sort snapshots
    // -------------------------------------------------------------------------

    let records =
        XlsxDepthDecoder::decode_observed(Cursor::new(bytes), |ordinal, timestamp, error| {
            increment(&mut metrics.counters.records_read, "records_read")?;
            if let Some(error) = error {
                record_integrity_error(
                    task.task_id.0,
                    scope,
                    metrics,
                    IntegrityCategory::ParseFailure,
                    ordinal,
                    timestamp,
                    error.to_string(),
                )?;
                increment(
                    &mut metrics.counters.records_rejected_unmatched,
                    "records_rejected_unmatched",
                )?;
            } else {
                metrics.scoped_integrity.record(
                    scope,
                    IntegrityCategory::ParseFailure,
                    false,
                    timestamp,
                )?;
            }
            Ok(())
        })
        .map_err(|error| match error {
            MarketForgeError::RecordIntegrity { .. } => MarketForgeError::ArchiveOpen {
                path: task.input_path.clone(),
                message: format!("unrecoverable XLSX workbook/row error: {error}"),
            },
            error => error,
        })?;

    // XLSX source ordinals refer to original worksheet positions.
    // Records are already sorted by (timestamp, source_ordinal).

    // -------------------------------------------------------------------------
    // Process snapshots
    // -------------------------------------------------------------------------

    for (index, source_record) in records.into_iter().enumerate() {
        if let Some(reporter) = progress.as_deref_mut() {
            reporter.maybe_update(&metrics.counters);
        }

        let event_ordinal = u64::try_from(index)
            .map_err(|_| {
                MarketForgeError::InvalidConfiguration("XLSX event ordinal overflow".to_owned())
            })?
            .checked_add(1)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("XLSX event ordinal overflow".to_owned())
            })?;

        let source_ordinal = source_record.source_ordinal;

        let result = processor.process_record_with_boundary(&source_record.record);
        let event_timestamp_ns = processor
            .observations()
            .event_timestamp_ns
            .or_else(|| processor.event_timestamp(&source_record.record));
        observe_depth(
            metrics,
            scope,
            processor,
            result.as_ref().err(),
            false,
            event_timestamp_ns,
        )?;
        let outcome = match result {
            Ok(outcome) => outcome,

            Err(MarketForgeError::RecordIntegrity { category, message }) => {
                // -------------------------------------------------------------
                // Reject malformed snapshot
                // -------------------------------------------------------------

                processor.invalidate_synchronization();
                boundary_tracker.invalidate();

                record_integrity_error(
                    task.task_id.0,
                    scope,
                    metrics,
                    category,
                    source_ordinal,
                    event_timestamp_ns,
                    message.clone(),
                )?;

                increment(
                    &mut metrics.counters.records_rejected_unmatched,
                    "records_rejected_unmatched",
                )?;

                enforce_integrity(policy, metrics, previous_job_integrity, category, &message)?;

                continue;
            }

            Err(error) => return Err(error),
        };

        // ---------------------------------------------------------------------
        // Accept canonical snapshot
        // ---------------------------------------------------------------------

        accept_outcome(
            outcome,
            event_ordinal,
            1,
            processor,
            sink,
            metrics,
            boundary_tracker,
            ordering,
        )?;
    }

    Ok(())
}

fn observe_depth(
    metrics: &mut TaskMetrics,
    scope: &IntegrityScope,
    processor: &DepthProcessor,
    error: Option<&MarketForgeError>,
    parsed: bool,
    timestamp: Option<i64>,
) -> Result<()> {
    let mut trace = processor.observations().clone();
    if parsed {
        trace.passed(IntegrityCategory::ParseFailure);
    }
    trace.event_timestamp_ns = timestamp;
    super::integrity::record_trace(metrics, scope, &trace, error)
}
fn record_snapshot_failure(
    task: &WorkTask,
    scope: &IntegrityScope,
    metrics: &mut TaskMetrics,
    category: IntegrityCategory,
    source_ordinal: u64,
    rows: u64,
    timestamp: Option<i64>,
    message: String,
) -> Result<()> {
    reject_snapshot_rows(metrics, rows)?;
    metrics
        .integrity
        .record(crate::historical::normalize::metrics::IntegrityDiagnostic {
            category,
            task_id: task.task_id.0,
            source_record: Some(source_ordinal),
            event_timestamp_ns: timestamp,
            message,
        })?;
    metrics
        .scoped_integrity
        .record(scope, category, true, timestamp)
}
