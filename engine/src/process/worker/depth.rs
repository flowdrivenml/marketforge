use super::{
    integrity::{enforce_integrity, record_integrity_error},
    sink::DepthSink,
};
use crate::formats::depth::DepthEventBoundary;
use crate::process::boundary::{BoundaryBookState, DepthBoundaryTracker};
use crate::{
    book::SequencePolicy,
    error::{MarketForgeError, Result},
    formats::depth::{DepthContext, DepthProcessor},
    job::{IntegrityPolicy, TargetSchema, WorkTask},
    process::metrics::{IntegrityCategory, IntegrityScope, ScopedIntegrityMetrics, TaskMetrics},
    source::JsonlDecoder,
    source::with_source_reader,
};
use std::io::Read;

use crate::{
    decode::{CsvDecoder, XlsxDepthDecoder},
    formats::depth::{DepthCsvAdapter, DepthSnapshotAssembler},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepthSourceFormat {
    Jsonl,
    Csv,
    Xlsx,
}

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

            match depth_source_format(task)? {
                // -------------------------------------------------------------
                // CSV depth processing
                // -------------------------------------------------------------
                DepthSourceFormat::Csv => {
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
                    )?;
                }

                // -------------------------------------------------------------
                // JSONL depth processing
                // -------------------------------------------------------------
                DepthSourceFormat::Jsonl => {
                    let mut decoder = JsonlDecoder::new(reader);

                    let mut source_record = 0u64;

                    // ---------------------------------------------------------
                    // Process JSONL source records
                    // ---------------------------------------------------------

                    loop {
                        let record = match decoder.next_json() {
                            Ok(Some(record)) => record,

                            Ok(None) => break,

                            // -------------------------------------------------
                            // JSON parsing failure
                            // -------------------------------------------------
                            Err(error) => {
                                source_record += 1;

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

                        source_record += 1;

                        increment(&mut metrics.counters.records_read, "records_read")?;

                        // -----------------------------------------------------
                        // Normalize and reconstruct depth
                        // -----------------------------------------------------

                        let outcome = match processor.process_record_with_boundary(&record) {
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
                                    None,
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
                            &processor,
                            sink,
                            metrics,
                            &mut boundary_tracker,
                        )?;
                    }
                }
                DepthSourceFormat::Xlsx => {
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

    match result {
        Ok(()) => {
            increment(&mut metrics.counters.tasks_completed, "tasks_completed")?;

            metrics.counters.validate()?;

            Ok(())
        }

        Err(error) => {
            increment(&mut metrics.counters.tasks_failed, "tasks_failed")?;

            Err(error)
        }
    }
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
) -> Result<()> {
    let mut decoder = CsvDecoder::with_headers(reader, false);
    let adapter = DepthCsvAdapter::new(&task.raw_schema)?;
    let mut assembler = DepthSnapshotAssembler::new();

    let mut source_ordinal = 0u64;
    let mut snapshot_rows = 0u64;

    for row in decoder.records() {
        source_ordinal = source_ordinal.checked_add(1).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("CSV source ordinal overflow".to_owned())
        })?;

        increment(&mut metrics.counters.records_read, "records_read")?;

        // -------------------------------------------------------------
        // Decode source row
        // -------------------------------------------------------------

        let record = match row {
            Ok(row) => adapter.adapt(&row),

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

        let is_snapshot = processor.is_scalar_snapshot_row(&record)?;

        if is_snapshot {
            let (timestamp_ns, sequence, side, level) =
                processor.extract_scalar_snapshot_row(&record)?;

            assembler.push(timestamp_ns, sequence, side, level)?;

            snapshot_rows += 1;

            continue;
        }

        // -------------------------------------------------------------
        // Flush pending snapshot before relative updates
        // -------------------------------------------------------------

        if assembler.has_pending() {
            let snapshot = assembler.finish()?.ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "pending snapshot unexpectedly missing".to_owned(),
                )
            })?;

            let outcome = processor.process_assembled_snapshot(snapshot)?;

            accept_outcome(
                outcome,
                source_ordinal - 1,
                snapshot_rows,
                processor,
                sink,
                metrics,
                boundary_tracker,
            )?;

            snapshot_rows = 0;
        }

        // -------------------------------------------------------------
        // Apply relative update
        // -------------------------------------------------------------

        let outcome = match processor.process_scalar_relative_update(&record) {
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
                    None,
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
        )?;
    }

    // -------------------------------------------------------------
    // Flush snapshot at EOF
    // -------------------------------------------------------------

    if assembler.has_pending() {
        let result = assembler
            .finish()
            .and_then(|snapshot| {
                snapshot.ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "pending snapshot unexpectedly missing".to_owned(),
                    )
                })
            })
            .and_then(|snapshot| processor.process_assembled_snapshot(snapshot));

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
                )?;
            }

            Err(MarketForgeError::RecordIntegrity { category, message }) => {
                processor.invalidate_synchronization();
                boundary_tracker.invalidate();

                // All pending rows belonged to the failed snapshot.
                reject_snapshot_rows(metrics, snapshot_rows)?;

                metrics
                    .integrity
                    .record(crate::process::metrics::IntegrityDiagnostic {
                        category,
                        task_id: task.task_id.0,
                        source_record: Some(source_ordinal),
                        event_timestamp_ns: None,
                        message: message.clone(),
                    })?;

                metrics
                    .scoped_integrity
                    .record(scope, category, true, None)?;

                enforce_integrity(policy, metrics, previous_job_integrity, category, &message)?;
            }

            Err(error) => return Err(error),
        }
    }

    Ok(())
}

fn accept_outcome<S: DepthSink>(
    mut outcome: crate::formats::depth::DepthProcessingOutcome,
    source_ordinal: u64,
    source_rows: u64,
    processor: &DepthProcessor,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    boundary_tracker: &mut DepthBoundaryTracker,
) -> Result<()> {
    if source_rows == 0 {
        return Err(MarketForgeError::InvalidConfiguration(
            "accepted depth outcome must contain at least one source row".to_owned(),
        ));
    }

    outcome.source.source_event_ordinal = Some(source_ordinal);

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

    sink.write_outcome(outcome)?;

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

fn depth_source_format(task: &WorkTask) -> Result<DepthSourceFormat> {
    let fields = task
        .raw_schema
        .get("fields")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("depth raw schema missing fields".to_owned())
        })?;

    // XLSX workbooks must be identified explicitly.
    //
    // Non-positional fields alone are insufficient because
    // JSONL sources also use non-positional schemas.
    let is_xlsx = task.raw_schema.get("workbook_sheets").is_some();

    if is_xlsx {
        return Ok(DepthSourceFormat::Xlsx);
    }

    let positional = fields
        .iter()
        .filter(|field| field.get("position").is_some())
        .count();

    if positional == fields.len() && !fields.is_empty() {
        return Ok(DepthSourceFormat::Csv);
    }

    if positional == 0 {
        return Ok(DepthSourceFormat::Jsonl);
    }

    Err(MarketForgeError::InvalidConfiguration(
        "depth raw schema mixes positional and non-positional fields".to_owned(),
    ))
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
) -> Result<()> {
    use std::io::Cursor;

    // -------------------------------------------------------------------------
    // Buffer XLSX workbook
    // -------------------------------------------------------------------------

    let mut bytes = Vec::new();

    reader
        .read_to_end(&mut bytes)
        .map_err(|error| MarketForgeError::SourceRead {
            path: task.input_path.clone(),
            source: error,
        })?;

    // -------------------------------------------------------------------------
    // Decode and chronologically sort snapshots
    // -------------------------------------------------------------------------

    let records = XlsxDepthDecoder::decode(Cursor::new(bytes))?;

    // XLSX source ordinals refer to original worksheet positions.
    // Records are already sorted by (timestamp, source_ordinal).

    // -------------------------------------------------------------------------
    // Process snapshots
    // -------------------------------------------------------------------------

    for (index, source_record) in records.into_iter().enumerate() {
        increment(&mut metrics.counters.records_read, "records_read")?;

        let event_ordinal = u64::try_from(index)
            .map_err(|_| {
                MarketForgeError::InvalidConfiguration("XLSX event ordinal overflow".to_owned())
            })?
            .checked_add(1)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("XLSX event ordinal overflow".to_owned())
            })?;

        let source_ordinal = source_record.source_ordinal;

        let outcome = match processor.process_record_with_boundary(&source_record.record) {
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
                    None,
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
        )?;
    }

    Ok(())
}
