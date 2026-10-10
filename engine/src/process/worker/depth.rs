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
            let mut decoder = JsonlDecoder::new(reader);

            let mut processor = DepthProcessor::new(
                &task.normalizations,
                task.instrument.clone(),
                context,
                sequence_policy,
            )?;

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

            let mut boundary_tracker = DepthBoundaryTracker::new(
                task.exchange,
                task.instrument_id,
                task.symbol.clone(),
                task.stream_id.0.clone(),
                sequence_policy,
                depth_per_side,
            );

            let mut source_record = 0u64;

            // -----------------------------------------------------------------
            // Process source records
            // -----------------------------------------------------------------

            loop {
                let record = match decoder.next_json() {
                    Ok(Some(record)) => record,

                    Ok(None) => break,

                    // ---------------------------------------------------------
                    // JSON parsing failure
                    // ---------------------------------------------------------
                    Err(error) => {
                        source_record += 1;

                        increment(&mut metrics.counters.records_read, "records_read")?;

                        // A corrupted message may contain missing book updates.
                        // Reconstruction is unsafe until another snapshot.
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

                // -------------------------------------------------------------
                // Source record accounting
                // -------------------------------------------------------------

                source_record += 1;

                increment(&mut metrics.counters.records_read, "records_read")?;

                // -------------------------------------------------------------
                // Normalize and reconstruct depth
                // -------------------------------------------------------------

                let mut outcome = match processor.process_record_with_boundary(&record) {
                    Ok(outcome) => outcome,

                    // ---------------------------------------------------------
                    // Canonical integrity failure
                    // ---------------------------------------------------------
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
                        // the processor does not yet report whether identity
                        // matching succeeded before the integrity failure.
                        increment(
                            &mut metrics.counters.records_rejected_unmatched,
                            "records_rejected_unmatched",
                        )?;

                        // Conservative recovery: any rejected depth message
                        // might have contained state-changing information.
                        // Require a new authoritative snapshot.
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
                outcome.source.source_event_ordinal = Some(source_record);

                // -------------------------------------------------------------
                // Validate complete canonical outcome
                // -------------------------------------------------------------

                let event_count = u64::try_from(outcome.events.len()).map_err(|_| {
                    MarketForgeError::InvalidConfiguration("depth event count overflow".to_owned())
                })?;

                if outcome.boundary == DepthEventBoundary::Initialization
                    && outcome.events.is_empty()
                {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "depth initialization produced no canonical levels".to_owned(),
                    ));
                }

                // Preserve timestamps before transferring ownership to sink.
                let first_timestamp = outcome
                    .events
                    .first()
                    .map(|event| event.envelope.event_timestamp_ns);

                let last_timestamp = outcome
                    .events
                    .last()
                    .map(|event| event.envelope.event_timestamp_ns);

                // -------------------------------------------------------------
                // Prevalidate counter capacity
                // -------------------------------------------------------------

                let next_records_matched = metrics
                    .counters
                    .records_matched
                    .checked_add(1)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "records_matched counter overflow".to_owned(),
                        )
                    })?;

                let next_records_processed = metrics
                    .counters
                    .records_processed
                    .checked_add(1)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "records_processed counter overflow".to_owned(),
                        )
                    })?;

                let next_events_normalized = metrics
                    .counters
                    .events_normalized
                    .checked_add(event_count)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "events_normalized counter overflow".to_owned(),
                        )
                    })?;

                let next_events_written = metrics
                    .counters
                    .events_written
                    .checked_add(event_count)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "events_written counter overflow".to_owned(),
                        )
                    })?;

                // -------------------------------------------------------------
                // Write complete source-event outcome
                // -------------------------------------------------------------

                // The sink owns SegmentTracker and records initialization
                // boundaries alongside canonical event offsets.
                //
                // Successful return means the entire outcome was accepted.
                // Disk persistence is finalized separately.
                // Preserve source metadata before transferring ownership to the sink.
                let source_metadata = outcome.source.clone();
                let event_boundary = outcome.boundary;

                // Capture the complete book only when initializing a segment.
                let initial_state = if event_boundary == DepthEventBoundary::Initialization {
                    BoundaryBookState::from_book(processor.book())
                } else {
                    None
                };

                // Commit the complete source event.
                sink.write_outcome(outcome)?;

                // Update boundary metadata only after successful sink acceptance.
                boundary_tracker.record_accepted(&source_metadata, event_boundary, initial_state);

                // -------------------------------------------------------------
                // Commit metrics after successful sink acceptance
                // -------------------------------------------------------------

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
            }

            // Capture the final valid reconstructed book once.
            boundary_tracker.set_final_state(BoundaryBookState::from_book(processor.book()));

            // Finalize boundary metadata.
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
// -----------------------------------------------------------------------------
// Counter helper
// -----------------------------------------------------------------------------

fn increment(counter: &mut u64, name: &str) -> Result<()> {
    *counter = counter.checked_add(1).ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!("processing counter overflow: {name}"))
    })?;

    Ok(())
}
