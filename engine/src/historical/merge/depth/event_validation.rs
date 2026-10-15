use crate::{
    error::{MarketForgeError, Result},
    formats::depth::DepthEventBoundary,
};

use super::{depth_cursor::DepthEventCursor, inventory::DepthTaskInventory};

#[derive(Debug, Clone, Default)]
pub struct DepthEventValidation {
    pub dataset_id: u64,
    pub task_id: u64,

    pub events: u64,
    pub canonical_rows: u64,
    pub zero_row_events: u64,
    pub initialization_events: u64,
    pub timestamp_regressions: u64,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,
}

pub fn validate_depth_events(task: &DepthTaskInventory) -> Result<DepthEventValidation> {
    let mut cursor = DepthEventCursor::open(&task.events_path)?;

    let mut report = DepthEventValidation {
        dataset_id: task.dataset_id,
        task_id: task.task_id,
        ..Default::default()
    };

    let mut expected_offset = 0_u64;
    let mut previous_ordinal = None;
    let mut previous_timestamp = None;
    let mut segment_index = 0_usize;

    while let Some(event) = cursor.next_event()? {
        // -------------------------------------------------------------
        // Event ordinal
        // -------------------------------------------------------------

        if let Some(previous) = previous_ordinal {
            if event.event_ordinal <= previous {
                return invalid(format!(
                    "dataset {} task {}: non-increasing event ordinal",
                    task.dataset_id, task.task_id
                ));
            }
        }

        previous_ordinal = Some(event.event_ordinal);

        // -------------------------------------------------------------
        // Canonical row offsets
        // -------------------------------------------------------------

        if event.canonical_row_offset != expected_offset {
            return invalid(format!(
                "dataset {} task {}: expected canonical offset {}, received {}",
                task.dataset_id, task.task_id, expected_offset, event.canonical_row_offset,
            ));
        }

        expected_offset = expected_offset
            .checked_add(event.canonical_row_count)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "depth event canonical offset overflow".to_owned(),
                )
            })?;

        if expected_offset > task.canonical_rows {
            return invalid(format!(
                "dataset {} task {}: event index exceeds canonical rows",
                task.dataset_id, task.task_id
            ));
        }

        // -------------------------------------------------------------
        // Timestamp ordering
        // -------------------------------------------------------------

        if let Some(previous) = previous_timestamp {
            if event.event_timestamp_ns < previous {
                report.timestamp_regressions += 1;
            }
        }

        previous_timestamp = Some(event.event_timestamp_ns);

        if report.first_timestamp_ns.is_none() {
            report.first_timestamp_ns = Some(event.event_timestamp_ns);
        }

        report.last_timestamp_ns = Some(event.event_timestamp_ns);

        // -------------------------------------------------------------
        // Initialization and reconstruction segments
        // -------------------------------------------------------------

        if event.event_boundary == DepthEventBoundary::Initialization {
            let segment = task.segments.segments.get(segment_index).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "dataset {} task {}: initialization without segment",
                    task.dataset_id, task.task_id
                ))
            })?;

            if segment.initial_event_offset != event.canonical_row_offset
                || segment.initial_level_count != event.canonical_row_count
                || segment.start_timestamp_ns != event.event_timestamp_ns
            {
                return invalid(format!(
                    "dataset {} task {}: initialization/segment mismatch",
                    task.dataset_id, task.task_id
                ));
            }

            segment_index += 1;
            report.initialization_events += 1;
        }

        // -------------------------------------------------------------
        // Counters
        // -------------------------------------------------------------

        if event.canonical_row_count == 0 {
            report.zero_row_events += 1;
        }

        report.events += 1;
    }

    // -----------------------------------------------------------------
    // Final validation
    // -----------------------------------------------------------------

    if expected_offset != task.canonical_rows {
        return invalid(format!(
            "dataset {} task {}: canonical row coverage mismatch: expected {}, received {}",
            task.dataset_id, task.task_id, task.canonical_rows, expected_offset,
        ));
    }

    if segment_index != task.segments.segments.len() {
        return invalid(format!(
            "dataset {} task {}: reconstruction segment count mismatch",
            task.dataset_id, task.task_id
        ));
    }

    report.canonical_rows = expected_offset;

    Ok(report)
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
