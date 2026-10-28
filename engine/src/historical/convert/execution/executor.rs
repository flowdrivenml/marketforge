use std::collections::HashMap;

use {
    crate::historical::job::ConvertJob, crate::historical::merge::PhysicalMergeManifest,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

use crate::historical::convert::{
    ConversionPlan,
    reference::{
        ConversionUnavailable, PhysicalReferenceReader, REFERENCE_READ_BATCH_SIZE, ReferenceState,
        evaluate_conversion_path,
    },
};

// -----------------------------------------------------------------------------
// Execution statistics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ConversionStreamStats {
    pub events: u64,
    pub identity: u64,
    pub converted: u64,
    pub unavailable: u64,
    pub missing_reference: u64,
    pub stale_reference: u64,
    pub future_reference: u64,
    pub arithmetic_failure: u64,
    pub identity_mismatch: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ConversionExecutionStats {
    pub events_read: u64,
    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,
    pub reference_updates: u64,
    pub streams: HashMap<u64, ConversionStreamStats>,
}

// -----------------------------------------------------------------------------
// Sequential execution
// -----------------------------------------------------------------------------

pub fn execute_conversion_sequential(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
) -> Result<ConversionExecutionStats> {
    job.validate()?;
    manifest.validate()?;

    let mut reader =
        PhysicalReferenceReader::new(&job.input_merge, manifest, REFERENCE_READ_BATCH_SIZE)?;

    let mut state = ReferenceState::new();
    let mut stats = ConversionExecutionStats::default();

    let streams = plan
        .streams
        .iter()
        .map(|stream| (stream.source.dataset_id, stream))
        .collect::<HashMap<_, _>>();

    let reference_streams = plan
        .streams
        .iter()
        .filter(|stream| job.conversion.references.contains(&stream.source.identity))
        .map(|stream| stream.source.stream_id.as_str())
        .collect::<std::collections::HashSet<_>>();

    while let Some(observation) = reader.next_observation()? {
        stats.events_read += 1;

        let stream = streams.get(&observation.dataset_id).ok_or_else(|| {
            invalid_error(format!(
                "unknown conversion source dataset {}",
                observation.dataset_id
            ))
        })?;

        let stream_stats = stats.streams.entry(observation.dataset_id).or_default();

        stream_stats.events += 1;

        // ---------------------------------------------------------------------
        // Evaluate source event before updating reference state.
        // ---------------------------------------------------------------------

        if stream.identity_conversion {
            stream_stats.identity += 1;
            stats.identity_events += 1;
        } else if stream.paths.is_empty() {
            stream_stats.unavailable += 1;
            stats.unavailable_events += 1;
        } else {
            let mut converted = false;
            let mut last_failure = None;

            for path in &stream.paths {
                match evaluate_conversion_path(
                    path,
                    &state,
                    observation.key,
                    job.conversion.max_staleness_ns,
                ) {
                    Ok(_result) => {
                        converted = true;
                        break;
                    }

                    Err(reason) => {
                        last_failure = Some(reason);
                    }
                }
            }

            if converted {
                stream_stats.converted += 1;
                stats.converted_events += 1;
            } else {
                stream_stats.unavailable += 1;
                stats.unavailable_events += 1;

                match last_failure {
                    Some(ConversionUnavailable::MissingReference(_)) => {
                        stream_stats.missing_reference += 1;
                    }

                    Some(ConversionUnavailable::StaleReference(_)) => {
                        stream_stats.stale_reference += 1;
                    }

                    Some(ConversionUnavailable::FutureReference(_)) => {
                        stream_stats.future_reference += 1;
                    }

                    Some(ConversionUnavailable::ArithmeticFailure(_)) => {
                        stream_stats.arithmetic_failure += 1;
                    }

                    Some(ConversionUnavailable::ReferenceIdentityMismatch(_)) => {
                        stream_stats.identity_mismatch += 1;
                    }

                    None => {}
                }
            }
        }

        // ---------------------------------------------------------------------
        // Update reference state after evaluating the current event.
        // ---------------------------------------------------------------------

        if reference_streams.contains(observation.stream_id.as_str()) {
            state.update(observation)?;
            stats.reference_updates += 1;
        }
    }

    if stats.events_read != manifest.events_written {
        return invalid(format!(
            "conversion event count mismatch: expected {}, read {}",
            manifest.events_written, stats.events_read
        ));
    }

    Ok(stats)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
