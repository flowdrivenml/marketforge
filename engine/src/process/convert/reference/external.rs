use super::{
    PhysicalReferenceReader, REFERENCE_READ_BATCH_SIZE, ReferenceObservation, ReferenceState,
};
use crate::process::merge::{UnifiedMergeMode, UnifiedMergeStreamKind};
use crate::{
    error::{MarketForgeError, Result},
    process::merge::PhysicalMergeManifest,
};
use std::{collections::HashSet, path::Path};

// -----------------------------------------------------------------------------
// External reference cursor
// -----------------------------------------------------------------------------

pub struct ExternalReferenceCursor {
    reader: PhysicalReferenceReader,

    selected_streams: HashSet<String>,

    pending: Option<ReferenceObservation>,

    observations_read: u64,
    reference_updates: u64,

    last_timestamp_ns: Option<i64>,
    observations_consumed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalReferencePosition {
    pub observations_consumed: u64,
    pub next_observation_index: u64,
}

impl ExternalReferenceCursor {
    pub fn new(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        reference_streams: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        let selected_streams = reference_streams.into_iter().collect::<HashSet<_>>();

        if selected_streams.is_empty() {
            return invalid("external reference cursor requires reference streams");
        }

        let available = manifest
            .streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<HashSet<_>>();

        for stream_id in &selected_streams {
            if !available.contains(stream_id.as_str()) {
                return invalid(format!(
                    "external reference stream not present in merge: {stream_id}"
                ));
            }
        }

        let reader =
            PhysicalReferenceReader::new(dataset_path, manifest, REFERENCE_READ_BATCH_SIZE)?;
        manifest.validate()?;

        if manifest.mode != UnifiedMergeMode::Trades {
            return invalid("external last-trade references require a trade-only physical merge");
        }

        if manifest
            .streams
            .iter()
            .any(|stream| stream.kind != UnifiedMergeStreamKind::Trade)
        {
            return invalid("external reference merge contains non-trade streams");
        }
        Ok(Self {
            reader,
            selected_streams,
            pending: None,
            observations_read: 0,
            reference_updates: 0,
            last_timestamp_ns: None,
            observations_consumed: 0,
        })
    }

    // -------------------------------------------------------------------------
    // Advance references strictly before timestamp
    // -------------------------------------------------------------------------

    pub fn advance_before(&mut self, timestamp_ns: i64, state: &mut ReferenceState) -> Result<u64> {
        if let Some(previous) = self.last_timestamp_ns {
            if timestamp_ns < previous {
                return invalid("external reference cursor timestamp regressed");
            }
        }

        self.last_timestamp_ns = Some(timestamp_ns);

        let mut updates = 0_u64;

        loop {
            let observation = match self.pending.take() {
                Some(observation) => observation,

                None => {
                    let Some(observation) = self.reader.next_observation()? else {
                        break;
                    };

                    self.observations_read = self
                        .observations_read
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("external reference read counter overflow"))?;

                    observation
                }
            };

            // -------------------------------------------------------------
            // Preserve first observation not strictly before target
            // -------------------------------------------------------------

            if observation.key.timestamp_ns >= timestamp_ns {
                self.pending = Some(observation);
                break;
            }
            self.observations_consumed = self
                .observations_consumed
                .checked_add(1)
                .ok_or_else(|| invalid_error("external reference consumed counter overflow"))?;

            // -------------------------------------------------------------
            // Ignore unselected trade streams
            // -------------------------------------------------------------

            if !self.selected_streams.contains(&observation.stream_id) {
                continue;
            }

            // -------------------------------------------------------------
            // Update historical reference state
            // -------------------------------------------------------------

            state.update(observation)?;

            updates = updates
                .checked_add(1)
                .ok_or_else(|| invalid_error("external reference update counter overflow"))?;
        }

        self.reference_updates = self
            .reference_updates
            .checked_add(updates)
            .ok_or_else(|| invalid_error("external reference total update overflow"))?;

        Ok(updates)
    }

    pub fn position(&self) -> ExternalReferencePosition {
        ExternalReferencePosition {
            observations_consumed: self.observations_consumed,
            next_observation_index: self.observations_consumed,
        }
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn observations_read(&self) -> u64 {
        self.observations_read
    }

    pub fn reference_updates(&self) -> u64 {
        self.reference_updates
    }

    pub fn pending_timestamp_ns(&self) -> Option<i64> {
        self.pending
            .as_ref()
            .map(|observation| observation.key.timestamp_ns)
    }
    pub fn resume(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        reference_streams: impl IntoIterator<Item = String>,
        position: ExternalReferencePosition,
    ) -> Result<Self> {
        manifest.validate()?;

        if manifest.mode != UnifiedMergeMode::Trades {
            return invalid("external reference resume requires a trade-only merge");
        }

        if manifest
            .streams
            .iter()
            .any(|stream| stream.kind != UnifiedMergeStreamKind::Trade)
        {
            return invalid("external reference resume contains non-trade streams");
        }

        if position.observations_consumed != position.next_observation_index {
            return invalid("inconsistent external reference checkpoint position");
        }

        if position.next_observation_index > manifest.events_written {
            return invalid("external reference checkpoint exceeds dataset length");
        }

        let selected_streams = reference_streams.into_iter().collect::<HashSet<_>>();

        if selected_streams.is_empty() {
            return invalid("external reference resume requires reference streams");
        }

        let available = manifest
            .streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<HashSet<_>>();

        for stream_id in &selected_streams {
            if !available.contains(stream_id.as_str()) {
                return invalid(format!(
                    "external reference stream not present in merge: {stream_id}"
                ));
            }
        }

        let reader = PhysicalReferenceReader::new_at(
            dataset_path,
            manifest,
            REFERENCE_READ_BATCH_SIZE,
            position.next_observation_index,
        )?;

        Ok(Self {
            reader,
            selected_streams,
            pending: None,

            observations_read: 0,
            observations_consumed: position.observations_consumed,
            reference_updates: 0,

            last_timestamp_ns: None,
        })
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
