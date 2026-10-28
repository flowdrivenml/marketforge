use std::collections::HashSet;

use {
    crate::historical::convert::reference::ReferenceObservation,
    crate::historical::convert::reference::ReferenceState, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

#[derive(Debug, Clone)]
pub struct ConversionReferenceCheckpoint {
    pub partition_id: u32,
    pub events_before_partition: u64,
    pub state: ReferenceState,
}

pub struct ConversionCheckpointBuilder {
    state: ReferenceState,
    reference_streams: HashSet<u32>,
    events_seen: u64,
}

impl ConversionCheckpointBuilder {
    pub fn new(reference_streams: HashSet<u32>) -> Self {
        Self {
            state: ReferenceState::new(),
            reference_streams,
            events_seen: 0,
        }
    }

    pub fn checkpoint(&self, partition_id: u32) -> ConversionReferenceCheckpoint {
        ConversionReferenceCheckpoint {
            partition_id,
            events_before_partition: self.events_seen,
            state: self.state.snapshot(),
        }
    }

    pub fn observe(&mut self, observation: ReferenceObservation) -> Result<()> {
        if self
            .reference_streams
            .contains(&observation.key.stream_rank)
        {
            self.state.update(observation)?;
        }

        self.events_seen = self
            .events_seen
            .checked_add(1)
            .ok_or_else(|| invalid_error("conversion checkpoint event counter overflow"))?;

        Ok(())
    }

    pub fn events_seen(&self) -> u64 {
        self.events_seen
    }

    pub fn reference_count(&self) -> usize {
        self.state.len()
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
