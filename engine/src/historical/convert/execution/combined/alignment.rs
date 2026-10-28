use std::collections::HashSet;

use {
    crate::historical::convert::reference::ExternalReferenceCursor,
    crate::historical::convert::reference::ReferenceObservation,
    crate::historical::convert::reference::ReferenceState, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

use crate::historical::convert::execution::CombinedReferenceSelection;

pub struct CombinedReferenceAlignment {
    state: ReferenceState,
    external: Option<ExternalReferenceCursor>,
    internal_streams: HashSet<String>,

    internal_updates: u64,
    external_updates: u64,
}

impl CombinedReferenceAlignment {
    pub fn new(
        selection: CombinedReferenceSelection,
        external: Option<ExternalReferenceCursor>,
    ) -> Result<Self> {
        if selection.internal_streams.is_empty() && selection.external_streams.is_empty() {
            return invalid("combined alignment requires reference streams");
        }

        if !selection
            .internal_streams
            .is_disjoint(&selection.external_streams)
        {
            return invalid("internal and external references overlap");
        }

        if selection.external_streams.is_empty() != external.is_none() {
            return invalid("external cursor configuration mismatch");
        }

        Ok(Self {
            state: ReferenceState::new(),
            external,
            internal_streams: selection.internal_streams,
            internal_updates: 0,
            external_updates: 0,
        })
    }

    pub fn advance_before(&mut self, timestamp_ns: i64) -> Result<()> {
        if let Some(cursor) = &mut self.external {
            let updates = cursor.advance_before(timestamp_ns, &mut self.state)?;

            self.external_updates = self
                .external_updates
                .checked_add(updates)
                .ok_or_else(|| invalid_error("external reference counter overflow"))?;
        }

        Ok(())
    }

    pub fn state(&self) -> &ReferenceState {
        &self.state
    }

    pub fn observe_internal(&mut self, observation: ReferenceObservation) -> Result<()> {
        if !self.internal_streams.contains(&observation.stream_id) {
            return Ok(());
        }

        self.state.update(observation)?;

        self.internal_updates = self
            .internal_updates
            .checked_add(1)
            .ok_or_else(|| invalid_error("internal reference counter overflow"))?;

        Ok(())
    }

    pub fn reference_updates(&self) -> u64 {
        self.internal_updates + self.external_updates
    }

    pub fn internal_updates(&self) -> u64 {
        self.internal_updates
    }

    pub fn external_updates(&self) -> u64 {
        self.external_updates
    }

    pub fn observations_read(&self) -> u64 {
        self.external
            .as_ref()
            .map_or(0, ExternalReferenceCursor::observations_read)
    }

    pub fn snapshot(&self) -> ReferenceState {
        self.state.snapshot()
    }
    pub fn state_mut(&mut self) -> &mut ReferenceState {
        &mut self.state
    }
    pub fn is_internal_reference(&self, stream_id: &str) -> bool {
        self.internal_streams.contains(stream_id)
    }
    pub fn from_checkpoint(
        selection: CombinedReferenceSelection,
        external: Option<ExternalReferenceCursor>,
        state: ReferenceState,
    ) -> Result<Self> {
        if selection.internal_streams.is_empty() && selection.external_streams.is_empty() {
            return invalid("combined alignment requires reference streams");
        }

        if !selection
            .internal_streams
            .is_disjoint(&selection.external_streams)
        {
            return invalid("internal and external references overlap");
        }

        if selection.external_streams.is_empty() != external.is_none() {
            return invalid("external cursor configuration mismatch");
        }

        Ok(Self {
            state,
            external,
            internal_streams: selection.internal_streams,
            internal_updates: 0,
            external_updates: 0,
        })
    }
    pub fn external_position(
        &self,
    ) -> Option<crate::historical::convert::reference::ExternalReferencePosition> {
        self.external
            .as_ref()
            .map(ExternalReferenceCursor::position)
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
