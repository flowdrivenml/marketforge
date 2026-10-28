use crate::shared::error::Result;

use {
    crate::historical::merge::core::event::MergeEventRef,
    crate::historical::merge::depth::partition_stream::PartitionDepthStream,
    crate::historical::merge::depth::stream::DepthStream,
    crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization,
    crate::historical::merge::trades::stream::TradeStream,
};

// -----------------------------------------------------------------------------
// Generic merge event source
// -----------------------------------------------------------------------------

pub trait MergeEventSource {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>>;

    fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        &[]
    }
}

// -----------------------------------------------------------------------------
// Trade stream adapter
// -----------------------------------------------------------------------------

impl MergeEventSource for TradeStream {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>> {
        TradeStream::next_event(self).map(|event| event.map(MergeEventRef::Trade))
    }
}

// -----------------------------------------------------------------------------
// Depth stream adapter
// -----------------------------------------------------------------------------

impl MergeEventSource for DepthStream {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>> {
        DepthStream::next_event(self).map(|event| event.map(MergeEventRef::Depth))
    }

    fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        DepthStream::skipped_initializations(self)
    }
}

// -----------------------------------------------------------------------------
// Partition depth stream adapter
// -----------------------------------------------------------------------------

impl MergeEventSource for PartitionDepthStream {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>> {
        PartitionDepthStream::next_event(self).map(|event| event.map(MergeEventRef::Depth))
    }

    fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        <PartitionDepthStream as crate::historical::merge::core::source::DepthEventSource>::skipped_initializations(self)
    }
}
