use crate::shared::error::Result;

use {
    crate::historical::merge::depth::stream::DepthEventRef,
    crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization,
};

// -----------------------------------------------------------------------------
// Depth event source
// -----------------------------------------------------------------------------

/// Common interface for streaming canonical depth-event references.
///
/// Implemented by:
/// - DepthStream: continuous logical stream across complete source archives.
/// - PartitionDepthStream: timestamp-restricted stream for parallel merging.
///
/// The K-way merger consumes this interface without depending on the
/// concrete stream implementation.
///
/// Implementations must:
/// - Return events in nondecreasing timestamp order.
/// - Preserve original dataset, task, and event identities.
/// - Preserve canonical row offsets and counts.
/// - Apply validated archive-boundary rules.
/// - Record skipped redundant initialization checkpoints.
/// - Propagate errors rather than silently discarding invalid events.
pub trait DepthEventSource {
    /// Return the next logical depth event.
    ///
    /// Returns `Ok(None)` when the stream is exhausted.
    fn next_event(&mut self) -> Result<Option<DepthEventRef>>;

    /// Return references to redundant initialization checkpoints
    /// skipped during logical stream construction.
    ///
    /// The original canonical data remains unchanged.
    fn skipped_initializations(&self) -> &[MergeSkippedInitialization];
    fn timestamp_regressions_dropped(&self) -> u64 {
        0
    }
}
