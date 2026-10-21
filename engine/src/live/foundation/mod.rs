//! Planned module: src/live/foundation.

// Document 17 scaffolding: declarations only; implementations are not complete.
pub mod cancellation;
pub mod clock;
pub(crate) mod diagnostics;
pub(crate) mod filesystem;
pub(crate) mod resources;
pub(crate) mod shutdown;

pub use cancellation::{CancellationToken, Cancelled};
pub use clock::{MonotonicClock, SystemMonotonicClock};

pub use filesystem::FileEpochStore;
