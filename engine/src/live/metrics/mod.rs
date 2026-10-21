//! Planned module: src/live/metrics.

// Document 17 scaffolding: declarations only; implementations are not complete.
pub mod aggregation;
pub(crate) mod counter;
pub(crate) mod diagnostic_history;
pub(crate) mod gauge;
pub(crate) mod handle;
pub mod health;
pub mod histogram;
pub(crate) mod prometheus;
pub mod registry;
pub(crate) mod snapshot;
