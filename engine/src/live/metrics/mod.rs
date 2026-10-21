//! Native scoped collection, bounded aggregation and observational health.
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
