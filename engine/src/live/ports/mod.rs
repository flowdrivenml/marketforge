//! Narrow Live service boundaries shared by production and deterministic adapters.

// Ports carry contracts; their production owners are separate work packages.
pub mod configuration;
pub mod control;
pub mod observability;
pub mod processing;
pub mod protocol;
pub mod publication;
pub mod recording;
pub mod recovery;
pub mod registry;
pub mod retention;
pub mod routing;
pub mod transport;

pub mod context;
pub mod lifecycle;
