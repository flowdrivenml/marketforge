//! Live execution and its independently usable external interfaces.

pub mod management;
pub mod streaming;

#[cfg(feature = "live")]
pub mod application;
#[cfg(feature = "live")]
pub mod config;
#[cfg(feature = "live")]
pub mod infrastructure;
#[cfg(feature = "live")]
pub mod integrity;
#[cfg(feature = "live")]
pub mod metrics;
#[cfg(feature = "live")]
pub mod recording;
#[cfg(feature = "live")]
pub mod recovery;
#[cfg(feature = "live")]
pub mod stream;
