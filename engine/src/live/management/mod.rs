//! Administrative control, configuration and observation.

#[cfg(feature = "management-client")]
pub mod client;
#[cfg(all(feature = "management-api", unix))]
pub mod http;
pub mod protocol;
#[cfg(all(feature = "live", unix))]
pub mod service;
#[cfg(feature = "management-tui")]
pub mod tui;

#[cfg(all(feature = "live", unix))]
pub use service::*;
