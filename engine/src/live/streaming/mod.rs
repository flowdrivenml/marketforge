//! External market-data delivery and consumption.

#[cfg(all(feature = "stream-client", unix))]
pub mod client;
pub mod protocol;
#[cfg(feature = "live")]
pub mod transport;
