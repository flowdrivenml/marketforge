//! Versioned management contracts and bounded codec. Runtime sessions/server
//! remain owned by WP-25.

// Private runtime modules remain scaffolded; public modules are WP-07 contracts.
pub(crate) mod authorization;
pub(crate) mod client;
pub(crate) mod dispatch;
pub mod framing;
pub(crate) mod notification;
pub mod operation;
pub mod protocol;
pub(crate) mod query;
pub(crate) mod server;
pub(crate) mod session;
