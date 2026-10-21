//! Cold-path configuration loading, planning and durable acceptance.

pub mod compiler;
pub mod diff;
pub mod effective;
pub(crate) mod inheritance;
pub mod loader;
pub mod manager;
pub(crate) mod revision;
pub(crate) mod schema;
pub mod store;
pub mod validation;

pub mod worker;
