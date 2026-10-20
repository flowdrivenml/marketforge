// Thin shim: delegate to the shared storage module.
// This preserves the existing `super::schema::*` import paths used by
// trades.rs, depth.rs, and depth_events.rs without changing their code.
pub use crate::storage::parquet::schema::*;
