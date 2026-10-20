// Thin shim: delegate to the shared storage module.
// This preserves the existing `super::decimal::decimal_to_i256` import path
// used by trades.rs and depth.rs without changing their code.
pub use crate::storage::parquet::decimal::*;
