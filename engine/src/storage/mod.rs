#[cfg(feature = "historical")]
pub mod parquet;

#[cfg(feature = "historical")]
pub mod historical_compat {
    pub use crate::storage::parquet::decimal::{decimal_to_i256, i256_to_decimal};
    pub use crate::storage::parquet::schema::{
        DECIMAL_PRECISION, DECIMAL_SCALE, depth_event_schema, depth_schema, trade_schema,
    };
}
