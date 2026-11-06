use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceConfig {
    pub workers: usize,

    pub memory_budget_bytes: u64,

    pub scratch_path: PathBuf,
    pub scratch_budget_bytes: u64,

    #[serde(deserialize_with = "strict_parquet")]
    pub parquet: ParquetResourceConfig,
}

pub use crate::shared::persistence::config::ParquetResourceConfig;

fn strict_parquet<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<ParquetResourceConfig, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire {
        row_group_target_bytes: u64,
        file_target_bytes: u64,
    }
    let wire = Wire::deserialize(deserializer)?;
    Ok(ParquetResourceConfig {
        row_group_target_bytes: wire.row_group_target_bytes,
        file_target_bytes: wire.file_target_bytes,
    })
}
