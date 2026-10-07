use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub workers: usize,

    pub memory_budget_bytes: u64,

    pub scratch_path: PathBuf,
    pub scratch_budget_bytes: u64,

    pub parquet: ParquetResourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetResourceConfig {
    pub row_group_target_bytes: u64,
    pub file_target_bytes: u64,
}
