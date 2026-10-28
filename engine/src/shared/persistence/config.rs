use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetResourceConfig {
    pub row_group_target_bytes: u64,
    pub file_target_bytes: u64,
}
