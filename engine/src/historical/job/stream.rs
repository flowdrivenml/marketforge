use serde::{Deserialize, Serialize};

use super::StreamId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub stream_id: StreamId,
    pub stream_rank: u32,
}
