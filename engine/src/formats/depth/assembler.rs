use crate::{
    canonical::BookSide,
    error::{MarketForgeError, Result},
};

use super::transforms::ExtractedLevel;

// -----------------------------------------------------------------------------
// Assembled snapshot
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AssembledSnapshot {
    pub timestamp_ns: i64,
    pub sequence: Option<u64>,

    pub bids: Vec<ExtractedLevel>,
    pub asks: Vec<ExtractedLevel>,
}

// -----------------------------------------------------------------------------
// Snapshot assembler
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct DepthSnapshotAssembler {
    pending: Option<AssembledSnapshot>,
}

impl DepthSnapshotAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Add one row belonging to a grouped snapshot.
    ///
    /// All rows must share the same timestamp and sequence.
    pub fn push(
        &mut self,
        timestamp_ns: i64,
        sequence: Option<u64>,
        side: BookSide,
        level: ExtractedLevel,
    ) -> Result<()> {
        match self.pending.as_mut() {
            Some(snapshot) => {
                if snapshot.timestamp_ns != timestamp_ns {
                    return invalid("snapshot rows have inconsistent timestamps");
                }

                if snapshot.sequence != sequence {
                    return invalid("snapshot rows have inconsistent sequences");
                }

                match side {
                    BookSide::Bid => snapshot.bids.push(level),
                    BookSide::Ask => snapshot.asks.push(level),
                }
            }

            None => {
                let mut snapshot = AssembledSnapshot {
                    timestamp_ns,
                    sequence,
                    bids: Vec::new(),
                    asks: Vec::new(),
                };

                match side {
                    BookSide::Bid => snapshot.bids.push(level),
                    BookSide::Ask => snapshot.asks.push(level),
                }

                self.pending = Some(snapshot);
            }
        }

        Ok(())
    }

    /// Finalize the pending snapshot.
    ///
    /// Called when the first non-snapshot row arrives or at EOF.
    pub fn finish(&mut self) -> Result<Option<AssembledSnapshot>> {
        let Some(snapshot) = self.pending.take() else {
            return Ok(None);
        };

        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return Err(MarketForgeError::RecordIntegrity {
                category: crate::job::IntegrityCategory::InvalidRecord,
                message: "grouped snapshot must contain both bids and asks".to_owned(),
            });
        }

        Ok(Some(snapshot))
    }
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
