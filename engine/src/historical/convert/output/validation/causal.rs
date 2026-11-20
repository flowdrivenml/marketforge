use arrow_array::{Array, BooleanArray, RecordBatch, UInt8Array, UInt64Array};

use crate::shared::error::{MarketForgeError, Result};

#[derive(Debug, Clone, Default)]
pub struct CausalPositions {
    pub first: Option<u64>,
    pub last: Option<u64>,
    pub reconstruction_rows: u64,
}

impl CausalPositions {
    pub(super) fn read(
        &mut self,
        batch: &RecordBatch,
        mode: crate::historical::merge::UnifiedMergeMode,
    ) -> Result<()> {
        use crate::historical::merge::UnifiedMergeMode;
        let sequences = batch.column_by_name("timeline_sequence");
        let markers = batch.column_by_name("reconstruction_only");
        if sequences.is_none() && markers.is_none() {
            return Ok(());
        }
        let sequences = sequences
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid("missing or invalid converted timeline sequence"))?;
        let markers = markers
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| invalid("missing or invalid converted reconstruction marker"))?;
        let kinds = batch
            .column_by_name("event_kind")
            .and_then(|array| array.as_any().downcast_ref::<UInt8Array>());
        for row in 0..batch.num_rows() {
            let sequence = sequences.value(row);
            let depth = match mode {
                UnifiedMergeMode::Depth => true,
                UnifiedMergeMode::Trades => false,
                UnifiedMergeMode::Combined => {
                    kinds.is_some_and(|column| !column.is_null(row) && column.value(row) == 1)
                }
            };
            if sequences.is_null(row)
                || markers.is_null(row)
                || self
                    .last
                    .is_some_and(|previous| previous.checked_add(1) != Some(sequence))
                || (markers.value(row) && !depth)
            {
                return Err(invalid("invalid converted causal position"));
            }
            self.first.get_or_insert(sequence);
            self.last = Some(sequence);
            self.reconstruction_rows += u64::from(markers.value(row));
        }
        Ok(())
    }

    pub(super) fn follows(&self, previous: Option<&Self>) -> bool {
        match (previous, self.first) {
            (None, first) => first.is_none_or(|sequence| sequence == 0),
            (Some(previous), Some(first)) => {
                previous.last.and_then(|last| last.checked_add(1)) == Some(first)
            }
            (Some(previous), None) => previous.last.is_none(),
        }
    }
}

fn invalid(message: &str) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
