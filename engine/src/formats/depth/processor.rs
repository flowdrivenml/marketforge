use serde_json::Value;

use crate::{
    book::{BookStore, emit_l2_changes},
    canonical::{EventEnvelope, Exchange, L2LevelUpdate},
    error::{MarketForgeError, Result},
    job::{InstrumentSpec, IntegrityCategory, NormalizationConfig},
    validate::validate_l2_level_update,
};

use super::outcome::{
    DepthEventBoundary, DepthProcessingOutcome, DepthSourceEventMetadata, DepthSourceOperation,
};
use super::{
    json::{matches_event_filter, required_json_field},
    snapshot::apply_snapshot,
    spec::{DepthOperation, DepthSpec},
    transforms::{extract_level_array, extract_optional_sequence, extract_timestamp_ns},
    update::apply_absolute_update,
};
use crate::book::{SequencePolicy, SequenceTracker};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthContext {
    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,
}

pub struct DepthProcessor {
    specs: Vec<DepthSpec>,
    instrument: InstrumentSpec,
    context: DepthContext,

    book: BookStore,
    sequence: SequenceTracker,
}

impl DepthProcessor {
    pub fn new(
        normalizations: &[NormalizationConfig],
        instrument: InstrumentSpec,
        context: DepthContext,
        sequence_policy: SequencePolicy,
    ) -> Result<Self> {
        if normalizations.is_empty() {
            return invalid("depth processor requires normalization rules");
        }

        let specs = normalizations
            .iter()
            .map(DepthSpec::from_normalization)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            specs,
            instrument,
            context,

            book: BookStore::new(),
            sequence: SequenceTracker::new(sequence_policy),
        })
    }

    pub fn book(&self) -> &BookStore {
        &self.book
    }

    pub fn sequence(&self) -> &SequenceTracker {
        &self.sequence
    }

    pub fn process_record_with_boundary(
        &mut self,
        record: &Value,
    ) -> Result<DepthProcessingOutcome> {
        // Resolve exactly one matching normalization rule.
        let mut matched = None;

        for spec in &self.specs {
            let matches = match &spec.event_filter {
                Some(filter) => matches_event_filter(record, filter)?,
                None => true,
            };

            if matches {
                if matched.is_some() {
                    return invalid("depth record matches multiple normalization rules");
                }

                matched = Some(spec);
            }
        }

        let Some(spec) = matched else {
            return invalid("depth record matches no normalization rule");
        };

        // Validate source instrument identity when provided.
        if let Some(instrument_spec) = &spec.instrument {
            let value = required_json_field(record, &instrument_spec.source)?;

            let symbol = value
                .as_str()
                .ok_or_else(|| invalid_error("depth instrument must be a string"))?;

            if symbol != self.context.symbol {
                return invalid(format!(
                    "depth instrument mismatch: expected {}, received {}",
                    self.context.symbol, symbol
                ));
            }
        }

        let event_timestamp_ns = extract_timestamp_ns(
            record,
            &spec.event_timestamp.source,
            spec.timestamp_encoding,
        )?;
        // -------------------------------------------------------------------------
        // Extract source sequence
        // -------------------------------------------------------------------------

        let source_sequence = extract_optional_sequence(
            record,
            spec.sequence_start
                .as_ref()
                .map(|field| field.source.as_str()),
        )?;

        let system_timestamp_ns = spec
            .system_timestamp
            .as_ref()
            .map(|field| extract_timestamp_ns(record, &field.source, spec.timestamp_encoding))
            .transpose()?;

        let bids_spec = spec
            .bids
            .as_ref()
            .ok_or_else(|| invalid_error("depth normalization missing bids specification"))?;

        let asks_spec = spec
            .asks
            .as_ref()
            .ok_or_else(|| invalid_error("depth normalization missing asks specification"))?;

        let bids = extract_level_array(record, bids_spec)?;
        let asks = extract_level_array(record, asks_spec)?;

        let requires_initialization = !self.book.is_initialized();

        let changes = match &spec.operation {
            DepthOperation::Snapshot(_) => {
                // Validate the snapshot sequence before modifying BookStore.
                //
                // Clone the tracker so initialization cannot partially commit
                // if snapshot normalization or book replacement fails.
                let mut next_sequence = self.sequence.clone();
                next_sequence.initialize(source_sequence)?;

                let changes = apply_snapshot(
                    &mut self.book,
                    bids,
                    asks,
                    spec.quantity_encoding,
                    &self.instrument,
                )?;

                self.sequence = next_sequence;

                changes
            }

            DepthOperation::AbsoluteUpdate => {
                // Reject missing snapshots and sequence discontinuities
                // before modifying BookStore.
                if let Err(error) = self.sequence.validate_update(source_sequence) {
                    // A sequence gap invalidates the reconstructed book.
                    //
                    // The next authoritative snapshot must reinitialize it.
                    self.sequence.invalidate();
                    self.book.clear();

                    return Err(error);
                }

                let changes = apply_absolute_update(
                    &mut self.book,
                    bids,
                    asks,
                    spec.quantity_encoding,
                    &self.instrument,
                )?;

                // Commit only after successful atomic book modification.
                self.sequence.commit_update(source_sequence);

                changes
            }

            DepthOperation::RelativeUpdate(_) => {
                return invalid("relative depth updates are not implemented yet");
            }
        };

        let envelope = EventEnvelope {
            event_timestamp_ns,
            system_timestamp_ns,
            exchange: self.context.exchange,
            instrument_id: self.context.instrument_id,
            symbol: self.context.symbol.clone(),
            stream_id: self.context.stream_id.clone(),
        };

        let events = emit_l2_changes(&envelope, changes);

        for event in &events {
            validate_l2_level_update(event)?;
        }

        let boundary = match &spec.operation {
            DepthOperation::Snapshot(_) if requires_initialization => {
                DepthEventBoundary::Initialization
            }

            _ => DepthEventBoundary::Changes,
        };

        let operation = match &spec.operation {
            DepthOperation::Snapshot(_) => DepthSourceOperation::Snapshot,
            DepthOperation::AbsoluteUpdate => DepthSourceOperation::AbsoluteUpdate,
            DepthOperation::RelativeUpdate(_) => DepthSourceOperation::RelativeUpdate,
        };

        let source = DepthSourceEventMetadata {
            source_event_ordinal: None,

            event_timestamp_ns,
            system_timestamp_ns,

            sequence_start: source_sequence,
            sequence_end: source_sequence,
        };

        Ok(DepthProcessingOutcome {
            boundary,
            operation,
            events,
            source,
        })
    }
    /// Invalidate reconstructed book state.
    ///
    /// Called when a source record may have been lost or corrupted.
    ///
    /// Processing may resume only after an authoritative snapshot.
    pub fn invalidate_synchronization(&mut self) {
        self.sequence.invalidate();
        self.book.clear();
    }
    pub fn process_record(&mut self, record: &Value) -> Result<Vec<L2LevelUpdate>> {
        Ok(self.process_record_with_boundary(record)?.events)
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
