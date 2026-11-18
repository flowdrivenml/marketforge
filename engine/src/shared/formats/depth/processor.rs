use serde_json::Value;

use super::assembler::AssembledSnapshot;
use super::outcome::{
    DepthEventBoundary, DepthProcessingOutcome, DepthSourceEventMetadata, DepthSourceOperation,
};
use super::transforms::ExtractedLevel;
use super::{
    json::{matches_event_filter, required_json_field},
    snapshot::apply_snapshot_observed,
    spec::{DepthOperation, DepthSpec},
    transforms::{extract_level_array, extract_optional_sequence, extract_timestamp_ns},
    update::apply_absolute_update_observed,
};
use super::{
    relative::{RelativeAction, apply_relative_update_observed},
    scalar::extract_scalar_level,
};
use crate::shared::domain::book::{SequencePolicy, SequenceTracker};
use crate::shared::normalization::QuantityEncoding;
use crate::{
    shared::domain::book::{BookStore, emit_l2_changes},
    shared::domain::canonical::{EventEnvelope, Exchange, L2LevelUpdate},
    shared::domain::instrument::InstrumentSpec,
    shared::error::{MarketForgeError, Result},
    shared::normalization::NormalizationConfig,
    shared::validation::IntegrityCategory,
    shared::validation::validate_l2_level_update,
};

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
    sequence_policy: SequencePolicy,
    observations: crate::shared::validation::IntegrityObservationTrace,
    last_observed_sequence: Option<u64>,
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
            sequence_policy,
            observations: Default::default(),
            last_observed_sequence: None,
        })
    }

    pub fn reset_observations(&mut self) {
        self.observations = Default::default();
    }
    pub fn observations(&self) -> &crate::shared::validation::IntegrityObservationTrace {
        &self.observations
    }
    /// Failure diagnostics reuse configured extraction, never adjacent event time.
    pub fn event_timestamp(&self, record: &Value) -> Option<i64> {
        self.specs
            .iter()
            .filter(|s| {
                s.event_filter
                    .as_ref()
                    .is_none_or(|f| matches_event_filter(record, f).unwrap_or(false))
            })
            .find_map(|s| {
                extract_timestamp_ns(record, &s.event_timestamp.source, s.timestamp_encoding).ok()
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
        self.observations = Default::default();
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
        self.observations.event_timestamp_ns = Some(event_timestamp_ns);
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
        self.observations.passed(IntegrityCategory::InvalidRecord);

        if let DepthOperation::Snapshot(snapshot) = &spec.operation {
            let bounded = snapshot.levels_per_side;
            let ordered_sequence = snapshot.sequence_order.is_some();
            if let Some(limit) = bounded {
                if bids.len() > limit as usize || asks.len() > limit as usize {
                    return invalid("bounded observation exceeds its declared depth");
                }
                if ordered_sequence
                    && self
                        .last_observed_sequence
                        .zip(source_sequence)
                        .is_some_and(|(previous, current)| current < previous)
                {
                    return invalid("native depth sequence strictly decreased");
                }
            }
            let mut result = self.apply_snapshot_event(
                bids,
                asks,
                spec.quantity_encoding,
                event_timestamp_ns,
                system_timestamp_ns,
                source_sequence,
            )?;
            if bounded.is_some() {
                result.operation = DepthSourceOperation::BoundedObservation;
                if ordered_sequence {
                    self.last_observed_sequence = source_sequence;
                }
            }
            return Ok(result);
        }

        let requires_initialization = !self.book.is_initialized();

        let changes = match &spec.operation {
            DepthOperation::Snapshot(_) => {
                unreachable!("snapshots are handled by apply_snapshot_event")
            }

            DepthOperation::AbsoluteUpdate => {
                // Reject missing snapshots and sequence discontinuities
                // before modifying BookStore.
                if self.sequence.is_synchronized() {
                    self.observations.passed(IntegrityCategory::MissingSnapshot);
                }
                if let Err(error) = self.sequence.validate_update(source_sequence) {
                    self.sequence.invalidate();
                    self.book.clear();

                    return Err(error);
                }

                if self.sequence_policy != SequencePolicy::Unsequenced {
                    self.observations.passed(IntegrityCategory::SequenceGap);
                }
                let changes = apply_absolute_update_observed(
                    &mut self.book,
                    bids,
                    asks,
                    spec.quantity_encoding,
                    &self.instrument,
                    &mut self.observations,
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
    fn apply_snapshot_event(
        &mut self,
        bids: Vec<ExtractedLevel>,
        asks: Vec<ExtractedLevel>,
        quantity_encoding: QuantityEncoding,
        event_timestamp_ns: i64,
        system_timestamp_ns: Option<i64>,
        source_sequence: Option<u64>,
    ) -> Result<DepthProcessingOutcome> {
        let requires_initialization = !self.book.is_initialized();

        // Validate sequence metadata before modifying the book.
        let mut next_sequence = self.sequence.clone();
        next_sequence.initialize(source_sequence)?;

        self.observations.event_timestamp_ns = Some(event_timestamp_ns);
        let changes = apply_snapshot_observed(
            &mut self.book,
            bids,
            asks,
            quantity_encoding,
            &self.instrument,
            &mut self.observations,
        )?;

        self.sequence = next_sequence;

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

        Ok(DepthProcessingOutcome {
            boundary: if requires_initialization {
                DepthEventBoundary::Initialization
            } else {
                DepthEventBoundary::Changes
            },
            operation: DepthSourceOperation::Snapshot,
            events,
            source: DepthSourceEventMetadata {
                source_event_ordinal: None,
                event_timestamp_ns,
                system_timestamp_ns,
                sequence_start: source_sequence,
                sequence_end: source_sequence,
            },
        })
    }
    pub fn process_assembled_snapshot(
        &mut self,
        snapshot: AssembledSnapshot,
    ) -> Result<DepthProcessingOutcome> {
        self.observations = Default::default();
        let quantity_encoding = self
            .specs
            .iter()
            .find_map(|spec| {
                if matches!(spec.operation, DepthOperation::Snapshot(_)) {
                    Some(spec.quantity_encoding)
                } else {
                    None
                }
            })
            .ok_or_else(|| invalid_error("depth processor has no snapshot specification"))?;

        self.apply_snapshot_event(
            snapshot.bids,
            snapshot.asks,
            quantity_encoding,
            snapshot.timestamp_ns,
            None,
            snapshot.sequence,
        )
    }
    pub fn process_scalar_relative_update(
        &mut self,
        record: &Value,
    ) -> Result<DepthProcessingOutcome> {
        self.observations = Default::default();
        // ---------------------------------------------------------
        // Resolve matching relative-update specification
        // ---------------------------------------------------------

        let mut matched = None;

        for spec in &self.specs {
            if !matches!(spec.operation, DepthOperation::RelativeUpdate(_)) {
                continue;
            }

            let matches = match &spec.event_filter {
                Some(filter) => matches_event_filter(record, filter)?,
                None => true,
            };

            if matches {
                if matched.is_some() {
                    return invalid("record matches multiple relative-update rules");
                }

                matched = Some(spec);
            }
        }

        let spec =
            matched.ok_or_else(|| invalid_error("record matches no relative-update rule"))?;

        let DepthOperation::RelativeUpdate(relative) = &spec.operation else {
            unreachable!();
        };

        // ---------------------------------------------------------
        // Extract scalar level and source metadata
        // ---------------------------------------------------------

        let scalar = spec
            .scalar_level
            .as_ref()
            .ok_or_else(|| invalid_error("relative update requires scalar level specification"))?;

        let (side, level) = extract_scalar_level(record, scalar)?;

        let event_timestamp_ns = extract_timestamp_ns(
            record,
            &spec.event_timestamp.source,
            spec.timestamp_encoding,
        )?;

        self.observations.event_timestamp_ns = Some(event_timestamp_ns);
        let source_sequence = extract_optional_sequence(
            record,
            spec.sequence_start
                .as_ref()
                .map(|field| field.source.as_str()),
        )?;

        let source_count = extract_optional_sequence(
            record,
            spec.sequence_count
                .as_ref()
                .map(|field| field.source.as_str()),
        )?;

        let action_value = required_json_field(record, &relative.action_source)?;

        let action = action_value
            .as_str()
            .ok_or_else(|| invalid_error("relative action must be a string"))?;

        let action = if action == relative.add_action {
            RelativeAction::Add
        } else if action == relative.subtract_action {
            RelativeAction::Subtract
        } else {
            return invalid("unknown relative depth action");
        };

        let quantity_encoding = spec.quantity_encoding;

        // ---------------------------------------------------------
        // Validate sequence before modifying the book
        // ---------------------------------------------------------

        self.observations.passed(IntegrityCategory::InvalidRecord);
        if self.sequence.is_synchronized() {
            self.observations.passed(IntegrityCategory::MissingSnapshot);
        }
        if let Err(error) = self
            .sequence
            .validate_update_range(source_sequence, source_count)
        {
            self.invalidate_synchronization();
            return Err(error);
        }

        // ---------------------------------------------------------
        // Apply relative update
        // ---------------------------------------------------------

        if self.sequence_policy != SequencePolicy::Unsequenced {
            self.observations.passed(IntegrityCategory::SequenceGap);
        }
        let changes = apply_relative_update_observed(
            &mut self.book,
            side,
            level,
            action,
            quantity_encoding,
            &self.instrument,
            &mut self.observations,
        )?;

        self.sequence
            .commit_update_range(source_sequence, source_count);

        // ---------------------------------------------------------
        // Emit canonical absolute changes
        // ---------------------------------------------------------

        let envelope = EventEnvelope {
            event_timestamp_ns,
            system_timestamp_ns: None,
            exchange: self.context.exchange,
            instrument_id: self.context.instrument_id,
            symbol: self.context.symbol.clone(),
            stream_id: self.context.stream_id.clone(),
        };

        let events = emit_l2_changes(&envelope, changes);

        for event in &events {
            validate_l2_level_update(event)?;
        }

        let sequence_end = match (source_sequence, source_count) {
            (Some(start), Some(count)) if count > 0 => Some(
                start
                    .checked_add(count - 1)
                    .ok_or_else(|| invalid_error("relative sequence range overflow"))?,
            ),
            _ => source_sequence,
        };

        Ok(DepthProcessingOutcome {
            boundary: DepthEventBoundary::Changes,
            operation: DepthSourceOperation::RelativeUpdate,
            events,
            source: DepthSourceEventMetadata {
                source_event_ordinal: None,
                event_timestamp_ns,
                system_timestamp_ns: None,
                sequence_start: source_sequence,
                sequence_end,
            },
        })
    }
    pub fn extract_scalar_snapshot_row(
        &self,
        record: &Value,
    ) -> Result<(
        i64,
        Option<u64>,
        crate::shared::domain::canonical::BookSide,
        ExtractedLevel,
    )> {
        let mut matched = None;

        for spec in &self.specs {
            if !matches!(spec.operation, DepthOperation::Snapshot(_)) {
                continue;
            }

            let matches = match &spec.event_filter {
                Some(filter) => matches_event_filter(record, filter)?,
                None => true,
            };

            if matches {
                if matched.is_some() {
                    return invalid("record matches multiple snapshot specifications");
                }

                matched = Some(spec);
            }
        }

        let spec =
            matched.ok_or_else(|| invalid_error("record matches no snapshot specification"))?;

        let scalar = spec
            .scalar_level
            .as_ref()
            .ok_or_else(|| invalid_error("snapshot requires scalar level specification"))?;

        let (side, level) = extract_scalar_level(record, scalar)?;

        let timestamp_ns = extract_timestamp_ns(
            record,
            &spec.event_timestamp.source,
            spec.timestamp_encoding,
        )?;

        let sequence = extract_optional_sequence(
            record,
            spec.sequence_start
                .as_ref()
                .map(|field| field.source.as_str()),
        )?;

        Ok((timestamp_ns, sequence, side, level))
    }
    pub fn is_scalar_snapshot_row(&self, record: &Value) -> Result<bool> {
        let mut matched = false;

        for spec in &self.specs {
            if !matches!(spec.operation, DepthOperation::Snapshot(_)) {
                continue;
            }

            let is_match = match &spec.event_filter {
                Some(filter) => matches_event_filter(record, filter)?,
                None => true,
            };

            if is_match {
                if matched {
                    return invalid("record matches multiple snapshot specifications");
                }

                matched = true;
            }
        }

        Ok(matched)
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
