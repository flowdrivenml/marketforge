use crate::shared::domain::canonical::{L2LevelUpdate, SequenceMetadata, Trade};

// ============================================================
// NORMALIZATION SOURCE
// ============================================================

/// Origin of the native message supplied to the normalizer.
///
/// This describes provenance, not normalization behavior.
///
/// The same generic decoding machinery may be used for
/// REST, WebSocket, and replayed exchange-native messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NormalizationSource {
    Rest,
    WebSocket,
    Replay,
}

impl NormalizationSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::WebSocket => "websocket",
            Self::Replay => "replay",
        }
    }
}

// ============================================================
// TRADE SEMANTICS
// ============================================================

/// Describes the economic meaning of native trade records.
///
/// Individual:
///     Each record represents one exchange-reported trade.
///
/// Aggregated:
///     Each record represents an exchange-defined
///     aggregation of underlying executions.
///
/// These distinctions must survive normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeRecordSemantics {
    Individual,
    Aggregated,
}

// ============================================================
// DEPTH MESSAGE KIND
// ============================================================

/// Classification of an exchange-native depth message.
///
/// Snapshot:
///     A source-declared authoritative representation
///     of the supplied book coverage.
///
/// Delta:
///     Incremental changes relative to previous state.
///
/// Classification alone does not establish that the
/// message is safe to apply to an existing book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DepthMessageKind {
    Snapshot,
    Delta,
}

// ============================================================
// NORMALIZED TRADE MESSAGE
// ============================================================

/// One completely decoded native trade message.
///
/// All trades belong to the same source stream.
///
/// Records retain their original source ordering.
///
/// The normalization layer does not deduplicate trades
/// or establish continuity with earlier messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedTrades {
    stream_id: String,

    source: NormalizationSource,

    semantics: TradeRecordSemantics,

    trades: Vec<Trade>,
}

impl NormalizedTrades {
    pub fn new(
        stream_id: impl Into<String>,
        source: NormalizationSource,
        semantics: TradeRecordSemantics,
        trades: Vec<Trade>,
    ) -> Result<Self, String> {
        let stream_id = stream_id.into();

        validate_stream_id(&stream_id)?;

        for (index, trade) in trades.iter().enumerate() {
            if trade.envelope.stream_id != stream_id {
                return Err(format!(
                    "trade[{index}] belongs to stream '{}', \
                     expected '{stream_id}'",
                    trade.envelope.stream_id,
                ));
            }
        }

        // An empty trade batch is valid.
        //
        // Some native messages can legitimately contain
        // no records. Distinguishing an empty message
        // from a malformed response belongs to the
        // format-specific extraction contract.

        Ok(Self {
            stream_id,
            source,
            semantics,
            trades,
        })
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn source(&self) -> NormalizationSource {
        self.source
    }

    pub fn semantics(&self) -> TradeRecordSemantics {
        self.semantics
    }

    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    pub fn record_count(&self) -> usize {
        self.trades.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    pub fn into_trades(self) -> Vec<Trade> {
        self.trades
    }
}
/// Provenance of event_timestamp_ns in canonical
/// levels belonging to one normalized depth message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthTimestampProvenance {
    /// Event timestamps originate from an explicitly
    /// defined exchange-native timestamp field.
    Exchange,

    /// The exchange did not provide a mapped timestamp.
    /// Canonical event timestamps use trusted receive time.
    ReceiveTimeFallback,
}
// ============================================================
// NORMALIZED DEPTH MESSAGE
// ============================================================

/// One atomically decoded native depth message.
///
/// `levels` contains absolute price-level quantities.
///
/// Snapshot:
///     Levels describe the snapshot's supplied coverage.
///
/// Delta:
///     Levels describe absolute resulting quantities
///     for the affected price levels.
///
/// The message is not automatically committed to an
/// order book. Sequence verification, snapshot bridging,
/// generation fencing, and atomic book application
/// belong to the downstream synchronization layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedDepth {
    stream_id: String,

    source: NormalizationSource,

    kind: DepthMessageKind,

    sequence: SequenceMetadata,

    levels: Vec<L2LevelUpdate>,

    /// Native source timestamp, converted to nanoseconds
    /// when available and explicitly defined by its format.
    ///
    /// This is message-level metadata and does not replace
    /// the timestamps in canonical level envelopes.
    message_timestamp_ns: Option<i64>,

    /// Depth explicitly requested or declared by the source.
    ///
    /// None means unavailable, not unlimited depth.
    ///
    /// This is not proof that every possible price level
    /// is represented.
    declared_depth: Option<u64>,
    /// None indicates that provenance was not established
    /// by the producer (legacy constructor or test fixture).
    ///
    /// Never interpret None as Exchange.
    timestamp_provenance: Option<DepthTimestampProvenance>,
}

impl NormalizedDepth {
    pub fn new(
        stream_id: impl Into<String>,
        source: NormalizationSource,
        kind: DepthMessageKind,
        sequence: SequenceMetadata,
        levels: Vec<L2LevelUpdate>,
        message_timestamp_ns: Option<i64>,
        declared_depth: Option<u64>,
    ) -> Result<Self, String> {
        let stream_id = stream_id.into();

        validate_stream_id(&stream_id)?;

        if declared_depth == Some(0) {
            return Err("declared depth must be positive when present".into());
        }

        for (index, level) in levels.iter().enumerate() {
            if level.envelope.stream_id != stream_id {
                return Err(format!(
                    "depth level[{index}] belongs to stream '{}', \
                     expected '{stream_id}'",
                    level.envelope.stream_id,
                ));
            }
        }

        // Empty snapshots and deltas are structurally
        // representable.
        //
        // Exchange-specific rules for keepalives, empty
        // snapshots, resets, and sequence continuity
        // must be enforced by format validation and the
        // downstream integrity/synchronization layer.

        Ok(Self {
            stream_id,
            source,
            kind,
            sequence,
            levels,
            message_timestamp_ns,
            declared_depth,
            timestamp_provenance: None,
        })
    }
    /// Attach authoritative timestamp provenance.
    ///
    /// This method validates that the supplied provenance
    /// agrees with the normalized message's timestamp
    /// metadata and canonical level envelopes.
    pub fn with_timestamp_provenance(
        mut self,
        provenance: DepthTimestampProvenance,
    ) -> Result<Self, String> {
        if self.timestamp_provenance.is_some() {
            return Err("depth timestamp provenance is already bound".into());
        }

        let expected_timestamp_ns = match provenance {
            DepthTimestampProvenance::Exchange => self.message_timestamp_ns.ok_or_else(|| {
                "exchange timestamp provenance requires \
                 a native message timestamp"
                    .to_string()
            })?,

            DepthTimestampProvenance::ReceiveTimeFallback => {
                if self.message_timestamp_ns.is_some() {
                    return Err("receive-time fallback cannot claim \
                     an exchange message timestamp"
                        .into());
                }

                // A fallback value is already present in
                // each canonical envelope. Confirm that
                // every level agrees.
                self.levels
                    .first()
                    .map(|level| level.envelope.event_timestamp_ns)
                    .unwrap_or(0)
            }
        };

        if provenance == DepthTimestampProvenance::Exchange {
            for (index, level) in self.levels.iter().enumerate() {
                if level.envelope.event_timestamp_ns != expected_timestamp_ns {
                    return Err(format!(
                        "level[{index}] timestamp disagrees \
                     with exchange timestamp"
                    ));
                }
            }
        } else {
            for (index, level) in self.levels.iter().enumerate() {
                if level.envelope.event_timestamp_ns != expected_timestamp_ns {
                    return Err(format!(
                        "level[{index}] receive-time \
                     timestamps are inconsistent"
                    ));
                }
            }
        }

        self.timestamp_provenance = Some(provenance);

        Ok(self)
    }

    pub fn timestamp_provenance(&self) -> Option<DepthTimestampProvenance> {
        self.timestamp_provenance
    }
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn source(&self) -> NormalizationSource {
        self.source
    }

    pub fn kind(&self) -> DepthMessageKind {
        self.kind
    }

    pub fn sequence(&self) -> &SequenceMetadata {
        &self.sequence
    }

    pub fn levels(&self) -> &[L2LevelUpdate] {
        &self.levels
    }

    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    pub fn message_timestamp_ns(&self) -> Option<i64> {
        self.message_timestamp_ns
    }

    pub fn declared_depth(&self) -> Option<u64> {
        self.declared_depth
    }

    pub fn into_levels(self) -> Vec<L2LevelUpdate> {
        self.levels
    }
}

// ============================================================
// NORMALIZED MESSAGE
// ============================================================

/// Complete result of decoding one native market-data
/// message or response.
///
/// Trade and depth records remain grouped until the
/// appropriate downstream component accepts them.
///
/// This structure owns its records and can be transferred
/// between bounded runtime queues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedMessage {
    Trades(NormalizedTrades),

    Depth(NormalizedDepth),
}

impl NormalizedMessage {
    pub fn stream_id(&self) -> &str {
        match self {
            Self::Trades(message) => message.stream_id(),
            Self::Depth(message) => message.stream_id(),
        }
    }

    pub fn source(&self) -> NormalizationSource {
        match self {
            Self::Trades(message) => message.source(),
            Self::Depth(message) => message.source(),
        }
    }

    pub fn record_count(&self) -> usize {
        match self {
            Self::Trades(message) => message.record_count(),
            Self::Depth(message) => message.level_count(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.record_count() == 0
    }
}

// ============================================================
// SHARED VALIDATION
// ============================================================

fn validate_stream_id(stream_id: &str) -> Result<(), String> {
    if stream_id.trim().is_empty() {
        return Err("normalized message stream_id cannot be empty".into());
    }

    Ok(())
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::shared::domain::canonical::{BookSide, EventEnvelope, Exchange, TradeSide};

    use super::*;

    // ========================================================
    // TEST HELPERS
    // ========================================================

    fn envelope(stream_id: &str) -> EventEnvelope {
        EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 42,
            symbol: "BTCUSDT".into(),
            stream_id: stream_id.into(),
        }
    }

    fn trade(stream_id: &str) -> Trade {
        Trade {
            envelope: envelope(stream_id),

            trade_id: Some("123".into()),
            sequence: None,

            side: TradeSide::Buy,

            price: Decimal::new(80_000, 0),

            quantity_base: Some(Decimal::new(1, 3)),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,

            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        }
    }

    fn level(stream_id: &str) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: envelope(stream_id),

            side: BookSide::Bid,

            price: Decimal::new(80_000, 0),

            quantity_base: Some(Decimal::new(1, 0)),
            quantity_quote: None,
            quantity_contracts: None,

            order_count: None,
        }
    }

    // ========================================================
    // TRADE BATCHES
    // ========================================================

    #[test]
    fn constructs_individual_trade_batch() {
        let message = NormalizedTrades::new(
            "stream-1",
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            vec![trade("stream-1")],
        )
        .unwrap();

        assert_eq!(message.record_count(), 1);

        assert_eq!(message.semantics(), TradeRecordSemantics::Individual,);

        assert_eq!(message.source(), NormalizationSource::Rest,);
    }

    #[test]
    fn preserves_aggregated_trade_semantics() {
        let message = NormalizedTrades::new(
            "stream-1",
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Aggregated,
            vec![trade("stream-1")],
        )
        .unwrap();

        assert_eq!(message.semantics(), TradeRecordSemantics::Aggregated,);

        assert_eq!(message.record_count(), 1);
    }

    #[test]
    fn rejects_trade_from_another_stream() {
        let result = NormalizedTrades::new(
            "stream-1",
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            vec![trade("stream-2")],
        );

        assert!(result.is_err());
    }

    #[test]
    fn accepts_empty_trade_batch() {
        let message = NormalizedTrades::new(
            "stream-1",
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            Vec::new(),
        )
        .unwrap();

        assert!(message.is_empty());
    }

    // ========================================================
    // DEPTH MESSAGES
    // ========================================================

    #[test]
    fn constructs_atomic_depth_snapshot() {
        let sequence = SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(123),
            sequence_previous: None,
            cross_sequence: None,
        };

        let message = NormalizedDepth::new(
            "stream-1",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            sequence,
            vec![level("stream-1"), level("stream-1")],
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap();

        assert_eq!(message.kind(), DepthMessageKind::Snapshot,);

        assert_eq!(message.level_count(), 2);

        assert_eq!(message.sequence().sequence_last, Some(123),);

        assert_eq!(message.declared_depth(), Some(200),);
    }

    #[test]
    fn constructs_incremental_depth_delta() {
        let sequence = SequenceMetadata {
            sequence_first: Some(124),
            sequence_last: Some(125),
            sequence_previous: Some(123),
            cross_sequence: None,
        };

        let message = NormalizedDepth::new(
            "stream-1",
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            sequence,
            vec![level("stream-1")],
            None,
            None,
        )
        .unwrap();

        assert_eq!(message.kind(), DepthMessageKind::Delta,);

        assert_eq!(message.sequence().sequence_previous, Some(123),);
    }

    #[test]
    fn rejects_depth_from_another_stream() {
        let result = NormalizedDepth::new(
            "stream-1",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            vec![level("stream-2")],
            None,
            Some(100),
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_zero_declared_depth() {
        let result = NormalizedDepth::new(
            "stream-1",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            Vec::new(),
            None,
            Some(0),
        );

        assert!(result.is_err());
    }

    #[test]
    fn preserves_missing_sequence_information() {
        let message = NormalizedDepth::new(
            "stream-1",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            vec![level("stream-1")],
            None,
            None,
        )
        .unwrap();

        assert_eq!(message.sequence(), &SequenceMetadata::default(),);

        assert_eq!(message.message_timestamp_ns(), None);
    }

    // ========================================================
    // UNIFIED MESSAGE
    // ========================================================

    #[test]
    fn unified_message_exposes_trade_count() {
        let trades = NormalizedTrades::new(
            "stream-1",
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            vec![trade("stream-1"), trade("stream-1")],
        )
        .unwrap();

        let message = NormalizedMessage::Trades(trades);

        assert_eq!(message.stream_id(), "stream-1");
        assert_eq!(message.record_count(), 2);
        assert!(!message.is_empty());
    }

    #[test]
    fn rejects_empty_stream_identity() {
        let result = NormalizedTrades::new(
            "   ",
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            Vec::new(),
        );

        assert!(result.is_err());
    }
}
