use crate::{
    live::infrastructure::normalization::{NormalizationSource, NormalizedDepth},
    shared::domain::canonical::Exchange,
};

// ============================================================
// SOURCE EPOCH
// ============================================================

/// Runtime-owned acquisition identity.
///
/// These values must be assigned by the trusted source
/// runtime, not parsed from untrusted exchange payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthSourceEpoch {
    pub stream_generation: u64,
    pub connection_generation: u64,
    pub subscription_generation: u64,
}

// ============================================================
// BINDING ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepthSourceBindingError {
    InvalidBinding(&'static str),

    ObsoleteEpoch,

    WrongStream,

    WrongSource,

    WrongExchange,

    WrongInstrument,

    WrongSymbol,

    WrongDepth,
}

// ============================================================
// IMMUTABLE SOURCE BINDING
// ============================================================

/// Authoritative runtime binding for one depth subscription.
///
/// Market identity is established by the source registry.
/// Canonical L2 envelopes do not independently expose it.
///
/// An empty depth message contains no level envelopes;
/// for that case provenance relies on the trusted runtime
/// epoch and normalized-message routing.
///
/// The binding must be replaced through an explicit
/// generation transition, never silently mutated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSourceBinding {
    stream_id: String,

    exchange: Exchange,

    market: String,

    instrument_id: i64,

    symbol: String,

    subscribed_depth: Option<u64>,

    epoch: DepthSourceEpoch,
}

impl DepthSourceBinding {
    pub fn new(
        stream_id: impl Into<String>,
        exchange: Exchange,
        market: impl Into<String>,
        instrument_id: i64,
        symbol: impl Into<String>,
        subscribed_depth: Option<u64>,
        epoch: DepthSourceEpoch,
    ) -> Result<Self, DepthSourceBindingError> {
        let stream_id = stream_id.into();
        let market = market.into();
        let symbol = symbol.into();

        if stream_id.trim().is_empty() {
            return Err(DepthSourceBindingError::InvalidBinding("empty stream ID"));
        }

        if market.trim().is_empty() {
            return Err(DepthSourceBindingError::InvalidBinding("empty market"));
        }

        if symbol.trim().is_empty() {
            return Err(DepthSourceBindingError::InvalidBinding("empty symbol"));
        }

        if instrument_id <= 0 {
            return Err(DepthSourceBindingError::InvalidBinding(
                "instrument ID must be positive",
            ));
        }

        if subscribed_depth == Some(0) {
            return Err(DepthSourceBindingError::InvalidBinding(
                "subscribed depth must be positive",
            ));
        }

        Ok(Self {
            stream_id,
            exchange,
            market,
            instrument_id,
            symbol,
            subscribed_depth,
            epoch,
        })
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    pub fn market(&self) -> &str {
        &self.market
    }

    pub fn instrument_id(&self) -> i64 {
        self.instrument_id
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn subscribed_depth(&self) -> Option<u64> {
        self.subscribed_depth
    }

    pub fn epoch(&self) -> DepthSourceEpoch {
        self.epoch
    }

    // ========================================================
    // EPOCH VALIDATION
    // ========================================================

    /// Validate trusted runtime provenance.
    ///
    /// Stream, connection, and subscription generation
    /// must all match the authoritative binding.
    pub fn validate_epoch(
        &self,
        received: DepthSourceEpoch,
    ) -> Result<(), DepthSourceBindingError> {
        if received != self.epoch {
            return Err(DepthSourceBindingError::ObsoleteEpoch);
        }

        Ok(())
    }

    // ========================================================
    // NORMALIZED MESSAGE VALIDATION
    // ========================================================

    pub fn validate_message(
        &self,
        received_epoch: DepthSourceEpoch,
        message: &NormalizedDepth,
        expected_source: NormalizationSource,
    ) -> Result<(), DepthSourceBindingError> {
        // Obsolete runtime work cannot invalidate the
        // currently authoritative source stream.
        self.validate_epoch(received_epoch)?;

        if message.stream_id() != self.stream_id {
            return Err(DepthSourceBindingError::WrongStream);
        }

        if message.source() != expected_source {
            return Err(DepthSourceBindingError::WrongSource);
        }

        // Per-message depth is optional.
        //
        // When present, it must agree with the authoritative
        // subscription binding. The runtime must not derive
        // the binding from this untrusted field.
        // Subscription depth belongs to the WebSocket channel.
        //
        // REST snapshot coverage belongs to the REST request
        // that produced the response. It is validated separately
        // at the bound REST admission boundary.
        if expected_source == NormalizationSource::WebSocket {
            if let (Some(expected), Some(received)) =
                (self.subscribed_depth, message.declared_depth())
            {
                if expected != received {
                    return Err(DepthSourceBindingError::WrongDepth);
                }
            }
        }

        // Every contained level must belong to the
        // authoritative exchange and instrument.
        //
        // Never assume the message-level stream ID
        // establishes this by itself.
        for level in message.levels() {
            let envelope = &level.envelope;

            if envelope.stream_id != self.stream_id {
                return Err(DepthSourceBindingError::WrongStream);
            }

            if envelope.exchange != self.exchange {
                return Err(DepthSourceBindingError::WrongExchange);
            }

            if envelope.instrument_id != self.instrument_id {
                return Err(DepthSourceBindingError::WrongInstrument);
            }

            if envelope.symbol != self.symbol {
                return Err(DepthSourceBindingError::WrongSymbol);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::{
        live::infrastructure::normalization::DepthMessageKind,
        shared::domain::canonical::{BookSide, EventEnvelope, L2LevelUpdate, SequenceMetadata},
    };

    const STREAM: &str = "bybit:spot:BTCUSDT:depth";

    fn epoch() -> DepthSourceEpoch {
        DepthSourceEpoch {
            stream_generation: 10,
            connection_generation: 20,
            subscription_generation: 30,
        }
    }

    fn binding() -> DepthSourceBinding {
        DepthSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            Some(200),
            epoch(),
        )
        .unwrap()
    }

    fn level(exchange: Exchange, instrument_id: i64, symbol: &str) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange,
                instrument_id,
                symbol: symbol.into(),
                stream_id: STREAM.into(),
            },

            side: BookSide::Bid,
            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::from(3)),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }
    }

    fn message(levels: Vec<L2LevelUpdate>) -> NormalizedDepth {
        NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            DepthMessageKind::Snapshot,
            SequenceMetadata {
                sequence_first: None,
                sequence_last: Some(100),
                sequence_previous: None,
                cross_sequence: Some(200),
            },
            levels,
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap()
    }

    fn valid_message() -> NormalizedDepth {
        message(vec![level(Exchange::Bybit, 42, "BTCUSDT")])
    }

    // ========================================================
    // BINDING CONSTRUCTION
    // ========================================================

    #[test]
    fn constructs_valid_source_binding() {
        let binding = binding();

        assert_eq!(binding.stream_id(), STREAM);
        assert_eq!(binding.exchange(), Exchange::Bybit);
        assert_eq!(binding.market(), "spot");
        assert_eq!(binding.instrument_id(), 42);
        assert_eq!(binding.symbol(), "BTCUSDT");
        assert_eq!(binding.subscribed_depth(), Some(200));
        assert_eq!(binding.epoch(), epoch());
    }

    #[test]
    fn rejects_empty_stream_id() {
        assert!(matches!(
            DepthSourceBinding::new(
                " ",
                Exchange::Bybit,
                "spot",
                42,
                "BTCUSDT",
                Some(200),
                epoch(),
            ),
            Err(DepthSourceBindingError::InvalidBinding(_))
        ));
    }

    #[test]
    fn rejects_empty_market() {
        assert!(matches!(
            DepthSourceBinding::new(
                STREAM,
                Exchange::Bybit,
                "",
                42,
                "BTCUSDT",
                Some(200),
                epoch(),
            ),
            Err(DepthSourceBindingError::InvalidBinding(_))
        ));
    }

    #[test]
    fn rejects_empty_symbol() {
        assert!(matches!(
            DepthSourceBinding::new(STREAM, Exchange::Bybit, "spot", 42, "", Some(200), epoch(),),
            Err(DepthSourceBindingError::InvalidBinding(_))
        ));
    }

    #[test]
    fn rejects_nonpositive_instrument_id() {
        for instrument_id in [0_i64, -1_i64] {
            assert!(matches!(
                DepthSourceBinding::new(
                    STREAM,
                    Exchange::Bybit,
                    "spot",
                    instrument_id,
                    "BTCUSDT",
                    Some(200),
                    epoch(),
                ),
                Err(DepthSourceBindingError::InvalidBinding(_))
            ));
        }
    }

    #[test]
    fn rejects_zero_subscription_depth() {
        assert!(matches!(
            DepthSourceBinding::new(
                STREAM,
                Exchange::Bybit,
                "spot",
                42,
                "BTCUSDT",
                Some(0),
                epoch(),
            ),
            Err(DepthSourceBindingError::InvalidBinding(_))
        ));
    }

    // ========================================================
    // EPOCH FENCING
    // ========================================================

    #[test]
    fn accepts_matching_epoch() {
        assert_eq!(binding().validate_epoch(epoch()), Ok(()));
    }

    #[test]
    fn rejects_obsolete_stream_generation() {
        let old = DepthSourceEpoch {
            stream_generation: 9,
            ..epoch()
        };

        assert_eq!(
            binding().validate_epoch(old),
            Err(DepthSourceBindingError::ObsoleteEpoch)
        );
    }

    #[test]
    fn rejects_different_connection_generation() {
        for generation in [19, 21] {
            let received = DepthSourceEpoch {
                connection_generation: generation,
                ..epoch()
            };

            assert_eq!(
                binding().validate_epoch(received),
                Err(DepthSourceBindingError::ObsoleteEpoch)
            );
        }
    }

    #[test]
    fn rejects_different_subscription_generation() {
        for generation in [29, 31] {
            let received = DepthSourceEpoch {
                subscription_generation: generation,
                ..epoch()
            };

            assert_eq!(
                binding().validate_epoch(received),
                Err(DepthSourceBindingError::ObsoleteEpoch)
            );
        }
    }

    // ========================================================
    // MESSAGE IDENTITY
    // ========================================================

    #[test]
    fn accepts_correct_source_and_instrument() {
        assert_eq!(
            binding().validate_message(epoch(), &valid_message(), NormalizationSource::WebSocket,),
            Ok(())
        );
    }

    #[test]
    fn rejects_wrong_exchange() {
        let input = message(vec![level(Exchange::Binance, 42, "BTCUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::WrongExchange)
        );
    }

    #[test]
    fn rejects_wrong_instrument() {
        let input = message(vec![level(Exchange::Bybit, 999, "BTCUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::WrongInstrument)
        );
    }

    #[test]
    fn rejects_wrong_symbol() {
        let input = message(vec![level(Exchange::Bybit, 42, "ETHUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::WrongSymbol)
        );
    }

    #[test]
    fn rejects_wrong_normalization_source() {
        let input = valid_message();

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::Rest,),
            Err(DepthSourceBindingError::WrongSource)
        );
    }

    #[test]
    fn rejects_subscription_depth_mismatch() {
        let input = NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            vec![level(Exchange::Bybit, 42, "BTCUSDT")],
            None,
            Some(50),
        )
        .unwrap();

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::WrongDepth)
        );
    }

    #[test]
    fn accepts_missing_message_depth_with_bound_subscription() {
        let input = NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            vec![level(Exchange::Bybit, 42, "BTCUSDT")],
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Ok(())
        );
    }

    #[test]
    fn rejects_mixed_instrument_batch() {
        let input = message(vec![
            level(Exchange::Bybit, 42, "BTCUSDT"),
            level(Exchange::Bybit, 999, "BTCUSDT"),
        ]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::WrongInstrument)
        );
    }

    // ========================================================
    // VALIDATION ORDER
    // ========================================================

    #[test]
    fn obsolete_epoch_is_rejected_before_payload_identity() {
        let old = DepthSourceEpoch {
            stream_generation: 9,
            ..epoch()
        };

        let invalid = message(vec![level(Exchange::Binance, 999, "ETHUSDT")]);

        assert_eq!(
            binding().validate_message(old, &invalid, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::ObsoleteEpoch)
        );
    }

    #[test]
    fn empty_level_batch_depends_on_trusted_runtime_epoch() {
        let empty = message(vec![]);

        assert_eq!(
            binding().validate_message(epoch(), &empty, NormalizationSource::WebSocket,),
            Ok(())
        );

        let stale = DepthSourceEpoch {
            subscription_generation: 29,
            ..epoch()
        };

        assert_eq!(
            binding().validate_message(stale, &empty, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::ObsoleteEpoch)
        );
    }
    #[test]
    fn rest_snapshot_depth_is_not_websocket_subscription_depth() {
        let source = binding();

        // The authoritative WebSocket subscription is 200.
        assert_eq!(source.subscribed_depth(), Some(200));

        let rest_snapshot = NormalizedDepth::new(
            STREAM,
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            vec![level(Exchange::Bybit, 42, "BTCUSDT")],
            None,
            Some(1000),
        )
        .unwrap();

        // REST coverage must not be compared against the
        // WebSocket subscription depth.
        assert_eq!(
            source.validate_message(epoch(), &rest_snapshot, NormalizationSource::Rest,),
            Ok(()),
        );

        // The same declared depth on a WebSocket message
        // must still be rejected.
        let websocket_snapshot = NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            vec![level(Exchange::Bybit, 42, "BTCUSDT")],
            None,
            Some(1000),
        )
        .unwrap();

        assert_eq!(
            source.validate_message(epoch(), &websocket_snapshot, NormalizationSource::WebSocket,),
            Err(DepthSourceBindingError::WrongDepth),
        );
    }
}
