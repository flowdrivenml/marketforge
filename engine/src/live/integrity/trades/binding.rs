use crate::{
    canonical::Exchange,
    live::infrastructure::normalization::{NormalizationSource, NormalizedTrades},
};

// ============================================================
// TRADE SOURCE EPOCH
// ============================================================

/// Trusted runtime identity of one trade acquisition epoch.
///
/// These values must come from the authoritative runtime
/// registry, never from exchange-controlled payload fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradeSourceEpoch {
    pub stream_generation: u64,
    pub connection_generation: u64,
    pub subscription_generation: u64,
}

// ============================================================
// SOURCE VALIDATION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeSourceBindingError {
    InvalidBinding(&'static str),

    ObsoleteEpoch,

    WrongStream,
    WrongSource,
    WrongExchange,
    WrongInstrument,
    WrongSymbol,
}

// ============================================================
// IMMUTABLE BINDING
// ============================================================

/// Authoritative binding of a logical trade stream.
///
/// The binding establishes:
///
/// - The owning logical stream.
/// - The expected exchange and market.
/// - The expected canonical instrument.
/// - The exact canonical symbol.
/// - The runtime acquisition epoch.
///
/// The canonical Trade envelope has no independent market
/// field. Market identity is established through the
/// trusted stream/subscription registry.
///
/// Individual trades must not be accepted merely because
/// their stream_id matches an expected string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeSourceBinding {
    stream_id: String,
    exchange: Exchange,
    market: String,
    instrument_id: i64,
    symbol: String,
    epoch: TradeSourceEpoch,
}

impl TradeSourceBinding {
    pub fn new(
        stream_id: impl Into<String>,
        exchange: Exchange,
        market: impl Into<String>,
        instrument_id: i64,
        symbol: impl Into<String>,
        epoch: TradeSourceEpoch,
    ) -> Result<Self, TradeSourceBindingError> {
        let stream_id = stream_id.into();
        let market = market.into();
        let symbol = symbol.into();

        if stream_id.trim().is_empty() {
            return Err(TradeSourceBindingError::InvalidBinding("empty stream ID"));
        }

        if market.trim().is_empty() {
            return Err(TradeSourceBindingError::InvalidBinding("empty market"));
        }

        if instrument_id <= 0 {
            return Err(TradeSourceBindingError::InvalidBinding(
                "instrument ID must be positive",
            ));
        }

        if symbol.trim().is_empty() {
            return Err(TradeSourceBindingError::InvalidBinding("empty symbol"));
        }

        Ok(Self {
            stream_id,
            exchange,
            market,
            instrument_id,
            symbol,
            epoch,
        })
    }

    // ========================================================
    // INSPECTION
    // ========================================================

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

    pub fn epoch(&self) -> TradeSourceEpoch {
        self.epoch
    }

    // ========================================================
    // RUNTIME EPOCH VALIDATION
    // ========================================================

    pub fn validate_epoch(
        &self,
        received: TradeSourceEpoch,
    ) -> Result<(), TradeSourceBindingError> {
        if received != self.epoch {
            return Err(TradeSourceBindingError::ObsoleteEpoch);
        }

        Ok(())
    }

    // ========================================================
    // TRADE MESSAGE VALIDATION
    // ========================================================

    /// Validate a complete normalized trade message.
    ///
    /// Validation does not mutate controller state.
    ///
    /// Epoch checks occur first, so an obsolete message
    /// cannot invalidate or quarantine the current stream.
    ///
    /// Every contained trade must match the authoritative
    /// exchange, instrument and canonical symbol.
    ///
    /// Empty batches have no individual trade envelopes.
    /// Their provenance therefore depends on trusted
    /// runtime epoch and message-level routing.
    pub fn validate_message(
        &self,
        received_epoch: TradeSourceEpoch,
        message: &NormalizedTrades,
        expected_source: NormalizationSource,
    ) -> Result<(), TradeSourceBindingError> {
        self.validate_epoch(received_epoch)?;

        if message.stream_id() != self.stream_id {
            return Err(TradeSourceBindingError::WrongStream);
        }

        if message.source() != expected_source {
            return Err(TradeSourceBindingError::WrongSource);
        }

        for trade in message.trades() {
            let envelope = &trade.envelope;

            if envelope.stream_id != self.stream_id {
                return Err(TradeSourceBindingError::WrongStream);
            }

            if envelope.exchange != self.exchange {
                return Err(TradeSourceBindingError::WrongExchange);
            }

            if envelope.instrument_id != self.instrument_id {
                return Err(TradeSourceBindingError::WrongInstrument);
            }

            if envelope.symbol != self.symbol {
                return Err(TradeSourceBindingError::WrongSymbol);
            }
        }

        Ok(())
    }

    // ========================================================
    // REBINDING IDENTITY
    // ========================================================

    /// Validate that a proposed newer acquisition epoch
    /// still belongs to the same logical trade stream.
    ///
    /// Changes to exchange, market, instrument or symbol
    /// require constructing a different logical stream.
    pub fn validate_successor(&self, next: &Self) -> Result<(), TradeSourceBindingError> {
        if next.stream_id != self.stream_id
            || next.exchange != self.exchange
            || next.market != self.market
            || next.instrument_id != self.instrument_id
            || next.symbol != self.symbol
        {
            return Err(TradeSourceBindingError::InvalidBinding(
                "logical source identity changed",
            ));
        }

        if next.epoch.stream_generation <= self.epoch.stream_generation {
            return Err(TradeSourceBindingError::InvalidBinding(
                "stream generation must increase",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::{
        canonical::{EventEnvelope, Trade, TradeSide},
        live::infrastructure::normalization::TradeRecordSemantics,
    };

    const STREAM: &str = "bybit:spot:BTCUSDT:trades";

    fn epoch() -> TradeSourceEpoch {
        TradeSourceEpoch {
            stream_generation: 10,
            connection_generation: 20,
            subscription_generation: 30,
        }
    }

    fn binding() -> TradeSourceBinding {
        TradeSourceBinding::new(STREAM, Exchange::Bybit, "spot", 42, "BTCUSDT", epoch()).unwrap()
    }

    fn trade(exchange: Exchange, instrument_id: i64, symbol: &str) -> Trade {
        Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange,
                instrument_id,
                symbol: symbol.into(),
                stream_id: STREAM.into(),
            },

            trade_id: Some("trade-123".into()),
            sequence: None,

            side: TradeSide::Buy,
            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,
            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        }
    }

    fn message(trades: Vec<Trade>) -> NormalizedTrades {
        NormalizedTrades::new(
            STREAM,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            trades,
        )
        .unwrap()
    }

    #[test]
    fn valid_binding_preserves_identity() {
        let binding = binding();

        assert_eq!(binding.stream_id(), STREAM);
        assert_eq!(binding.exchange(), Exchange::Bybit);
        assert_eq!(binding.market(), "spot");
        assert_eq!(binding.instrument_id(), 42);
        assert_eq!(binding.symbol(), "BTCUSDT");
        assert_eq!(binding.epoch(), epoch());
    }

    #[test]
    fn rejects_nonpositive_instrument_id() {
        for id in [0_i64, -1_i64] {
            assert!(matches!(
                TradeSourceBinding::new(STREAM, Exchange::Bybit, "spot", id, "BTCUSDT", epoch(),),
                Err(TradeSourceBindingError::InvalidBinding(_))
            ));
        }
    }

    #[test]
    fn validates_correct_trade_message() {
        let input = message(vec![trade(Exchange::Bybit, 42, "BTCUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Ok(())
        );
    }

    #[test]
    fn rejects_wrong_exchange() {
        let input = message(vec![trade(Exchange::Binance, 42, "BTCUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(TradeSourceBindingError::WrongExchange)
        );
    }

    #[test]
    fn rejects_wrong_instrument() {
        let input = message(vec![trade(Exchange::Bybit, 999, "BTCUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(TradeSourceBindingError::WrongInstrument)
        );
    }

    #[test]
    fn rejects_wrong_symbol() {
        let input = message(vec![trade(Exchange::Bybit, 42, "ETHUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(TradeSourceBindingError::WrongSymbol)
        );
    }

    #[test]
    fn rejects_mixed_instrument_batch() {
        let input = message(vec![
            trade(Exchange::Bybit, 42, "BTCUSDT"),
            trade(Exchange::Bybit, 999, "BTCUSDT"),
        ]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::WebSocket,),
            Err(TradeSourceBindingError::WrongInstrument)
        );
    }

    #[test]
    fn rejects_wrong_normalization_source() {
        let input = message(vec![trade(Exchange::Bybit, 42, "BTCUSDT")]);

        assert_eq!(
            binding().validate_message(epoch(), &input, NormalizationSource::Rest,),
            Err(TradeSourceBindingError::WrongSource)
        );
    }

    #[test]
    fn old_epoch_is_checked_before_bad_payload() {
        let input = message(vec![trade(Exchange::Binance, 999, "ETHUSDT")]);

        let old = TradeSourceEpoch {
            connection_generation: 19,
            ..epoch()
        };

        assert_eq!(
            binding().validate_message(old, &input, NormalizationSource::WebSocket,),
            Err(TradeSourceBindingError::ObsoleteEpoch)
        );
    }

    #[test]
    fn rejects_new_epoch_with_different_instrument() {
        let next = TradeSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            999,
            "BTCUSDT",
            TradeSourceEpoch {
                stream_generation: 11,
                connection_generation: 21,
                subscription_generation: 31,
            },
        )
        .unwrap();

        assert!(binding().validate_successor(&next).is_err());
    }

    #[test]
    fn accepts_new_epoch_for_same_stream() {
        let next = TradeSourceBinding::new(
            STREAM,
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            TradeSourceEpoch {
                stream_generation: 11,
                connection_generation: 21,
                subscription_generation: 31,
            },
        )
        .unwrap();

        assert_eq!(binding().validate_successor(&next), Ok(()));
    }
}
