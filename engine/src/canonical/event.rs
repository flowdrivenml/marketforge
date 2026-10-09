use super::{envelope::EventEnvelope, l2::L2LevelUpdate, trade::Trade};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalEvent {
    Trade(Trade),
    L2LevelUpdate(L2LevelUpdate),
}

impl CanonicalEvent {
    pub fn envelope(&self) -> &EventEnvelope {
        match self {
            Self::Trade(event) => &event.envelope,
            Self::L2LevelUpdate(event) => &event.envelope,
        }
    }

    pub fn event_timestamp_ns(&self) -> i64 {
        self.envelope().event_timestamp_ns
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::canonical::{BookSide, Exchange, TradeSide};

    fn envelope() -> EventEnvelope {
        EventEnvelope {
            event_timestamp_ns: 123,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:trades".to_owned(),
        }
    }

    #[test]
    fn trade_event_exposes_timestamp() {
        let event = CanonicalEvent::Trade(Trade {
            envelope: envelope(),

            trade_id: Some("42".to_owned()),
            sequence: None,

            side: TradeSide::Buy,
            price: Decimal::new(100_000, 0),

            quantity_base: Some(Decimal::new(1, 0)),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,

            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        });

        assert_eq!(event.event_timestamp_ns(), 123);
    }

    #[test]
    fn depth_event_exposes_timestamp() {
        let mut metadata = envelope();
        metadata.stream_id = "bybit:BTCUSDT:depth".to_owned();

        let event = CanonicalEvent::L2LevelUpdate(L2LevelUpdate {
            envelope: metadata,

            side: BookSide::Bid,
            price: Decimal::new(100_000, 0),

            quantity_base: Some(Decimal::new(5, 0)),
            quantity_quote: Some(Decimal::new(500_000, 0)),
            quantity_contracts: None,

            order_count: None,
        });

        assert_eq!(event.event_timestamp_ns(), 123);
    }

    #[test]
    fn depth_deletion_uses_zero_quantity() {
        let event = L2LevelUpdate {
            envelope: envelope(),

            side: BookSide::Ask,
            price: Decimal::new(100_000, 0),

            quantity_base: Some(Decimal::ZERO),
            quantity_quote: Some(Decimal::ZERO),
            quantity_contracts: None,

            order_count: None,
        };

        assert_eq!(event.quantity_base, Some(Decimal::ZERO));
        assert_ne!(event.quantity_base, None);
    }
}
