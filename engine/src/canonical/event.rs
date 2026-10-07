use super::envelope::EventEnvelope;
use super::l2::{L2Snapshot, L2Update};
use super::trade::Trade;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalEvent {
    Trade(Trade),
    L2Snapshot(L2Snapshot),
    L2Update(L2Update),
}

impl CanonicalEvent {
    pub fn envelope(&self) -> &EventEnvelope {
        match self {
            Self::Trade(event) => &event.envelope,
            Self::L2Snapshot(event) => &event.envelope,
            Self::L2Update(event) => &event.envelope,
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
    use crate::canonical::{Exchange, TradeSide};

    #[test]
    fn trade_event_exposes_timestamp() {
        let event = CanonicalEvent::Trade(Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: 123,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 1,
                symbol: "BTCUSDT".to_owned(),
                stream_id: "bybit:BTCUSDT:trades".to_owned(),
            },

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
}
