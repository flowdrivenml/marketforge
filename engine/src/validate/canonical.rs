use rust_decimal::Decimal;

use crate::{
    canonical::{
        CanonicalEvent, EventEnvelope, L2Action, L2Level, L2LevelUpdate, L2Snapshot, L2Update,
        Quantity, SequenceMetadata, Trade,
    },
    error::{MarketForgeError, Result},
};

pub fn validate_canonical_event(event: &CanonicalEvent) -> Result<()> {
    match event {
        CanonicalEvent::Trade(trade) => validate_trade(trade),
        CanonicalEvent::L2Snapshot(snapshot) => validate_l2_snapshot(snapshot),
        CanonicalEvent::L2Update(update) => validate_l2_update(update),
    }
}

pub fn validate_trade(trade: &Trade) -> Result<()> {
    validate_envelope(&trade.envelope)?;

    if trade.price <= Decimal::ZERO {
        return invalid("trade price must be greater than zero");
    }

    validate_positive_quantities(
        trade.quantity_base,
        trade.quantity_quote,
        trade.quantity_contracts,
        "trade",
    )?;

    Ok(())
}

pub fn validate_l2_level(level: &L2Level) -> Result<()> {
    if level.price <= Decimal::ZERO {
        return invalid("L2 level price must be greater than zero");
    }

    validate_positive_quantities(
        level.quantity_base,
        level.quantity_quote,
        level.quantity_contracts,
        "L2 level",
    )?;

    Ok(())
}

pub fn validate_l2_level_update(update: &L2LevelUpdate) -> Result<()> {
    if update.price <= Decimal::ZERO {
        return invalid("L2 level update price must be greater than zero");
    }

    match update.action {
        L2Action::Set => {
            validate_positive_quantities(
                update.quantity_base,
                update.quantity_quote,
                update.quantity_contracts,
                "L2 set update",
            )?;
        }

        L2Action::Delete => {
            validate_nonnegative_optional_quantity(
                update.quantity_base,
                "L2 delete quantity_base",
            )?;
            validate_nonnegative_optional_quantity(
                update.quantity_quote,
                "L2 delete quantity_quote",
            )?;
            validate_nonnegative_optional_quantity(
                update.quantity_contracts,
                "L2 delete quantity_contracts",
            )?;
        }
    }

    Ok(())
}

pub fn validate_l2_snapshot(snapshot: &L2Snapshot) -> Result<()> {
    validate_envelope(&snapshot.envelope)?;
    validate_sequence(&snapshot.sequence)?;

    for level in &snapshot.bids {
        validate_l2_level(level)?;
    }

    for level in &snapshot.asks {
        validate_l2_level(level)?;
    }

    if !snapshot
        .bids
        .windows(2)
        .all(|levels| levels[0].price >= levels[1].price)
    {
        return invalid("L2 snapshot bids must be ordered by descending price");
    }

    if !snapshot
        .asks
        .windows(2)
        .all(|levels| levels[0].price <= levels[1].price)
    {
        return invalid("L2 snapshot asks must be ordered by ascending price");
    }

    Ok(())
}

pub fn validate_l2_update(update: &L2Update) -> Result<()> {
    validate_envelope(&update.envelope)?;
    validate_sequence(&update.sequence)?;

    if update.changes.is_empty() {
        return invalid("L2 update must contain at least one change");
    }

    for change in &update.changes {
        validate_l2_level_update(change)?;
    }

    Ok(())
}

fn validate_envelope(envelope: &EventEnvelope) -> Result<()> {
    if envelope.instrument_id <= 0 {
        return invalid("canonical event instrument_id must be greater than zero");
    }

    if envelope.symbol.trim().is_empty() {
        return invalid("canonical event symbol must not be empty");
    }

    if envelope.stream_id.trim().is_empty() {
        return invalid("canonical event stream_id must not be empty");
    }

    Ok(())
}

fn validate_sequence(sequence: &SequenceMetadata) -> Result<()> {
    if let (Some(first), Some(last)) = (sequence.sequence_first, sequence.sequence_last) {
        if first > last {
            return invalid("sequence_first cannot be greater than sequence_last");
        }
    }

    Ok(())
}

fn validate_positive_quantities(
    quantity_base: Option<Quantity>,
    quantity_quote: Option<Quantity>,
    quantity_contracts: Option<Quantity>,
    context: &str,
) -> Result<()> {
    if quantity_base.is_none() && quantity_quote.is_none() && quantity_contracts.is_none() {
        return invalid(format!(
            "{context} must contain at least one quantity representation"
        ));
    }

    for (name, quantity) in [
        ("quantity_base", quantity_base),
        ("quantity_quote", quantity_quote),
        ("quantity_contracts", quantity_contracts),
    ] {
        if let Some(quantity) = quantity {
            if quantity <= Decimal::ZERO {
                return invalid(format!("{context} {name} must be greater than zero"));
            }
        }
    }

    Ok(())
}

fn validate_nonnegative_optional_quantity(quantity: Option<Quantity>, name: &str) -> Result<()> {
    if let Some(quantity) = quantity {
        if quantity < Decimal::ZERO {
            return invalid(format!("{name} cannot be negative"));
        }
    }

    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::canonical::{
        BookSide, EventEnvelope, Exchange, L2Level, L2Snapshot, SequenceMetadata, TradeSide,
    };

    fn valid_trade() -> Trade {
        Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 1,
                symbol: "BTCUSDT".to_owned(),
                stream_id: "bybit:BTCUSDT:trades".to_owned(),
            },

            trade_id: Some("trade-42".to_owned()),
            sequence: None,

            side: TradeSide::Buy,
            price: Decimal::new(85_000, 0),

            quantity_base: Some(Decimal::new(25, 1)),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,
            trade_iv: None,
            mark_iv: None,
            index_price: None,
            mark_price: None,
        }
    }

    #[test]
    fn accepts_valid_trade() {
        let trade = valid_trade();

        assert!(validate_trade(&trade).is_ok());
    }

    #[test]
    fn rejects_zero_price() {
        let mut trade = valid_trade();
        trade.price = Decimal::ZERO;

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_negative_price() {
        let mut trade = valid_trade();
        trade.price = Decimal::new(-100, 0);

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_missing_quantities() {
        let mut trade = valid_trade();
        trade.quantity_base = None;

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_zero_quantity() {
        let mut trade = valid_trade();
        trade.quantity_base = Some(Decimal::ZERO);

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_negative_quantity() {
        let mut trade = valid_trade();
        trade.quantity_base = Some(Decimal::new(-1, 0));

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn accepts_multiple_quantity_representations() {
        let mut trade = valid_trade();

        trade.quantity_quote = Some(Decimal::new(212_500, 0));

        assert!(validate_trade(&trade).is_ok());
    }

    #[test]
    fn rejects_invalid_instrument_id() {
        let mut trade = valid_trade();
        trade.envelope.instrument_id = 0;

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_empty_symbol() {
        let mut trade = valid_trade();
        trade.envelope.symbol = String::new();

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_empty_stream_id() {
        let mut trade = valid_trade();
        trade.envelope.stream_id = String::new();

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn accepts_negative_timestamp() {
        let mut trade = valid_trade();
        trade.envelope.event_timestamp_ns = -1;

        assert!(validate_trade(&trade).is_ok());
    }

    #[test]
    fn validates_trade_through_canonical_event() {
        let event = CanonicalEvent::Trade(valid_trade());

        assert!(validate_canonical_event(&event).is_ok());
    }

    fn valid_snapshot() -> L2Snapshot {
        L2Snapshot {
            envelope: valid_trade().envelope,

            sequence: SequenceMetadata {
                sequence_first: Some(100),
                sequence_last: Some(100),
                sequence_previous: None,
                cross_sequence: None,
            },

            bids: vec![
                L2Level {
                    price: Decimal::new(85_000, 0),
                    quantity_base: Some(Decimal::new(2, 0)),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: Some(5),
                },
                L2Level {
                    price: Decimal::new(84_999, 0),
                    quantity_base: Some(Decimal::new(3, 0)),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: Some(4),
                },
            ],

            asks: vec![
                L2Level {
                    price: Decimal::new(85_001, 0),
                    quantity_base: Some(Decimal::new(1, 0)),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: Some(3),
                },
                L2Level {
                    price: Decimal::new(85_002, 0),
                    quantity_base: Some(Decimal::new(4, 0)),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: Some(2),
                },
            ],
        }
    }

    #[test]
    fn accepts_valid_l2_snapshot() {
        assert!(validate_l2_snapshot(&valid_snapshot()).is_ok());
    }

    #[test]
    fn rejects_snapshot_with_ascending_bids() {
        let mut snapshot = valid_snapshot();
        snapshot.bids.reverse();

        assert!(validate_l2_snapshot(&snapshot).is_err());
    }

    #[test]
    fn rejects_snapshot_with_descending_asks() {
        let mut snapshot = valid_snapshot();
        snapshot.asks.reverse();

        assert!(validate_l2_snapshot(&snapshot).is_err());
    }

    #[test]
    fn rejects_snapshot_level_with_zero_price() {
        let mut snapshot = valid_snapshot();
        snapshot.bids[0].price = Decimal::ZERO;

        assert!(validate_l2_snapshot(&snapshot).is_err());
    }

    #[test]
    fn rejects_snapshot_level_with_negative_quantity() {
        let mut snapshot = valid_snapshot();
        snapshot.asks[0].quantity_base = Some(Decimal::new(-1, 0));

        assert!(validate_l2_snapshot(&snapshot).is_err());
    }

    #[test]
    fn rejects_snapshot_level_without_quantity() {
        let mut snapshot = valid_snapshot();
        snapshot.bids[0].quantity_base = None;

        assert!(validate_l2_snapshot(&snapshot).is_err());
    }

    #[test]
    fn accepts_snapshot_with_empty_book_sides() {
        let mut snapshot = valid_snapshot();
        snapshot.bids.clear();
        snapshot.asks.clear();

        assert!(validate_l2_snapshot(&snapshot).is_ok());
    }

    #[test]
    fn accepts_snapshot_with_multiple_quantity_representations() {
        let mut snapshot = valid_snapshot();

        snapshot.bids[0].quantity_quote = Some(Decimal::new(170_000, 0));

        assert!(validate_l2_snapshot(&snapshot).is_ok());
    }

    #[test]
    fn rejects_snapshot_with_invalid_sequence_range() {
        let mut snapshot = valid_snapshot();

        snapshot.sequence.sequence_first = Some(200);
        snapshot.sequence.sequence_last = Some(100);

        assert!(validate_l2_snapshot(&snapshot).is_err());
    }

    #[test]
    fn accepts_snapshot_without_sequence_metadata() {
        let mut snapshot = valid_snapshot();
        snapshot.sequence = SequenceMetadata::default();

        assert!(validate_l2_snapshot(&snapshot).is_ok());
    }

    #[test]
    fn validates_snapshot_through_canonical_event() {
        let event = CanonicalEvent::L2Snapshot(valid_snapshot());

        assert!(validate_canonical_event(&event).is_ok());
    }
    fn valid_update() -> L2Update {
        L2Update {
            envelope: valid_trade().envelope,
            sequence: SequenceMetadata {
                sequence_first: Some(500),
                sequence_last: Some(500),
                sequence_previous: Some(499),
                cross_sequence: None,
            },
            changes: vec![
                L2LevelUpdate {
                    side: BookSide::Bid,
                    action: L2Action::Set,
                    price: Decimal::new(85_000, 0),
                    quantity_base: Some(Decimal::new(5, 0)),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: None,
                },
                L2LevelUpdate {
                    side: BookSide::Bid,
                    action: L2Action::Delete,
                    price: Decimal::new(84_999, 0),
                    quantity_base: None,
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: None,
                },
                L2LevelUpdate {
                    side: BookSide::Ask,
                    action: L2Action::Set,
                    price: Decimal::new(85_001, 0),
                    quantity_base: Some(Decimal::new(8, 0)),
                    quantity_quote: None,
                    quantity_contracts: None,
                    order_count: None,
                },
            ],
        }
    }
    #[test]
    fn accepts_valid_l2_update() {
        assert!(validate_l2_update(&valid_update()).is_ok());
    }

    #[test]
    fn rejects_empty_l2_update() {
        let mut update = valid_update();
        update.changes.clear();

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn rejects_set_without_quantity() {
        let mut update = valid_update();
        update.changes[0].quantity_base = None;

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn rejects_set_with_zero_quantity() {
        let mut update = valid_update();
        update.changes[0].quantity_base = Some(Decimal::ZERO);

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn rejects_set_with_negative_quantity() {
        let mut update = valid_update();
        update.changes[0].quantity_base = Some(Decimal::new(-1, 0));

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn accepts_delete_without_quantity() {
        assert!(validate_l2_update(&valid_update()).is_ok());
    }

    #[test]
    fn accepts_delete_with_zero_quantity() {
        let mut update = valid_update();
        update.changes[1].quantity_base = Some(Decimal::ZERO);

        assert!(validate_l2_update(&update).is_ok());
    }

    #[test]
    fn rejects_delete_with_negative_quantity() {
        let mut update = valid_update();
        update.changes[1].quantity_base = Some(Decimal::new(-1, 0));

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn rejects_update_with_invalid_price() {
        let mut update = valid_update();
        update.changes[0].price = Decimal::ZERO;

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn rejects_update_with_invalid_sequence_range() {
        let mut update = valid_update();

        update.sequence.sequence_first = Some(501);
        update.sequence.sequence_last = Some(500);

        assert!(validate_l2_update(&update).is_err());
    }

    #[test]
    fn accepts_update_without_sequence_metadata() {
        let mut update = valid_update();
        update.sequence = SequenceMetadata::default();

        assert!(validate_l2_update(&update).is_ok());
    }

    #[test]
    fn preserves_atomic_update_structure() {
        let update = valid_update();

        assert_eq!(update.changes.len(), 3);
        assert!(validate_l2_update(&update).is_ok());
    }

    #[test]
    fn validates_update_through_canonical_event() {
        let event = CanonicalEvent::L2Update(valid_update());

        assert!(validate_canonical_event(&event).is_ok());
    }
}
