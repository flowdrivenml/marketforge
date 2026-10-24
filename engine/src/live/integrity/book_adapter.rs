use crate::{
    book::{BookChange, BookLevel},
    canonical::L2LevelUpdate,
};

/// Convert one canonical L2 level into the representation
/// accepted by the existing shared BookStore.
///
/// No quantity conversion is performed here:
/// normalization has already established canonical units.
///
/// No book state is mutated.
pub fn to_book_change(level: &L2LevelUpdate) -> BookChange {
    (
        level.side,
        level.price,
        BookLevel {
            quantity_base: level.quantity_base,
            quantity_quote: level.quantity_quote,
            quantity_contracts: level.quantity_contracts,
            order_count: level.order_count,
        },
    )
}

/// Convert a complete normalized level batch while
/// preserving its original order.
///
/// The caller remains responsible for validating message
/// integrity and applying the batch atomically.
pub fn to_book_changes(levels: &[L2LevelUpdate]) -> Vec<BookChange> {
    levels.iter().map(to_book_change).collect()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate};

    use super::*;

    fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Binance,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: "stream-42".into(),
            },
            side,
            price: Decimal::from(price),
            quantity_base: Some(Decimal::from(quantity)),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }
    }

    #[test]
    fn preserves_canonical_level_values() {
        let source = level(BookSide::Bid, 80_000, 3);

        let (side, price, book_level) = to_book_change(&source);

        assert_eq!(side, BookSide::Bid);
        assert_eq!(price, Decimal::from(80_000));

        assert_eq!(book_level.quantity_base, Some(Decimal::from(3)),);

        assert_eq!(book_level.quantity_quote, None);
        assert_eq!(book_level.quantity_contracts, None);
        assert_eq!(book_level.order_count, None);
    }

    #[test]
    fn preserves_batch_order_and_zero_deletions() {
        let source = vec![
            level(BookSide::Ask, 80_010, 2),
            level(BookSide::Bid, 80_000, 1),
            level(BookSide::Bid, 79_990, 0),
        ];

        let changes = to_book_changes(&source);

        assert_eq!(changes.len(), 3);

        assert_eq!(changes[0].0, BookSide::Ask);
        assert_eq!(changes[1].0, BookSide::Bid);
        assert_eq!(changes[2].0, BookSide::Bid);

        assert_eq!(changes[2].2.quantity_base, Some(Decimal::ZERO),);
    }
}
