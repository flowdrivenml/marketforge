use std::collections::BTreeMap;
use std::mem::size_of;

use rust_decimal::Decimal;

use crate::{
    shared::domain::book::{BookChange, BookLevel, BookStore},
    shared::domain::canonical::{BookSide, L2LevelUpdate},
};

// ============================================================
// RETAINED BOOK LIMITS
// ============================================================

/// Admission limits for one authoritative retained order book.
///
/// These limits apply to the combined bid and ask sides.
///
/// `max_estimated_bytes` uses a conservative per-level
/// accounting charge. It does not measure exact allocator
/// usage or establish a process-wide RSS ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthBookLimits {
    pub max_levels: usize,
    pub max_estimated_bytes: usize,
}

impl Default for DepthBookLimits {
    fn default() -> Self {
        Self {
            max_levels: 20_000,
            max_estimated_bytes: 32 * 1024 * 1024,
        }
    }
}

impl DepthBookLimits {
    pub fn validate(self) -> Result<Self, DepthBookBudgetError> {
        if self.max_levels == 0 || self.max_estimated_bytes == 0 {
            return Err(DepthBookBudgetError::InvalidLimits);
        }

        Ok(self)
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthBookBudgetError {
    InvalidLimits,

    TooManyLevels { projected: usize, maximum: usize },

    EstimatedMemoryExceeded { projected: usize, maximum: usize },

    ArithmeticOverflow,
}

// ============================================================
// BOOK BUDGET
// ============================================================

/// Calculates whether an order-book operation fits
/// the configured retained-state budget.
///
/// The operation does not mutate BookStore.
///
/// It does not replace BookStore's validation of:
/// - Prices.
/// - Quantities.
/// - Duplicate price levels.
/// - Snapshot initialization.
/// - Atomic batch application.
pub struct DepthBookBudget;

impl DepthBookBudget {
    /// Conservative accounting charge for one retained
    /// BTreeMap price level.
    ///
    /// The charge includes:
    /// - The map key and BookLevel value.
    /// - Additional node/pointer overhead.
    /// - A multiplier for allocator and structural overhead.
    ///
    /// This is intentionally a budgeting policy rather
    /// than an exact measurement of Rust allocations.
    pub fn bytes_per_level() -> Result<usize, DepthBookBudgetError> {
        let entry = size_of::<Decimal>()
            .checked_add(size_of::<BookLevel>())
            .and_then(|n| n.checked_add(64))
            .ok_or(DepthBookBudgetError::ArithmeticOverflow)?;

        entry
            .checked_mul(4)
            .ok_or(DepthBookBudgetError::ArithmeticOverflow)
    }

    /// Validate a proposed total retained-level count.
    pub fn validate_count(
        projected_levels: usize,
        limits: DepthBookLimits,
    ) -> Result<(), DepthBookBudgetError> {
        limits.validate()?;

        if projected_levels > limits.max_levels {
            return Err(DepthBookBudgetError::TooManyLevels {
                projected: projected_levels,
                maximum: limits.max_levels,
            });
        }

        let projected_bytes = projected_levels
            .checked_mul(Self::bytes_per_level()?)
            .ok_or(DepthBookBudgetError::ArithmeticOverflow)?;

        if projected_bytes > limits.max_estimated_bytes {
            return Err(DepthBookBudgetError::EstimatedMemoryExceeded {
                projected: projected_bytes,
                maximum: limits.max_estimated_bytes,
            });
        }

        Ok(())
    }

    /// Validate the complete retained state of a book.
    ///
    /// Useful as a final check before a private candidate
    /// becomes authoritative.
    pub fn validate_book(
        book: &BookStore,
        limits: DepthBookLimits,
    ) -> Result<(), DepthBookBudgetError> {
        let total = book
            .bids()
            .len()
            .checked_add(book.asks().len())
            .ok_or(DepthBookBudgetError::ArithmeticOverflow)?;

        Self::validate_count(total, limits)
    }

    /// Preflight the resource budget for a complete snapshot.
    ///
    /// Snapshot input is expected to represent the final
    /// retained levels, not an incremental diff.
    ///
    /// BookStore remains responsible for detecting malformed
    /// or duplicate snapshot levels.
    ///
    /// We deliberately charge every supplied level rather
    /// than trusting that duplicate levels will collapse.
    pub fn validate_snapshot(
        levels: &[L2LevelUpdate],
        limits: DepthBookLimits,
    ) -> Result<(), DepthBookBudgetError> {
        Self::validate_count(levels.len(), limits)
    }

    /// Project the final retained-level count after one
    /// atomic batch of absolute updates.
    ///
    /// No BookStore state is changed.
    ///
    /// A repeated price inside a batch is accounted for
    /// only once. BookStore will still reject that duplicate
    /// during its own structural validation.
    ///
    /// Zero quantities represent deletions.
    pub fn validate_delta(
        book: &BookStore,
        changes: &[BookChange],
        limits: DepthBookLimits,
    ) -> Result<(), DepthBookBudgetError> {
        let initial_count = book
            .bids()
            .len()
            .checked_add(book.asks().len())
            .ok_or(DepthBookBudgetError::ArithmeticOverflow)?;

        // Last proposed presence state for each affected
        // side/price pair.
        let mut proposed: BTreeMap<(BookSide, Decimal), bool> = BTreeMap::new();

        for (side, price, level) in changes {
            proposed.insert((*side, *price), !level.is_zero());
        }

        let mut projected = initial_count;

        for ((side, price), will_exist) in proposed {
            let currently_exists = match side {
                BookSide::Bid => book.bids().contains_key(&price),
                BookSide::Ask => book.asks().contains_key(&price),
            };

            match (currently_exists, will_exist) {
                (false, true) => {
                    projected = projected
                        .checked_add(1)
                        .ok_or(DepthBookBudgetError::ArithmeticOverflow)?;
                }

                (true, false) => {
                    projected = projected
                        .checked_sub(1)
                        .ok_or(DepthBookBudgetError::ArithmeticOverflow)?;
                }

                (true, true) | (false, false) => {}
            }
        }

        Self::validate_count(projected, limits)
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::shared::domain::canonical::{EventEnvelope, Exchange};

    fn limits(max_levels: usize) -> DepthBookLimits {
        DepthBookLimits {
            max_levels,
            max_estimated_bytes: 32 * 1024 * 1024,
        }
    }

    fn level(quantity: i64) -> BookLevel {
        BookLevel {
            quantity_base: Some(Decimal::from(quantity)),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }
    }

    fn initialized_book() -> BookStore {
        let mut book = BookStore::new();

        book.apply_snapshot(
            vec![
                (Decimal::from(100), level(3)),
                (Decimal::from(99), level(2)),
            ],
            vec![(Decimal::from(101), level(4))],
        )
        .unwrap();

        book
    }

    fn change(side: BookSide, price: i64, quantity: i64) -> BookChange {
        (side, Decimal::from(price), level(quantity))
    }

    #[test]
    fn rejects_zero_limits() {
        assert_eq!(
            DepthBookLimits {
                max_levels: 0,
                max_estimated_bytes: 1024,
            }
            .validate(),
            Err(DepthBookBudgetError::InvalidLimits),
        );
    }

    #[test]
    fn accepts_book_at_exact_capacity() {
        let book = initialized_book();

        assert!(DepthBookBudget::validate_book(&book, limits(3),).is_ok());
    }

    #[test]
    fn rejects_book_over_level_capacity() {
        let book = initialized_book();

        assert!(matches!(
            DepthBookBudget::validate_book(&book, limits(2),),
            Err(DepthBookBudgetError::TooManyLevels {
                projected: 3,
                maximum: 2,
            })
        ));
    }

    #[test]
    fn replacement_update_does_not_increase_level_count() {
        let book = initialized_book();

        let updates = vec![change(BookSide::Bid, 100, 8)];

        assert!(DepthBookBudget::validate_delta(&book, &updates, limits(3),).is_ok());
    }

    #[test]
    fn deletion_frees_capacity_for_new_level() {
        let book = initialized_book();

        let updates = vec![change(BookSide::Bid, 99, 0), change(BookSide::Ask, 102, 5)];

        assert!(DepthBookBudget::validate_delta(&book, &updates, limits(3),).is_ok());
    }

    #[test]
    fn rejects_delta_that_grows_book_beyond_capacity() {
        let book = initialized_book();

        let updates = vec![change(BookSide::Ask, 102, 5)];

        assert!(matches!(
            DepthBookBudget::validate_delta(&book, &updates, limits(3),),
            Err(DepthBookBudgetError::TooManyLevels {
                projected: 4,
                maximum: 3,
            })
        ));

        // Budget preflight does not change the book.
        assert_eq!(book.bids().len(), 2);
        assert_eq!(book.asks().len(), 1);
    }

    #[test]
    fn duplicate_price_is_not_double_counted() {
        let book = initialized_book();

        let updates = vec![change(BookSide::Ask, 102, 5), change(BookSide::Ask, 102, 7)];

        // Resource projection sees one new price.
        // BookStore remains responsible for rejecting
        // duplicate keys in an atomic batch.
        assert!(DepthBookBudget::validate_delta(&book, &updates, limits(4),).is_ok());
    }

    #[test]
    fn deletion_of_unknown_price_does_not_change_count() {
        let book = initialized_book();

        let updates = vec![change(BookSide::Bid, 1, 0)];

        assert!(DepthBookBudget::validate_delta(&book, &updates, limits(3),).is_ok());
    }

    #[test]
    fn bid_and_ask_at_same_price_are_separate_entries() {
        let book = initialized_book();

        let updates = vec![change(BookSide::Ask, 100, 5)];

        assert!(matches!(
            DepthBookBudget::validate_delta(&book, &updates, limits(3),),
            Err(DepthBookBudgetError::TooManyLevels {
                projected: 4,
                maximum: 3,
            })
        ));
    }

    #[test]
    fn enforces_estimated_byte_limit() {
        let book = initialized_book();

        let bytes_per_level = DepthBookBudget::bytes_per_level().unwrap();

        let limits = DepthBookLimits {
            max_levels: 100,
            max_estimated_bytes: bytes_per_level * 2,
        };

        assert!(matches!(
            DepthBookBudget::validate_book(&book, limits),
            Err(DepthBookBudgetError::EstimatedMemoryExceeded { .. })
        ));
    }

    #[test]
    fn snapshot_count_is_checked_before_book_construction() {
        let make_level = |price: i64| L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1,
                system_timestamp_ns: None,
                exchange: Exchange::Binance,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: "stream-42".into(),
            },

            side: BookSide::Bid,
            price: Decimal::from(price),
            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        };

        let levels = vec![make_level(100), make_level(99), make_level(98)];

        assert!(matches!(
            DepthBookBudget::validate_snapshot(&levels, limits(2),),
            Err(DepthBookBudgetError::TooManyLevels {
                projected: 3,
                maximum: 2,
            })
        ));
    }
}
