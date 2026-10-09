use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::{
    canonical::{BookSide, Price},
    error::Result,
};

use super::{
    BookLevel, BookStore,
    validation::{invalid, validate_level, validate_price},
};

impl BookStore {
    /// Replace the book with an authoritative snapshot.
    ///
    /// Emits only changed levels.
    pub fn apply_snapshot(
        &mut self,
        bids: Vec<(Price, BookLevel)>,
        asks: Vec<(Price, BookLevel)>,
    ) -> Result<Vec<(BookSide, Price, BookLevel)>> {
        let new_bids = build_side(bids)?;
        let new_asks = build_side(asks)?;

        let mut changes = Vec::new();

        diff_side(BookSide::Bid, &self.bids, &new_bids, &mut changes);
        diff_side(BookSide::Ask, &self.asks, &new_asks, &mut changes);

        self.bids = new_bids;
        self.asks = new_asks;
        self.initialized = true;

        Ok(changes)
    }
}

fn build_side(levels: Vec<(Price, BookLevel)>) -> Result<BTreeMap<Price, BookLevel>> {
    let mut book = BTreeMap::new();

    for (price, level) in levels {
        validate_price(price)?;
        validate_level(&level)?;

        if level.is_zero() {
            return invalid("snapshot levels must contain positive quantities");
        }

        if book.insert(price, level).is_some() {
            return invalid("snapshot contains duplicate price levels");
        }
    }

    Ok(book)
}

fn diff_side(
    side: BookSide,
    previous: &BTreeMap<Price, BookLevel>,
    current: &BTreeMap<Price, BookLevel>,
    changes: &mut Vec<(BookSide, Price, BookLevel)>,
) {
    for (price, old_level) in previous {
        if !current.contains_key(price) {
            changes.push((
                side,
                *price,
                BookLevel {
                    quantity_base: old_level.quantity_base.map(|_| Decimal::ZERO),
                    quantity_quote: old_level.quantity_quote.map(|_| Decimal::ZERO),
                    quantity_contracts: old_level.quantity_contracts.map(|_| Decimal::ZERO),
                    order_count: old_level.order_count.map(|_| 0),
                },
            ));
        }
    }

    for (price, level) in current {
        if previous.get(price) != Some(level) {
            changes.push((side, *price, level.clone()));
        }
    }
}
