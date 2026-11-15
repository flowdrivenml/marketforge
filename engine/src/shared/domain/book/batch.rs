use std::collections::BTreeSet;

use crate::{
    shared::domain::canonical::{BookSide, Price},
    shared::error::Result,
};

use super::{
    BookChange, BookStore,
    validation::{invalid, validate_level, validate_price},
};

impl BookStore {
    /// Apply a batch of absolute L2 level updates atomically.
    ///
    /// All updates are validated before any state mutation.
    ///
    /// Returns only changes that modify the current book.
    /// Input ordering is preserved.
    pub fn apply_batch(&mut self, updates: Vec<BookChange>) -> Result<Vec<BookChange>> {
        if !self.is_initialized() {
            return invalid("cannot apply batch before book initialization");
        }

        // ---------------------------------------------------------------------
        // Validate complete batch before mutation
        // ---------------------------------------------------------------------

        let mut seen = BTreeSet::<(BookSide, Price)>::new();

        for (side, price, level) in &updates {
            validate_price(*price)?;
            validate_level(level)?;

            if !seen.insert((*side, *price)) {
                return invalid(format!(
                    "duplicate price level in atomic batch: side={side:?}, price={price}"
                ));
            }
        }

        if crate::shared::historical::limits::current().is_some() {
            // Check the peak in source order before mutating the atomic batch.
            let mut count = self
                .bids
                .len()
                .checked_add(self.asks.len())
                .ok_or_else(|| {
                    crate::shared::error::MarketForgeError::InvalidConfiguration(
                        "book size overflow".into(),
                    )
                })?;
            for (side, price, level) in &updates {
                let present = match side {
                    BookSide::Bid => self.bids.contains_key(price),
                    BookSide::Ask => self.asks.contains_key(price),
                };
                if level.is_zero() && present {
                    count = count.checked_sub(1).ok_or_else(|| {
                        crate::shared::error::MarketForgeError::InvalidConfiguration(
                            "book size underflow".into(),
                        )
                    })?;
                } else if !level.is_zero() && !present {
                    count = count.checked_add(1).ok_or_else(|| {
                        crate::shared::error::MarketForgeError::InvalidConfiguration(
                            "book size overflow".into(),
                        )
                    })?;
                    crate::shared::historical::limits::depth(count, "atomic book update")?;
                }
            }
        }
        // ---------------------------------------------------------------------
        // Apply validated updates
        // ---------------------------------------------------------------------

        let mut changes = Vec::new();

        for (side, price, level) in updates {
            if let Some(change) = self.set_level(side, price, level)? {
                changes.push(change);
            }
        }

        Ok(changes)
    }
}
