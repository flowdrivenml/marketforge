use std::collections::BTreeSet;

use crate::{
    canonical::{BookSide, Price},
    error::Result,
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
