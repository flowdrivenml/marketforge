use std::collections::BTreeMap;

use crate::{
    canonical::{BookSide, Price},
    error::Result,
};

use super::{
    BookLevel,
    validation::{invalid, validate_level, validate_price},
};

#[derive(Debug, Default)]
pub struct BookStore {
    pub(super) bids: BTreeMap<Price, BookLevel>,
    pub(super) asks: BTreeMap<Price, BookLevel>,
    pub(super) initialized: bool,
}

impl BookStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn bids(&self) -> &BTreeMap<Price, BookLevel> {
        &self.bids
    }

    pub fn asks(&self) -> &BTreeMap<Price, BookLevel> {
        &self.asks
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.initialized = false;
    }

    /// Apply an absolute level update.
    ///
    /// Returns `Some` only when the book state changes.
    pub fn set_level(
        &mut self,
        side: BookSide,
        price: Price,
        level: BookLevel,
    ) -> Result<Option<(BookSide, Price, BookLevel)>> {
        validate_price(price)?;
        validate_level(&level)?;

        if !self.initialized {
            return invalid("cannot apply level update before book initialization");
        }

        let book = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };

        if level.is_zero() {
            if book.remove(&price).is_some() {
                return Ok(Some((side, price, level)));
            }

            return Ok(None);
        }

        if book.get(&price) == Some(&level) {
            return Ok(None);
        }

        book.insert(price, level.clone());

        Ok(Some((side, price, level)))
    }
}
