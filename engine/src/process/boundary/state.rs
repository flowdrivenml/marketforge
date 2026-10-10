use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::BookStore;

/// One canonical price level in a boundary book state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryBookLevel {
    pub price: Decimal,

    pub quantity_base: Option<Decimal>,
    pub quantity_quote: Option<Decimal>,
    pub quantity_contracts: Option<Decimal>,

    pub order_count: Option<u64>,
}

/// Complete reconstructed order-book state.
///
/// Bids and asks are stored in deterministic ascending price order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryBookState {
    pub bids: Vec<BoundaryBookLevel>,
    pub asks: Vec<BoundaryBookLevel>,
}

impl BoundaryBookState {
    /// Capture the current reconstructed book.
    ///
    /// Returns None when the book is not initialized.
    pub fn from_book(book: &BookStore) -> Option<Self> {
        if !book.is_initialized() {
            return None;
        }

        let bids = book
            .bids()
            .iter()
            .map(|(price, level)| BoundaryBookLevel {
                price: *price,
                quantity_base: level.quantity_base,
                quantity_quote: level.quantity_quote,
                quantity_contracts: level.quantity_contracts,
                order_count: level.order_count,
            })
            .collect();

        let asks = book
            .asks()
            .iter()
            .map(|(price, level)| BoundaryBookLevel {
                price: *price,
                quantity_base: level.quantity_base,
                quantity_quote: level.quantity_quote,
                quantity_contracts: level.quantity_contracts,
                order_count: level.order_count,
            })
            .collect();

        Some(Self { bids, asks })
    }

    pub fn level_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }
}
