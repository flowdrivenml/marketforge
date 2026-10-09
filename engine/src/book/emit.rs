use crate::canonical::{BookSide, EventEnvelope, L2LevelUpdate, Price};

use super::BookLevel;

pub type BookChange = (BookSide, Price, BookLevel);

pub fn emit_l2_changes(envelope: &EventEnvelope, changes: Vec<BookChange>) -> Vec<L2LevelUpdate> {
    changes
        .into_iter()
        .map(|(side, price, level)| L2LevelUpdate {
            envelope: envelope.clone(),

            side,
            price,

            quantity_base: level.quantity_base,
            quantity_quote: level.quantity_quote,
            quantity_contracts: level.quantity_contracts,

            order_count: level.order_count,
        })
        .collect()
}
