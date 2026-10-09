use crate::{
    book::{BookChange, BookLevel, BookStore},
    canonical::Price,
    error::Result,
    job::{InstrumentSpec, QuantityEncoding},
    normalize::normalize_quantity,
};

use super::transforms::ExtractedLevel;

pub fn apply_snapshot(
    book: &mut BookStore,
    bids: Vec<ExtractedLevel>,
    asks: Vec<ExtractedLevel>,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
) -> Result<Vec<BookChange>> {
    let bids = normalize_levels(bids, encoding, instrument)?;
    let asks = normalize_levels(asks, encoding, instrument)?;

    book.apply_snapshot(bids, asks)
}

pub(super) fn normalize_levels(
    levels: Vec<ExtractedLevel>,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
) -> Result<Vec<(Price, BookLevel)>> {
    levels
        .into_iter()
        .map(|level| {
            let quantity = normalize_quantity(level.quantity, level.price, encoding, instrument)?;

            Ok((
                level.price,
                BookLevel {
                    quantity_base: quantity.base,
                    quantity_quote: quantity.quote,
                    quantity_contracts: quantity.contracts,
                    order_count: level.order_count,
                },
            ))
        })
        .collect()
}
