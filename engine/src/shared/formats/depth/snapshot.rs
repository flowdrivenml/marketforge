use crate::{
    shared::domain::book::{BookChange, BookLevel, BookStore},
    shared::domain::canonical::Price,
    shared::domain::instrument::InstrumentSpec,
    shared::error::Result,
    shared::normalization::QuantityEncoding,
    shared::normalization::normalize_quantity,
};

use super::transforms::ExtractedLevel;

pub(super) fn apply_snapshot_observed(
    book: &mut BookStore,
    bids: Vec<ExtractedLevel>,
    asks: Vec<ExtractedLevel>,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
    trace: &mut crate::shared::validation::IntegrityObservationTrace,
) -> Result<Vec<BookChange>> {
    let has_levels = !bids.is_empty() || !asks.is_empty();
    let bids = normalize_levels(bids, encoding, instrument)?;
    let asks = normalize_levels(asks, encoding, instrument)?;

    if has_levels {
        trace.passed(crate::shared::validation::IntegrityCategory::TransformationFailure);
    }
    let result = book.apply_snapshot(bids, asks);
    if result.is_ok() {
        trace.passed(crate::shared::validation::IntegrityCategory::InvalidRecord);
    }
    result
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
