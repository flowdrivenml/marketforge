use rust_decimal::Decimal;

use crate::{
    book::{BookChange, BookLevel, BookStore},
    canonical::BookSide,
    error::Result,
    job::{InstrumentSpec, QuantityEncoding},
    normalize::normalize_quantity,
};

use super::transforms::ExtractedLevel;

pub fn apply_absolute_update(
    book: &mut BookStore,
    bids: Vec<ExtractedLevel>,
    asks: Vec<ExtractedLevel>,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
) -> Result<Vec<BookChange>> {
    let mut updates = Vec::with_capacity(bids.len() + asks.len());

    for level in bids {
        updates.push(normalize_update(
            BookSide::Bid,
            level,
            encoding,
            instrument,
        )?);
    }

    for level in asks {
        updates.push(normalize_update(
            BookSide::Ask,
            level,
            encoding,
            instrument,
        )?);
    }

    book.apply_batch(updates)
}

fn normalize_update(
    side: BookSide,
    level: ExtractedLevel,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
) -> Result<BookChange> {
    let quantity = if level.quantity == Decimal::ZERO {
        // A zero native quantity removes the level.
        //
        // All normalized representations are zero for a deletion.
        // The authoritative native representation remains present.
        let (base, quote, contracts) = match encoding {
            QuantityEncoding::Base => (Some(Decimal::ZERO), Some(Decimal::ZERO), None),
            QuantityEncoding::Quote => (Some(Decimal::ZERO), Some(Decimal::ZERO), None),
            QuantityEncoding::Contracts => (None, None, Some(Decimal::ZERO)),
        };

        BookLevel {
            quantity_base: base,
            quantity_quote: quote,
            quantity_contracts: contracts,
            order_count: level.order_count.map(|_| 0),
        }
    } else {
        let quantity = normalize_quantity(level.quantity, level.price, encoding, instrument)?;

        BookLevel {
            quantity_base: quantity.base,
            quantity_quote: quantity.quote,
            quantity_contracts: quantity.contracts,
            order_count: level.order_count,
        }
    };

    Ok((side, level.price, quantity))
}
