use rust_decimal::Decimal;

use crate::{
    book::{BookChange, BookLevel, BookStore},
    canonical::BookSide,
    error::{MarketForgeError, Result},
    job::{InstrumentSpec, IntegrityCategory, QuantityEncoding},
    normalize::normalize_quantity,
};

use super::transforms::ExtractedLevel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelativeAction {
    Add,
    Subtract,
}

pub fn apply_relative_update(
    book: &mut BookStore,
    side: BookSide,
    level: ExtractedLevel,
    action: RelativeAction,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
) -> Result<Vec<BookChange>> {
    if !book.is_initialized() {
        return invalid("cannot apply relative update before initialization");
    }

    if level.price <= Decimal::ZERO {
        return invalid("relative update price must be positive");
    }

    if level.quantity <= Decimal::ZERO {
        return invalid("relative update quantity must be positive");
    }

    // ---------------------------------------------------------
    // Read current native quantity
    // ---------------------------------------------------------

    let existing = match side {
        BookSide::Bid => book.bids().get(&level.price),
        BookSide::Ask => book.asks().get(&level.price),
    };

    let current = match existing {
        Some(existing) => match encoding {
            QuantityEncoding::Base => existing.quantity_base,
            QuantityEncoding::Quote => existing.quantity_quote,
            QuantityEncoding::Contracts => existing.quantity_contracts,
        }
        .ok_or_else(|| invalid_error("existing level missing native quantity"))?,

        None => Decimal::ZERO,
    };

    // ---------------------------------------------------------
    // Calculate resulting native quantity
    // ---------------------------------------------------------

    let result = match action {
        RelativeAction::Add => current.checked_add(level.quantity),

        RelativeAction::Subtract => {
            if existing.is_none() {
                return invalid("cannot subtract from nonexistent level");
            }

            current.checked_sub(level.quantity)
        }
    }
    .ok_or_else(|| invalid_error("relative quantity arithmetic overflow"))?;

    if result < Decimal::ZERO {
        return invalid("relative update produces negative quantity");
    }

    // ---------------------------------------------------------
    // Normalize resulting absolute quantity
    // ---------------------------------------------------------

    let normalized = if result.is_zero() {
        match encoding {
            QuantityEncoding::Base | QuantityEncoding::Quote => BookLevel {
                quantity_base: Some(Decimal::ZERO),
                quantity_quote: Some(Decimal::ZERO),
                quantity_contracts: None,
                order_count: None,
            },

            QuantityEncoding::Contracts => BookLevel {
                quantity_base: None,
                quantity_quote: None,
                quantity_contracts: Some(Decimal::ZERO),
                order_count: None,
            },
        }
    } else {
        let quantity = normalize_quantity(result, level.price, encoding, instrument)?;

        BookLevel {
            quantity_base: quantity.base,
            quantity_quote: quantity.quote,
            quantity_contracts: quantity.contracts,
            order_count: None,
        }
    };

    // ---------------------------------------------------------
    // Commit through existing atomic batch mechanism
    // ---------------------------------------------------------

    book.apply_batch(vec![(side, level.price, normalized)])
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
