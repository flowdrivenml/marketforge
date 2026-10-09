use rust_decimal::Decimal;

use crate::{
    canonical::Price,
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

use super::BookLevel;

pub(super) fn validate_price(price: Price) -> Result<()> {
    if price <= Decimal::ZERO {
        return invalid("book price must be greater than zero");
    }

    Ok(())
}

pub(super) fn validate_level(level: &BookLevel) -> Result<()> {
    let quantities = [
        level.quantity_base,
        level.quantity_quote,
        level.quantity_contracts,
    ];

    if quantities.iter().all(Option::is_none) {
        return invalid("book level must contain at least one quantity");
    }

    let mut has_zero = false;
    let mut has_positive = false;

    for quantity in quantities.into_iter().flatten() {
        if quantity < Decimal::ZERO {
            return invalid("book level quantity cannot be negative");
        }

        if quantity.is_zero() {
            has_zero = true;
        } else {
            has_positive = true;
        }
    }

    if has_zero && has_positive {
        return invalid("book level cannot mix zero and positive quantities");
    }

    Ok(())
}

pub(super) fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    })
}
