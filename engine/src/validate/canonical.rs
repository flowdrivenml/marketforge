use rust_decimal::Decimal;

use crate::{
    canonical::{CanonicalEvent, EventEnvelope, L2LevelUpdate, Quantity, Trade},
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

// -----------------------------------------------------------------------------
// Canonical event validation
// -----------------------------------------------------------------------------

pub fn validate_canonical_event(event: &CanonicalEvent) -> Result<()> {
    match event {
        CanonicalEvent::Trade(trade) => validate_trade(trade),
        CanonicalEvent::L2LevelUpdate(update) => validate_l2_level_update(update),
    }
}

// -----------------------------------------------------------------------------
// Trade validation
// -----------------------------------------------------------------------------

pub fn validate_trade(trade: &Trade) -> Result<()> {
    validate_envelope(&trade.envelope)?;

    if trade.price <= Decimal::ZERO {
        return invalid_record("trade price must be greater than zero");
    }

    validate_positive_quantities(
        trade.quantity_base,
        trade.quantity_quote,
        trade.quantity_contracts,
        "trade",
    )?;

    Ok(())
}

// -----------------------------------------------------------------------------
// Canonical L2 validation
// -----------------------------------------------------------------------------

/// Validate a canonical absolute L2 level update.
///
/// Every update represents the resulting quantity at a price level.
///
/// - Positive quantity: insert or replace the level.
/// - Zero quantity: remove the level.
/// - Negative quantity: invalid.
/// - None: unavailable quantity representation, not deletion.
///
/// At least one quantity representation must be present.
///
/// Snapshot initialization levels follow exactly the same rules.
pub fn validate_l2_level_update(update: &L2LevelUpdate) -> Result<()> {
    validate_envelope(&update.envelope)?;

    if update.price <= Decimal::ZERO {
        return invalid_record("L2 level price must be greater than zero");
    }

    validate_nonnegative_quantities(
        update.quantity_base,
        update.quantity_quote,
        update.quantity_contracts,
        "L2 level update",
    )?;

    Ok(())
}

// -----------------------------------------------------------------------------
// Shared envelope validation
// -----------------------------------------------------------------------------

fn validate_envelope(envelope: &EventEnvelope) -> Result<()> {
    if envelope.instrument_id <= 0 {
        return invalid_configuration("canonical event instrument_id must be greater than zero");
    }

    if envelope.symbol.trim().is_empty() {
        return invalid_configuration("canonical event symbol must not be empty");
    }

    if envelope.stream_id.trim().is_empty() {
        return invalid_configuration("canonical event stream_id must not be empty");
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Trade quantity validation
// -----------------------------------------------------------------------------

fn validate_positive_quantities(
    quantity_base: Option<Quantity>,
    quantity_quote: Option<Quantity>,
    quantity_contracts: Option<Quantity>,
    context: &str,
) -> Result<()> {
    if quantity_base.is_none() && quantity_quote.is_none() && quantity_contracts.is_none() {
        return invalid_record(format!(
            "{context} must contain at least one quantity representation"
        ));
    }

    for (name, quantity) in [
        ("quantity_base", quantity_base),
        ("quantity_quote", quantity_quote),
        ("quantity_contracts", quantity_contracts),
    ] {
        if let Some(quantity) = quantity {
            if quantity <= Decimal::ZERO {
                return invalid_record(format!("{context} {name} must be greater than zero"));
            }
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// L2 quantity validation
// -----------------------------------------------------------------------------

fn validate_nonnegative_quantities(
    quantity_base: Option<Quantity>,
    quantity_quote: Option<Quantity>,
    quantity_contracts: Option<Quantity>,
    context: &str,
) -> Result<()> {
    if quantity_base.is_none() && quantity_quote.is_none() && quantity_contracts.is_none() {
        return invalid_record(format!(
            "{context} must contain at least one quantity representation"
        ));
    }

    let mut has_zero = false;
    let mut has_positive = false;

    for (name, quantity) in [
        ("quantity_base", quantity_base),
        ("quantity_quote", quantity_quote),
        ("quantity_contracts", quantity_contracts),
    ] {
        if let Some(quantity) = quantity {
            if quantity < Decimal::ZERO {
                return invalid_record(format!("{context} {name} cannot be negative"));
            }

            if quantity.is_zero() {
                has_zero = true;
            } else {
                has_positive = true;
            }
        }
    }

    // All available quantities describe the same resting liquidity.
    // A deletion must not mix zero and positive quantities.
    if has_zero && has_positive {
        return invalid_record(format!(
            "{context} cannot mix zero and positive quantity representations"
        ));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Error helpers
// -----------------------------------------------------------------------------

fn invalid_record<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    })
}

fn invalid_configuration<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use crate::canonical::{BookSide, Exchange, TradeSide};

    fn envelope() -> EventEnvelope {
        EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:depth".to_owned(),
        }
    }

    fn valid_trade() -> Trade {
        Trade {
            envelope: envelope(),

            trade_id: Some("trade-42".to_owned()),
            sequence: None,

            side: TradeSide::Buy,
            price: Decimal::new(85_000, 0),

            quantity_base: Some(Decimal::new(25, 1)),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,
            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        }
    }

    fn valid_l2_update() -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: envelope(),

            side: BookSide::Bid,
            price: Decimal::new(85_000, 0),

            quantity_base: Some(Decimal::new(5, 0)),
            quantity_quote: None,
            quantity_contracts: None,

            order_count: Some(3),
        }
    }

    // -------------------------------------------------------------------------
    // Trade tests
    // -------------------------------------------------------------------------

    #[test]
    fn accepts_valid_trade() {
        assert!(validate_trade(&valid_trade()).is_ok());
    }

    #[test]
    fn rejects_zero_trade_price() {
        let mut trade = valid_trade();
        trade.price = Decimal::ZERO;

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_negative_trade_price() {
        let mut trade = valid_trade();
        trade.price = Decimal::new(-100, 0);

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_missing_trade_quantities() {
        let mut trade = valid_trade();
        trade.quantity_base = None;

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_zero_trade_quantity() {
        let mut trade = valid_trade();
        trade.quantity_base = Some(Decimal::ZERO);

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn rejects_negative_trade_quantity() {
        let mut trade = valid_trade();
        trade.quantity_base = Some(Decimal::new(-1, 0));

        assert!(validate_trade(&trade).is_err());
    }

    #[test]
    fn accepts_multiple_trade_quantities() {
        let mut trade = valid_trade();
        trade.quantity_quote = Some(Decimal::new(212_500, 0));

        assert!(validate_trade(&trade).is_ok());
    }

    // -------------------------------------------------------------------------
    // L2 tests
    // -------------------------------------------------------------------------

    #[test]
    fn accepts_valid_l2_update() {
        assert!(validate_l2_level_update(&valid_l2_update()).is_ok());
    }

    #[test]
    fn accepts_l2_level_deletion() {
        let mut update = valid_l2_update();

        update.quantity_base = Some(Decimal::ZERO);

        assert!(validate_l2_level_update(&update).is_ok());
    }

    #[test]
    fn accepts_l2_deletion_with_multiple_zero_quantities() {
        let mut update = valid_l2_update();

        update.quantity_base = Some(Decimal::ZERO);
        update.quantity_quote = Some(Decimal::ZERO);
        update.quantity_contracts = Some(Decimal::ZERO);

        assert!(validate_l2_level_update(&update).is_ok());
    }

    #[test]
    fn rejects_l2_update_without_quantities() {
        let mut update = valid_l2_update();

        update.quantity_base = None;

        assert!(validate_l2_level_update(&update).is_err());
    }

    #[test]
    fn rejects_negative_l2_quantity() {
        let mut update = valid_l2_update();

        update.quantity_base = Some(Decimal::new(-1, 0));

        assert!(validate_l2_level_update(&update).is_err());
    }

    #[test]
    fn rejects_mixed_zero_and_positive_l2_quantities() {
        let mut update = valid_l2_update();

        update.quantity_base = Some(Decimal::ZERO);
        update.quantity_quote = Some(Decimal::new(100, 0));

        assert!(validate_l2_level_update(&update).is_err());
    }

    #[test]
    fn accepts_multiple_positive_l2_quantities() {
        let mut update = valid_l2_update();

        update.quantity_quote = Some(Decimal::new(425_000, 0));

        assert!(validate_l2_level_update(&update).is_ok());
    }

    #[test]
    fn rejects_zero_l2_price() {
        let mut update = valid_l2_update();

        update.price = Decimal::ZERO;

        assert!(validate_l2_level_update(&update).is_err());
    }

    #[test]
    fn rejects_negative_l2_price() {
        let mut update = valid_l2_update();

        update.price = Decimal::new(-100, 0);

        assert!(validate_l2_level_update(&update).is_err());
    }

    // -------------------------------------------------------------------------
    // Canonical event dispatch
    // -------------------------------------------------------------------------

    #[test]
    fn validates_trade_through_canonical_event() {
        let event = CanonicalEvent::Trade(valid_trade());

        assert!(validate_canonical_event(&event).is_ok());
    }

    #[test]
    fn validates_depth_through_canonical_event() {
        let event = CanonicalEvent::L2LevelUpdate(valid_l2_update());

        assert!(validate_canonical_event(&event).is_ok());
    }

    // -------------------------------------------------------------------------
    // Error classification
    // -------------------------------------------------------------------------

    #[test]
    fn invalid_trade_price_is_record_integrity_error() {
        let mut trade = valid_trade();
        trade.price = Decimal::ZERO;

        let error = validate_trade(&trade).unwrap_err();

        assert!(matches!(
            error,
            MarketForgeError::RecordIntegrity {
                category: IntegrityCategory::InvalidRecord,
                ..
            }
        ));
    }

    #[test]
    fn invalid_instrument_id_is_configuration_error() {
        let mut trade = valid_trade();
        trade.envelope.instrument_id = 0;

        let error = validate_trade(&trade).unwrap_err();

        assert!(matches!(error, MarketForgeError::InvalidConfiguration(_)));
    }
}
