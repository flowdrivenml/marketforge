use rust_decimal::Decimal;

use crate::{
    canonical::Quantity,
    error::{MarketForgeError, Result},
    job::{ContractKind, InstrumentSpec, IntegrityCategory, QuantityEncoding},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedQuantity {
    pub base: Option<Quantity>,
    pub quote: Option<Quantity>,
    pub contracts: Option<Quantity>,
}

impl NormalizedQuantity {
    pub fn new(
        base: Option<Quantity>,
        quote: Option<Quantity>,
        contracts: Option<Quantity>,
    ) -> Self {
        Self {
            base,
            quote,
            contracts,
        }
    }
}

pub fn normalize_quantity(
    raw_quantity: Decimal,
    price: Decimal,
    encoding: QuantityEncoding,
    instrument: &InstrumentSpec,
) -> Result<NormalizedQuantity> {
    if raw_quantity <= Decimal::ZERO {
        return invalid_record("raw quantity must be greater than zero");
    }

    if price <= Decimal::ZERO {
        return invalid_record("price must be greater than zero");
    }

    match encoding {
        QuantityEncoding::Base => {
            let quote = checked_mul(raw_quantity, price)?;

            Ok(NormalizedQuantity::new(
                Some(raw_quantity),
                Some(quote),
                None,
            ))
        }

        QuantityEncoding::Quote => {
            let base = checked_div(raw_quantity, price)?;

            Ok(NormalizedQuantity::new(
                Some(base),
                Some(raw_quantity),
                None,
            ))
        }

        QuantityEncoding::Contracts => normalize_contracts(raw_quantity, price, instrument),
    }
}

fn normalize_contracts(
    contracts: Decimal,
    price: Decimal,
    instrument: &InstrumentSpec,
) -> Result<NormalizedQuantity> {
    let contract_kind = instrument.contract_kind.ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(
            "contract quantity requires contract_kind".to_owned(),
        )
    })?;

    let contract_value = instrument.contract_value.ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(
            "contract quantity requires contract_value".to_owned(),
        )
    })?;

    if contract_value <= Decimal::ZERO {
        return invalid_configuration("contract_value must be greater than zero");
    }

    if instrument
        .contract_value_asset
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return invalid_configuration("contract quantity requires contract_value_asset");
    }

    match contract_kind {
        ContractKind::Linear => {
            let base = checked_mul(contracts, contract_value)?;
            let quote = checked_mul(base, price)?;

            Ok(NormalizedQuantity::new(
                Some(base),
                Some(quote),
                Some(contracts),
            ))
        }

        ContractKind::Inverse => {
            let quote = checked_mul(contracts, contract_value)?;
            let base = checked_div(quote, price)?;

            Ok(NormalizedQuantity::new(
                Some(base),
                Some(quote),
                Some(contracts),
            ))
        }
    }
}

fn checked_mul(left: Decimal, right: Decimal) -> Result<Decimal> {
    left.checked_mul(right)
        .ok_or_else(|| MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::TransformationFailure,
            message: "decimal multiplication overflow during quantity normalization".to_owned(),
        })
}

fn checked_div(left: Decimal, right: Decimal) -> Result<Decimal> {
    left.checked_div(right)
        .ok_or_else(|| MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::TransformationFailure,
            message: "decimal division failed during quantity normalization".to_owned(),
        })
}

fn invalid_record<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    })
}

fn invalid_configuration<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::job::InstrumentKind;

    fn spot_instrument() -> InstrumentSpec {
        InstrumentSpec {
            instrument_kind: InstrumentKind::Spot,
            contract_kind: None,
            tick_size: Decimal::new(1, 2),
            contract_value: None,
            contract_value_asset: None,
        }
    }

    fn linear_instrument() -> InstrumentSpec {
        InstrumentSpec {
            instrument_kind: InstrumentKind::Perpetual,
            contract_kind: Some(ContractKind::Linear),
            tick_size: Decimal::new(1, 1),
            contract_value: Some(Decimal::new(1, 2)),
            contract_value_asset: Some("BTC".to_owned()),
        }
    }

    fn inverse_instrument() -> InstrumentSpec {
        InstrumentSpec {
            instrument_kind: InstrumentKind::Future,
            contract_kind: Some(ContractKind::Inverse),
            tick_size: Decimal::new(1, 1),
            contract_value: Some(Decimal::new(100, 0)),
            contract_value_asset: Some("USD".to_owned()),
        }
    }

    #[test]
    fn normalizes_base_quantity() {
        let result = normalize_quantity(
            Decimal::new(25, 1),
            Decimal::new(85_000, 0),
            QuantityEncoding::Base,
            &spot_instrument(),
        )
        .unwrap();

        assert_eq!(result.base, Some(Decimal::new(25, 1)));
        assert_eq!(result.quote, Some(Decimal::new(212_500, 0)));
        assert_eq!(result.contracts, None);
    }

    #[test]
    fn normalizes_quote_quantity() {
        let result = normalize_quantity(
            Decimal::new(170_000, 0),
            Decimal::new(85_000, 0),
            QuantityEncoding::Quote,
            &spot_instrument(),
        )
        .unwrap();

        assert_eq!(result.base, Some(Decimal::new(2, 0)));
        assert_eq!(result.quote, Some(Decimal::new(170_000, 0)));
        assert_eq!(result.contracts, None);
    }

    #[test]
    fn normalizes_linear_contracts() {
        let result = normalize_quantity(
            Decimal::new(10, 0),
            Decimal::new(85_000, 0),
            QuantityEncoding::Contracts,
            &linear_instrument(),
        )
        .unwrap();

        assert_eq!(result.contracts, Some(Decimal::new(10, 0)));
        assert_eq!(result.base, Some(Decimal::new(1, 1)));
        assert_eq!(result.quote, Some(Decimal::new(8_500, 0)));
    }

    #[test]
    fn normalizes_inverse_contracts() {
        let result = normalize_quantity(
            Decimal::new(100, 0),
            Decimal::new(50_000, 0),
            QuantityEncoding::Contracts,
            &inverse_instrument(),
        )
        .unwrap();

        assert_eq!(result.contracts, Some(Decimal::new(100, 0)));
        assert_eq!(result.quote, Some(Decimal::new(10_000, 0)));
        assert_eq!(result.base, Some(Decimal::new(2, 1)));
    }

    #[test]
    fn rejects_zero_quantity() {
        let result = normalize_quantity(
            Decimal::ZERO,
            Decimal::new(85_000, 0),
            QuantityEncoding::Base,
            &spot_instrument(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_negative_quantity() {
        let result = normalize_quantity(
            Decimal::new(-1, 0),
            Decimal::new(85_000, 0),
            QuantityEncoding::Base,
            &spot_instrument(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_zero_price() {
        let result = normalize_quantity(
            Decimal::ONE,
            Decimal::ZERO,
            QuantityEncoding::Base,
            &spot_instrument(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_missing_contract_value() {
        let mut instrument = linear_instrument();
        instrument.contract_value = None;

        let result = normalize_quantity(
            Decimal::ONE,
            Decimal::new(85_000, 0),
            QuantityEncoding::Contracts,
            &instrument,
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_missing_contract_kind() {
        let mut instrument = linear_instrument();
        instrument.contract_kind = None;

        let result = normalize_quantity(
            Decimal::ONE,
            Decimal::new(85_000, 0),
            QuantityEncoding::Contracts,
            &instrument,
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_missing_contract_value_asset() {
        let mut instrument = linear_instrument();
        instrument.contract_value_asset = None;

        let result = normalize_quantity(
            Decimal::ONE,
            Decimal::new(85_000, 0),
            QuantityEncoding::Contracts,
            &instrument,
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_multiplication_overflow() {
        let result = normalize_quantity(
            Decimal::MAX,
            Decimal::new(2, 0),
            QuantityEncoding::Base,
            &spot_instrument(),
        );

        assert!(result.is_err());
    }
}
