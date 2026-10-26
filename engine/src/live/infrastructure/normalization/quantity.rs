use rust_decimal::{Decimal, RoundingStrategy};
use thiserror::Error;

use crate::live::stream::request::QuantityType;

use crate::live::infrastructure::normalization::context::NormalizationContext;

// ============================================================
// QUANTITY NORMALIZATION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QuantityNormalizationError {
    #[error("price must be positive")]
    InvalidPrice,

    #[error("native quantity must not be negative")]
    NegativeQuantity,

    #[error("trade quantity must be positive")]
    NonPositiveTradeQuantity,

    #[error("missing instrument metadata: {0}")]
    MissingMetadata(&'static str),

    #[error("invalid instrument metadata: {0}")]
    InvalidMetadata(String),

    #[error("unsupported contract denomination: {0}")]
    UnsupportedContractAsset(String),

    #[error("checked decimal arithmetic failed: {0}")]
    Arithmetic(&'static str),

    #[error("decimal division requires rounding")]
    InexactDivision,

    #[error("decimal precision must be between 0 and 28")]
    InvalidPrecision,
}

// ============================================================
// PRECISION POLICY
// ============================================================

/// Controls how nonexact quantity conversions are handled.
///
/// Exact:
///     Reject a division if its result cannot be
///     represented exactly by rust_decimal::Decimal.
///
/// Round:
///     Allow rounding to the explicitly selected number
///     of decimal places.
///
/// This policy is chosen by the caller, not inferred
/// from the exchange or instrument symbol.
///
/// Rounding uses MidpointAwayFromZero.
///
/// A later instrument-metadata component may supply
/// an explicit asset precision or quantity increment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityPrecisionPolicy {
    Exact,

    Round { decimal_places: u32 },
}

impl QuantityPrecisionPolicy {
    pub fn validate(self) -> Result<(), QuantityNormalizationError> {
        if let Self::Round { decimal_places } = self {
            if decimal_places > 28 {
                return Err(QuantityNormalizationError::InvalidPrecision);
            }
        }

        Ok(())
    }
}

// ============================================================
// CANONICAL QUANTITIES
// ============================================================

/// Canonical economic quantities for one native quantity.
///
/// Exactly one source quantity representation is specified
/// by NormalizationContext.
///
/// The converter derives additional representations when
/// the instrument's denomination permits it.
///
/// `was_rounded` indicates that at least one derived
/// representation required rounding.
///
/// The original quantity is never replaced with its
/// derived representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalQuantities {
    pub quantity_base: Option<Decimal>,

    pub quantity_quote: Option<Decimal>,

    pub quantity_contracts: Option<Decimal>,

    pub was_rounded: bool,
}

// ============================================================
// QUANTITY NORMALIZER
// ============================================================

/// Generic, stateless economic quantity normalizer.
///
/// Supported native quantity semantics:
///
/// Base:
///     native quantity is measured in base-asset units.
///
/// Quote:
///     native quantity is measured in quote-asset units.
///
/// Contracts:
///     native quantity is measured in contract counts.
///
/// Contract denomination determines whether contract
/// face value is expressed in the base or quote asset.
///
/// The normalizer does not infer denomination from
/// exchange names, symbols, or market categories.
pub struct QuantityNormalizer;

impl QuantityNormalizer {
    // ========================================================
    // DEPTH QUANTITY
    // ========================================================

    /// Normalize an absolute L2 quantity.
    ///
    /// Zero is permitted because L2 zero quantities
    /// represent price-level deletion.
    pub fn normalize_level(
        native_quantity: Decimal,
        price: Decimal,
        context: &NormalizationContext,
        precision: QuantityPrecisionPolicy,
    ) -> Result<CanonicalQuantities, QuantityNormalizationError> {
        Self::normalize(native_quantity, price, context, precision)
    }

    // ========================================================
    // TRADE QUANTITY
    // ========================================================

    /// Normalize an execution quantity.
    ///
    /// Trades must contain a strictly positive quantity
    /// after interpreting any exchange-native signed
    /// direction representation.
    pub fn normalize_trade(
        native_quantity: Decimal,
        price: Decimal,
        context: &NormalizationContext,
        precision: QuantityPrecisionPolicy,
    ) -> Result<CanonicalQuantities, QuantityNormalizationError> {
        if native_quantity <= Decimal::ZERO {
            return Err(QuantityNormalizationError::NonPositiveTradeQuantity);
        }

        Self::normalize(native_quantity, price, context, precision)
    }

    // ========================================================
    // GENERIC CONVERSION
    // ========================================================

    pub fn normalize(
        native_quantity: Decimal,
        price: Decimal,
        context: &NormalizationContext,
        precision: QuantityPrecisionPolicy,
    ) -> Result<CanonicalQuantities, QuantityNormalizationError> {
        precision.validate()?;

        if price <= Decimal::ZERO {
            return Err(QuantityNormalizationError::InvalidPrice);
        }

        if native_quantity < Decimal::ZERO {
            return Err(QuantityNormalizationError::NegativeQuantity);
        }

        let base_asset = context
            .base_asset()
            .ok_or(QuantityNormalizationError::MissingMetadata("base_asset"))?;

        let quote_asset = context
            .quote_asset()
            .ok_or(QuantityNormalizationError::MissingMetadata("quote_asset"))?;

        if base_asset.trim().is_empty()
            || quote_asset.trim().is_empty()
            || base_asset.eq_ignore_ascii_case(quote_asset)
        {
            return Err(QuantityNormalizationError::InvalidMetadata(
                "base and quote assets must be distinct and nonempty".into(),
            ));
        }

        match context.quantity_type() {
            // =================================================
            // BASE-ASSET QUANTITY
            // =================================================
            QuantityType::Base => {
                let quote = multiply(native_quantity, price)?;

                Ok(CanonicalQuantities {
                    quantity_base: Some(native_quantity),
                    quantity_quote: Some(quote),
                    quantity_contracts: None,
                    was_rounded: false,
                })
            }

            // =================================================
            // QUOTE-ASSET QUANTITY
            // =================================================
            QuantityType::Quote => {
                let (base, rounded) = divide(native_quantity, price, precision)?;

                Ok(CanonicalQuantities {
                    quantity_base: Some(base),
                    quantity_quote: Some(native_quantity),
                    quantity_contracts: None,
                    was_rounded: rounded,
                })
            }

            // =================================================
            // CONTRACT QUANTITY
            // =================================================
            QuantityType::Contracts => {
                let contract_value =
                    context
                        .contract_value()
                        .ok_or(QuantityNormalizationError::MissingMetadata(
                            "contract_value",
                        ))?;

                let contract_asset = context.contract_value_asset().ok_or(
                    QuantityNormalizationError::MissingMetadata("contract_value_asset"),
                )?;

                if contract_value <= Decimal::ZERO {
                    return Err(QuantityNormalizationError::InvalidMetadata(
                        "contract_value must be positive".into(),
                    ));
                }

                if contract_asset.trim().is_empty() {
                    return Err(QuantityNormalizationError::InvalidMetadata(
                        "contract_value_asset cannot be empty".into(),
                    ));
                }

                let face_quantity = multiply(native_quantity, contract_value)?;

                // =============================================
                // BASE-DENOMINATED CONTRACT
                // =============================================

                if contract_asset.eq_ignore_ascii_case(base_asset) {
                    let quote = multiply(face_quantity, price)?;

                    return Ok(CanonicalQuantities {
                        quantity_base: Some(face_quantity),
                        quantity_quote: Some(quote),
                        quantity_contracts: Some(native_quantity),
                        was_rounded: false,
                    });
                }

                // =============================================
                // QUOTE-DENOMINATED CONTRACT
                // =============================================

                if contract_asset.eq_ignore_ascii_case(quote_asset) {
                    let (base, rounded) = divide(face_quantity, price, precision)?;

                    return Ok(CanonicalQuantities {
                        quantity_base: Some(base),
                        quantity_quote: Some(face_quantity),
                        quantity_contracts: Some(native_quantity),
                        was_rounded: rounded,
                    });
                }

                // =============================================
                // UNKNOWN CONTRACT DENOMINATION
                // =============================================

                Err(QuantityNormalizationError::UnsupportedContractAsset(
                    contract_asset.to_owned(),
                ))
            }
        }
    }
}

// ============================================================
// CHECKED MULTIPLICATION
// ============================================================

fn multiply(lhs: Decimal, rhs: Decimal) -> Result<Decimal, QuantityNormalizationError> {
    lhs.checked_mul(rhs)
        .ok_or(QuantityNormalizationError::Arithmetic(
            "decimal multiplication overflow or precision loss",
        ))
}

// ============================================================
// CHECKED DIVISION
// ============================================================

/// Divide two exact decimal values under an explicit
/// precision policy.
///
/// `checked_div` alone does not prove exactness because
/// rust_decimal can produce rounded division results.
///
/// We therefore verify the quotient by multiplying it
/// back by the denominator.
///
/// Exact mode rejects nonexact results.
///
/// Rounded mode returns the explicitly rounded quotient
/// and reports whether the result was inexact.
fn divide(
    numerator: Decimal,
    denominator: Decimal,
    precision: QuantityPrecisionPolicy,
) -> Result<(Decimal, bool), QuantityNormalizationError> {
    let quotient =
        numerator
            .checked_div(denominator)
            .ok_or(QuantityNormalizationError::Arithmetic(
                "decimal division failed",
            ))?;

    let exact = quotient
        .checked_mul(denominator)
        .is_some_and(|reconstructed| reconstructed == numerator);

    match precision {
        QuantityPrecisionPolicy::Exact => {
            if !exact {
                return Err(QuantityNormalizationError::InexactDivision);
            }

            Ok((quotient, false))
        }

        QuantityPrecisionPolicy::Round { decimal_places } => {
            if decimal_places > 28 {
                return Err(QuantityNormalizationError::InvalidPrecision);
            }

            let rounded = quotient
                .round_dp_with_strategy(decimal_places, RoundingStrategy::MidpointAwayFromZero);

            let was_rounded = !exact || rounded != quotient;

            Ok((rounded, was_rounded))
        }
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::live::{
        config::exchange::{ExchangeId, MarketType},
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    };

    use super::*;

    // ========================================================
    // TEST CONTEXT FACTORY
    // ========================================================

    fn context(
        quantity_type: QuantityType,
        contract_value: Option<&str>,
        contract_asset: Option<&str>,
    ) -> NormalizationContext {
        let request = CreateSourceStreamRequest {
            instrument_id: 42,

            exchange: ExchangeId::Binance,

            symbol: "BTCUSD_PERP".into(),

            market_category: MarketType::Inverse,

            instrument_type: "perpetual".into(),

            base_asset: Some("BTC".into()),

            quote_asset: Some("USD".into()),

            quantity_type,

            contract_value: contract_value.map(str::to_owned),

            contract_value_asset: contract_asset.map(str::to_owned),

            channel: SourceChannel::Trades,

            settlement_asset: Some("BTC".into()),
        };

        NormalizationContext::new(&request, "stream-42").unwrap()
    }

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    // ========================================================
    // BASE QUANTITY
    // ========================================================

    #[test]
    fn converts_base_quantity_to_quote() {
        let context = context(QuantityType::Base, None, None);

        let result = QuantityNormalizer::normalize_trade(
            d("0.5"),
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        assert_eq!(result.quantity_base, Some(d("0.5")));
        assert_eq!(result.quantity_quote, Some(d("40000")));
        assert_eq!(result.quantity_contracts, None);
        assert!(!result.was_rounded);
    }

    // ========================================================
    // QUOTE QUANTITY
    // ========================================================

    #[test]
    fn converts_quote_quantity_to_base() {
        let context = context(QuantityType::Quote, None, None);

        let result = QuantityNormalizer::normalize_trade(
            d("1000"),
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        assert_eq!(result.quantity_base, Some(d("0.0125")));
        assert_eq!(result.quantity_quote, Some(d("1000")));
        assert_eq!(result.quantity_contracts, None);
    }

    // ========================================================
    // BINANCE INVERSE CONTRACT
    // ========================================================

    #[test]
    fn converts_binance_inverse_btcusd_contracts() {
        let context = context(QuantityType::Contracts, Some("100"), Some("USD"));

        let result = QuantityNormalizer::normalize_trade(
            d("10"),
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        assert_eq!(result.quantity_contracts, Some(d("10")),);

        assert_eq!(result.quantity_quote, Some(d("1000")),);

        assert_eq!(result.quantity_base, Some(d("0.0125")),);

        assert!(!result.was_rounded);
    }

    // ========================================================
    // BASE-DENOMINATED CONTRACT
    // ========================================================

    #[test]
    fn converts_base_denomination_contracts() {
        let context = context(QuantityType::Contracts, Some("0.001"), Some("BTC"));

        let result = QuantityNormalizer::normalize_trade(
            d("10"),
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        assert_eq!(result.quantity_contracts, Some(d("10")),);

        assert_eq!(result.quantity_base, Some(d("0.010")),);

        assert_eq!(result.quantity_quote, Some(d("800")),);
    }

    // ========================================================
    // L2 ZERO QUANTITY
    // ========================================================

    #[test]
    fn zero_depth_quantity_preserves_all_zero_representations() {
        let context = context(QuantityType::Contracts, Some("100"), Some("USD"));

        let result = QuantityNormalizer::normalize_level(
            Decimal::ZERO,
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        assert_eq!(result.quantity_contracts, Some(Decimal::ZERO),);

        assert_eq!(result.quantity_quote, Some(Decimal::ZERO),);

        assert_eq!(result.quantity_base, Some(Decimal::ZERO),);
    }

    #[test]
    fn zero_trade_quantity_is_rejected() {
        let context = context(QuantityType::Contracts, Some("100"), Some("USD"));

        let result = QuantityNormalizer::normalize_trade(
            Decimal::ZERO,
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        );

        assert!(matches!(
            result,
            Err(QuantityNormalizationError::NonPositiveTradeQuantity)
        ));
    }

    // ========================================================
    // NONTERMINATING DECIMAL DIVISION
    // ========================================================

    #[test]
    fn exact_policy_rejects_nonterminating_division() {
        let context = context(QuantityType::Contracts, Some("100"), Some("USD"));

        let result = QuantityNormalizer::normalize_trade(
            d("1"),
            d("30000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        );

        assert!(matches!(
            result,
            Err(QuantityNormalizationError::InexactDivision)
        ));
    }

    #[test]
    fn explicit_rounding_allows_nonterminating_division() {
        let context = context(QuantityType::Contracts, Some("100"), Some("USD"));

        let result = QuantityNormalizer::normalize_trade(
            d("1"),
            d("30000"),
            &context,
            QuantityPrecisionPolicy::Round { decimal_places: 12 },
        )
        .unwrap();

        assert_eq!(result.quantity_contracts, Some(d("1")),);

        assert_eq!(result.quantity_quote, Some(d("100")),);

        assert_eq!(result.quantity_base, Some(d("0.003333333333")),);

        assert!(result.was_rounded);
    }

    // ========================================================
    // MISSING CONTRACT METADATA
    // ========================================================

    #[test]
    fn rejects_missing_contract_value() {
        // Some source-stream request validators already
        // reject incomplete contract metadata.
        //
        // Verify the error at the source-contract boundary.
        let request = CreateSourceStreamRequest {
            instrument_id: 42,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSD_PERP".into(),
            market_category: MarketType::Inverse,
            instrument_type: "perpetual".into(),
            base_asset: Some("BTC".into()),
            quote_asset: Some("USD".into()),
            quantity_type: QuantityType::Contracts,
            contract_value: None,
            contract_value_asset: Some("USD".into()),
            channel: SourceChannel::Trades,
            settlement_asset: Some("BTC".into()),
        };

        assert!(NormalizationContext::new(&request, "stream-42",).is_err(),);
    }

    // ========================================================
    // UNSUPPORTED CONTRACT DENOMINATION
    // ========================================================

    #[test]
    fn rejects_unknown_contract_asset() {
        let context = context(QuantityType::Contracts, Some("100"), Some("EUR"));

        let result = QuantityNormalizer::normalize_trade(
            d("10"),
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        );

        assert!(matches!(
            result,
            Err(QuantityNormalizationError::UnsupportedContractAsset(_))
        ));
    }

    // ========================================================
    // INVALID PRICE AND QUANTITY
    // ========================================================

    #[test]
    fn rejects_zero_price() {
        let context = context(QuantityType::Base, None, None);

        assert!(
            QuantityNormalizer::normalize_level(
                d("1"),
                Decimal::ZERO,
                &context,
                QuantityPrecisionPolicy::Exact,
            )
            .is_err(),
        );
    }

    #[test]
    fn rejects_negative_level_quantity() {
        let context = context(QuantityType::Base, None, None);

        assert!(matches!(
            QuantityNormalizer::normalize_level(
                d("-1"),
                d("80000"),
                &context,
                QuantityPrecisionPolicy::Exact,
            ),
            Err(QuantityNormalizationError::NegativeQuantity)
        ));
    }

    // ========================================================
    // PRECISION CONFIGURATION
    // ========================================================

    #[test]
    fn rejects_invalid_rounding_precision() {
        assert!(
            QuantityPrecisionPolicy::Round { decimal_places: 29 }
                .validate()
                .is_err(),
        );
    }

    // ========================================================
    // NO FLOATING-POINT CALCULATIONS
    // ========================================================

    #[test]
    fn retains_exact_contract_count() {
        let context = context(QuantityType::Contracts, Some("100"), Some("USD"));

        let result = QuantityNormalizer::normalize_trade(
            d("0.12345678"),
            d("80000"),
            &context,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        assert_eq!(result.quantity_contracts, Some(d("0.12345678")),);

        assert_eq!(result.quantity_quote, Some(d("12.34567800")),);
    }
}
